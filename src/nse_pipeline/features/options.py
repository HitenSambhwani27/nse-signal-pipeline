"""
Stage 2B — index options features (Nifty + Bank Nifty only).

OI buildup (4-state), PCR with buildup context, Black-Scholes Greeks via greeks.py.
Expiry / lot size must come from the instrument cache (live master), never hardcoded.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from nse_pipeline.features.greeks import compute_greeks
from nse_pipeline.features.tick_stats import lookup_by_bucket


def classify_oi_buildup(price_change: float, oi_change: float) -> str:
    """
    Classic 4-state OI table:

    price↑ oi↑ → long_buildup
    price↓ oi↑ → short_buildup
    price↓ oi↓ → long_unwinding
    price↑ oi↓ → short_covering
    """
    if price_change > 0 and oi_change > 0:
        return "long_buildup"
    if price_change < 0 and oi_change > 0:
        return "short_buildup"
    if price_change < 0 and oi_change < 0:
        return "long_unwinding"
    if price_change > 0 and oi_change < 0:
        return "short_covering"
    return "neutral"


def _bucket_end(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    floored = ts.floor(f"{minutes}min")
    return floored + pd.Timedelta(minutes=minutes)


def _parse_expiry_date(expiry: str | None) -> date | None:
    if not expiry:
        return None
    text = str(expiry)
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19] if " " in text else text, fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def bucket_option_series(
    ticks: pd.DataFrame,
    *,
    bucket_minutes: int,
) -> pd.DataFrame:
    """Collapse cleaned option ticks to last LTP/OI per bucket."""
    if ticks.empty:
        return pd.DataFrame()
    df = ticks.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["bucket"] = df["timestamp"].map(lambda t: _bucket_end(t, bucket_minutes))
    agg: dict[str, tuple[str, str]] = {
        "ltp": ("last_price", "last"),
        "oi": ("oi", "last"),
        "volume": ("volume", "last"),
        "tick_count": ("last_price", "size"),
    }
    if "vwap" in df.columns:
        agg["vwap"] = ("vwap", "last")
    out = df.groupby("bucket", sort=True).agg(**agg).reset_index()
    return out


def compute_option_features_for_symbol(
    buckets: pd.DataFrame,
    *,
    symbol: str,
    underlying: str,
    option_type: str,
    strike: float | None,
    expiry: str | None,
    lot_size: int | None,
    spot_by_bucket: dict[pd.Timestamp, float],
    pcr_by_bucket: dict[pd.Timestamp, dict[str, Any]],
    bucket_minutes: int,
) -> list[dict[str, Any]]:
    """Attach buildup, PCR context, VWAP, DTE, and Black-Scholes Greeks."""
    if buckets.empty:
        return []

    rows: list[dict[str, Any]] = []
    prev_ltp: float | None = None
    prev_oi: float | None = None
    expiry_date = _parse_expiry_date(expiry)

    for _, row in buckets.iterrows():
        bucket_ts = pd.Timestamp(row["bucket"])
        ltp = float(row["ltp"]) if pd.notna(row["ltp"]) else None
        oi = float(row["oi"]) if pd.notna(row["oi"]) else None
        if ltp is None:
            continue

        price_change = (ltp - prev_ltp) if prev_ltp is not None else 0.0
        oi_change = (oi - prev_oi) if prev_oi is not None and oi is not None else 0.0
        buildup = classify_oi_buildup(price_change, oi_change)

        spot = lookup_by_bucket(spot_by_bucket, bucket_ts)
        if spot is not None:
            spot = float(spot)
        days_to_expiry = None
        tte_years = None
        if expiry_date is not None:
            days_to_expiry = (expiry_date - bucket_ts.date()).days
            if days_to_expiry >= 0:
                tte_years = max(days_to_expiry, 1) / 365.0
            days_to_expiry = max(days_to_expiry, 0)

        vwap = float(row["vwap"]) if "vwap" in row.index and pd.notna(row["vwap"]) else None
        vwap_dev_bps = (
            ((ltp / vwap - 1.0) * 10000.0) if vwap and vwap > 0 else None
        )

        greeks = None
        if spot is not None and strike is not None and tte_years is not None:
            # IV assumed 0.15 until a market-IV solver exists; BS greeks still populate.
            greeks = compute_greeks(
                spot=spot,
                strike=float(strike),
                time_to_expiry_years=tte_years,
                volatility=0.15,
                option_type=option_type,
            )

        pcr_ctx = lookup_by_bucket(pcr_by_bucket, bucket_ts) or {}
        greeks_dict = greeks if isinstance(greeks, dict) else {}
        features: dict[str, Any] = {
            "underlying": underlying,
            "option_type": option_type,
            "strike": strike,
            "expiry": expiry,
            "days_to_expiry": days_to_expiry,
            "lot_size": lot_size,
            "ltp": ltp,
            "oi": oi,
            "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
            "vwap": vwap,
            "vwap_deviation_bps": vwap_dev_bps,
            "price_change": price_change,
            "oi_change": oi_change,
            "oi_buildup_state": buildup,
            "pcr": pcr_ctx.get("pcr"),
            "pcr_put_oi": pcr_ctx.get("put_oi"),
            "pcr_call_oi": pcr_ctx.get("call_oi"),
            "pcr_buildup_context": pcr_ctx.get("buildup_context"),
            "spot": spot,
            "delta": greeks_dict.get("delta"),
            "gamma": greeks_dict.get("gamma"),
            "greeks": greeks,
            "bucket_minutes": bucket_minutes,
            "tick_count": int(row["tick_count"]),
        }
        rows.append(
            {
                "timestamp": bucket_ts.isoformat(),
                "trade_date": str(bucket_ts.date()),
                "symbol": symbol,
                "track": "options",
                "features": features,
            }
        )
        prev_ltp = ltp
        if oi is not None:
            prev_oi = oi

    return rows


def compute_pcr_by_bucket(
    option_buckets: dict[str, pd.DataFrame],
    meta_by_symbol: dict[str, dict[str, Any]],
) -> dict[str, dict[pd.Timestamp, dict[str, Any]]]:
    """
    PCR = total put OI / total call OI per underlying per bucket.

    Also attaches a coarse buildup context from ATM-ish aggregate price/OI drift.
    """
    # underlying -> bucket -> lists
    from collections import defaultdict

    put_oi: dict[str, dict[pd.Timestamp, float]] = defaultdict(lambda: defaultdict(float))
    call_oi: dict[str, dict[pd.Timestamp, float]] = defaultdict(lambda: defaultdict(float))
    # For context: average signed price change weighted lightly
    price_dir: dict[str, dict[pd.Timestamp, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    oi_dir: dict[str, dict[pd.Timestamp, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    prev_ltp: dict[str, float] = {}
    prev_oi: dict[str, float] = {}

    for symbol, buckets in option_buckets.items():
        meta = meta_by_symbol[symbol]
        underlying = str(meta["underlying"])
        opt_type = str(meta["option_type"])
        if buckets.empty:
            continue
        for _, row in buckets.iterrows():
            bucket_ts = pd.Timestamp(row["bucket"])
            oi = float(row["oi"]) if pd.notna(row["oi"]) else 0.0
            ltp = float(row["ltp"]) if pd.notna(row["ltp"]) else None
            if opt_type == "PE":
                put_oi[underlying][bucket_ts] += oi
            elif opt_type == "CE":
                call_oi[underlying][bucket_ts] += oi
            if ltp is not None:
                if symbol in prev_ltp:
                    price_dir[underlying][bucket_ts].append(ltp - prev_ltp[symbol])
                prev_ltp[symbol] = ltp
            if symbol in prev_oi:
                oi_dir[underlying][bucket_ts].append(oi - prev_oi[symbol])
            prev_oi[symbol] = oi

    result: dict[str, dict[pd.Timestamp, dict[str, Any]]] = {}
    underlyings = set(put_oi) | set(call_oi)
    for underlying in underlyings:
        result[underlying] = {}
        buckets = set(put_oi[underlying]) | set(call_oi[underlying])
        for bucket_ts in sorted(buckets):
            p_oi = put_oi[underlying].get(bucket_ts, 0.0)
            c_oi = call_oi[underlying].get(bucket_ts, 0.0)
            pcr = (p_oi / c_oi) if c_oi > 0 else None
            avg_px = float(np.mean(price_dir[underlying][bucket_ts])) if price_dir[underlying][bucket_ts] else 0.0
            avg_oi = float(np.mean(oi_dir[underlying][bucket_ts])) if oi_dir[underlying][bucket_ts] else 0.0
            result[underlying][bucket_ts] = {
                "pcr": pcr,
                "put_oi": p_oi,
                "call_oi": c_oi,
                "buildup_context": classify_oi_buildup(avg_px, avg_oi),
            }
    return result
