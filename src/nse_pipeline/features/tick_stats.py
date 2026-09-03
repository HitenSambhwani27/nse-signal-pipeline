"""Shared tick → VWAP / best-book helpers for equity, options, and futures."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from nse_pipeline.features.equity import compute_ofi


def as_utc(ts: Any) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def lookup_by_bucket(mapping: dict[Any, Any] | None, bucket_ts: pd.Timestamp) -> Any:
    """O(1) lookup that ignores tz-naive vs tz-aware bucket-key mismatches."""
    if not mapping:
        return None
    if bucket_ts in mapping:
        return mapping[bucket_ts]
    target = as_utc(bucket_ts)
    if target in mapping:
        return mapping[target]
    for key, val in mapping.items():
        if as_utc(key) == target:
            return val
    return None


def _level1(prices: Any, qtys: Any) -> tuple[float | None, float | None]:
    try:
        if prices is None or qtys is None or len(prices) == 0 or len(qtys) == 0:
            return None, None
        return float(prices[0]), float(qtys[0])
    except (TypeError, ValueError, IndexError):
        return None, None


def add_session_vwap(ticks: pd.DataFrame, *, source: str) -> pd.DataFrame:
    """Cumulative session VWAP on each tick (same rules as equity features)."""
    if ticks.empty:
        return ticks
    df = ticks.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    skip_depth = source == "historical"
    if skip_depth and {"high", "low", "close"}.issubset(df.columns):
        typical = (
            df["high"].astype(float) + df["low"].astype(float) + df["close"].astype(float)
        ) / 3.0
        trade_qty = df["volume"].fillna(0).astype(float).clip(lower=0)
        vol_delta = trade_qty.diff()
        if vol_delta.fillna(0).ge(0).all():
            trade_qty = vol_delta.fillna(trade_qty.iloc[0]).clip(lower=0)
        df["_trade_qty"] = trade_qty
        df["_pv"] = typical * df["_trade_qty"]
    else:
        if "last_quantity" in df.columns:
            qty = df["last_quantity"].fillna(0).astype(float).clip(lower=0)
        else:
            qty = pd.Series(0.0, index=df.index)
        vol = (
            df["volume"].fillna(0).astype(float)
            if "volume" in df.columns
            else pd.Series(0.0, index=df.index)
        )
        vol_delta = vol.diff().fillna(vol.iloc[0] if len(vol) else 0.0).clip(lower=0)
        df["_trade_qty"] = np.where(qty > 0, qty, vol_delta)
        df["_pv"] = df["last_price"].astype(float) * df["_trade_qty"]
    df["_cum_pv"] = df["_pv"].cumsum()
    df["_cum_qty"] = df["_trade_qty"].cumsum().replace(0, np.nan)
    df["vwap"] = df["_cum_pv"] / df["_cum_qty"]
    return df


def _bucket_end(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    floored = ts.floor(f"{minutes}min")
    return floored + pd.Timedelta(minutes=minutes)


def depth_features_by_bucket(
    ticks: pd.DataFrame,
    *,
    bucket_minutes: int,
) -> dict[pd.Timestamp, dict[str, Any]]:
    """Best-level spread / depth ratio / OFI sum per bucket. Empty if no book."""
    if ticks.empty or "bid_prices" not in ticks.columns:
        return {}
    df = ticks.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["bucket"] = df["timestamp"].map(lambda t: _bucket_end(t, bucket_minutes))

    prev_bid_p = prev_bid_q = prev_ask_p = prev_ask_q = None
    ofi_vals: list[float | None] = []
    for _, row in df.iterrows():
        bid_p, bid_q = _level1(row.get("bid_prices"), row.get("bid_quantities"))
        ask_p, ask_q = _level1(row.get("ask_prices"), row.get("ask_quantities"))
        ofi = compute_ofi(
            bid_price_prev=prev_bid_p,
            bid_qty_prev=prev_bid_q,
            ask_price_prev=prev_ask_p,
            ask_qty_prev=prev_ask_q,
            bid_price=bid_p,
            bid_qty=bid_q,
            ask_price=ask_p,
            ask_qty=ask_q,
        )
        ofi_vals.append(ofi)
        if bid_p is not None:
            prev_bid_p, prev_bid_q = bid_p, bid_q
        if ask_p is not None:
            prev_ask_p, prev_ask_q = ask_p, ask_q
    df["_ofi"] = ofi_vals

    out: dict[pd.Timestamp, dict[str, Any]] = {}
    for bucket_ts, grp in df.groupby("bucket", sort=True):
        last = grp.iloc[-1]
        bid_p, bid_q = _level1(last.get("bid_prices"), last.get("bid_quantities"))
        ask_p, ask_q = _level1(last.get("ask_prices"), last.get("ask_quantities"))
        if bid_p is None and ask_p is None:
            continue
        spread_abs = (ask_p - bid_p) if bid_p is not None and ask_p is not None else None
        mid = ((ask_p + bid_p) / 2.0) if bid_p is not None and ask_p is not None else None
        spread_bps = (spread_abs / mid * 10000.0) if spread_abs is not None and mid else None
        depth_ratio = (
            (bid_q / ask_q) if bid_q is not None and ask_q not in (None, 0) else None
        )
        ofi_sum = float(np.nansum([x for x in grp["_ofi"] if x is not None]))
        ts = pd.Timestamp(bucket_ts)
        out[ts] = {
            "best_bid": bid_p,
            "best_ask": ask_p,
            "best_bid_qty": bid_q,
            "best_ask_qty": ask_q,
            "spread_abs": spread_abs,
            "spread_bps": spread_bps,
            "depth_ratio_bid_ask": depth_ratio,
            "ofi_bucket_sum": ofi_sum,
        }
    return out
