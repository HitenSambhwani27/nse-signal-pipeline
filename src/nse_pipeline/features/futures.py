"""
Stage 2C — index futures basis features (Nifty + Bank Nifty, near + next).

Expiry and lot size come from the instrument cache (live master).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd


def _bucket_end(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    floored = ts.floor(f"{minutes}min")
    return floored + pd.Timedelta(minutes=minutes)


def _parse_expiry_date(expiry: str | None) -> date | None:
    if not expiry:
        return None
    text = str(expiry)
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(text[:10], fmt).date()
            except ValueError:
                continue
    return None


def bucket_futures_series(ticks: pd.DataFrame, *, bucket_minutes: int) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame()
    df = ticks.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["bucket"] = df["timestamp"].map(lambda t: _bucket_end(t, bucket_minutes))
    return (
        df.groupby("bucket", sort=True)
        .agg(
            ltp=("last_price", "last"),
            oi=("oi", "last"),
            volume=("volume", "last"),
            tick_count=("last_price", "size"),
        )
        .reset_index()
    )


def compute_futures_features(
    buckets: pd.DataFrame,
    *,
    symbol: str,
    underlying: str,
    expiry: str | None,
    lot_size: int | None,
    spot_by_bucket: dict[pd.Timestamp, float],
    near_ltp_by_bucket: dict[pd.Timestamp, float] | None,
    next_ltp_by_bucket: dict[pd.Timestamp, float] | None,
    is_near: bool,
    bucket_minutes: int,
    basis_days_per_year: int,
) -> list[dict[str, Any]]:
    """Futures vs spot basis + near/next calendar spread context."""
    if buckets.empty:
        return []

    expiry_date = _parse_expiry_date(expiry)
    rows: list[dict[str, Any]] = []

    for _, row in buckets.iterrows():
        bucket_ts = pd.Timestamp(row["bucket"])
        fut = float(row["ltp"]) if pd.notna(row["ltp"]) else None
        if fut is None:
            continue
        spot = spot_by_bucket.get(bucket_ts)
        basis_abs = (fut - spot) if spot is not None else None
        basis_bps = ((fut / spot - 1.0) * 10000.0) if spot not in (None, 0) else None

        days_to_expiry = None
        basis_annualized = None
        if expiry_date is not None:
            days_to_expiry = max((expiry_date - bucket_ts.date()).days, 0)
            if basis_bps is not None and days_to_expiry > 0:
                basis_annualized = basis_bps * (basis_days_per_year / days_to_expiry)

        near_ltp = near_ltp_by_bucket.get(bucket_ts) if near_ltp_by_bucket else None
        next_ltp = next_ltp_by_bucket.get(bucket_ts) if next_ltp_by_bucket else None
        calendar_spread = None
        if near_ltp is not None and next_ltp is not None:
            calendar_spread = near_ltp - next_ltp

        features: dict[str, Any] = {
            "underlying": underlying,
            "expiry": expiry,
            "lot_size": lot_size,
            "is_near_month": is_near,
            "ltp": fut,
            "oi": float(row["oi"]) if pd.notna(row["oi"]) else None,
            "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
            "spot": spot,
            "basis_abs": basis_abs,
            "basis_bps": basis_bps,
            "basis_annualized_bps": basis_annualized,
            "days_to_expiry": days_to_expiry,
            "near_ltp": near_ltp,
            "next_ltp": next_ltp,
            "calendar_spread_near_minus_next": calendar_spread,
            "bucket_minutes": bucket_minutes,
            "tick_count": int(row["tick_count"]),
        }
        rows.append(
            {
                "timestamp": bucket_ts.isoformat(),
                "trade_date": str(bucket_ts.date()),
                "symbol": symbol,
                "track": "futures",
                "features": features,
            }
        )
    return rows
