"""Write historical OHLCV into the Stage 1H compacted partition layout."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from nse_pipeline.storage.schemas import CandleRecord

logger = logging.getLogger(__name__)

SOURCE_HISTORICAL = "historical"
SOURCE_LIVE = "live"


def _date_str(ts: pd.Timestamp) -> str:
    t = pd.Timestamp(ts)
    if t.tzinfo is not None:
        t = t.tz_convert("UTC")
    return str(t.date())


def candles_to_rows(
    candles: list[CandleRecord], *, interval: str, source: str = SOURCE_HISTORICAL
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in candles:
        typical = (c.high + c.low + c.close) / 3.0
        rows.append(
            {
                "timestamp": c.timestamp,
                "instrument_token": c.instrument_token,
                "symbol": c.symbol,
                "exchange": c.exchange,
                "last_price": c.close,
                "volume": c.volume,
                "last_quantity": 0,
                "average_price": typical,
                "oi": c.oi,
                "bid_prices": [],
                "bid_quantities": [],
                "bid_orders": [],
                "ask_prices": [],
                "ask_quantities": [],
                "ask_orders": [],
                "source": source,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "interval": interval,
            }
        )
    return rows


def peek_ticks_source(path: Path) -> str | None:
    """Return 'live' if file looks live-captured, 'historical' if only historical, None if missing."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["source"] if _has_source(path) else None)
    except Exception:
        return SOURCE_LIVE
    if "source" not in df.columns:
        return SOURCE_LIVE
    sources = {str(s) for s in df["source"].dropna().unique()}
    if SOURCE_LIVE in sources:
        return SOURCE_LIVE
    if SOURCE_HISTORICAL in sources:
        return SOURCE_HISTORICAL
    return SOURCE_LIVE


def _has_source(path: Path) -> bool:
    try:
        import pyarrow.parquet as pq

        return "source" in pq.read_schema(path).names
    except Exception:
        return False


def write_historical_partition(
    compacted_dir: Path,
    candles: list[CandleRecord],
    *,
    interval: str,
    overwrite_historical: bool = True,
) -> dict[str, int]:
    """
    Write candles into compacted/{date}/{symbol}/.

    Minute/intraday intervals materialize ticks.parquet unless a live file
    already occupies that slot (live wins). Daily bars go to daily.parquet.
    Re-running replaces historical files; never duplicates rows.
    """
    if not candles:
        return {"dates": 0, "rows": 0, "skipped_live": 0}

    rows = candles_to_rows(candles, interval=interval)
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        date_str = _date_str(row["timestamp"])
        by_key[(date_str, str(row["symbol"]))].append(row)

    dates = 0
    written = 0
    skipped_live = 0
    is_daily = interval == "day"

    for (date_str, symbol), group in by_key.items():
        out_dir = compacted_dir / date_str / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(group).sort_values("timestamp").drop_duplicates(
            subset=["timestamp", "instrument_token"], keep="last"
        )
        if is_daily:
            out_path = out_dir / "daily.parquet"
            df.to_parquet(out_path, index=False)
            written += len(df)
            dates += 1
            continue

        ticks_path = out_dir / "ticks.parquet"
        existing = peek_ticks_source(ticks_path)
        if existing == SOURCE_LIVE:
            skipped_live += 1
            # Keep OHLC archive even when live ticks occupy the slot.
            df.to_parquet(out_dir / "candles.parquet", index=False)
            continue
        if existing == SOURCE_HISTORICAL and not overwrite_historical:
            continue
        df.to_parquet(ticks_path, index=False)
        df.to_parquet(out_dir / "candles.parquet", index=False)
        written += len(df)
        dates += 1

    return {"dates": dates, "rows": written, "skipped_live": skipped_live}


def daterange_bounds(candles: list[CandleRecord]) -> tuple[date | None, date | None]:
    if not candles:
        return None, None
    stamps = [pd.Timestamp(c.timestamp) for c in candles]
    first = min(stamps).date()
    last = max(stamps).date()
    return first, last
