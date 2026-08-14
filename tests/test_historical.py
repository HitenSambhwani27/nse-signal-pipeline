"""Historical chunking, writer idempotency, and estimate math."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from nse_pipeline.historical.chunks import chunk_range
from nse_pipeline.historical.writer import (
    SOURCE_HISTORICAL,
    SOURCE_LIVE,
    peek_ticks_source,
    write_historical_partition,
)
from nse_pipeline.storage.schemas import CandleRecord


def test_chunk_range_respects_cap() -> None:
    chunks = chunk_range(date(2022, 1, 1), date(2022, 4, 30), max_days=60)
    assert chunks[0] == (date(2022, 1, 1), date(2022, 3, 1))
    assert all((b - a).days + 1 <= 60 for a, b in chunks)
    assert chunks[-1][1] == date(2022, 4, 30)


def _candle(ts: datetime, close: float = 100.0) -> CandleRecord:
    return CandleRecord(
        timestamp=ts,
        instrument_token=1,
        symbol="RELIANCE",
        exchange="NSE",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=10,
        oi=None,
    )


def test_historical_writer_tags_source_and_is_idempotent(tmp_path: Path) -> None:
    ts = datetime(2022, 1, 3, 4, 0, tzinfo=timezone.utc)
    candles = [_candle(ts), _candle(ts.replace(minute=1), 101.0)]
    stats = write_historical_partition(tmp_path, candles, interval="minute")
    assert stats["rows"] == 2
    path = tmp_path / "2022-01-03" / "RELIANCE" / "ticks.parquet"
    df = pd.read_parquet(path)
    assert set(df["source"].unique()) == {SOURCE_HISTORICAL}
    stats2 = write_historical_partition(tmp_path, candles, interval="minute")
    assert stats2["rows"] == 2
    assert len(pd.read_parquet(path)) == 2


def test_historical_writer_does_not_overwrite_live(tmp_path: Path) -> None:
    day = tmp_path / "2022-01-03" / "RELIANCE"
    day.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": [datetime(2022, 1, 3, 4, 0, tzinfo=timezone.utc)],
            "instrument_token": [1],
            "symbol": ["RELIANCE"],
            "last_price": [100.0],
            "source": [SOURCE_LIVE],
        }
    ).to_parquet(day / "ticks.parquet", index=False)
    candles = [_candle(datetime(2022, 1, 3, 4, 0, tzinfo=timezone.utc))]
    stats = write_historical_partition(tmp_path, candles, interval="minute")
    assert stats["skipped_live"] == 1
    assert peek_ticks_source(day / "ticks.parquet") == SOURCE_LIVE
    assert (day / "candles.parquet").exists()
