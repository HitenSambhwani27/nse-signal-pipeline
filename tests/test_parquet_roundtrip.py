"""Tests for buffered Parquet tick writer roundtrip."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from nse_pipeline.storage.parquet_writer import BufferedParquetWriter
from nse_pipeline.storage.schemas import NormalizedTick


def _sample_tick(symbol: str = "RELIANCE") -> NormalizedTick:
    return NormalizedTick(
        timestamp=datetime(2026, 8, 9, 10, 15, tzinfo=timezone.utc),
        instrument_token=123456,
        symbol=symbol,
        exchange="NSE",
        last_price=2500.5,
        volume=1000,
        last_quantity=10,
        average_price=2499.0,
        oi=None,
        bid_prices=[2500.0, 2499.5],
        bid_quantities=[100, 200],
        bid_orders=[5, 3],
        ask_prices=[2500.5, 2501.0],
        ask_quantities=[150, 120],
        ask_orders=[4, 2],
    )


def test_parquet_roundtrip_flush(tmp_path: Path) -> None:
    writer = BufferedParquetWriter(
        raw_dir=tmp_path,
        flush_interval_seconds=1,
        flush_max_rows=1,
    )

    tick = _sample_tick()
    flushed = writer.add_tick(tick)
    assert flushed >= 1

    files = list(tmp_path.glob("*/*/*.parquet"))
    assert len(files) == 1

    df = pd.read_parquet(files[0])
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "RELIANCE"
    assert df.iloc[0]["last_price"] == 2500.5
    assert len(df.iloc[0]["bid_prices"]) == 2
