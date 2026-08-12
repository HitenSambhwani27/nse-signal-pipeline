"""
Buffered Parquet writer for raw ticks and candles.

Parquet is columnar and batch-oriented. We accumulate rows in memory and flush
periodically instead of appending one row at a time to disk.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from nse_pipeline.storage.schemas import CandleRecord, NormalizedTick


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BufferedParquetWriter:
    """
    Buffers tick rows per symbol, flushing to partitioned Parquet files.

    Partition layout:
      {raw_dir}/{YYYY-MM-DD}/{symbol}/ticks_{HHmm}.parquet
      {raw_dir}/{YYYY-MM-DD}/{symbol}/candles.parquet
    """

    def __init__(
        self,
        raw_dir: Path,
        flush_interval_seconds: int = 45,
        flush_max_rows: int = 5000,
    ) -> None:
        self.raw_dir = raw_dir
        self.flush_interval_seconds = flush_interval_seconds
        self.flush_max_rows = flush_max_rows

        # defaultdict(list) auto-creates an empty list for new keys — handy grouping map.
        self._tick_buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._last_flush_monotonic: dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def add_tick(self, tick: NormalizedTick) -> int:
        """Add one tick; returns number of rows flushed (0 if still buffering)."""
        symbol = tick.symbol
        row = tick.to_row_dict()
        with self._lock:
            self._tick_buffers[symbol].append(row)
            buffer_size = len(self._tick_buffers[symbol])
            if self._should_flush(symbol, buffer_size):
                return self._flush_symbol_locked(symbol)
        return 0

    def flush_all(self) -> int:
        """Flush every symbol buffer — call on shutdown."""
        total = 0
        with self._lock:
            for symbol in list(self._tick_buffers.keys()):
                total += self._flush_symbol_locked(symbol)
        return total

    def _should_flush(self, symbol: str, buffer_size: int) -> bool:
        if buffer_size >= self.flush_max_rows:
            return True
        now = _utc_now().timestamp()
        last = self._last_flush_monotonic.get(symbol, 0.0)
        if last == 0.0:
            self._last_flush_monotonic[symbol] = now
            return False
        if (now - last) >= self.flush_interval_seconds:
            return True
        return False

    def _flush_symbol_locked(self, symbol: str) -> int:
        rows = self._tick_buffers.get(symbol, [])
        if not rows:
            return 0

        df = pd.DataFrame(rows)
        if df.empty:
            return 0

        # Use first row timestamp for partition folder/file naming.
        first_ts = pd.to_datetime(df["timestamp"].iloc[0], utc=True)
        date_str = first_ts.strftime("%Y-%m-%d")
        minute_str = first_ts.strftime("%H%M")

        out_dir = self.raw_dir / date_str / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"ticks_{minute_str}.parquet"

        # If file exists (same minute bucket), append by reading then rewriting.
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(out_path, index=False)

        self._tick_buffers[symbol] = []
        self._last_flush_monotonic[symbol] = _utc_now().timestamp()
        return len(df)

    def write_candles(self, symbol: str, candles: list[CandleRecord]) -> Path | None:
        """Write/replace candles.parquet for a symbol on a given day."""
        if not candles:
            return None

        rows: list[dict[str, Any]] = []
        for candle in candles:
            rows.append(
                {
                    "timestamp": candle.timestamp,
                    "instrument_token": candle.instrument_token,
                    "symbol": candle.symbol,
                    "exchange": candle.exchange,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "oi": candle.oi,
                }
            )

        df = pd.DataFrame(rows)
        first_ts = pd.to_datetime(df["timestamp"].iloc[0], utc=True)
        date_str = first_ts.strftime("%Y-%m-%d")
        out_dir = self.raw_dir / date_str / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "candles.parquet"
        df.to_parquet(out_path, index=False)
        return out_path

    @staticmethod
    def read_parquet(path: Path) -> pd.DataFrame:
        """Utility for tests and inspection scripts."""
        return pd.read_parquet(path)

    @staticmethod
    def list_tick_files(raw_dir: Path, date_str: str | None = None) -> list[Path]:
        """List tick parquet files, optionally filtered by YYYY-MM-DD."""
        if date_str:
            base = raw_dir / date_str
            if not base.exists():
                return []
            return sorted(base.glob("*/*.parquet"))

        return sorted(raw_dir.glob("*/*/*.parquet"))
