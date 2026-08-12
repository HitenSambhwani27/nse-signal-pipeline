#!/usr/bin/env python3
"""
Stage 1 verification helper (offline-friendly).

Runs unit tests and writes sample tick data through the same Parquet/SQLite
path used by live ingestion, so you can validate storage without market hours.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.storage.parquet_writer import BufferedParquetWriter  # noqa: E402
from nse_pipeline.storage.schemas import NormalizedTick  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def _sample_tick(symbol: str, token: int, price: float) -> NormalizedTick:
    now = datetime.now(timezone.utc)
    return NormalizedTick(
        timestamp=now,
        instrument_token=token,
        symbol=symbol,
        exchange="NSE",
        last_price=price,
        volume=5000,
        last_quantity=50,
        average_price=price - 0.5,
        oi=None,
        bid_prices=[price - 0.5, price - 1.0],
        bid_quantities=[100, 200],
        bid_orders=[3, 2],
        ask_prices=[price + 0.5, price + 1.0],
        ask_quantities=[120, 80],
        ask_orders=[2, 1],
    )


def main() -> int:
    print("=== Running pytest ===")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=PROJECT_ROOT,
        check=False,
    )
    if result.returncode != 0:
        print("pytest failed.")
        return result.returncode

    settings = load_settings()
    store = SQLiteStore(settings.paths.sqlite_db)
    writer = BufferedParquetWriter(
        raw_dir=settings.paths.raw_dir,
        flush_interval_seconds=1,
        flush_max_rows=1,
    )

    samples = [
        _sample_tick("RELIANCE", 738561, 2500.0),
        _sample_tick("HDFCBANK", 341249, 1650.0),
        _sample_tick("INFY", 408065, 1800.0),
    ]

    total_flushed = 0
    for tick in samples:
        total_flushed += writer.add_tick(tick)
    total_flushed += writer.flush_all()

    store.log_ingestion_event(
        event_type="verify_sample",
        message="Stage 1 offline verification sample ticks written",
        rows_written=total_flushed,
    )

    print(f"Wrote sample ticks (flushed rows={total_flushed}).")
    print("Run: python scripts/inspect_data.py")
    print("\nStage 1 offline verification passed.")
    print("Next: run 00_kite_auth.py + 01_run_ingestion.py during market hours for live data.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
