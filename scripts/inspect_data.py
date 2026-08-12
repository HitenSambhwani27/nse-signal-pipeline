#!/usr/bin/env python3
"""
Inspect logged Parquet ticks/candles and SQLite ingestion metadata.

Works offline — useful for Stage 1 verification after ingestion/backfill runs.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import pandas as pd  # noqa: E402

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.storage.parquet_writer import BufferedParquetWriter  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def _level_count(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes)):
        return None
    if hasattr(value, "__len__"):
        return len(value)  # type: ignore[arg-type]
    return None


def _print_tick_schema(path: Path, label: str) -> None:
    """Print the FULL column list and depth/OI samples for one tick file."""
    df = pd.read_parquet(path)
    cols = list(df.columns)
    print(f"\n=== {label}: {path.relative_to(PROJECT_ROOT)} ===")
    print(f"rows={len(df)}")
    print(f"FULL columns ({len(cols)}): {cols}")

    for col in ("bid_prices", "ask_prices", "oi"):
        if col not in cols:
            print(f"  {col}: MISSING")
            continue
        if len(df) == 0:
            print(f"  {col}: present (empty file)")
            continue
        sample = df.iloc[0][col]
        levels = _level_count(sample)
        if levels is not None:
            print(f"  {col}: present sample={sample!r} levels={levels}")
        else:
            print(f"  {col}: present sample={sample!r}")


def main() -> int:
    settings = load_settings()
    store = SQLiteStore(settings.paths.sqlite_db)

    print("=== Recent ingestion_meta events ===")
    events = store.get_recent_ingestion_events(limit=20)
    if not events:
        print("(none yet)")
    for event in events:
        print(
            f"{event['timestamp']} [{event['event_type']}] "
            f"{event.get('symbol') or ''} rows={event.get('rows_written')} "
            f"{event.get('message') or ''}"
        )

    print("\n=== Parquet files ===")
    files = BufferedParquetWriter.list_tick_files(settings.paths.raw_dir)
    candle_files = sorted(settings.paths.raw_dir.glob("*/*/candles.parquet"))
    all_files = sorted(set(files + candle_files))
    if not all_files:
        print("(none yet)")
        return 0

    for path in all_files[:20]:
        df = pd.read_parquet(path)
        print(f"{path.relative_to(PROJECT_ROOT)} -> rows={len(df)} cols={list(df.columns)}")
        if len(df) > 0:
            print(f"  first_ts={df['timestamp'].iloc[0]} last_ts={df['timestamp'].iloc[-1]}")

    if len(all_files) > 20:
        print(f"... and {len(all_files) - 20} more files")

    equity_names = set(settings.equity_symbols)
    index_names = set(settings.index_symbols)
    tick_files = [p for p in all_files if p.name.startswith("ticks_")]

    equity_tick = next((p for p in tick_files if p.parent.name in equity_names), None)
    option_tick = next(
        (
            p
            for p in tick_files
            if p.parent.name.startswith("NIFTY") and p.parent.name not in index_names
        ),
        None,
    )
    index_tick = next((p for p in tick_files if p.parent.name in index_names), None)

    if equity_tick is not None:
        _print_tick_schema(equity_tick, "Equity tick schema")
    else:
        print("\n(no equity tick file found for full schema dump)")

    if option_tick is not None:
        _print_tick_schema(option_tick, "Option tick schema")
    else:
        print("\n(no option tick file found for full schema dump)")

    if index_tick is not None:
        _print_tick_schema(index_tick, "Index tick schema")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
