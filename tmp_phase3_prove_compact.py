#!/usr/bin/env python3
"""Prove compact_symbol_day -> ticks.parquet -> RM-1 1m+daily for one session/symbol."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path("/home/nse/nse-signal-pipeline")
sys.path.insert(0, str(ROOT / "src"))

from nse_pipeline.config import load_settings
from nse_pipeline.storage.bars import materialize_symbol_session, minute_bar_path, daily_bar_path
from nse_pipeline.storage.compaction import compact_symbol_day
from nse_pipeline.market.ohlc import load_ohlc_candles


def main() -> int:
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-09-08"
    symbol = sys.argv[2] if len(sys.argv) > 2 else "HDFCBANK"
    settings = load_settings()
    raw = settings.paths.raw_dir / date_str / symbol
    compacted = settings.paths.compacted_dir / date_str / symbol
    result = compact_symbol_day(
        raw_symbol_dir=raw,
        compacted_symbol_dir=compacted,
        archive_minute_files=settings.compaction.archive_minute_files,
        archive_subdir=settings.compaction.archive_subdir,
    )
    bars = materialize_symbol_session(settings, date_str, symbol)
    payload = load_ohlc_candles(settings, symbol, interval="5m", lookback_days=10)
    out = {
        "date": date_str,
        "symbol": symbol,
        "compacted_rows": result.rows,
        "compacted_path": str(result.output_path) if result.output_path else None,
        "skipped_reason": result.skipped_reason,
        "minute_rows": bars.minute_rows,
        "daily_rows": bars.daily_rows,
        "minute_path": str(minute_bar_path(settings, date_str, symbol)),
        "daily_path": str(daily_bar_path(settings, symbol)),
        "minute_exists": minute_bar_path(settings, date_str, symbol).is_file(),
        "daily_exists": daily_bar_path(settings, symbol).is_file(),
        "candles_source": payload.get("candles_source"),
        "candles_status": payload.get("candles_status"),
        "coverage": payload.get("coverage"),
        "returned_bars": len(payload.get("candles") or []),
    }
    print(json.dumps(out, indent=2, default=str))
    if result.skipped_reason or not out["minute_exists"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
