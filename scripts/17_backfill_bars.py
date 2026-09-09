#!/usr/bin/env python3
"""
One-time / idempotent RM-1 bar backfill from existing compacted ticks.

Does not call Kite. Does not rewrite compacted ticks. Does not fabricate candles.

Usage:
  python scripts/17_backfill_bars.py
  python scripts/17_backfill_bars.py --date 2026-09-04
  python scripts/17_backfill_bars.py --symbol HDFCBANK
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.storage.bars import (  # noqa: E402
    backfill_compacted_bars,
    list_compacted_sessions,
    summarize_bar_build,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize RM-1 bars from compacted ticks")
    parser.add_argument("--date", help="YYYY-MM-DD (default: every compacted session)")
    parser.add_argument("--symbol", help="Single symbol (default: every compacted symbol)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    discovered = list_compacted_sessions(settings)
    dates = [args.date] if args.date else None
    symbols = [args.symbol] if args.symbol else None
    print(f"compacted_sessions_discovered={discovered}")
    results = backfill_compacted_bars(settings, dates=dates, symbols=symbols)
    summary = summarize_bar_build(results)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
