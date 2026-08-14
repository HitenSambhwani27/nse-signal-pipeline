#!/usr/bin/env python3
"""
Stage 2E — batch feature engineering over compacted ticks (DuckDB → feature_log).

Usage:
  python scripts/04_run_features.py --date 2026-08-13
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.features.batch import run_feature_batch  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Stage 2 feature batch for a date")
    parser.add_argument("--date", help="YYYY-MM-DD compacted trade date")
    parser.add_argument("--start-date", help="Inclusive start YYYY-MM-DD")
    parser.add_argument("--end-date", help="Inclusive end YYYY-MM-DD")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = load_settings()

    if args.start_date or args.end_date:
        from nse_pipeline.storage.duckdb_store import DuckDBTickStore

        start = args.start_date or "0000-01-01"
        end = args.end_date or "9999-12-31"
        with DuckDBTickStore(settings) as store:
            dates = [d for d in store.list_compacted_dates() if start <= d <= end]
        if not dates:
            print("No compacted dates in range.")
            return 1
    elif args.date:
        dates = [args.date]
    else:
        parser.error("Provide --date or --start-date/--end-date")
        return 1

    failed = 0
    for date_str in dates:
        try:
            summary = run_feature_batch(settings, date_str)
        except Exception as exc:
            print(f"[{date_str}] Feature batch failed: {exc}")
            failed += 1
            continue
        print(f"trade_date={summary['trade_date']}")
        print(summary.get("coverage_banner", "coverage=unknown"))
        print(f"symbols_seen={summary['symbols_seen']}")
        print(f"feature_rows={summary['feature_rows']}")
        print(f"by_track={summary['by_track']}")
        print(f"by_source={summary.get('by_source')}")
        print(f"by_completeness={summary.get('by_completeness')}")
        print(f"quality_rejects={summary['quality_rejects']}")
        print(f"stock_quote_freeze_flags={summary['stock_quote_freeze_flags']}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
