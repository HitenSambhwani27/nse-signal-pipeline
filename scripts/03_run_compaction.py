#!/usr/bin/env python3
"""
After-close compaction: merge per-minute ticks_*.parquet into one file per symbol/day.

Usage:
  python scripts/03_run_compaction.py                  # all dates with minute parts
  python scripts/03_run_compaction.py --date 2026-08-12
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.session_coverage import (  # noqa: E402
    assess_session_coverage,
    format_coverage_banner,
    log_session_coverage,
)
from nse_pipeline.storage.compaction import compact_all_dates, compact_date  # noqa: E402
from nse_pipeline.storage.bars import backfill_compacted_bars, summarize_bar_build  # noqa: E402
from nse_pipeline.storage.duckdb_store import DuckDBTickStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Compact minute tick Parquet files")
    parser.add_argument(
        "--date",
        help="YYYY-MM-DD to compact (default: all dates under data/raw with minute parts)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="After compact, print DuckDB row counts for the date(s)",
    )
    parser.add_argument(
        "--bars-only",
        action="store_true",
        help="Skip tick merge; materialize RM-1 bars from existing compacted ticks",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = load_settings()

    if args.bars_only:
        dates = [args.date] if args.date else None
        bar_results = backfill_compacted_bars(settings, dates=dates)
        summary = summarize_bar_build(bar_results)
        print(f"RM-1 backfill {summary}")
        return 0

    if args.date:
        results = compact_date(settings, args.date)
        dates = [args.date]
    else:
        results = compact_all_dates(settings)
        dates = sorted({r.date_str for r in results})

    compacted = [r for r in results if r.output_path is not None]
    skipped = [r for r in results if r.output_path is None]
    total_rows = sum(r.rows for r in compacted)
    print(
        f"Compacted {len(compacted)} symbol-days "
        f"({total_rows} rows); skipped {len(skipped)} folders without minute parts."
    )
    print(f"Output root: {settings.paths.compacted_dir}")

    # Flag partial sessions so later stages do not treat them as full days.
    with DuckDBTickStore(settings) as store:
        for date_str in dates:
            coverage = assess_session_coverage(settings, date_str, store=store)
            log_session_coverage(settings, coverage)
            print(f"[{date_str}] {format_coverage_banner(coverage)}")

    bar_dates = dates or []
    if bar_dates:
        extra = backfill_compacted_bars(settings, dates=bar_dates)
        print(f"RM-1 bars: {summarize_bar_build(extra)}")

    if args.verify and dates:
        with DuckDBTickStore(settings) as store:
            for date_str in dates:
                counts = store.tick_row_counts(date_str)
                if counts.empty:
                    print(f"[{date_str}] no compacted symbols")
                    continue
                print(
                    f"[{date_str}] symbols={len(counts)} "
                    f"rows={int(counts['rows'].sum())}"
                )
                print(counts.sort_values("rows", ascending=False).head(10).to_string(index=False))
                # Sample one symbol via DuckDB.
                sample_symbol = str(counts.sort_values("rows", ascending=False).iloc[0]["symbol"])
                sample = store.read_ticks(date_str, sample_symbol)
                print(
                    f"Sample {sample_symbol}: rows={len(sample)} "
                    f"cols={list(sample.columns)} "
                    f"first_ts={sample['timestamp'].iloc[0]} "
                    f"last_ts={sample['timestamp'].iloc[-1]}"
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
