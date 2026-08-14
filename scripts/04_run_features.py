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
    parser.add_argument("--date", required=True, help="YYYY-MM-DD compacted trade date")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = load_settings()

    try:
        summary = run_feature_batch(settings, args.date)
    except Exception as exc:
        print(f"Feature batch failed: {exc}")
        return 1

    print(f"trade_date={summary['trade_date']}")
    print(summary.get("coverage_banner", "coverage=unknown"))
    print(f"symbols_seen={summary['symbols_seen']}")
    print(f"feature_rows={summary['feature_rows']}")
    print(f"by_track={summary['by_track']}")
    print(f"quality_rejects={summary['quality_rejects']}")
    print(f"stock_quote_freeze_flags={summary['stock_quote_freeze_flags']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
