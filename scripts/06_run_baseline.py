#!/usr/bin/env python3
"""
Stage 3B — YAML baseline scorer → signal_log.

Usage:
  python scripts/06_run_baseline.py --start-date 2026-08-13 --end-date 2026-08-13
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.scoring.baseline import run_baseline_scorer  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage 3B baseline scorer")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    summary = run_baseline_scorer(settings, args.start_date, args.end_date)
    print(f"scored={summary['scored']}")
    print(f"by_track={summary['by_track']}")
    print(f"by_source={summary['by_source']}")
    print(f"by_completeness={summary['by_completeness']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
