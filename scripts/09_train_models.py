#!/usr/bin/env python3
"""
Stage 5 — train coarse+fine logistic models and run them through the Part 5 harness.

Stop before Part 7 if OOS is weak or IS/OOS diverges beyond threshold
(harness_passed=false on a class).

Usage:
  python scripts/09_train_models.py --start-date 2022-01-01 --end-date 2026-08-14
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
from nse_pipeline.models.train import train_all  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Train Stage 5 models + harness gate")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument(
        "--skip-harness",
        action="store_true",
        help="Fit only (not eligible for live load)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    summary = train_all(
        settings,
        start_date=args.start_date,
        end_date=args.end_date,
        run_harness=not args.skip_harness,
    )
    print(json.dumps(summary, indent=2, default=str))
    failed = []
    for class_key, payload in summary.get("classes", {}).items():
        for role, entry in (payload.get("roles") or {}).items():
            if entry.get("error"):
                failed.append(f"{class_key}/{role}: {entry['error']}")
            elif not args.skip_harness and entry.get("harness_passed") is False:
                failed.append(f"{class_key}/{role}: harness failed")
    if failed:
        print("STOP before Part 7 — models did not pass the walk-forward gate:")
        for line in failed:
            print(" ", line)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
