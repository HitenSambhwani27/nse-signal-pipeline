#!/usr/bin/env python3
"""Retention planner. Default is dry-run. Never deletes signal_log."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.storage.retention import apply_retention, plan_retention  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="NSE pipeline retention (default: dry-run)")
    parser.add_argument("--apply", action="store_true", help="Actually delete. Requires --i-have-reviewed-the-plan")
    parser.add_argument(
        "--i-have-reviewed-the-plan",
        action="store_true",
        help="Safety latch. --apply is ignored without this flag.",
    )
    args = parser.parse_args()
    settings = load_settings()
    dry = not (args.apply and args.i_have_reviewed_the_plan)
    plan = plan_retention(settings, dry_run=True)
    print(json.dumps(plan.to_dict(), indent=2))
    if args.apply and not args.i_have_reviewed_the_plan:
        print("Refusing --apply without --i-have-reviewed-the-plan", file=sys.stderr)
        return 2
    if dry:
        print("DRY RUN — nothing deleted.")
        return 0
    apply_retention(settings, plan_retention(settings, dry_run=False))
    print("Retention applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
