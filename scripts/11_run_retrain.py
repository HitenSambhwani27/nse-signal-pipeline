#!/usr/bin/env python3
"""
Part 8 — sliding-window retrain. Writes new versioned models; does not overwrite.

Usage:
  python scripts/11_run_retrain.py
  python scripts/11_run_retrain.py --rebuild-features
  python scripts/11_run_retrain.py --maturity-only
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
from nse_pipeline.retrain.loop import run_retrain  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Sliding-window retrain loop")
    parser.add_argument("--rebuild-features", action="store_true")
    parser.add_argument("--skip-harness", action="store_true")
    parser.add_argument(
        "--maturity-only",
        action="store_true",
        help="Print maturity + 2x2 counts; do not train or promote",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    if args.maturity_only:
        from nse_pipeline.signals.maturity import maturity_snapshot
        from nse_pipeline.storage.sqlite_store import SQLiteStore

        store = SQLiteStore(settings.paths.sqlite_db)
        payload = {
            "maturity": maturity_snapshot(settings, has_model={"equity": False, "options": False, "futures": False}),
            "decision_quality": store.outcome_class_counts(),
            "promoted": False,
            "auto_promote": settings.retrain.auto_promote,
            "note": "maturity-only: no training, no promotion",
        }
        print(json.dumps(payload, indent=2, default=str))
        return 0
    result = run_retrain(
        settings,
        rebuild_features=args.rebuild_features,
        run_harness=not args.skip_harness,
    )
    print(json.dumps(
        {
            "window": result["window"],
            "source_composition": result["source_composition"],
            "comparison": result["comparison"],
            "promoted": result["promoted"],
            "maturity": result["maturity"],
            "note": result["note"],
        },
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
