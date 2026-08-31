#!/usr/bin/env python3
"""
Stage 4 — reusable walk-forward harness.

Default scorer is the Stage 3B YAML baseline. Stage 5 models are gated
through this same harness inside scripts/09_train_models.py.

Usage:
  python scripts/08_run_backtest.py --start-date 2022-01-01 --end-date 2026-08-14
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.backtest.harness import run_walk_forward  # noqa: E402
from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.scoring.baseline import load_baseline_weights, score_feature_row  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Walk-forward backtest harness")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--model-id", default="baseline_yaml")
    parser.add_argument(
        "--tracks",
        help="Comma-separated tracks (e.g. equity_depth,equity_quote or options)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    weights = load_baseline_weights()

    def scorer(row: dict) -> float:
        return float(score_feature_row(row, weights)["score"])

    tracks = (
        tuple(part.strip() for part in args.tracks.split(",") if part.strip())
        if args.tracks
        else None
    )
    report = run_walk_forward(
        settings,
        scorer,
        start_date=args.start_date,
        end_date=args.end_date,
        model_id=args.model_id,
        tracks=tracks,
    )
    print(json.dumps(
        {
            "harness_passed": report["harness_passed"],
            "fail_reasons": report["fail_reasons"],
            "n_folds": report["n_folds"],
            "is": report["is"],
            "oos": report["oos"],
            "hit_rate_gap": report["hit_rate_gap"],
            "overfit_threshold": report["overfit_threshold"],
            "skipped_dates": report.get("skipped_dates"),
            "tracks": report.get("tracks"),
            "report_path": report["report_path"],
        },
        indent=2,
    ))
    if not report["harness_passed"]:
        print("Harness DID NOT PASS — do not promote this scorer live.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
