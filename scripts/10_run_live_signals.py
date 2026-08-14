#!/usr/bin/env python3
"""
Stage 6 — score feature_log rows with the latest harness-passed coarse+fine pair.

Does not place orders. Decoupled from Streamlit.

Usage:
  python scripts/10_run_live_signals.py --start-date 2026-08-14 --end-date 2026-08-14
  python scripts/10_run_live_signals.py --maturity-only
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
from nse_pipeline.signals.engine import LiveSignalEngine  # noqa: E402
from nse_pipeline.signals.maturity import maturity_snapshot  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage 6 live signal scoring")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--maturity-only", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    snap = maturity_snapshot(settings)
    print("=== pooled live-days maturity (per class, not per contract) ===")
    print(json.dumps(snap, indent=2))
    if args.maturity_only:
        return 0

    if not args.start_date or not args.end_date:
        print("Provide --start-date and --end-date (or --maturity-only).")
        return 1

    engine = LiveSignalEngine(settings)
    if not engine.pairs:
        print("No harness-passed models in the registry. Stop: do not go live.")
        return 2
    rows = SQLiteStore(settings.paths.sqlite_db).fetch_feature_logs_range(
        args.start_date, args.end_date
    )
    result = engine.score_and_log(rows)
    print(f"scored={result['scored']} suppressed={result['suppressed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
