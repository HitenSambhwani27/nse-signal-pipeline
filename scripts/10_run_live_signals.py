#!/usr/bin/env python3
"""
Stage 6 — score feature_log rows with the latest harness-passed coarse+fine pair.

Does not place orders. Decoupled from the dashboard.

Usage:
  python scripts/10_run_live_signals.py --maturity-only
  python scripts/10_run_live_signals.py --start-date 2026-09-01 --end-date 2026-09-02
  python scripts/10_run_live_signals.py --watch
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.signals.engine import LiveSignalEngine  # noqa: E402
from nse_pipeline.signals.maturity import maturity_snapshot  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def _today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()


def _print_snapshot(snap: dict) -> None:
    print("=== pooled live-days maturity (per class, not per contract) ===")
    print(json.dumps(snap, indent=2))
    for key, view in snap.items():
        if not isinstance(view, dict):
            continue
        permitted = view.get("probability_permitted")
        print(
            f"{key}: {view.get('display')}  "
            f"tier={view.get('tier')}  probability_permitted={permitted}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage 6 live signal scoring")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--maturity-only", action="store_true")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Rescore today's feature_log buckets on live_signal.score_every_seconds",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    engine = LiveSignalEngine(settings)
    has_model = {k: engine.has_model(k) for k in ("equity", "options", "futures")}
    snap = maturity_snapshot(settings, has_model=has_model)
    _print_snapshot(snap)
    if args.maturity_only:
        return 0

    store = SQLiteStore(settings.paths.sqlite_db)

    def _run(start: str, end: str) -> dict:
        rows = store.fetch_feature_logs_range(start, end)
        result = engine.score_and_log(rows)
        print(
            f"scored={result['scored']} suppressed={result['suppressed']} "
            f"public_probability_null={result['public_probability_null']}"
        )
        return result

    if args.watch:
        interval = max(1, int(settings.live_signal.score_every_seconds))
        logging.info("Watching today's feature_log every %ss (IST calendar date)", interval)
        while True:
            today = _today_ist()
            status = store.fetch_processing_status("signals")
            after = None
            replace = True
            if (
                status.get("last_trade_date") == today
                and status.get("last_timestamp")
                and status.get("status") not in ("unknown", None)
            ):
                after = status["last_timestamp"]
                replace = False
            rows = store.fetch_feature_logs_since(today, after_timestamp=after)
            result = engine.score_and_log(rows, replace_dates=replace)
            print(
                f"scored={result['scored']} suppressed={result['suppressed']} "
                f"public_probability_null={result['public_probability_null']}"
            )
            time.sleep(interval)
        return 0

    if not args.start_date or not args.end_date:
        print("Provide --start-date and --end-date (or --maturity-only / --watch).")
        return 1

    _run(args.start_date, args.end_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
