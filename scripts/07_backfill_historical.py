#!/usr/bin/env python3
"""
Part 1 — historical Kite backfill into Stage 1H compacted partitions.

Usage:
  python scripts/07_backfill_historical.py --estimate
  python scripts/07_backfill_historical.py --universe-only
  python scripts/07_backfill_historical.py --probe RELIANCE,INFY,"NIFTY 50"
  python scripts/07_backfill_historical.py --execute --start-date 2022-01-01

Do not run --execute at full scale until the estimate is confirmed.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.broker.auth import get_authenticated_kite  # noqa: E402
from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.historical.backfill import run_backfill  # noqa: E402
from nse_pipeline.historical.estimate import estimate_backfill  # noqa: E402
from nse_pipeline.historical.fetch import HistoricalAuthError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Part 1 historical backfill")
    parser.add_argument("--start-date", help="YYYY-MM-DD (default: historical.equity_start_date)")
    parser.add_argument("--end-date", help="YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--universe-only",
        action="store_true",
        help="Print confirmed-universe instrument list; do not call Kite historical",
    )
    parser.add_argument(
        "--estimate",
        action="store_true",
        help="Print request-count / ETA for full-minute vs daily+recent-minute",
    )
    parser.add_argument(
        "--probe",
        help="Comma-separated tradingsymbols for a small test pull",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run the backfill (confirm estimate first at full scale)",
    )
    parser.add_argument("--skip-minute", action="store_true")
    parser.add_argument("--skip-daily", action="store_true")
    parser.add_argument(
        "--skip-completed",
        action="store_true",
        default=None,
        help="Skip instruments already logged complete in a full --execute (default on with --execute)",
    )
    parser.add_argument(
        "--no-skip-completed",
        action="store_true",
        help="Re-pull every instrument even if sqlite already has a completion row",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    start = date.fromisoformat(args.start_date) if args.start_date else date.fromisoformat(
        settings.historical.equity_start_date
    )
    end = date.fromisoformat(args.end_date) if args.end_date else date.today()

    if args.estimate or not (args.execute or args.probe or args.universe_only):
        est = estimate_backfill(
            settings,
            start=start,
            end=end,
            minute_lookback_days=settings.historical.minute_lookback_days,
        )
        print(json.dumps(est, indent=2))
        if not (args.execute or args.probe or args.universe_only):
            print(
                "\nConfirm scenario (a) full minute vs (b) daily+recent minute "
                "before running --execute at full scale."
            )
            return 0

    if args.universe_only:
        result = run_backfill(
            None,  # type: ignore[arg-type]
            settings,
            start_date=start,
            end_date=end,
            universe_only=True,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    try:
        kite = get_authenticated_kite(
            settings, timeout=settings.historical.http_timeout_seconds
        )
    except ValueError as exc:
        print(exc)
        return 1

    probe = [s.strip() for s in args.probe.split(",") if s.strip()] if args.probe else None
    skip_completed = bool(args.execute) and not args.no_skip_completed
    if args.skip_completed:
        skip_completed = True
    try:
        result = run_backfill(
            kite,
            settings,
            start_date=start,
            end_date=end,
            probe_symbols=probe,
            include_minute=not args.skip_minute,
            include_daily=not args.skip_daily,
            skip_completed=skip_completed,
        )
    except HistoricalAuthError as exc:
        logging.getLogger(__name__).error("Backfill aborted on Kite auth failure: %s", exc)
        out_path = settings.paths.logs_dir / "historical_backfill_last.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(
                {"aborted": True, "reason": "kite_auth_failure", "error": str(exc)},
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"AUTH_ABORT full_report={out_path}")
        return 2
    print(json.dumps(result["summary"], indent=2, default=str))
    print(f"skipped_completed={result['summary'].get('skipped_completed', 0)}")
    out_path = settings.paths.logs_dir / "historical_backfill_last.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(result, indent=2, default=str), encoding="utf-8"
    )
    print(f"full_report={out_path}")
    zeros = []
    failed_chunks = []
    for row in result["per_symbol"]:
        if row.get("zero_data"):
            zeros.append(row["symbol"])
        failed_chunks.extend(row.get("failed_chunks") or [])
    print(f"symbols={len(result['per_symbol'])} zero_data={len(zeros)}")
    if zeros[:20]:
        print("zero_data_sample=", zeros[:20])
    if failed_chunks:
        print(f"failed_chunks={len(failed_chunks)}")
        for chunk in failed_chunks[:20]:
            print("  ", chunk)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
