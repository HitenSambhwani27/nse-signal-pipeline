#!/usr/bin/env python3
"""Stage 7 — poll Kite account/order REST. Does not place orders. Independent of models."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.broker.auth import create_kite_client  # noqa: E402
from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.processing.jobs import ExistingAccountCapture  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Kite account capture (REST poll)")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    store = SQLiteStore(settings.paths.sqlite_db)
    kite = create_kite_client(settings.kite)
    capture = ExistingAccountCapture()
    if args.once:
        result = capture.capture_once(
            store, kite, include_holdings=True, include_session_audit=True
        )
        print(json.dumps(result, indent=2))
        return 0
    first = capture.capture_once(
        store, kite, include_holdings=True, include_session_audit=True
    )
    print(json.dumps(first, indent=2))

    while True:
        time.sleep(max(5, int(args.poll_seconds)))
        try:
            capture.capture_once(store, kite)
        except Exception:
            logging.exception("account capture poll failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
