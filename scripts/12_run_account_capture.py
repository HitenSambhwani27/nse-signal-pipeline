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
from nse_pipeline.broker.session import (  # noqa: E402
    AUTH_BACKOFF_SECONDS,
    TokenState,
    classify_exception,
)
from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.processing.jobs import ExistingAccountCapture  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def _capture(capture: ExistingAccountCapture, store: SQLiteStore, kite) -> dict | None:
    try:
        return capture.capture_once(
            store, kite, include_holdings=True, include_session_audit=True
        )
    except Exception as exc:
        state = classify_exception(exc)
        if state == TokenState.INVALID:
            logging.error(
                "Kite account token invalid (%s). Backing off %ss — not crash-looping.",
                exc,
                AUTH_BACKOFF_SECONDS,
            )
        else:
            logging.exception("account capture failed")
        return None


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
        result = _capture(capture, store, kite)
        if result is None:
            print(json.dumps({"ok": False, "reason": "capture_failed"}, indent=2))
            return 1
        print(json.dumps(result, indent=2))
        return 0

    # Stay alive even when the token is invalid. systemd must not restart us
    # every 30s — TokenException is expected when the daily access token expires.
    while True:
        result = _capture(capture, store, kite)
        if result is not None:
            print(json.dumps(result, indent=2))
            time.sleep(max(5, int(args.poll_seconds)))
            continue
        time.sleep(AUTH_BACKOFF_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
