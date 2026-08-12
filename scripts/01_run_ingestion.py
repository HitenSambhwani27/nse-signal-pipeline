#!/usr/bin/env python3
"""
Run live WebSocket ingestion -> partitioned Parquet + SQLite ingestion_meta.

Prerequisites:
  - Valid .env with KITE_API_KEY, KITE_API_SECRET, KITE_ACCESS_TOKEN
  - config/instruments_cache.json (created by scripts/00_kite_auth.py)
  - Market hours for live ticks (script will still connect off-hours but may be quiet)
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.broker.auth import get_authenticated_kite  # noqa: E402
from nse_pipeline.broker.websocket_listener import (  # noqa: E402
    WebSocketIngestionService,
    configure_logging,
)
from nse_pipeline.config import load_settings


def main() -> int:
    settings = load_settings()
    configure_logging(settings.paths.logs_dir)

    try:
        kite = get_authenticated_kite(settings)
    except ValueError as exc:
        print(exc)
        return 1

    service = WebSocketIngestionService(settings=settings, kite=kite)
    print(
        f"Starting ingestion for {len(service.tokens)} instruments. "
        "Press Ctrl+C to stop and flush buffers."
    )
    service.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
