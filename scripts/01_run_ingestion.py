#!/usr/bin/env python3
"""
Run live WebSocket ingestion -> partitioned Parquet + SQLite ingestion_meta.

Prerequisites:
  - Valid .env with KITE_API_KEY, KITE_API_SECRET, KITE_ACCESS_TOKEN
  - config/instruments_cache.json (created by scripts/00_kite_auth.py)
  - Market hours for live ticks (script will still connect off-hours but may be quiet)

On startup, if NSE membership snapshot is older than
universe.membership_max_age_days, CSVs + instrument cache are refreshed.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.broker.auth import get_authenticated_kite  # noqa: E402
from nse_pipeline.broker.instruments import refresh_instrument_cache  # noqa: E402
from nse_pipeline.broker.nse_membership import membership_is_stale  # noqa: E402
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

    if membership_is_stale(settings) or not settings.paths.instruments_cache.exists():
        print(
            "Membership/instrument cache missing or stale "
            f"(>{settings.universe.membership_max_age_days} days) — refreshing..."
        )
        try:
            cache = refresh_instrument_cache(kite, settings, force_membership=True)
            counts = cache.get("counts", {})
            print(
                "Refreshed cache: "
                f"depth={counts.get('equity_depth')} quote={counts.get('equity_quote')} "
                f"options={counts.get('options')} futures={counts.get('futures')} "
                f"total={counts.get('total')}"
            )
        except Exception as exc:
            print(f"Startup membership refresh failed: {exc}")
            return 1

    service = WebSocketIngestionService(settings=settings, kite=kite)
    print(
        f"Starting ingestion for {len(service.tokens)} instruments "
        f"(full={len(service.full_tokens)}, quote={len(service.quote_tokens)}). "
        "Press Ctrl+C to stop and flush buffers."
    )
    service.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
