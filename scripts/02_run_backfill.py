#!/usr/bin/env python3
"""Backfill historical OHLCV+OI candles for cached instruments."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.broker.auth import get_authenticated_kite  # noqa: E402
from nse_pipeline.broker.historical import backfill_from_cache  # noqa: E402
from nse_pipeline.config import load_settings


def main() -> int:
    settings = load_settings()
    try:
        kite = get_authenticated_kite(settings)
    except ValueError as exc:
        print(exc)
        return 1

    counts = backfill_from_cache(kite, settings)
    total = sum(counts.values())
    print(f"Backfill complete. Total candles written: {total}")
    for symbol, count in sorted(counts.items()):
        print(f"  {symbol}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
