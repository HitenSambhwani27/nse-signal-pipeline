#!/usr/bin/env python3
"""Stage 8 — record a manual (or overridden) decision with frozen maturity view.

System-suggested decisions are refused until probability_permitted is true.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.decisions.link import log_decision  # noqa: E402
from nse_pipeline.signals.maturity import maturity_public_view, pooled_live_days  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Log a decision with frozen maturity view")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--actor", choices=("manual", "overridden", "system"), default="manual")
    parser.add_argument("--side", choices=("BUY", "SELL"))
    parser.add_argument("--qty", type=float)
    parser.add_argument("--price", type=float)
    parser.add_argument("--track", default="equity_depth")
    parser.add_argument("--class-key", default="equity")
    args = parser.parse_args()

    settings = load_settings()
    store = SQLiteStore(settings.paths.sqlite_db)
    days = pooled_live_days(settings, class_key=args.class_key)
    view = maturity_public_view(
        days, settings, class_key=args.class_key, has_model=False
    )
    try:
        trade_id = log_decision(
            store,
            symbol=args.symbol,
            actor=args.actor,
            maturity_view=view,
            track=args.track,
            side=args.side,
            suggested_qty=args.qty,
            suggested_price=args.price,
        )
    except PermissionError as exc:
        print(json.dumps({"ok": False, "error": str(exc), "maturity": view}, indent=2))
        return 2
    print(json.dumps({"ok": True, "trade_id": trade_id, "maturity": view}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
