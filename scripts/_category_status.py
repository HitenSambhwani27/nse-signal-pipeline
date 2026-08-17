"""Live backfill completion vs confirmed universe."""

from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings
from nse_pipeline.historical.universe import flatten_universe


def main() -> int:
    settings = load_settings()
    pairs = flatten_universe(settings)
    universe: dict[str, int] = Counter(cls for cls, _ in pairs)
    con = sqlite3.connect(settings.paths.sqlite_db)
    execute_start = "2026-08-14T11:42:00"
    rows = con.execute(
        "select timestamp, symbol, rows_written, details_json from ingestion_meta "
        "where event_type='historical_backfill_symbol' and timestamp >= ? "
        "order by id",
        (execute_start,),
    ).fetchall()
    seen: set[str] = set()
    by_class: dict[str, list[dict]] = defaultdict(list)
    last = None
    for ts, symbol, rows_written, details_json in rows:
        payload = json.loads(details_json) if details_json else {}
        cls = str(payload.get("class") or "?")
        key = f"{cls}:{symbol}:{payload.get('expiry')}"
        if key in seen:
            continue
        seen.add(key)
        rec = {
            "symbol": symbol,
            "expiry": payload.get("expiry"),
            "zero_data": bool(payload.get("zero_data")),
            "minute_rows": int(payload.get("minute_rows") or 0),
            "daily_rows": int(payload.get("daily_rows") or 0),
            "failed_chunks": len(payload.get("failed_chunks") or []),
            "errors": payload.get("errors") or [],
            "ts": ts,
            "rows_written": rows_written,
        }
        by_class[cls].append(rec)
        last = (ts, cls, symbol, rows_written)
    completes = con.execute(
        "select timestamp from ingestion_meta "
        "where event_type='historical_backfill_complete' and timestamp >= ? "
        "order by id desc limit 3",
        (execute_start,),
    ).fetchall()
    auth = con.execute(
        "select timestamp, symbol, message from ingestion_meta "
        "where event_type='historical_backfill_auth_failed'"
    ).fetchall()
    print("last", last)
    print("complete_events_since_execute", completes)
    print("auth_failed", auth)
    print("total_completed", len(seen), "/", sum(universe.values()))
    for cls in ("equity_depth", "equity_quote", "index", "futures", "options"):
        done = by_class.get(cls, [])
        n_u = universe.get(cls, 0)
        with_min = sum(1 for r in done if r["minute_rows"] > 0)
        zero = [r["symbol"] for r in done if r["zero_data"]]
        failed = sum(r["failed_chunks"] for r in done)
        status = (
            "complete"
            if n_u > 0 and len(done) >= n_u
            else ("partial" if done else "not_started")
        )
        print(
            json.dumps(
                {
                    "class": cls,
                    "status": status,
                    "universe": n_u,
                    "attempted": len(done),
                    "remaining": max(0, n_u - len(done)),
                    "with_minute_rows": with_min,
                    "zero_data": zero,
                    "failed_chunks_n": failed,
                    "error_symbols": [r["symbol"] for r in done if r["errors"]],
                },
                default=str,
            )
        )
    if by_class.get("options"):
        print("options_attempted_sample:")
        for r in by_class["options"][:8]:
            print(" ", r["symbol"], "min", r["minute_rows"], "zero", r["zero_data"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
