"""Post-session state: backfill + feature/label/signal coverage."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings
from nse_pipeline.historical.backfill import load_completed_backfill_keys
from nse_pipeline.historical.universe import flatten_universe
from nse_pipeline.storage.sqlite_store import SQLiteStore


def main() -> int:
    settings = load_settings()
    pairs = flatten_universe(settings)
    store = SQLiteStore(settings.paths.sqlite_db)
    keys = load_completed_backfill_keys(store)
    remaining = [
        (cls, info.tradingsymbol, info.expiry)
        for cls, info in pairs
        if (cls, info.tradingsymbol, info.expiry or "") not in keys
    ]
    con = sqlite3.connect(settings.paths.sqlite_db)

    last = con.execute(
        "select timestamp, symbol, rows_written, details_json from ingestion_meta "
        "where event_type='historical_backfill_symbol' order by id desc limit 1"
    ).fetchone()
    last_payload = json.loads(last[3]) if last and last[3] else {}
    complete = con.execute(
        "select timestamp, substr(details_json,1,400) from ingestion_meta "
        "where event_type='historical_backfill_complete' "
        "and timestamp >= '2026-08-14T11:42' order by id desc limit 1"
    ).fetchone()
    print("=== BACKFILL ===")
    print("completed_keys", len(keys), "universe", len(pairs), "remaining", remaining)
    print(
        "last_symbol",
        last[0] if last else None,
        last[1] if last else None,
        last[2] if last else None,
        {
            k: last_payload.get(k)
            for k in (
                "class",
                "minute_rows",
                "daily_rows",
                "zero_data",
                "errors",
                "failed_chunks",
                "minute_range",
            )
        },
    )
    print("complete_event", complete[0] if complete else None)
    last_json = settings.paths.logs_dir / "historical_backfill_last.json"
    print("last_json_exists", last_json.exists(), last_json)

    print("\n=== FEATURE_LOG ===")
    for row in con.execute(
        "select track, count(*), min(trade_date), max(trade_date), "
        "count(distinct trade_date), count(distinct symbol) "
        "from feature_log group by track"
    ):
        print(" ", row)
    print("feature dates depth", [
        r[0]
        for r in con.execute(
            "select distinct trade_date from feature_log where track='equity_depth' order by 1"
        )
    ][:3], "... n=",
        con.execute("select count(distinct trade_date) from feature_log where track='equity_depth'").fetchone()[0],
    )
    print("feature dates quote hist-range", con.execute(
        "select min(trade_date), max(trade_date), count(distinct trade_date), count(*) "
        "from feature_log where track='equity_quote' and trade_date>='2025-08-15'"
    ).fetchone())
    print("feature dates options", con.execute(
        "select min(trade_date), max(trade_date), count(distinct trade_date), count(*), count(distinct symbol) "
        "from feature_log where track='options'"
    ).fetchone())
    print("feature dates futures", con.execute(
        "select min(trade_date), max(trade_date), count(distinct trade_date), count(*), count(distinct symbol) "
        "from feature_log where track='futures'"
    ).fetchone())

    print("\n=== LABEL_AUDIT ===")
    for row in con.execute(
        "select track, mode, count(*), min(trade_date), max(trade_date), count(distinct trade_date) "
        "from label_audit group by track, mode"
    ):
        print(" ", row)

    print("\n=== FEATURE OUTCOMES ===")
    for row in con.execute(
        "select track, count(*), sum(case when actual_outcome is not null then 1 else 0 end) "
        "from feature_log group by track"
    ):
        print(" labeled", row)

    print("\n=== SIGNAL_LOG ===")
    for row in con.execute(
        "select track, count(*), min(trade_date), max(trade_date), count(distinct trade_date) "
        "from signal_log group by track"
    ):
        print(" ", row)
    if not con.execute("select count(*) from signal_log").fetchone()[0]:
        print("  empty")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
