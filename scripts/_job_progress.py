"""Progress of in-flight feature/label jobs vs expected coverage."""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings


def main() -> int:
    settings = load_settings()
    con = sqlite3.connect(s := settings.paths.sqlite_db)
    con = sqlite3.connect(s)
    print("now_utc", datetime.now(timezone.utc).isoformat())
    print("\nFEATURE_LOG")
    for row in con.execute(
        "select track, count(*), min(trade_date), max(trade_date), "
        "count(distinct trade_date), count(distinct symbol) "
        "from feature_log group by track"
    ):
        print(" ", row)
    print("quote dates:")
    for row in con.execute(
        "select trade_date, count(distinct symbol), count(*) from feature_log "
        "where track='equity_quote' and trade_date>='2025-08-15' "
        "group by trade_date order by trade_date"
    ):
        print(" ", row)

    print("\nLABEL_AUDIT TBM")
    for row in con.execute(
        "select track, count(*), min(trade_date), max(trade_date), count(distinct trade_date) "
        "from label_audit where mode='triple_barrier' group by track"
    ):
        print(" ", row)
    print("depth label dates last 5:")
    for row in con.execute(
        "select trade_date, count(*) from label_audit "
        "where track='equity_depth' and mode='triple_barrier' "
        "group by trade_date order by trade_date desc limit 5"
    ):
        print(" ", row)
    print("fo label dates last 5:")
    for row in con.execute(
        "select trade_date, track, count(*) from label_audit "
        "where track in ('options','futures') and mode='triple_barrier' "
        "group by trade_date, track order by trade_date desc, track limit 10"
    ):
        print(" ", row)

    print("\nFEATURE OUTCOMES")
    for row in con.execute(
        "select track, count(*), sum(case when actual_outcome is not null then 1 else 0 end) "
        "from feature_log group by track"
    ):
        print(" ", row)

    print("\nSIGNAL_LOG")
    for row in con.execute(
        "select track, count(*), min(trade_date), max(trade_date), count(distinct trade_date) "
        "from signal_log group by track"
    ):
        print(" ", row)
    n = con.execute("select count(*) from signal_log").fetchone()[0]
    if not n:
        print("  empty")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
