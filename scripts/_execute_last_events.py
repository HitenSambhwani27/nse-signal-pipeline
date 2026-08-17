"""Last execute-run events from sqlite."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
from nse_pipeline.config import load_settings

s = load_settings()
con = sqlite3.connect(s.paths.sqlite_db)
print("event_types since execute:")
for row in con.execute(
    "select event_type, count(*) from ingestion_meta "
    "where timestamp >= '2026-08-14T11:42:00' group by 1 order by 2 desc"
):
    print(" ", row)
print("last 8 events:")
for row in con.execute(
    "select timestamp, event_type, symbol, rows_written, substr(message,1,120) "
    "from ingestion_meta where timestamp >= '2026-08-14T11:42:00' "
    "order by id desc limit 8"
):
    print(" ", row)
print("errors:")
for row in con.execute(
    "select timestamp, symbol, message from ingestion_meta "
    "where event_type='historical_backfill_error' order by id desc limit 10"
):
    print(" ", row)
print("complete:")
for row in con.execute(
    "select timestamp, substr(details_json,1,200) from ingestion_meta "
    "where event_type='historical_backfill_complete' order by id desc limit 3"
):
    print(" ", row)
last = con.execute(
    "select timestamp, symbol, details_json from ingestion_meta "
    "where event_type='historical_backfill_symbol' "
    "and timestamp >= '2026-08-14T11:42:00' order by id desc limit 1"
).fetchone()
if last:
    payload = json.loads(last[2]) if last[2] else {}
    print("last_symbol", last[0], last[1], {
        k: payload.get(k)
        for k in ("class", "daily_rows", "minute_rows", "zero_data", "errors", "failed_chunks", "minute_range")
    })
