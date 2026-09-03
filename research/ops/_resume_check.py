from pathlib import Path
import sys
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / 'src'))

import sqlite3
from nse_pipeline.config import load_settings

s = load_settings()
c = sqlite3.connect(s.paths.sqlite_db)
print("latest_starts:")
for row in c.execute(
    "select timestamp, substr(details_json,1,180) from ingestion_meta "
    "where event_type='historical_backfill_start' order by id desc limit 2"
):
    print(" ", row)
print("symbols_since_resume:")
for row in c.execute(
    "select timestamp, symbol, rows_written from ingestion_meta "
    "where event_type='historical_backfill_symbol' "
    "and timestamp>='2026-08-15T10:30:00' order by id"
):
    print(" ", row)
print("auth_failed:")
for row in c.execute(
    "select timestamp, symbol, message from ingestion_meta "
    "where event_type='historical_backfill_auth_failed'"
):
    print(" ", row)
print("errors_today:")
for row in c.execute(
    "select timestamp, symbol, message from ingestion_meta "
    "where event_type='historical_backfill_error' and timestamp>='2026-08-15'"
):
    print(" ", row)
