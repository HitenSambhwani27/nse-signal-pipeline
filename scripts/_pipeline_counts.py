import sqlite3
from nse_pipeline.config import load_settings

s = load_settings()
c = sqlite3.connect(s.paths.sqlite_db)
print("feature_by_track")
for row in c.execute(
    "select track, count(*), min(trade_date), max(trade_date) from feature_log group by track"
):
    print(" ", row)
print("label_audit", c.execute("select count(*), min(trade_date), max(trade_date) from label_audit").fetchone())
print("last_backfill", c.execute(
    "select timestamp, symbol from ingestion_meta where event_type='historical_backfill_symbol' order by id desc limit 1"
).fetchone())
print("complete", c.execute(
    "select timestamp from ingestion_meta where event_type='historical_backfill_complete' and timestamp>='2026-08-14T11:42' "
).fetchall())
print("auth", c.execute("select count(*) from ingestion_meta where event_type='historical_backfill_auth_failed'").fetchone())
