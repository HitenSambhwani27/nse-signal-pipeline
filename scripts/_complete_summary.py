import json
import sqlite3
from nse_pipeline.config import load_settings

s = load_settings()
c = sqlite3.connect(s.paths.sqlite_db)
row = c.execute(
    "select timestamp, details_json from ingestion_meta "
    "where event_type='historical_backfill_complete' order by id desc limit 1"
).fetchone()
print("complete_ts", row[0])
details = json.loads(row[1])
print(json.dumps({k: details.get(k) for k in ("requested_equity_window", "by_class", "options_nifty_weekly", "options_banknifty_monthly", "options_strike_retrieval_pct", "skipped_completed")}, indent=2, default=str))
print("futures:")
for r in c.execute(
    "select symbol, details_json from ingestion_meta "
    "where event_type='historical_backfill_symbol' and timestamp>='2026-08-14T11:42' "
    "order by id"
):
    p = json.loads(r[1] or "{}")
    if p.get("class") == "futures":
        print(" ", r[0], "min", p.get("minute_rows"), "daily", p.get("daily_rows"), "range", p.get("minute_range"), "zero", p.get("zero_data"))
opt_zero = 0
opt_min = 0
for r in c.execute(
    "select details_json from ingestion_meta "
    "where event_type='historical_backfill_symbol' and timestamp>='2026-08-14T11:42'"
):
    p = json.loads(r[0] or "{}")
    if p.get("class") != "options":
        continue
    if p.get("zero_data"):
        opt_zero += 1
    if int(p.get("minute_rows") or 0) > 0:
        opt_min += 1
print("options with_minute", opt_min, "zero_data", opt_zero)
