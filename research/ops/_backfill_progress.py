from pathlib import Path
import sys
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / 'src'))

import json
import sqlite3
from nse_pipeline.config import load_settings

s = load_settings()
con = sqlite3.connect(s.paths.sqlite_db)
n = con.execute(
    "select count(*) from ingestion_meta where event_type='historical_backfill_symbol'"
).fetchone()[0]
err = con.execute(
    "select count(*) from ingestion_meta where event_type='historical_backfill_error'"
).fetchone()[0]
last = con.execute(
    "select timestamp, symbol, rows_written from ingestion_meta "
    "where event_type='historical_backfill_symbol' order by id desc limit 1"
).fetchone()
print("done_symbols", n, "errors", err, "last", last)
failed = 0
for details, symbol in con.execute(
    "select details_json, symbol from ingestion_meta "
    "where event_type='historical_backfill_symbol'"
):
    if not details:
        continue
    payload = json.loads(details) if isinstance(details, str) else details
    chunks = payload.get("failed_chunks") or []
    if chunks:
        failed += len(chunks)
        print("failed_chunks", symbol, chunks)
print("failed_chunks_total", failed)
