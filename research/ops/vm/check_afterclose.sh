#!/bin/bash
set -euo pipefail
echo "=== now ==="
date -u
date
echo "=== crontab nse ==="
crontab -l 2>/dev/null || echo "NO_USER_CRONTAB"
echo "=== crontab root ==="
sudo crontab -l 2>/dev/null || echo "NO_ROOT_CRONTAB"
echo "=== /etc/cron.d nse ==="
sudo ls -la /etc/cron.d 2>/dev/null | grep -i nse || echo "NO_ETC_CRON_D_NSE"
echo "=== systemd timers nse ==="
systemctl list-timers --all --no-pager 2>/dev/null | grep -i nse || echo "NO_NSE_TIMERS"
echo "=== compact/feature/label processes ==="
pgrep -af '03_run_compaction|04_run_features|05_run_labels' || echo "NO_AFTERCLOSE_PROCS"
echo "=== sqlite checks ==="
cd /home/nse/nse-signal-pipeline
PYTHONPATH=src .venv/bin/python - <<'PY'
from nse_pipeline.config import load_settings
from nse_pipeline.storage.sqlite_store import SQLiteStore
s = load_settings()
store = SQLiteStore(s.paths.sqlite_db)
print("db=", s.paths.sqlite_db)
print("exists=", s.paths.sqlite_db.exists())
h = store.pipeline_health()
print("last_ingestion=", h.get("last_ingestion_meta"))
print("last_compact=", h.get("last_compact"))
print("last_coverage=", h.get("last_coverage"))
print("last_feature_trade_date=", h.get("last_feature_trade_date"))
print("last_live_feature_trade_date=", h.get("last_live_feature_trade_date"))
print("processing_status=", h.get("processing_status"))
print("feature_dates=", store.list_feature_trade_dates()[-10:])
with store.connection() as conn:
    compact = conn.execute(
        "SELECT timestamp, event_type, message FROM ingestion_meta "
        "WHERE event_type LIKE '%compact%' OR message LIKE '%compact%' "
        "ORDER BY id DESC LIMIT 5"
    ).fetchall()
    print("compact_like_events=", [dict(r) for r in compact])
    cov = conn.execute(
        "SELECT timestamp, event_type FROM ingestion_meta "
        "WHERE event_type='session_coverage' ORDER BY id DESC LIMIT 3"
    ).fetchall()
    print("session_coverage=", [dict(r) for r in cov])
    nfeat = conn.execute("SELECT COUNT(*) FROM feature_log").fetchone()[0]
    nlive = conn.execute("SELECT COUNT(*) FROM feature_log WHERE source='live'").fetchone()[0]
    print("feature_log_rows=", nfeat, "live_rows=", nlive)
print("compacted_dir=", s.paths.compacted_dir, "exists=", s.paths.compacted_dir.exists())
from pathlib import Path
raw = s.paths.raw_dir
comp = s.paths.compacted_dir
print("raw_dates=", sorted(p.name for p in raw.glob("20*") if p.is_dir())[-8:] if raw.exists() else [])
print("compacted_dates=", sorted(p.name for p in comp.glob("20*") if p.is_dir())[-8:] if comp.exists() else [])
PY
