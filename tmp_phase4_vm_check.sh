#!/bin/bash
set -euo pipefail
echo "=== processes ==="
pgrep -af "01_run_ingestion|14_run_ui_api|nse-api|nse-ingest" || true
echo "=== systemd ==="
systemctl is-active nse-ingest nse-api nse-live-signals 2>/dev/null || true
systemctl show nse-ingest -p MainPID -p NRestarts --no-pager
echo "=== ingest count ==="
pgrep -c -f "scripts/01_run_ingestion.py" || true
echo "=== load ==="
uptime
echo "=== health ==="
curl -sS http://127.0.0.1:8080/api/v1/health | python3 -c "import json,sys; h=json.load(sys.stdin); print({k:h.get(k) for k in ('as_of','data_state')}); print((h.get('health') or {}).get('stream')); print('busy', (h.get('health') or {}).get('sqlite_busy_retries'))"
