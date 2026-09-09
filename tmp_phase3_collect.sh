#!/bin/bash
set -euo pipefail
echo "=== idle ==="
uptime
ps -p 143158 -o pid,etime,pcpu,cmd --no-headers || echo ingest_gone
pgrep -af 03_run_compaction || echo no_compact
echo "=== log errors ==="
if grep -E "ERROR|Traceback|ValueError|materialise failed|Bars skip" /tmp/nse_compact_20260908.log; then
  echo HAD_MATCHES
else
  echo NO_ERRORS
fi
echo "=== counts ==="
echo -n raw:; ls /home/nse/nse-signal-pipeline/data/raw/2026-09-08 | wc -l
echo -n compacted:; ls /home/nse/nse-signal-pipeline/data/compacted/2026-09-08 | wc -l
echo -n bars1m:; ls /home/nse/nse-signal-pipeline/data/bars/2026-09-08 | wc -l
echo -n daily:; ls /home/nse/nse-signal-pipeline/data/bars/daily | wc -l
echo "=== api ==="
curl -s -o /dev/null -w "health:%{http_code}\n" http://127.0.0.1:8080/api/v1/health || true
echo "=== accept ==="
cd /home/nse/nse-signal-pipeline
PYTHONPATH=src .venv/bin/python /tmp/tmp_phase3_accept.py
echo "=== perf ==="
PYTHONPATH=src .venv/bin/python /tmp/tmp_phase3_perf.py
echo "=== collect done ==="
