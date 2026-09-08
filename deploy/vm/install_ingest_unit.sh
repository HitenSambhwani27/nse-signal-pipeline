#!/bin/bash
set -euo pipefail
# Install the ingest unit and weekday start timer. Does NOT start ingest.
# Does NOT enable the service while a manual ingest process may be running.
# Cut over with cutover_ingest_to_systemd.sh.

ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"
sed -i 's/\r$//' "$ROOT/deploy/vm/nse-ingest.service" "$ROOT/deploy/vm/nse-ingest.timer" || true
sudo cp "$ROOT/deploy/vm/nse-ingest.service" /etc/systemd/system/nse-ingest.service
sudo cp "$ROOT/deploy/vm/nse-ingest.timer" /etc/systemd/system/nse-ingest.timer
sudo chmod 644 /etc/systemd/system/nse-ingest.service /etc/systemd/system/nse-ingest.timer
sudo systemctl daemon-reload
echo "nse-ingest unit+timer installed (ingest process not started)."
if systemctl is-enabled --quiet nse-ingest 2>/dev/null; then
  sudo systemctl enable nse-ingest.timer
  sudo systemctl start nse-ingest.timer
  echo "nse-ingest.timer enabled and started (Persistent=false; will not catch up today's 08:45)."
else
  echo "nse-ingest.service is not enabled; timer left disabled until cutover."
fi
systemctl is-enabled nse-ingest 2>/dev/null || echo "nse-ingest: disabled (expected until cutover)"
systemctl is-active nse-ingest 2>/dev/null || echo "nse-ingest: inactive (expected until cutover)"
systemctl is-enabled nse-ingest.timer 2>/dev/null || echo "nse-ingest.timer: not enabled"
systemctl list-timers nse-ingest.timer --no-pager || true
echo "manual ingest PIDs:"
pgrep -af '[p]ython.*01_run_ingestion' || echo "NO_MANUAL_INGEST"
