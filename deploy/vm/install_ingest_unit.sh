#!/bin/bash
set -euo pipefail
# Install the ingest unit file. Does NOT start it. Does NOT enable it while a
# manual ingest process may be running — that would risk two Kite WebSockets
# after reboot. Cut over with cutover_ingest_to_systemd.sh.

ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"
sed -i 's/\r$//' "$ROOT/deploy/vm/nse-ingest.service" || true
sudo cp "$ROOT/deploy/vm/nse-ingest.service" /etc/systemd/system/nse-ingest.service
sudo systemctl daemon-reload
echo "nse-ingest unit installed (not started, not enabled)."
systemctl is-enabled nse-ingest 2>/dev/null || echo "nse-ingest: disabled (expected until cutover)"
systemctl is-active nse-ingest 2>/dev/null || echo "nse-ingest: inactive (expected until cutover)"
echo "manual ingest PIDs:"
pgrep -af '[p]ython.*01_run_ingestion' || echo "NO_MANUAL_INGEST"
