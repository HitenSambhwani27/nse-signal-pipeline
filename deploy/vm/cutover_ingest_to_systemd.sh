#!/bin/bash
set -euo pipefail
# Safe cutover: one manual ingest -> one systemd nse-ingest.service.
# Run ONLY after market close (15:30 IST) or when ticks have already stopped.
# Never run this while a healthy live session is receiving ticks.

ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"

if systemctl is-active --quiet nse-ingest; then
  echo "nse-ingest.service is already active. Refusing to start a second process."
  systemctl status nse-ingest --no-pager -l || true
  pgrep -af '[p]ython.*01_run_ingestion' || true
  exit 1
fi

PIDS="$(pgrep -f '[p]ython.*01_run_ingestion' || true)"
COUNT="$(printf '%s\n' "$PIDS" | grep -c . || true)"
echo "manual ingest PIDs: ${PIDS:-<none>} (count=${COUNT})"

if [[ "${COUNT}" -gt 1 ]]; then
  echo "ERROR: more than one ingest process is already running. Resolve by hand." >&2
  exit 1
fi

if [[ "${COUNT}" -eq 1 ]]; then
  echo "Sending SIGTERM to ${PIDS} and waiting for clean shutdown..."
  kill -TERM ${PIDS}
  for i in $(seq 1 60); do
    if ! pgrep -f '[p]ython.*01_run_ingestion' >/dev/null; then
      echo "manual ingest exited after ${i}s"
      break
    fi
    sleep 1
  done
  if pgrep -f '[p]ython.*01_run_ingestion' >/dev/null; then
    echo "ERROR: manual ingest did not exit within 60s. Not starting systemd." >&2
    exit 1
  fi
fi

sudo cp "$ROOT/deploy/vm/nse-ingest.service" /etc/systemd/system/nse-ingest.service
sudo cp "$ROOT/deploy/vm/nse-ingest.timer" /etc/systemd/system/nse-ingest.timer
sudo chmod 644 /etc/systemd/system/nse-ingest.service /etc/systemd/system/nse-ingest.timer
sudo systemctl daemon-reload
sudo systemctl enable nse-ingest nse-ingest.timer
sudo systemctl start nse-ingest.timer
sudo systemctl start nse-ingest
sleep 2
systemctl is-active nse-ingest
systemctl is-enabled nse-ingest
systemctl is-enabled nse-ingest.timer
systemctl list-timers nse-ingest.timer --no-pager || true
echo "=== remaining python ingest processes (must be exactly one) ==="
pgrep -af '[p]ython.*01_run_ingestion' || echo "NO_INGEST_PROCESS"
COUNT_AFTER="$(pgrep -c -f '[p]ython.*01_run_ingestion' || true)"
if [[ "${COUNT_AFTER}" -ne 1 ]]; then
  echo "ERROR: expected exactly one ingest process, found ${COUNT_AFTER}" >&2
  exit 1
fi
echo "cutover complete"
