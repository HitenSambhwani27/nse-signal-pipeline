#!/bin/bash
set -euo pipefail
# Install API + live-signal + account-capture units. Does NOT touch ingest.

ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"

sudo cp "$ROOT/deploy/vm/nse-api.service" /etc/systemd/system/nse-api.service
sudo cp "$ROOT/deploy/vm/nse-live-signals.service" /etc/systemd/system/nse-live-signals.service
sudo cp "$ROOT/deploy/vm/nse-account-capture.service" /etc/systemd/system/nse-account-capture.service
sudo systemctl daemon-reload
sudo systemctl enable nse-api nse-live-signals nse-account-capture
sudo systemctl restart nse-api nse-live-signals nse-account-capture
sleep 2
echo "=== is-active ==="
systemctl is-active nse-api nse-live-signals nse-account-capture || true
echo "=== enabled ==="
systemctl is-enabled nse-api nse-live-signals nse-account-capture || true
echo "=== ingest not started by this script ==="
systemctl is-active nse-ingest 2>/dev/null || echo "nse-ingest: not a systemd unit (expected)"
