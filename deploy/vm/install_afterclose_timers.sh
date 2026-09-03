#!/bin/bash
set -euo pipefail
ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"
sed -i 's/\r$//' "$ROOT/deploy/vm/run_afterclose_today.sh" \
  "$ROOT/deploy/vm/nse-compact.service" \
  "$ROOT/deploy/vm/nse-compact.timer" \
  "$ROOT/deploy/vm/nse-features.service" \
  "$ROOT/deploy/vm/nse-features.timer" \
  "$ROOT/deploy/vm/nse-labels.service" \
  "$ROOT/deploy/vm/nse-labels.timer" || true
chmod +x "$ROOT/deploy/vm/run_afterclose_today.sh"

sudo cp "$ROOT/deploy/vm/nse-compact.service" /etc/systemd/system/nse-compact.service
sudo cp "$ROOT/deploy/vm/nse-compact.timer" /etc/systemd/system/nse-compact.timer
sudo cp "$ROOT/deploy/vm/nse-features.service" /etc/systemd/system/nse-features.service
sudo cp "$ROOT/deploy/vm/nse-features.timer" /etc/systemd/system/nse-features.timer
sudo cp "$ROOT/deploy/vm/nse-labels.service" /etc/systemd/system/nse-labels.service
sudo cp "$ROOT/deploy/vm/nse-labels.timer" /etc/systemd/system/nse-labels.timer
sudo systemctl daemon-reload
sudo systemctl enable --now nse-compact.timer nse-features.timer nse-labels.timer

echo "=== is-enabled timers ==="
systemctl is-enabled nse-compact.timer nse-features.timer nse-labels.timer
echo "=== is-active timers ==="
systemctl is-active nse-compact.timer nse-features.timer nse-labels.timer
echo "=== list-timers ==="
systemctl list-timers nse-compact.timer nse-features.timer nse-labels.timer --no-pager
echo "=== status ==="
systemctl status nse-compact.timer nse-features.timer nse-labels.timer --no-pager -l
echo "=== timedatectl ==="
timedatectl | head -6
