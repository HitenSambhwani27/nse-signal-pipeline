#!/bin/bash
set -euo pipefail
echo === is-active ===
systemctl is-active nse-api nse-live-signals nse-account-capture
echo === is-enabled ===
systemctl is-enabled nse-api nse-live-signals nse-account-capture
echo === journal last 40 ===
journalctl -u nse-api -u nse-live-signals -u nse-account-capture -n 40 --no-pager || true
echo === local-api-health ===
curl -sS -m 15 http://127.0.0.1:8080/api/v1/health
echo
echo === MATURITY_ONLY ===
cd /home/nse/nse-signal-pipeline
PYTHONPATH=src .venv/bin/python scripts/10_run_live_signals.py --maturity-only
