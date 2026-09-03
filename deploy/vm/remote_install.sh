#!/bin/bash
set -euo pipefail
cd /home/nse/nse-signal-pipeline
echo "WHOAMI=$(whoami)"
echo "HOST=$(hostname)"
.venv/bin/pip install -e ".[dev]" -q
echo PIP_OK
PYTHONPATH=src .venv/bin/python -c 'from nse_pipeline.api.app import create_app; print("API_IMPORT_OK")'
sed -i 's/\r$//' deploy/vm/install_runtime_units.sh deploy/vm/*.service || true
chmod +x deploy/vm/install_runtime_units.sh
bash deploy/vm/install_runtime_units.sh
echo "=== status ==="
systemctl status nse-api nse-live-signals nse-account-capture --no-pager -l || true
echo "=== ingest process (must remain the existing one) ==="
pgrep -af '01_run_ingestion' || echo 'NO_INGEST_PROCESS'
