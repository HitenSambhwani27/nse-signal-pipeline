#!/bin/bash
set -euo pipefail
# Resolve today's IST calendar date for after-close jobs.
ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"
D=$(TZ=Asia/Kolkata date +%F)
export PYTHONPATH=src
case "${1:-}" in
  compact)
    exec "$ROOT/.venv/bin/python" scripts/03_run_compaction.py --date "$D" --verify
    ;;
  features)
    exec "$ROOT/.venv/bin/python" scripts/04_run_features.py --date "$D"
    ;;
  labels)
    exec "$ROOT/.venv/bin/python" scripts/05_run_labels.py --date "$D"
    ;;
  *)
    echo "usage: $0 compact|features|labels" >&2
    exit 2
    ;;
esac
