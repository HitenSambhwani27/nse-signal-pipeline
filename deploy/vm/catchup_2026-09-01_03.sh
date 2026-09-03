#!/bin/bash
set -euo pipefail
ROOT=/home/nse/nse-signal-pipeline
cd "$ROOT"
mkdir -p "$ROOT/logs"
LOG="$ROOT/logs/catchup_2026-09-01_03.log"
export PYTHONPATH=src
{
  echo "CATCHUP_START $(date -Is)"
  for d in 2026-09-01 2026-09-02 2026-09-03; do
    echo "=== COMPACT $d ==="
    "$ROOT/.venv/bin/python" scripts/03_run_compaction.py --date "$d" --verify
  done
  echo "=== FEATURES 2026-09-01..2026-09-03 ==="
  "$ROOT/.venv/bin/python" scripts/04_run_features.py --start-date 2026-09-01 --end-date 2026-09-03 --workers 1
  echo "=== LABELS 2026-09-01..2026-09-03 ==="
  "$ROOT/.venv/bin/python" scripts/05_run_labels.py --start-date 2026-09-01 --end-date 2026-09-03
  echo "CATCHUP_DONE $(date -Is)"
} 2>&1 | tee -a "$LOG"
