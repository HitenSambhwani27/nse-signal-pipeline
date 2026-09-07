#!/usr/bin/env bash
# Deploy application code from this Windows checkout to the production VM.
#
# From the repository root:
#   ./deploy/deploy.sh
#
# Requires bash + rsync + ssh (Git Bash, MSYS2, or WSL).
# Does NOT restart live market-data ingestion / the Kite WebSocket.
# Does NOT use rsync --delete.
# Does NOT copy local data/, logs/, .venv/, or .env to the VM.

set -euo pipefail

VM_HOST="${VM_HOST:-168.144.66.235}"
VM_USER="${VM_USER:-nse}"
REMOTE_DIR="${REMOTE_DIR:-/home/nse/nse-signal-pipeline}"
SSH_KEY="${SSH_KEY:-${HOME}/.ssh/id_ed25519_do_nse}"

RESTART_SERVICES=(nse-api nse-live-signals nse-account-capture)
API_BASE="http://127.0.0.1:8080"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"

TMP_KEY=""
FAILED=0

cleanup() {
  if [[ -n "${TMP_KEY}" && -f "${TMP_KEY}" ]]; then
    rm -f "${TMP_KEY}"
  fi
}

on_error() {
  FAILED=1
  echo
  echo "DEPLOYMENT FAILED"
}

die() {
  echo "ERROR: $*" >&2
  echo
  echo "DEPLOYMENT FAILED"
  exit 1
}

trap on_error ERR
trap cleanup EXIT

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $1" >&2
    echo "Run this script from Git Bash, MSYS2, or WSL where rsync and ssh exist." >&2
    return 1
  }
}

resolve_ssh_key() {
  local candidates=(
    "${SSH_KEY}"
    "${HOME}/.ssh/id_ed25519_do_nse"
    "/c/Users/${USER:-sambh}/.ssh/id_ed25519_do_nse"
    "/mnt/c/Users/${USER:-sambh}/.ssh/id_ed25519_do_nse"
  )
  if [[ -n "${USERPROFILE:-}" ]]; then
    local drive rest
    drive="$(echo "${USERPROFILE}" | cut -c1 | tr 'A-Z' 'a-z')"
    rest="$(echo "${USERPROFILE}" | cut -c4- | tr '\\' '/')"
    candidates+=("/${drive}/${rest}/.ssh/id_ed25519_do_nse")
    candidates+=("/mnt/${drive}/${rest}/.ssh/id_ed25519_do_nse")
  fi
  local key
  for key in "${candidates[@]}"; do
    [[ -n "${key}" && -f "${key}" ]] || continue
    echo "${key}"
    return 0
  done
  echo "ERROR: SSH key not found. Set SSH_KEY or place id_ed25519_do_nse in ~/.ssh/" >&2
  return 1
}

prepare_key() {
  local src="$1"
  # NTFS/Drvfs mounts often ignore chmod 600; OpenSSH then refuses the key.
  if [[ "${src}" == /mnt/* || "${src}" == /c/* || "${src}" == /d/* ]]; then
    TMP_KEY="$(mktemp)"
    cp "${src}" "${TMP_KEY}"
    chmod 600 "${TMP_KEY}"
    echo "${TMP_KEY}"
    return 0
  fi
  chmod 600 "${src}" 2>/dev/null || true
  echo "${src}"
}

ssh_cmd() {
  ssh -i "${KEY_FILE}" \
    -o BatchMode=yes \
    -o IdentitiesOnly=yes \
    -o ConnectTimeout=20 \
    "${VM_USER}@${VM_HOST}" \
    "$@"
}

http_code() {
  local url="$1"
  ssh_cmd "curl -sS -o /tmp/nse_deploy_body.json -w '%{http_code}' --max-time 90 '${url}'"
}

echo "============================================================"
echo "NSE pipeline code deployment (ingest will NOT be restarted)"
echo "============================================================"
echo "Local repo : ${LOCAL_REPO}"
echo "Target     : ${VM_USER}@${VM_HOST}:${REMOTE_DIR}"
echo

need_cmd ssh
need_cmd rsync
KEY_SRC="$(resolve_ssh_key)"
KEY_FILE="$(prepare_key "${KEY_SRC}")"
echo "SSH key    : ${KEY_SRC}"
echo

# ------------------------------------------------------------
echo "[1/7] Inspecting deployment target"

ssh_cmd "test -d '${REMOTE_DIR}'"
ssh_cmd "test -x '${REMOTE_DIR}/.venv/bin/python'"

INGEST_BEFORE="$(ssh_cmd "pgrep -af '[p]ython.*01_run_ingestion' || true")"
echo "Ingest before deploy:"
echo "${INGEST_BEFORE:-<none>}"
INGEST_PIDS_BEFORE="$(ssh_cmd "pgrep -f '[p]ython.*01_run_ingestion' | tr '\n' ' ' || true")"

echo "Service status before deploy:"
ssh_cmd "systemctl is-active nse-api nse-live-signals nse-account-capture || true"
ssh_cmd "systemctl is-active nse-ingest 2>/dev/null || echo 'nse-ingest: not a systemd unit (expected)'"

# ------------------------------------------------------------
echo
echo "[2/7] Creating source backup (no data/ logs/ .venv/ .env)"

BACKUP_DIR="$(ssh_cmd 'date +%Y%m%d_%H%M%S')"
ssh_cmd "set -euo pipefail
DEST=\"\${HOME}/deploy_backups/${BACKUP_DIR}\"
mkdir -p \"\${DEST}\"
cd '${REMOTE_DIR}'
for item in src scripts config deploy tests docs research pyproject.toml README.md PROJECT_PLAN.md MASTER_REFERENCE.md TRADE_OFFS.md .env.example; do
  if [ -e \"\${item}\" ]; then
    mkdir -p \"\${DEST}/\$(dirname \"\${item}\")\"
    rsync -a --exclude .env --exclude instruments_cache.json --exclude __pycache__ --exclude .pytest_cache \"\${item}\" \"\${DEST}/\$(dirname \"\${item}\")/\"
  fi
done
echo \"\${DEST}\"
du -sh \"\${DEST}\"
"
echo "Backup directory: ${VM_USER}@${VM_HOST}:~/deploy_backups/${BACKUP_DIR}"

# ------------------------------------------------------------
echo
echo "[3/7] Syncing application code (no --delete)"

rsync -avz --progress \
  --exclude .venv \
  --exclude data \
  --exclude logs \
  --exclude .env \
  --exclude .git \
  --exclude __pycache__ \
  --exclude .pytest_cache \
  --exclude config/instruments_cache.json \
  --exclude .egg-info \
  --exclude '*.egg-info' \
  --exclude dist \
  --exclude build \
  --exclude .vscode \
  --exclude .idea \
  --exclude .cursor \
  --exclude .mypy_cache \
  --exclude .ruff_cache \
  -e "ssh -i ${KEY_FILE} -o BatchMode=yes -o IdentitiesOnly=yes -o ConnectTimeout=20" \
  "${LOCAL_REPO}/" \
  "${VM_USER}@${VM_HOST}:${REMOTE_DIR}/"

ssh_cmd "set -euo pipefail
cd '${REMOTE_DIR}'
find deploy scripts research/ops -type f \\( -name '*.sh' -o -name '*.service' -o -name '*.timer' \\) -print0 2>/dev/null \
  | xargs -0 -r sed -i 's/\r$//' || true
"

# ------------------------------------------------------------
echo
echo "[4/7] Restarting application services (NOT ingest)"

ssh_cmd "set -euo pipefail
for svc in nse-api nse-live-signals nse-account-capture; do
  if ! systemctl cat \"\${svc}.service\" >/dev/null 2>&1; then
    echo \"ERROR: systemd unit missing: \${svc}.service\" >&2
    exit 1
  fi
done
sudo systemctl restart nse-api nse-live-signals nse-account-capture
for i in \$(seq 1 20); do
  if curl -sS -o /dev/null --max-time 2 http://127.0.0.1:8080/api/v1/health; then
    echo \"nse-api listening after \${i}s\"
    break
  fi
  if [ \"\${i}\" -eq 20 ]; then
    echo \"ERROR: nse-api did not accept connections within 20s\" >&2
    systemctl --no-pager --full status nse-api || true
    exit 1
  fi
  sleep 1
done
for svc in nse-api nse-live-signals nse-account-capture; do
  state=\$(systemctl is-active \"\${svc}\")
  echo \"\${svc}: \${state}\"
  if [ \"\${state}\" != active ]; then
    systemctl --no-pager --full status \"\${svc}\" || true
    echo \"ERROR: \${svc} failed to become active\" >&2
    exit 1
  fi
done
echo 'ingest unit (must remain untouched):'
systemctl is-active nse-ingest 2>/dev/null || echo 'nse-ingest: not a systemd unit (expected)'
"

# ------------------------------------------------------------
echo
echo "[5/7] Verifying API"

HEALTH_CODE="$(http_code "${API_BASE}/api/v1/health")"
echo "GET /api/v1/health -> HTTP ${HEALTH_CODE}"
if [[ "${HEALTH_CODE}" != "200" ]]; then
  ssh_cmd "systemctl --no-pager --full status nse-api || true"
  die "health endpoint did not return HTTP 200"
fi

API_PROBES=(
  "/api/v1/quotes/NIFTY%2050"
  "/api/v1/options/NIFTY"
  "/api/v1/options/BANKNIFTY"
  "/api/v1/futures/NIFTY"
  "/api/v1/futures/BANKNIFTY"
  "/api/v1/market-activity/NIFTY%2050"
  "/api/v1/unusual-activity"
  "/api/v1/watchlists"
  "/api/v1/watchlists/quotes"
  "/api/v1/charts/NIFTY%2050"
  "/api/v1/cross-market/NIFTY"
)

for path in "${API_PROBES[@]}"; do
  code="$(http_code "${API_BASE}${path}")"
  echo "GET ${path} -> HTTP ${code}"
  if [[ "${code}" != "200" ]]; then
    die "${path} returned HTTP ${code}"
  fi
done

echo
echo "OpenAPI registered paths:"
ssh_cmd "python3 -" <<'PY'
import json
import sys
import urllib.request

required = [
    "/api/v1/health",
    "/api/v1/quotes/{symbol}",
    "/api/v1/options/{underlying}",
    "/api/v1/options/{underlying}/{expiry}",
    "/api/v1/options/{underlying}/{expiry}/oi",
    "/api/v1/options/{underlying}/{expiry}/activity",
    "/api/v1/futures/{underlying}",
    "/api/v1/market-activity/{symbol}",
    "/api/v1/unusual-activity",
    "/api/v1/watchlists",
    "/api/v1/watchlists/quotes",
    "/api/v1/charts/{symbol}",
    "/api/v1/cross-market/{underlying}",
]
spec = json.load(urllib.request.urlopen("http://127.0.0.1:8080/openapi.json", timeout=15))
paths = sorted(spec.get("paths", {}))
for path in paths:
    print(f"  {path}")
missing = [p for p in required if p not in paths]
if missing:
    print("MISSING required Run 2/2D paths:", file=sys.stderr)
    for path in missing:
        print(f"  {path}", file=sys.stderr)
    sys.exit(1)
print("openapi_required_paths_ok")
PY

# ------------------------------------------------------------
echo
echo "[6/7] Running tests"

ssh_cmd "set -euo pipefail
cd '${REMOTE_DIR}'
if [ ! -x .venv/bin/pytest ]; then
  echo 'ERROR: .venv/bin/pytest missing' >&2
  exit 1
fi
PYTHONPATH=. .venv/bin/python -m pytest -q --tb=short
"

# ------------------------------------------------------------
echo
echo "[7/7] Deployment verification"

ssh_cmd "set -euo pipefail
cd '${REMOTE_DIR}'
echo '=== production-only paths (must still exist) ==='
for p in data logs .env .venv; do
  if [ -e \"\${p}\" ]; then
    ls -ld \"\${p}\"
  else
    echo \"ERROR: missing production path: \${p}\" >&2
    exit 1
  fi
done
echo '=== sqlite / cache names only ==='
ls -ld data/nse_pipeline.db data/nse_pipeline.db-wal data/nse_pipeline.db-shm 2>/dev/null || true
ls -ld config/instruments_cache.json 2>/dev/null || echo 'instruments_cache.json: absent'
echo '=== ingest process after deploy ==='
pgrep -af '[p]ython.*01_run_ingestion' || echo 'NO_INGEST_PROCESS'
"

INGEST_AFTER="$(ssh_cmd "pgrep -af '[p]ython.*01_run_ingestion' || true")"
INGEST_PIDS_AFTER="$(ssh_cmd "pgrep -f '[p]ython.*01_run_ingestion' | tr '\n' ' ' || true")"
echo "Ingest after deploy:"
echo "${INGEST_AFTER:-<none>}"

if [[ -n "${INGEST_PIDS_BEFORE// /}" && "${INGEST_PIDS_BEFORE}" != "${INGEST_PIDS_AFTER}" ]]; then
  echo "  before: ${INGEST_PIDS_BEFORE}" >&2
  echo "  after:  ${INGEST_PIDS_AFTER}" >&2
  die "ingest PIDs changed during deploy"
fi
echo "Ingest PIDs unchanged: ${INGEST_PIDS_BEFORE:-<none>}"

echo
echo "DEPLOYMENT SUCCESSFUL"
echo "Backup: ~/deploy_backups/${BACKUP_DIR}"
echo "Live ingestion / Kite WebSocket was NOT restarted."
echo "New stock F&O universe / FULL subscriptions activate only on the planned ingest restart."
