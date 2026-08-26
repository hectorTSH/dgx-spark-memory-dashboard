#!/bin/bash
# Watchdog for com.hamallar.spark-memory-dashboard (or generic label).
# Detects hung-but-listening servers (TCP accept + empty reply) and force-restarts.
set -euo pipefail

LABEL="${DGX_SMD_LAUNCH_LABEL:-com.hamallar.spark-memory-dashboard}"
URL="${DGX_SMD_HEALTH_URL:-http://127.0.0.1:7474/api/health}"
LOG_DIR="${HOME}/Library/Logs/spark-memory-dashboard"
LOG="${LOG_DIR}/watchdog.log"
UID_NUM="$(id -u)"
DOMAIN="gui/${UID_NUM}"

mkdir -p "${LOG_DIR}"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "$(ts) $*" >>"${LOG}"; }

body="$(mktemp)"
trap 'rm -f "${body}"' EXIT

http_code=0
curl_err=0
http_code="$(curl -sS -m 5 -o "${body}" -w '%{http_code}' "${URL}" 2>/dev/null)" || curl_err=$?

ok=0
if [[ "${curl_err}" -eq 0 && "${http_code}" == "200" ]]; then
  if grep -q '"ok"[[:space:]]*:[[:space:]]*true' "${body}" 2>/dev/null; then
    ok=1
  fi
fi

if [[ "${ok}" -eq 1 ]]; then
  exit 0
fi

detail="curl_err=${curl_err} http=${http_code} body=$(head -c 120 "${body}" | tr '\n' ' ')"
log "UNHEALTHY ${detail} — kickstart -k ${LABEL}"

if launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
  launchctl kickstart -k "${DOMAIN}/${LABEL}" >>"${LOG}" 2>&1 || {
    log "kickstart failed; falling back to kill port 7474"
    if pids="$(lsof -tiTCP:7474 -sTCP:LISTEN 2>/dev/null || true)"; then
      # shellcheck disable=SC2086
      kill -9 ${pids} 2>/dev/null || true
    fi
  }
else
  log "service not loaded; attempting port kill only"
  if pids="$(lsof -tiTCP:7474 -sTCP:LISTEN 2>/dev/null || true)"; then
    # shellcheck disable=SC2086
    kill -9 ${pids} 2>/dev/null || true
  fi
fi

exit 0
