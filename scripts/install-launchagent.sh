#!/bin/bash
# Install macOS LaunchAgents so the dashboard survives reboot, sleep, and crashes.
#
# Usage (Hector / default paths):
#   ./scripts/install-launchagent.sh
#
# Override root:
#   ROOT=/path/to/dgx-spark-memory-dashboard ./scripts/install-launchagent.sh
set -euo pipefail

UID_NUM="$(id -u)"
DOMAIN="gui/${UID_NUM}"
LABEL_DASH="${DGX_SMD_LAUNCH_LABEL:-com.hamallar.spark-memory-dashboard}"
LABEL_WATCH="${LABEL_DASH}-watchdog"
PLIST_DASH="${HOME}/Library/LaunchAgents/${LABEL_DASH}.plist"
PLIST_WATCH="${HOME}/Library/LaunchAgents/${LABEL_WATCH}.plist"
LOG_DIR="${HOME}/Library/Logs/spark-memory-dashboard"
ROOT="${ROOT:-${HOME}/Projects/dgx-spark-memory-dashboard}"
# Prefer lowercase projects/ if that is where the repo actually lives
if [[ ! -d "${ROOT}" && -d "${HOME}/projects/dgx-spark-memory-dashboard" ]]; then
  ROOT="${HOME}/projects/dgx-spark-memory-dashboard"
fi
PYTHON="${PYTHON:-/usr/bin/python3}"
CONFIG="${CONFIG:-${ROOT}/config.yaml}"
PORT="${PORT:-7474}"

mkdir -p "${LOG_DIR}" "${HOME}/Library/LaunchAgents"
chmod +x "${ROOT}/scripts/healthcheck-restart.sh" "${ROOT}/scripts/install-launchagent.sh" "${ROOT}/scripts/run.py" 2>/dev/null || true

if [[ ! -f "${CONFIG}" ]]; then
  echo "Missing config: ${CONFIG}" >&2
  echo "Copy config.example.yaml → config.yaml and edit first." >&2
  exit 1
fi

# Free port from any manual run
if pids="$(lsof -tiTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"; then
  # shellcheck disable=SC2086
  kill -9 ${pids} 2>/dev/null || true
  sleep 1
fi

bootout() {
  local label="$1" plist="$2"
  launchctl bootout "${DOMAIN}/${label}" 2>/dev/null || true
  launchctl unload "${plist}" 2>/dev/null || true
}

bootout "${LABEL_DASH}" "${PLIST_DASH}"
bootout "${LABEL_WATCH}" "${PLIST_WATCH}"
sleep 1

# Dashboard service — KeepAlive forever
cat >"${PLIST_DASH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>${LABEL_DASH}</string>
	<key>ProgramArguments</key>
	<array>
		<string>${PYTHON}</string>
		<string>${ROOT}/scripts/run.py</string>
		<string>-c</string>
		<string>${CONFIG}</string>
	</array>
	<key>WorkingDirectory</key>
	<string>${ROOT}</string>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>ThrottleInterval</key>
	<integer>3</integer>
	<key>ProcessType</key>
	<string>Interactive</string>
	<key>StandardOutPath</key>
	<string>${LOG_DIR}/stdout.log</string>
	<key>StandardErrorPath</key>
	<string>${LOG_DIR}/stderr.log</string>
</dict>
</plist>
EOF

# Watchdog every 30s — catches hung-but-listening processes
cat >"${PLIST_WATCH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>${LABEL_WATCH}</string>
	<key>ProgramArguments</key>
	<array>
		<string>/bin/bash</string>
		<string>${ROOT}/scripts/healthcheck-restart.sh</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>StartInterval</key>
	<integer>30</integer>
	<key>StandardOutPath</key>
	<string>${LOG_DIR}/watchdog-stdout.log</string>
	<key>StandardErrorPath</key>
	<string>${LOG_DIR}/watchdog-stderr.log</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>DGX_SMD_LAUNCH_LABEL</key>
		<string>${LABEL_DASH}</string>
		<key>DGX_SMD_HEALTH_URL</key>
		<string>http://127.0.0.1:${PORT}/api/health</string>
	</dict>
</dict>
</plist>
EOF

launchctl bootstrap "${DOMAIN}" "${PLIST_DASH}"
launchctl bootstrap "${DOMAIN}" "${PLIST_WATCH}"
launchctl enable "${DOMAIN}/${LABEL_DASH}" 2>/dev/null || true
launchctl enable "${DOMAIN}/${LABEL_WATCH}" 2>/dev/null || true
launchctl kickstart -k "${DOMAIN}/${LABEL_DASH}" || true

ok=0
for i in $(seq 1 30); do
  if body="$(curl -sf -m 3 "http://127.0.0.1:${PORT}/api/health" 2>/dev/null || true)"; then
    if printf '%s' "${body}" | grep -q '"ok"[[:space:]]*:[[:space:]]*true'; then
      ok=1
      break
    fi
  fi
  sleep 1
done

echo "=== launchctl ${LABEL_DASH} ==="
launchctl print "${DOMAIN}/${LABEL_DASH}" 2>&1 | egrep 'state =|pid =|path =|last exit|runs =|spawn type' || true
echo "=== launchctl ${LABEL_WATCH} ==="
launchctl print "${DOMAIN}/${LABEL_WATCH}" 2>&1 | egrep 'state =|pid =|path =|last exit|runs =' || true
echo "=== port ${PORT} ==="
lsof -nP -iTCP:${PORT} -sTCP:LISTEN || true
echo "=== health ==="
curl -sS -m 5 "http://127.0.0.1:${PORT}/api/health" || true
echo
if [[ "${ok}" -eq 1 ]]; then
  echo "OK: dashboard LaunchAgent is live (KeepAlive + 30s watchdog)"
  echo "UI: http://127.0.0.1:${PORT}/"
  exit 0
fi
echo "FAIL: dashboard did not become healthy within 30s" >&2
tail -40 "${LOG_DIR}/stderr.log" 2>/dev/null || true
tail -40 "${LOG_DIR}/stdout.log" 2>/dev/null || true
exit 1
