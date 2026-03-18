#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

GRAFANA_HOST="${GRAFANA_HOST:-localhost}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
GRAFANA_DASHBOARD_UID="${GRAFANA_DASHBOARD_UID:-ddos_dashboard}"
GRAFANA_DASHBOARD_SLUG="${GRAFANA_DASHBOARD_SLUG:-ddos-realtime-dashboard}"
GRAFANA_BASE_URL="http://${GRAFANA_HOST}:${GRAFANA_PORT}"
DASHBOARD_URL="${GRAFANA_BASE_URL}/d/${GRAFANA_DASHBOARD_UID}/${GRAFANA_DASHBOARD_SLUG}?orgId=1"
LOGIN_URL="${GRAFANA_BASE_URL}/login"

open_url() {
  local url="$1"

  if command -v open >/dev/null 2>&1; then
    open "$url"
    return 0
  fi

  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$url" >/dev/null 2>&1 &
    return 0
  fi

  if command -v start >/dev/null 2>&1; then
    start "$url"
    return 0
  fi

  return 1
}

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "[error] Docker Compose not found. Install Docker Desktop first."
  exit 1
fi

echo "[step] Starting project with Compose..."
"${COMPOSE_CMD[@]}" up -d --build

echo "[step] Waiting for Grafana at ${LOGIN_URL} ..."
max_attempts=180
attempt=1
until curl -fsS "$LOGIN_URL" >/dev/null 2>&1; do
  if (( attempt >= max_attempts )); then
    echo "[error] Grafana did not become ready after $((max_attempts * 2)) seconds."
    exit 1
  fi
  if (( attempt % 10 == 0 )); then
    echo "[wait] Grafana still starting... (${attempt}/${max_attempts})"
  fi
  attempt=$((attempt + 1))
  sleep 2
done

echo "[ok] Grafana is ready. Opening dashboard in your default browser..."

if open_url "$DASHBOARD_URL"; then
  echo "[done] Dashboard opened: ${DASHBOARD_URL}"
else
  echo "[warn] Could not auto-open browser on this machine."
  echo "[info] Open manually: ${DASHBOARD_URL}"
fi

echo "[info] Login manually in Grafana. Default is admin/admin."
