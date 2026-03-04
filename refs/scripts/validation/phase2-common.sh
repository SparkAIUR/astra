#!/usr/bin/env bash
set -euo pipefail

# Shared helpers for phase-2 validation scripts.
# Script is location-stable and can be invoked from any working directory.

export PATH="/home/linuxbrew/.linuxbrew/bin:${PATH}"

COMMON_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_DIR=${PROJECT_DIR:-${COMMON_DIR}}
COMPOSE_BASE=${COMPOSE_BASE:-${COMMON_DIR}/docker-compose.image.yml}
COMPOSE_BLKIO=${COMPOSE_BLKIO:-${COMMON_DIR}/docker-compose.blkio.yml}
RESULTS_DIR=${RESULTS_DIR:-${COMMON_DIR}/results}

mkdir -p "${RESULTS_DIR}"

compose() {
  docker compose --project-directory "${PROJECT_DIR}" -f "${COMPOSE_BASE}" "$@"
}

compose_blkio() {
  docker compose --project-directory "${PROJECT_DIR}" -f "${COMPOSE_BASE}" -f "${COMPOSE_BLKIO}" "$@"
}

project_name() {
  basename "${PROJECT_DIR}"
}

project_network() {
  echo "$(project_name)_default"
}

ensure_tools() {
  local missing=0
  for t in docker etcdctl ghz python3 curl; do
    if ! command -v "$t" >/dev/null 2>&1; then
      echo "missing required tool: $t" >&2
      missing=1
    fi
  done
  if [ "$missing" -ne 0 ]; then
    exit 1
  fi
}

wait_for_ports() {
  local attempts=${1:-60}
  local delay=${2:-1}
  local eps=(127.0.0.1:2379 127.0.0.1:32391 127.0.0.1:32392)

  for ((i = 1; i <= attempts; i++)); do
    local ok=0
    for ep in "${eps[@]}"; do
      if timeout 1 bash -c "</dev/tcp/${ep/:/\/}" >/dev/null 2>&1; then
        ok=$((ok + 1))
      fi
    done
    if [ "$ok" -ge 3 ]; then
      return 0
    fi
    sleep "$delay"
  done

  return 1
}

find_writable_endpoint() {
  local key=${1:-/phase2/probe/leader}
  local value=${2:-probe}
  local loops=${3:-60}
  local eps=(127.0.0.1:2379 127.0.0.1:32391 127.0.0.1:32392)

  for ((i = 1; i <= loops; i++)); do
    for ep in "${eps[@]}"; do
      local put_json
      if put_json=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="$ep" --write-out=json put "$key" "$value" 2>/dev/null); then
        local member_id
        member_id=$(python3 - <<'PY' "${put_json}"
import json
import sys

try:
    obj = json.loads(sys.argv[1])
except Exception:
    print("")
    raise SystemExit(0)

header = obj.get("header") or {}
member_id = header.get("member_id")
if member_id is None:
    member_id = header.get("memberId")

print(str(member_id) if member_id is not None else "")
PY
)

        local leader_ep=""
        case "${member_id}" in
          1) leader_ep="127.0.0.1:2379" ;;
          2) leader_ep="127.0.0.1:32391" ;;
          3) leader_ep="127.0.0.1:32392" ;;
        esac

        if [ -n "${leader_ep}" ]; then
          echo "${leader_ep}"
        else
          echo "$ep"
        fi
        return 0
      fi
    done
    sleep 1
  done

  return 1
}

endpoint_to_container() {
  case "$1" in
    127.0.0.1:2379) echo "$(project_name)-astra-node1-1 astra-node1" ;;
    127.0.0.1:32391) echo "$(project_name)-astra-node2-1 astra-node2" ;;
    127.0.0.1:32392) echo "$(project_name)-astra-node3-1 astra-node3" ;;
    *) return 1 ;;
  esac
}

other_endpoints() {
  case "$1" in
    127.0.0.1:2379) echo "127.0.0.1:32391 127.0.0.1:32392" ;;
    127.0.0.1:32391) echo "127.0.0.1:2379 127.0.0.1:32392" ;;
    127.0.0.1:32392) echo "127.0.0.1:2379 127.0.0.1:32391" ;;
    *) return 1 ;;
  esac
}

endpoint_to_metrics_endpoint() {
  case "$1" in
    127.0.0.1:2379) echo "127.0.0.1:19479" ;;
    127.0.0.1:32391) echo "127.0.0.1:19480" ;;
    127.0.0.1:32392) echo "127.0.0.1:19481" ;;
    *) return 1 ;;
  esac
}
