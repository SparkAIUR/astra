#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase2-common.sh
source "${SCRIPT_DIR}/phase2-common.sh"

ensure_tools

if [ -z "${ASTRA_IMAGE:-}" ]; then
  echo "ASTRA_IMAGE is required, e.g. halceon/astra-alpha:<tag>" >&2
  exit 1
fi

WRITES=${ASTRA_PHASE5_CHAOS_WRITES:-5000}
SUMMARY_JSON="${RESULTS_DIR}/phase5-scenario-chaos-summary.json"

compose down -v || true
if [ "${ASTRA_SKIP_PULL:-false}" != "true" ]; then
  compose pull
fi
compose up -d

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  compose logs --tail 120
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase5/chaos/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable Astra endpoint" >&2
  compose logs --tail 120
  exit 2
}

project=$(project_name)
network=$(project_network)
node1="${project}-astra-node1-1"
node2="${project}-astra-node2-1"
node3="${project}-astra-node3-1"

success=0
failed=0
eps=("${leader_ep}")
for ep in $(other_endpoints "${leader_ep}" || true); do
  eps+=("${ep}")
done

refresh_writable_endpoint() {
  local probe_key="/phase5/chaos/reprobe"
  local probe_value
  probe_value="$(date +%s%N)"
  local new_leader
  if new_leader=$(find_writable_endpoint "${probe_key}" "${probe_value}" 5 2>/dev/null); then
    eps=("${new_leader}")
    for ep in $(other_endpoints "${new_leader}" || true); do
      eps+=("${ep}")
    done
  fi
}

put_with_retry() {
  local key=$1
  local value=$2
  local attempt ep
  for attempt in 1 2 3; do
    for ep in "${eps[@]}"; do
      if timeout -k 1s 4s etcdctl --dial-timeout=1s --command-timeout=2s --endpoints="${ep}" put "${key}" "${value}" >/dev/null 2>&1; then
        return 0
      fi
    done
    refresh_writable_endpoint
    sleep 0.05
  done
  return 1
}

writer() {
  local counts_file=$1
  local s=0
  local f=0
  local i
  for i in $(seq 1 "${WRITES}"); do
    if put_with_retry "/phase5/chaos/${i}" "v${i}"; then
      s=$((s + 1))
    else
      f=$((f + 1))
    fi
    if (( i % 100 == 0 )); then
      echo "chaos writer progress=${i}/${WRITES}"
    fi
  done
  printf '%s %s\n' "${s}" "${f}" >"${counts_file}"
}

counts_file=$(mktemp)
trap 'rm -f "${counts_file}"' EXIT

writer "${counts_file}" &
writer_pid=$!

sleep 2
docker pause "${node2}" >/dev/null 2>&1 || true
sleep 4
docker unpause "${node2}" >/dev/null 2>&1 || true

sleep 2
docker network disconnect "${network}" "${node3}" >/dev/null 2>&1 || true
sleep 4
docker network connect "${network}" "${node3}" >/dev/null 2>&1 || true

sleep 2
docker kill --signal=SIGKILL "${node1}" >/dev/null 2>&1 || true
compose up -d astra-node1 >/dev/null

wait "${writer_pid}" || true

if read -r success failed <"${counts_file}"; then
  :
else
  success=0
  failed=${WRITES}
fi

sleep 4

count=0
for ep in "${eps[@]}"; do
  if raw=$(timeout -k 1s 8s etcdctl --dial-timeout=1s --command-timeout=5s --endpoints="${ep}" get /phase5/chaos/ --prefix --keys-only 2>/dev/null); then
    count=$(printf '%s\n' "${raw}" | grep -c '^/phase5/chaos/' || true)
    break
  fi
done

python3 - <<'PY' "${SUMMARY_JSON}" "${success}" "${failed}" "${count}"
import json
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
success = int(sys.argv[2])
failed = int(sys.argv[3])
count = int(sys.argv[4])
summary = {
    "writes_success": success,
    "writes_failed": failed,
    "keys_visible_after_faults": count,
    "pass": success > 0 and count >= success,
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "phase5 chaos summary: ${SUMMARY_JSON}"
