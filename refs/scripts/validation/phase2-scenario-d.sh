#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase2-common.sh
source "${SCRIPT_DIR}/phase2-common.sh"

ensure_tools

if [ -z "${ASTRA_IMAGE:-}" ]; then
  echo "ASTRA_IMAGE is required, e.g. halceon/astra:<tag>" >&2
  exit 1
fi

SUMMARY_TXT="${RESULTS_DIR}/phase2-scenario-d-summary.txt"

compose down -v || true
compose pull
compose up -d

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  compose logs --tail 120
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase2/d/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable endpoint" >&2
  compose logs --tail 120
  exit 2
}

read -r leader_container leader_alias <<<"$(endpoint_to_container "${leader_ep}")"
read -r other_a other_b <<<"$(other_endpoints "${leader_ep}")"
network_name=$(project_network)

base_key="/phase2/d/base"
isolated_key="/phase2/d/isolated"
majority_key="/phase2/d/majority"

etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" put "${base_key}" before >/dev/null

docker network disconnect "${network_name}" "${leader_container}" || true
sleep 10

set +e
isolated_out=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" put "${isolated_key}" no 2>&1)
isolated_rc=$?
set -e

majority_ep=""
for ep in "${other_a}" "${other_b}"; do
  if etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${ep}" put "${majority_key}" yes >/dev/null 2>&1; then
    majority_ep="${ep}"
    break
  fi
done

docker network connect --alias "${leader_alias}" --alias "${leader_container}" "${network_name}" "${leader_container}" || true
sleep 15

read1=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints=127.0.0.1:2379 get "${majority_key}" --print-value-only 2>/dev/null || true)
read2=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints=127.0.0.1:32391 get "${majority_key}" --print-value-only 2>/dev/null || true)
read3=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints=127.0.0.1:32392 get "${majority_key}" --print-value-only 2>/dev/null || true)

pass="false"
if [ "${read1}" = "yes" ] && [ "${read2}" = "yes" ] && [ "${read3}" = "yes" ] && [ -n "${majority_ep}" ]; then
  pass="true"
fi

cat > "${SUMMARY_TXT}" <<TXT
leader_ep=${leader_ep}
leader_container=${leader_container}
isolated_write_rc=${isolated_rc}
isolated_write_out=${isolated_out}
majority_ep=${majority_ep}
read_node1=${read1}
read_node2=${read2}
read_node3=${read3}
strict_convergence_pass=${pass}
TXT

cat "${SUMMARY_TXT}"

echo "scenario_d_summary=${SUMMARY_TXT}"
