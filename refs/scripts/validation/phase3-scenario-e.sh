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

export ASTRAD_TIERING_INTERVAL_SECS=${ASTRAD_TIERING_INTERVAL_SECS:-10}
export ASTRAD_SST_TARGET_BYTES=${ASTRAD_SST_TARGET_BYTES:-67108864}
export ASTRAD_WAL_IO_ENGINE=${ASTRAD_WAL_IO_ENGINE:-io_uring}

SUMMARY_TXT="${RESULTS_DIR}/phase3-scenario-e-summary.txt"
OBJECTS_TXT="${RESULTS_DIR}/phase3-scenario-e-minio-list.txt"

compose down -v || true
compose pull
compose up -d

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  compose logs --tail 120
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase3/e/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable endpoint" >&2
  compose logs --tail 120
  exit 2
}

echo "scenario-e writable endpoint: ${leader_ep}"

network_name=$(project_network)
docker run --rm --network "${network_name}" --entrypoint /bin/sh minio/mc -c '
  mc alias set m http://minio:9000 minioadmin minioadmin >/dev/null &&
  mc rm --recursive --force m/astra-tier/astra/cluster-1/chunks >/dev/null 2>&1 || true &&
  mc rm --force m/astra-tier/astra/cluster-1/manifest.json >/dev/null 2>&1 || true
'

payload=$(python3 - <<'PY'
print('v' * 2048)
PY
)

for i in $(seq 1 4000); do
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" put "/phase3/e/${i}" "${payload}" >/dev/null
  if (( i % 500 == 0 )); then
    echo "loaded=${i}"
  fi
done

sleep 25

docker run --rm --network "${network_name}" --entrypoint /bin/sh minio/mc -c '
  mc alias set m http://minio:9000 minioadmin minioadmin >/dev/null &&
  mc ls --recursive m/astra-tier/astra/cluster-1
' > "${OBJECTS_TXT}"

sst_count=$(grep -c '\.sst$' "${OBJECTS_TXT}" || true)
manifest_count=$(grep -c 'manifest.json$' "${OBJECTS_TXT}" || true)

sanity_before=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints=127.0.0.1:2379 get /phase3/e/100 --print-value-only 2>/dev/null || true)

project=$(project_name)
vol1="${project}_astra-node1-data"
vol2="${project}_astra-node2-data"
vol3="${project}_astra-node3-data"

compose stop astra-node1 astra-node2 astra-node3
compose rm -f astra-node1 astra-node2 astra-node3

docker volume rm "${vol1}" "${vol2}" "${vol3}" >/dev/null

start_ms=$(date +%s%3N)
compose up -d astra-node1 astra-node2 astra-node3

restored=""
for _ in $(seq 1 180); do
  restored=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints=127.0.0.1:2379 get /phase3/e/100 --print-value-only 2>/dev/null || true)
  if [ -n "${restored}" ]; then
    break
  fi
  sleep 1
done
end_ms=$(date +%s%3N)

ttfr_ms=$((end_ms - start_ms))
pass="false"
if [ -n "${restored}" ] && [ "${sst_count}" -eq 1 ] && [ "${manifest_count}" -eq 1 ]; then
  pass="true"
fi

cat > "${SUMMARY_TXT}" <<TXT
leader_ep=${leader_ep}
loaded_keys=4000
payload_bytes=2048
sanity_before=${sanity_before}
restored_value=${restored}
time_to_first_read_ms=${ttfr_ms}
sst_object_count=${sst_count}
manifest_object_count=${manifest_count}
exact_single_sst_pass=${pass}
objects_file=${OBJECTS_TXT}
TXT

cat "${SUMMARY_TXT}"

echo "scenario_e_summary=${SUMMARY_TXT}"
