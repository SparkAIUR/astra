#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

REPO_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
OMNI_IMAGE=${OMNI_IMAGE:-ghcr.io/siderolabs/omni:latest}
OMNI_REQUIRE_IMAGE=${OMNI_REQUIRE_IMAGE:-true}
OMNI_BOOT_WAIT_SECS=${OMNI_BOOT_WAIT_SECS:-45}
OMNI_ACCOUNT_ID=${OMNI_ACCOUNT_ID:-11111111-1111-1111-1111-111111111111}
OMNI_ACCOUNT_NAME=${OMNI_ACCOUNT_NAME:-phase6}
OMNI_BIND_PORT=${OMNI_BIND_PORT:-18080}
OMNI_MACHINE_API_PORT=${OMNI_MACHINE_API_PORT:-18090}
OMNI_K8S_PROXY_PORT=${OMNI_K8S_PROXY_PORT:-18091}
OMNI_WG_PORT=${OMNI_WG_PORT:-15180}
OMNI_METRICS_PORT=${OMNI_METRICS_PORT:-12112}
STORM_NODES=${PHASE6_OMNI_STORM_NODES:-500}
STORM_PARALLEL=${PHASE6_OMNI_STORM_PARALLEL:-64}

phase6_require_tools docker etcdctl jq python3 cargo

RUN_OMNI_DIR="${RUN_DIR}/omni"
mkdir -p "${RUN_OMNI_DIR}"
SUMMARY_JSON="${RUN_DIR}/phase6-omni-summary.json"
LEGACY_ETCD_NAME=phase6-legacy-etcd
LEGACY_ETCD_PORT=${PHASE6_LEGACY_ETCD_PORT:-32379}
SNAPSHOT_PATH="${RUN_OMNI_DIR}/legacy-etcd.snap"
BUNDLE_DIR="${RUN_OMNI_DIR}/forge-bundle"
WASM_WAT="${RUN_OMNI_DIR}/identity.wat"

cleanup() {
  docker rm -f "${LEGACY_ETCD_NAME}" >/dev/null 2>&1 || true
  docker rm -f phase6-omni >/dev/null 2>&1 || true
}
trap cleanup EXIT

phase6_stop_backend all || true
phase6_start_backend astra

phase6_log "starting legacy omni etcd source"
docker rm -f "${LEGACY_ETCD_NAME}" >/dev/null 2>&1 || true
docker run -d --name "${LEGACY_ETCD_NAME}" \
  -p "${LEGACY_ETCD_PORT}:2379" \
  quay.io/coreos/etcd:v3.6.8 \
  /usr/local/bin/etcd \
    --name legacy-omni \
    --data-dir /etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://127.0.0.1:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --initial-advertise-peer-urls http://127.0.0.1:2380 \
    --initial-cluster legacy-omni=http://127.0.0.1:2380 \
    --initial-cluster-state new >/dev/null

phase6_wait_for_tcp "127.0.0.1:${LEGACY_ETCD_PORT}" 120 1

phase6_log "seeding legacy omni state"
seq 1 2000 | xargs -I{} -P 32 sh -c \
  'etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="127.0.0.1:'"${LEGACY_ETCD_PORT}"'" put "/omni/machines/node-{}" "{\"state\":\"registered\",\"id\":{}}" >/dev/null'
seq 1 500 | xargs -I{} -P 32 sh -c \
  'etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="127.0.0.1:'"${LEGACY_ETCD_PORT}"'" put "/omni/rbac/role-{}" "role-{}" >/dev/null'

phase6_log "saving legacy snapshot"
etcdctl --dial-timeout=2s --command-timeout=20s --endpoints="127.0.0.1:${LEGACY_ETCD_PORT}" snapshot save "${SNAPSHOT_PATH}" >/dev/null
etcdctl snapshot status "${SNAPSHOT_PATH}" -w json >"${RUN_OMNI_DIR}/snapshot-status.json"

cat >"${WASM_WAT}" <<'WAT'
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param $len i32) (result i32)
    (i32.const 0))
  (func (export "dealloc") (param i32 i32))
  (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))))
)
WAT

rm -rf "${BUNDLE_DIR}"
mkdir -p "${BUNDLE_DIR}"

phase6_log "compiling forge bundle from db snapshot"
(cd "${REPO_DIR}" && cargo run --quiet --bin astra-forge -- compile \
  --input "${SNAPSHOT_PATH}" \
  --input-format db-snapshot \
  --out-dir "${BUNDLE_DIR}" \
  --chunk-target-bytes 4194304 \
  --wasm "${WASM_WAT}")

manifest_source="${BUNDLE_DIR}/manifest.json"
case "${PHASE6_BACKEND_ENDPOINT}" in
  127.0.0.1:2379) astra_container="phase6-astra-node1-1" ;;
  127.0.0.1:32391) astra_container="phase6-astra-node2-1" ;;
  127.0.0.1:32392) astra_container="phase6-astra-node3-1" ;;
  *) astra_container="" ;;
esac

if [ -n "${astra_container}" ]; then
  container_bundle_dir="/tmp/phase6-omni-forge-bundle"
  phase6_log "staging forge bundle into ${astra_container}:${container_bundle_dir}"
  docker exec "${astra_container}" mkdir -p "${container_bundle_dir}"
  docker cp "${BUNDLE_DIR}/." "${astra_container}:${container_bundle_dir}/"
  manifest_source="${container_bundle_dir}/manifest.json"
fi

phase6_log "bulk-loading forge manifest into astra"
(cd "${REPO_DIR}" && cargo run --quiet --bin astra-forge -- bulk-load \
  --endpoint "http://${PHASE6_BACKEND_ENDPOINT}" \
  --manifest "${manifest_source}" \
  --tenant-id phase6-omni \
  --wait)

probe_value=$(etcdctl --dial-timeout=2s --command-timeout=5s --endpoints="${PHASE6_BACKEND_ENDPOINT}" \
  get /omni/machines/node-42 --print-value-only 2>/dev/null || true)

omni_mode="image"
omni_boot_ok=false
omni_external_etcd=false
omni_logs="${RUN_OMNI_DIR}/omni-container.log"

if [ -z "${OMNI_IMAGE}" ]; then
  phase6_log "omni image is empty"
else
  phase6_log "attempting real omni boot with image=${OMNI_IMAGE}"
  docker rm -f phase6-omni >/dev/null 2>&1 || true
  mkdir -p "${RUN_OMNI_DIR}/omni-data"

  if docker run -d --name phase6-omni \
    --network host \
    -v "${RUN_OMNI_DIR}/omni-data:/var/lib/omni" \
    "${OMNI_IMAGE}" \
    --storage-kind=etcd \
    --etcd-embedded=false \
    --etcd-endpoints="http://${PHASE6_BACKEND_ENDPOINT}" \
    --etcd-client-cert-path= \
    --etcd-client-key-path= \
    --etcd-ca-path= \
    --sqlite-storage-path=/var/lib/omni/omni.sqlite \
    --private-key-source=random \
    --account-id="${OMNI_ACCOUNT_ID}" \
    --name="${OMNI_ACCOUNT_NAME}" \
    --bind-addr="0.0.0.0:${OMNI_BIND_PORT}" \
    --machine-api-bind-addr="0.0.0.0:${OMNI_MACHINE_API_PORT}" \
    --k8s-proxy-bind-addr="0.0.0.0:${OMNI_K8S_PROXY_PORT}" \
    --siderolink-wireguard-bind-addr="127.0.0.1:${OMNI_WG_PORT}" \
    --siderolink-wireguard-advertised-addr="127.0.0.1:${OMNI_WG_PORT}" \
    --metrics-bind-addr="0.0.0.0:${OMNI_METRICS_PORT}" \
    >/dev/null 2>&1; then
    for _ in $(seq 1 "${OMNI_BOOT_WAIT_SECS}"); do
      docker logs phase6-omni >"${omni_logs}" 2>&1 || true
      if ! docker inspect -f '{{.State.Running}}' phase6-omni 2>/dev/null | grep -q true; then
        break
      fi
      if grep -q 'starting etcd client' "${omni_logs}"; then
        omni_boot_ok=true
        break
      fi
      sleep 1
    done
    docker logs phase6-omni >"${omni_logs}" 2>&1 || true
    if grep -q 'starting etcd client' "${omni_logs}" && ! grep -q 'starting embedded etcd server' "${omni_logs}"; then
      omni_external_etcd=true
    fi
  else
    phase6_log "failed to start omni container"
  fi
fi

if [ "${OMNI_REQUIRE_IMAGE}" = "true" ] && [ "${omni_boot_ok}" != "true" ]; then
  phase6_log "real omni boot required but readiness was not reached"
fi

phase6_log "running provisioning storm simulation nodes=${STORM_NODES}"
storm_log="${RUN_OMNI_DIR}/provisioning-storm.log"
storm_start_ns=$(date +%s%N)
seq 1 "${STORM_NODES}" | xargs -I{} -P "${STORM_PARALLEL}" sh -c \
  'if etcdctl --dial-timeout=1s --command-timeout=4s --endpoints="'"${PHASE6_BACKEND_ENDPOINT}"'" put "/omni/provision/node-{}" "{\"status\":\"bootstrapped\",\"node\":{}}" >/dev/null 2>&1; then echo ok; else echo fail; fi' \
  >"${storm_log}"
storm_end_ns=$(date +%s%N)
storm_duration_ms=$(( (storm_end_ns - storm_start_ns) / 1000000 ))
storm_ok=$(grep -c '^ok$' "${storm_log}" || true)
storm_fail=$(grep -c '^fail$' "${storm_log}" || true)

python3 - <<'PY' \
"${SNAPSHOT_PATH}" \
"${RUN_OMNI_DIR}/snapshot-status.json" \
"${BUNDLE_DIR}/manifest.json" \
"${probe_value}" \
"${omni_mode}" \
"${omni_boot_ok}" \
"${omni_external_etcd}" \
"${omni_logs}" \
"${STORM_NODES}" \
"${storm_ok}" \
"${storm_fail}" \
"${storm_duration_ms}" \
"${SUMMARY_JSON}"
import json
import pathlib
import sys

snapshot_path = pathlib.Path(sys.argv[1])
snapshot_status_path = pathlib.Path(sys.argv[2])
manifest_path = pathlib.Path(sys.argv[3])
probe_value = sys.argv[4]
omni_mode = sys.argv[5]
omni_boot_ok = sys.argv[6].lower() == "true"
omni_external_etcd = sys.argv[7].lower() == "true"
omni_logs = sys.argv[8]
storm_nodes = int(sys.argv[9])
storm_ok = int(sys.argv[10])
storm_fail = int(sys.argv[11])
storm_duration_ms = int(sys.argv[12])
summary_path = pathlib.Path(sys.argv[13])

snapshot_status = {}
if snapshot_status_path.exists():
    try:
        snapshot_status = json.loads(snapshot_status_path.read_text(encoding="utf-8"))
    except Exception:
        snapshot_status = {}

manifest = {}
if manifest_path.exists():
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        manifest = {}

chunks = manifest.get("chunks") or []
summary = {
    "snapshot_path": str(snapshot_path),
    "snapshot_status_path": str(snapshot_status_path),
    "forge_manifest_path": str(manifest_path),
    "forge_chunk_count": len(chunks),
    "probe_value": probe_value,
    "omni_mode": omni_mode,
    "omni_boot_ok": omni_boot_ok,
    "omni_external_etcd": omni_external_etcd,
    "omni_logs": omni_logs,
    "provisioning_storm": {
        "target_nodes": storm_nodes,
        "ok": storm_ok,
        "fail": storm_fail,
        "duration_ms": storm_duration_ms,
    },
    "pass": (
        snapshot_path.exists()
        and len(chunks) > 0
        and probe_value != ""
        and omni_mode == "image"
        and omni_boot_ok
        and omni_external_etcd
        and storm_ok == storm_nodes
        and storm_fail == 0
    ),
    "caveats": [],
}
if not omni_boot_ok:
    summary["caveats"].append("omni image boot failed readiness check")
if not omni_external_etcd:
    summary["caveats"].append("omni did not confirm external etcd mode")
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "omni summary: ${SUMMARY_JSON}"
phase6_stop_backend all || true
