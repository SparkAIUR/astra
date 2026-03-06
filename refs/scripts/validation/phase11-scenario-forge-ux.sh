#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

REPO_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)

LEGACY_COUNT=${PHASE11_LEGACY_COUNT:-5}
LEGACY_BASE_PORT=${PHASE11_LEGACY_BASE_PORT:-33379}
LEGACY_MACHINE_KEYS=${PHASE11_LEGACY_MACHINE_KEYS:-120}
LEGACY_RBAC_KEYS=${PHASE11_LEGACY_RBAC_KEYS:-40}
S3_ENDPOINT=${PHASE11_S3_ENDPOINT:-http://127.0.0.1:9000}
S3_REGION=${PHASE11_S3_REGION:-us-east-1}
DEST_URI=${PHASE11_DEST_URI:-s3://astra-tier/phase11/${RUN_ID}/forge}
ETCD_BIN=${PHASE11_ETCD_BIN:-etcd}
ETCDUTL_BIN=${PHASE11_ETCDUTL_BIN:-etcdutl}

RUN_PHASE11_DIR="${RUN_DIR}/phase11-forge-ux"
LEGACY_DIR="${RUN_PHASE11_DIR}/legacy"
WASM_PATH="${RUN_PHASE11_DIR}/identity.wat"
CONVERGE_LOG="${RUN_PHASE11_DIR}/converge.log"
CONVERGE_SUMMARY_JSON="${RUN_PHASE11_DIR}/converge-summary.json"
SUMMARY_JSON="${RUN_DIR}/phase11-forge-ux-summary.json"

mkdir -p "${RUN_PHASE11_DIR}" "${LEGACY_DIR}"

phase6_require_tools docker etcdctl jq python3 cargo "${ETCDUTL_BIN}" "${ETCD_BIN}"

phase11_pick_free_port() {
  local start=${1:?start port required}
  python3 - <<'PY' "${start}"
import socket
import sys

start = int(sys.argv[1])

for port in range(start, start + 4096):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        print(port)
        raise SystemExit(0)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

cleanup_legacy() {
  local i
  for i in $(seq 1 "${LEGACY_COUNT}"); do
    docker rm -f "phase11-legacy-${i}" >/dev/null 2>&1 || true
  done
}
trap cleanup_legacy EXIT

phase6_stop_backend all || true
phase6_start_backend astra

cat >"${WASM_PATH}" <<'WAT'
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

sources=()
tenants=()
expected_counts=()

for i in $(seq 1 "${LEGACY_COUNT}"); do
  name="phase11-legacy-${i}"
  port=$(phase11_pick_free_port "$((LEGACY_BASE_PORT + i - 1))")
  snap="${LEGACY_DIR}/omni-${i}.snap"
  tenant="tenant-$(printf '%02d' "${i}")"

  phase6_log "phase11 forge: starting legacy silo ${name} on port ${port}"
  docker rm -f "${name}" >/dev/null 2>&1 || true
  docker run -d --name "${name}" \
    -p "${port}:2379" \
    quay.io/coreos/etcd:v3.6.8 \
    /usr/local/bin/etcd \
      --name "${name}" \
      --data-dir /etcd-data \
      --listen-client-urls http://0.0.0.0:2379 \
      --advertise-client-urls http://127.0.0.1:2379 \
      --listen-peer-urls http://0.0.0.0:2380 \
      --initial-advertise-peer-urls http://127.0.0.1:2380 \
      --initial-cluster "${name}=http://127.0.0.1:2380" \
      --initial-cluster-state new >/dev/null

  phase6_wait_for_tcp "127.0.0.1:${port}" 90 1

  phase6_log "phase11 forge: seeding silo ${name}"
  seq 1 "${LEGACY_MACHINE_KEYS}" | xargs -I{} -P 32 sh -c \
    'etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="127.0.0.1:'"${port}"'" put "/omni/machines/node-'"${i}"'-{}" "{\"id\":{},\"site\":'"${i}"',\"state\":\"registered\"}" >/dev/null'
  seq 1 "${LEGACY_RBAC_KEYS}" | xargs -I{} -P 16 sh -c \
    'etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="127.0.0.1:'"${port}"'" put "/omni/rbac/role-'"${i}"'-{}" "role-'"${i}"'-{}" >/dev/null'
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="127.0.0.1:${port}" put "/omni/shared/region" "region-${i}" >/dev/null

  phase6_log "phase11 forge: snapshot silo ${name} -> ${snap}"
  etcdctl --dial-timeout=2s --command-timeout=20s --endpoints="127.0.0.1:${port}" snapshot save "${snap}" >/dev/null

  expected=$((LEGACY_MACHINE_KEYS + LEGACY_RBAC_KEYS + 1))
  sources+=("${snap}")
  tenants+=("${tenant}")
  expected_counts+=("${expected}")
done

phase6_log "phase11 forge: running astra-forge converge"
cmd=(cargo run --quiet --bin astra-forge -- converge
  --dest "${DEST_URI}"
  --endpoint "${S3_ENDPOINT}"
  --region "${S3_REGION}"
  --astra-endpoint "http://${PHASE6_BACKEND_ENDPOINT}"
  --apply-wasm "${WASM_PATH}"
  --allow-overwrite
  --etcd-bin "${ETCD_BIN}"
  --etcdutl-bin "${ETCDUTL_BIN}")
for i in $(seq 0 $((LEGACY_COUNT - 1))); do
  cmd+=(--source "${sources[$i]}" --tenant-id "${tenants[$i]}")
done
(
  cd "${REPO_DIR}"
  "${cmd[@]}"
) >"${CONVERGE_LOG}" 2>&1

python3 - <<'PY' "${CONVERGE_LOG}" "${CONVERGE_SUMMARY_JSON}"
from __future__ import annotations
import json
import pathlib
import sys

log_path = pathlib.Path(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])
text = log_path.read_text(encoding="utf-8", errors="ignore")
summary = None
for idx in range(len(text) - 1, -1, -1):
    if text[idx] != "{":
        continue
    try:
        summary = json.loads(text[idx:])
        break
    except Exception:
        continue
if summary is None:
    summary = {
        "error": "failed to parse converge JSON summary",
        "log": str(log_path),
    }
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

actual_counts=()
for i in $(seq 0 $((LEGACY_COUNT - 1))); do
  tenant="${tenants[$i]}"
  prefix="/__tenant/${tenant}/"
  count=$(etcdctl --dial-timeout=2s --command-timeout=8s --endpoints="${PHASE6_BACKEND_ENDPOINT}" get "${prefix}" --prefix --keys-only \
    | awk '/^\/__tenant\// {c++} END {print c+0}')
  actual_counts+=("${count}")
done

python3 - <<'PY' \
"${SUMMARY_JSON}" \
"${CONVERGE_SUMMARY_JSON}" \
"${DEST_URI}" \
"${PHASE6_BACKEND_ENDPOINT}" \
"${LEGACY_COUNT}" \
"${CONVERGE_LOG}" \
"${sources[*]}" \
"${tenants[*]}" \
"${expected_counts[*]}" \
"${actual_counts[*]}"
from __future__ import annotations
import json
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
converge_summary_path = pathlib.Path(sys.argv[2])
dest_uri = sys.argv[3]
backend = sys.argv[4]
legacy_count = int(sys.argv[5])
converge_log = sys.argv[6]
sources = sys.argv[7].split()
tenants = sys.argv[8].split()
expected_counts = [int(x) for x in sys.argv[9].split()]
actual_counts = [int(x) for x in sys.argv[10].split()]

converge = {}
if converge_summary_path.exists():
    try:
        converge = json.loads(converge_summary_path.read_text(encoding="utf-8"))
    except Exception:
        converge = {}

tenant_results = []
for src, tenant, expected, actual in zip(sources, tenants, expected_counts, actual_counts):
    tenant_results.append(
        {
            "source": src,
            "tenant_id": tenant,
            "expected_records": expected,
            "actual_records": actual,
            "match": expected == actual,
        }
    )

pass_flag = (
    len(tenant_results) == legacy_count
    and all(item["match"] for item in tenant_results)
    and isinstance(converge, dict)
    and "applied" in converge
)

summary = {
    "legacy_count": legacy_count,
    "dest_uri": dest_uri,
    "backend_endpoint": backend,
    "converge_log": converge_log,
    "converge_summary": converge,
    "tenants": tenant_results,
    "pass": pass_flag,
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase11 forge summary: ${SUMMARY_JSON}"
