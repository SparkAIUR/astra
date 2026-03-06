#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase6_require_tools docker ghz etcdctl python3
: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra:phase12-<sha>}"

: "${PHASE12_MR_TOTAL:=60000}"
: "${PHASE12_MR_CONCURRENCY:=256}"
: "${PHASE12_MR_CONNECTIONS:=64}"
: "${PHASE12_MR_SCALE_GAIN_PASS:=1.20}"

SUMMARY_JSON="${RUN_DIR}/phase12-multiraft-summary.json"
OUT_DIR="${RUN_DIR}/phase12-multiraft"
mkdir -p "${OUT_DIR}"

req_a="${OUT_DIR}/put-lease.json"
req_b="${OUT_DIR}/put-pods.json"
req_c="${OUT_DIR}/put-default.json"

python3 - <<'PY' "${req_a}" "${req_b}" "${req_c}"
import base64
import json
import sys

rows = [
    ("/registry/leases/kube-system/leader", "v-lease"),
    ("/registry/pods/default/pod-a", "v-pod"),
    ("/registry/configmaps/default/cm-a", "v-cm"),
]
for idx, (path, value) in enumerate(rows):
    out = {
        "key": base64.b64encode(path.encode()).decode(),
        "value": base64.b64encode(value.encode()).decode(),
        "lease": 0,
        "prevKv": False,
        "ignoreValue": False,
        "ignoreLease": False,
    }
    with open(sys.argv[idx + 1], "w", encoding="utf-8") as f:
        json.dump(out, f)
PY

run_ghz_put() {
  local endpoint=${1:?endpoint}
  local req_file=${2:?req file}
  local out_file=${3:?out file}
  ghz --insecure \
    --proto "${SCRIPT_DIR}/proto/rpc.proto" \
    --call etcdserverpb.KV.Put \
    --data-file "${req_file}" \
    --total "${PHASE12_MR_TOTAL}" \
    --concurrency "${PHASE12_MR_CONCURRENCY}" \
    --connections "${PHASE12_MR_CONNECTIONS}" \
    --format json \
    "${endpoint}" >"${out_file}"
}

ghz_rps() {
  local in_file=${1:?input}
  python3 - <<'PY' "${in_file}"
import json
import sys
from pathlib import Path

obj = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
summary = obj.get("summary")
if not isinstance(summary, dict) or not summary:
    summary = obj
print(float(summary.get("rps") or obj.get("rps") or 0.0))
PY
}

start_single_node() {
  local name=${1:?name}
  local client_port=${2:?client port}
  local raft_port=${3:?raft port}
  local metrics_port=${4:?metrics port}
  local data_dir=${5:?data dir}

  mkdir -p "${data_dir}"
  docker rm -f "${name}" >/dev/null 2>&1 || true
  docker run -d \
    --name "${name}" \
    -p "${client_port}:2379" \
    -p "${raft_port}:2380" \
    -p "${metrics_port}:9479" \
    -v "${data_dir}:/app/data" \
    -e ASTRAD_DATA_DIR=/app/data \
    -e ASTRAD_CLIENT_ADDR=0.0.0.0:2379 \
    -e ASTRAD_RAFT_ADDR=0.0.0.0:2380 \
    -e ASTRAD_RAFT_ADVERTISE_ADDR=127.0.0.1:${raft_port} \
    -e ASTRAD_METRICS_ENABLED=true \
    -e ASTRAD_METRICS_ADDR=0.0.0.0:9479 \
    "${ASTRA_IMAGE}" >/dev/null
}

cleanup() {
  phase6_stop_backend all || true
  for name in phase12-mr-lease phase12-mr-pods phase12-mr-default; do
    docker rm -f "${name}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

phase6_log "phase12 multiraft: baseline single-group run"
phase6_start_backend astra
baseline_ep=$(phase6_find_astra_leader 120 1)
run_ghz_put "${baseline_ep}" "${req_c}" "${OUT_DIR}/baseline.json"
baseline_rps=$(ghz_rps "${OUT_DIR}/baseline.json")

phase6_log "phase12 multiraft: external prefix-sharded run (3 independent groups)"
phase6_stop_backend all || true
start_single_node phase12-mr-lease 23791 24801 19591 "${RUNTIME_DIR}/phase12-mr-lease"
start_single_node phase12-mr-pods 23792 24802 19592 "${RUNTIME_DIR}/phase12-mr-pods"
start_single_node phase12-mr-default 23793 24803 19593 "${RUNTIME_DIR}/phase12-mr-default"

phase6_wait_for_tcp "127.0.0.1:23791" 120 1
phase6_wait_for_tcp "127.0.0.1:23792" 120 1
phase6_wait_for_tcp "127.0.0.1:23793" 120 1

run_ghz_put "127.0.0.1:23791" "${req_a}" "${OUT_DIR}/shard-lease.json" &
pid_a=$!
run_ghz_put "127.0.0.1:23792" "${req_b}" "${OUT_DIR}/shard-pods.json" &
pid_b=$!
run_ghz_put "127.0.0.1:23793" "${req_c}" "${OUT_DIR}/shard-default.json" &
pid_c=$!
wait "${pid_a}" "${pid_b}" "${pid_c}"

rps_a=$(ghz_rps "${OUT_DIR}/shard-lease.json")
rps_b=$(ghz_rps "${OUT_DIR}/shard-pods.json")
rps_c=$(ghz_rps "${OUT_DIR}/shard-default.json")

python3 - <<'PY' \
  "${baseline_rps}" \
  "${rps_a}" \
  "${rps_b}" \
  "${rps_c}" \
  "${PHASE12_MR_SCALE_GAIN_PASS}" \
  "${SUMMARY_JSON}"
import json
import sys
from pathlib import Path

baseline = float(sys.argv[1])
rps_a = float(sys.argv[2])
rps_b = float(sys.argv[3])
rps_c = float(sys.argv[4])
threshold = float(sys.argv[5])
out_path = Path(sys.argv[6])

sharded_total = rps_a + rps_b + rps_c
gain = (sharded_total / baseline) if baseline > 0 else 0.0

summary = {
    "mode": "external_prefix_shard_poc",
    "baseline_rps": baseline,
    "sharded": {
        "lease_rps": rps_a,
        "pods_rps": rps_b,
        "default_rps": rps_c,
        "total_rps": sharded_total,
    },
    "scale_gain": gain,
    "scale_gain_pass_threshold": threshold,
    "pass": gain >= threshold,
    "notes": [
        "This scenario is an external prefix-routed shard PoC; in-process multi-group consensus remains follow-up work.",
    ],
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase12 multiraft summary: ${SUMMARY_JSON}"
