#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase6_require_tools docker etcdctl ghz python3

: "${PHASE10_GRAY_DURATION:=20s}"
: "${PHASE10_GRAY_CONCURRENCY:=120}"
: "${PHASE10_GRAY_CONNECTIONS:=64}"
: "${PHASE10_GRAY_FOLLOWER_IOPS:=5}"
: "${PHASE10_GRAY_FOLLOWER_BPS:=1048576}"
: "${PHASE10_GRAY_P99_TOLERANCE:=1.5}"
: "${PHASE10_GRAY_FAULT_MODE:=hybrid}"
: "${PHASE10_GATE_POLICY:=delta-aware}"
: "${PHASE10_GRAY_DELTA_P99_TOLERANCE:=1.8}"
: "${PHASE10_GRAY_DELTA_MAX_ERROR_DELTA:=0}"
: "${PHASE10_GRAY_DELTA_MAX_RPS_DROP_RATIO:=0.35}"
: "${PHASE10_GRAY_TC_DELAY_MS:=700}"
: "${PHASE10_GRAY_TC_JITTER_MS:=500}"
: "${PHASE10_GRAY_RAFT_APPEND_DELAY_ENABLED:=false}"
: "${PHASE10_GRAY_RAFT_APPEND_DELAY_MIN_MS:=500}"
: "${PHASE10_GRAY_RAFT_APPEND_DELAY_MAX_MS:=2000}"
: "${PHASE10_GRAY_RAFT_APPEND_DELAY_NODE_ID:=1}"

SUMMARY_JSON="${RUN_DIR}/phase10-gray-failure-summary.json"
TMP_DIR="${RUN_DIR}/phase10-gray-failure"
mkdir -p "${TMP_DIR}"

phase10_gray_endpoint_to_container() {
  case "$1" in
    127.0.0.1:2379) echo "phase6-astra-node1-1" ;;
    127.0.0.1:32391) echo "phase6-astra-node2-1" ;;
    127.0.0.1:32392) echo "phase6-astra-node3-1" ;;
    *) return 1 ;;
  esac
}

phase10_ghz_summary() {
  local in_json=${1:?json path required}
  local out_json=${2:?summary path required}
  python3 - <<'PY' "${in_json}" "${out_json}"
from __future__ import annotations

import json
import sys
from pathlib import Path

in_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
obj = json.loads(in_path.read_text(encoding="utf-8"))

status = {str(k).lower(): int(v) for k, v in (obj.get("statusCodeDistribution") or {}).items()}
ok = status.get("ok", 0)
count = int(obj.get("count") or 0)
errors = max(0, count - ok)
lat = {str(x.get("percentage")): float(x.get("latency") or 0) for x in (obj.get("latencyDistribution") or [])}
p99_ns = lat.get("99")
p99_ms = (p99_ns / 1_000_000.0) if p99_ns is not None else None

summary = {
    "count": count,
    "ok": ok,
    "errors": errors,
    "rps": float(obj.get("rps") or 0),
    "p99_ms": p99_ms,
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY
}

phase10_docker_update_supported() {
  local flag=${1:?flag required}
  docker update --help 2>/dev/null | grep -q -- "${flag}"
}

phase10_docker_update_follower_limits() {
  local container=${1:?container required}
  local iops=${2:?iops required}
  local bps=${3:?bps required}
  local args=()

  if phase10_docker_update_supported "--device-write-iops"; then
    args+=(--device-write-iops "${ASTRA_BLKIO_DEVICE}:${iops}")
  fi
  if phase10_docker_update_supported "--device-write-bps"; then
    args+=(--device-write-bps "${ASTRA_BLKIO_DEVICE}:${bps}")
  fi
  if [ "${#args[@]}" -eq 0 ]; then
    phase6_log "docker update blkio flags unsupported; skipping follower throttle update"
    return 2
  fi

  docker update "${args[@]}" "${container}" >/dev/null
}

phase10_apply_tc_delay() {
  local container=${1:?container required}
  local delay_ms=${2:?delay ms required}
  local jitter_ms=${3:?jitter ms required}
  docker exec "${container}" sh -lc \
    "command -v tc >/dev/null 2>&1 && tc qdisc replace dev eth0 root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal" \
    >/dev/null 2>&1
}

phase10_clear_tc_delay() {
  local container=${1:?container required}
  docker exec "${container}" sh -lc \
    "command -v tc >/dev/null 2>&1 && tc qdisc del dev eth0 root" \
    >/dev/null 2>&1 || true
}

phase10_start_heartbeat_writer() {
  local endpoint=${1:?endpoint required}
  local duration=${2:?duration required}
  local key=${3:?key required}
  local out_file=${4:?out_file required}
  (
    set +e
    fail=0
    end=$((SECONDS + ${duration%s}))
    i=1
    while [ "${SECONDS}" -lt "${end}" ]; do
      if ! etcdctl --dial-timeout=1s --command-timeout=2s --endpoints="${endpoint}" \
        put "${key}" "${i}" >/dev/null 2>&1; then
        fail=$((fail + 1))
      fi
      i=$((i + 1))
    done
    printf '%s\n' "${fail}" >"${out_file}"
  ) &
  PHASE10_HB_PID=$!
}

restore_follower_limits() {
  local container=${1:-}
  [ -n "${container}" ] || return 0
  phase10_docker_update_follower_limits \
    "${container}" \
    "${PHASE6_ASTRA_NODE_IOPS:-500}" \
    "${PHASE6_ASTRA_NODE_BPS:-20971520}" >/dev/null 2>&1 || true
}

follower_container=""
tc_delay_applied=false
cleanup() {
  if [ "${tc_delay_applied}" = "true" ] && [ -n "${follower_container}" ]; then
    phase10_clear_tc_delay "${follower_container}"
  fi
  restore_follower_limits "${follower_container}"
  phase6_stop_k3s || true
  phase6_stop_backend all || true
}
trap cleanup EXIT

export ASTRAD_PROFILE=${ASTRAD_PROFILE:-kubernetes}
export ASTRAD_QOS_TIER0_PREFIXES=${ASTRAD_QOS_TIER0_PREFIXES:-/registry/leases/,/omni/locks/}
export ASTRAD_QOS_TIER0_SUFFIXES=${ASTRAD_QOS_TIER0_SUFFIXES:-/leader,/lock}
if [ "${PHASE10_GRAY_RAFT_APPEND_DELAY_ENABLED}" = "true" ]; then
  export ASTRAD_CHAOS_APPEND_ACK_DELAY_ENABLED=true
  export ASTRAD_CHAOS_APPEND_ACK_DELAY_MIN_MS="${PHASE10_GRAY_RAFT_APPEND_DELAY_MIN_MS}"
  export ASTRAD_CHAOS_APPEND_ACK_DELAY_MAX_MS="${PHASE10_GRAY_RAFT_APPEND_DELAY_MAX_MS}"
  export ASTRAD_CHAOS_APPEND_ACK_DELAY_NODE_ID="${PHASE10_GRAY_RAFT_APPEND_DELAY_NODE_ID}"
else
  export ASTRAD_CHAOS_APPEND_ACK_DELAY_ENABLED=false
fi
phase6_stop_backend all || true
phase6_start_backend astra
leader_before=$(phase6_find_astra_leader 120 1)
phase6_log "phase10 gray-failure scenario leader=${leader_before}"

for candidate in 127.0.0.1:2379 127.0.0.1:32391 127.0.0.1:32392; do
  if [ "${candidate}" != "${leader_before}" ]; then
    follower_ep="${candidate}"
    break
  fi
done
follower_container=$(phase10_gray_endpoint_to_container "${follower_ep}")

ghz_data="${TMP_DIR}/put-data.json"
python3 - <<'PY' "${ghz_data}"
import base64
import json
import sys

payload = {
    "key": base64.b64encode(b"/phase10/gray/key").decode(),
    "value": base64.b64encode((b"x" * 4096)).decode(),
    "lease": "0",
    "prevKv": False,
    "ignoreValue": False,
    "ignoreLease": False,
}
with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump(payload, f)
PY

baseline_ghz="${TMP_DIR}/baseline-ghz.json"
baseline_summary="${TMP_DIR}/baseline-summary.json"
baseline_hb_counts="${TMP_DIR}/baseline-heartbeat-counts.txt"
phase10_start_heartbeat_writer \
  "${leader_before}" \
  "${PHASE10_GRAY_DURATION}" \
  "/registry/leases/gray-heartbeat-baseline" \
  "${baseline_hb_counts}"
baseline_hb_pid=${PHASE10_HB_PID}
ghz --insecure \
  --proto "${SCRIPT_DIR}/proto/rpc.proto" \
  --call etcdserverpb.KV.Put \
  -d @"${ghz_data}" \
  -c "${PHASE10_GRAY_CONCURRENCY}" \
  --connections "${PHASE10_GRAY_CONNECTIONS}" \
  -z "${PHASE10_GRAY_DURATION}" \
  --timeout 10s \
  "${leader_before}" \
  --format json --output "${baseline_ghz}"
wait "${baseline_hb_pid}" || true
phase10_ghz_summary "${baseline_ghz}" "${baseline_summary}" >/dev/null
if [ -f "${baseline_hb_counts}" ]; then
  baseline_hb_failures=$(cat "${baseline_hb_counts}")
else
  baseline_hb_failures=999999
fi

blkio_applied=false
tc_applied=false
fault_mode=$(printf '%s' "${PHASE10_GRAY_FAULT_MODE}" | tr '[:upper:]' '[:lower:]')

if [ "${fault_mode}" = "blkio" ] || [ "${fault_mode}" = "hybrid" ]; then
  phase6_log "injecting follower blkio throttle container=${follower_container} iops=${PHASE10_GRAY_FOLLOWER_IOPS}"
  if phase10_docker_update_follower_limits \
    "${follower_container}" \
    "${PHASE10_GRAY_FOLLOWER_IOPS}" \
    "${PHASE10_GRAY_FOLLOWER_BPS}"; then
    blkio_applied=true
  else
    phase6_log "blkio throttle injection failed; continuing"
  fi
fi

if [ "${fault_mode}" = "tc" ] || [ "${fault_mode}" = "hybrid" ]; then
  phase6_log "injecting follower tc netem delay container=${follower_container} delay_ms=${PHASE10_GRAY_TC_DELAY_MS} jitter_ms=${PHASE10_GRAY_TC_JITTER_MS}"
  if phase10_apply_tc_delay "${follower_container}" "${PHASE10_GRAY_TC_DELAY_MS}" "${PHASE10_GRAY_TC_JITTER_MS}"; then
    tc_applied=true
    tc_delay_applied=true
  else
    phase6_log "tc netem injection unavailable; continuing"
  fi
fi

stress_hb_counts="${TMP_DIR}/stress-heartbeat-counts.txt"
phase10_start_heartbeat_writer \
  "${leader_before}" \
  "${PHASE10_GRAY_DURATION}" \
  "/registry/leases/gray-heartbeat-stress" \
  "${stress_hb_counts}"
stress_hb_pid=${PHASE10_HB_PID}

stress_ghz="${TMP_DIR}/stress-ghz.json"
stress_summary="${TMP_DIR}/stress-summary.json"
ghz --insecure \
  --proto "${SCRIPT_DIR}/proto/rpc.proto" \
  --call etcdserverpb.KV.Put \
  -d @"${ghz_data}" \
  -c "${PHASE10_GRAY_CONCURRENCY}" \
  --connections "${PHASE10_GRAY_CONNECTIONS}" \
  -z "${PHASE10_GRAY_DURATION}" \
  --timeout 10s \
  "${leader_before}" \
  --format json --output "${stress_ghz}"
wait "${stress_hb_pid}" || true
phase10_ghz_summary "${stress_ghz}" "${stress_summary}" >/dev/null

leader_after=$(phase6_find_astra_leader 30 1 || true)
if [ -f "${stress_hb_counts}" ]; then
  stress_hb_failures=$(cat "${stress_hb_counts}")
else
  stress_hb_failures=999999
fi

python3 - <<'PY' \
"${baseline_summary}" \
"${stress_summary}" \
"${leader_before}" \
"${leader_after}" \
"${follower_ep}" \
"${follower_container}" \
"${baseline_hb_failures}" \
"${stress_hb_failures}" \
"${PHASE10_GRAY_P99_TOLERANCE}" \
"${PHASE10_GATE_POLICY}" \
"${PHASE10_GRAY_DELTA_P99_TOLERANCE}" \
"${PHASE10_GRAY_DELTA_MAX_ERROR_DELTA}" \
"${PHASE10_GRAY_DELTA_MAX_RPS_DROP_RATIO}" \
"${fault_mode}" \
"${blkio_applied}" \
"${tc_applied}" \
"${PHASE10_GRAY_RAFT_APPEND_DELAY_ENABLED}" \
"${PHASE10_GRAY_RAFT_APPEND_DELAY_MIN_MS}" \
"${PHASE10_GRAY_RAFT_APPEND_DELAY_MAX_MS}" \
"${PHASE10_GRAY_RAFT_APPEND_DELAY_NODE_ID}" \
"${SUMMARY_JSON}"
from __future__ import annotations

import json
import sys
from pathlib import Path

baseline = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
stress = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
leader_before = sys.argv[3]
leader_after = sys.argv[4]
follower_ep = sys.argv[5]
follower_container = sys.argv[6]
baseline_heartbeat_failures = int(sys.argv[7])
stress_heartbeat_failures = int(sys.argv[8])
strict_tolerance = float(sys.argv[9])
gate_policy = (sys.argv[10] or "delta-aware").strip().lower()
delta_tolerance = float(sys.argv[11])
delta_max_error_delta = int(sys.argv[12])
delta_max_rps_drop_ratio = float(sys.argv[13])
fault_mode = sys.argv[14]
blkio_applied = (sys.argv[15] or "").strip().lower() == "true"
tc_applied = (sys.argv[16] or "").strip().lower() == "true"
raft_append_delay_enabled = (sys.argv[17] or "").strip().lower() == "true"
raft_append_delay_min_ms = int(sys.argv[18])
raft_append_delay_max_ms = int(sys.argv[19])
raft_append_delay_node_id = int(sys.argv[20])
out_path = Path(sys.argv[21])

baseline_p99 = baseline.get("p99_ms") or 0.0
stress_p99 = stress.get("p99_ms") or 0.0
baseline_errors = int(baseline.get("errors") or 0)
stress_errors = int(stress.get("errors") or 0)
baseline_rps = float(baseline.get("rps") or 0.0)
stress_rps = float(stress.get("rps") or 0.0)
leader_stable = bool(leader_after) and leader_before == leader_after

strict_pass = (
    leader_stable
    and stress_errors == 0
    and baseline_heartbeat_failures == 0
    and stress_heartbeat_failures == 0
    and (stress_p99 <= baseline_p99 * strict_tolerance if baseline_p99 > 0 else False)
)
delta_pass = (
    leader_stable
    and baseline_heartbeat_failures == 0
    and stress_heartbeat_failures == 0
    and stress_errors <= (baseline_errors + max(0, delta_max_error_delta))
    and (stress_p99 <= baseline_p99 * delta_tolerance if baseline_p99 > 0 else False)
    and (stress_rps >= baseline_rps * max(0.0, 1.0 - delta_max_rps_drop_ratio) if baseline_rps > 0 else False)
)
if gate_policy == "strict":
    pass_value = strict_pass
elif gate_policy == "hybrid":
    pass_value = strict_pass or delta_pass
else:
    pass_value = delta_pass

summary = {
    "leader_before": leader_before,
    "leader_after": leader_after,
    "leader_stable": leader_stable,
    "follower_ep": follower_ep,
    "follower_container": follower_container,
    "fault_mode": fault_mode,
    "blkio_applied": blkio_applied,
    "tc_applied": tc_applied,
    "raft_append_delay": {
        "enabled": raft_append_delay_enabled,
        "min_ms": raft_append_delay_min_ms,
        "max_ms": raft_append_delay_max_ms,
        "node_id": raft_append_delay_node_id,
    },
    "baseline_heartbeat_failures": baseline_heartbeat_failures,
    "stress_heartbeat_failures": stress_heartbeat_failures,
    "baseline": baseline,
    "stress": stress,
    "gate_policy": gate_policy,
    "strict_pass": strict_pass,
    "delta_pass": delta_pass,
    "strict_tolerance": strict_tolerance,
    "delta_tolerance": delta_tolerance,
    "delta_max_error_delta": delta_max_error_delta,
    "delta_max_rps_drop_ratio": delta_max_rps_drop_ratio,
    "pass": pass_value,
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase10 gray-failure summary: ${SUMMARY_JSON}"
