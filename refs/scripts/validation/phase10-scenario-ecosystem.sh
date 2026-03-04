#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase6_require_tools docker etcdctl ghz python3

: "${PHASE10_CNI_CYCLES:=8000}"
: "${PHASE10_CNI_P99_TARGET_MS:=250}"

: "${PHASE10_GATEWAY_PRELOAD_KEYS:=2000}"
: "${PHASE10_GATEWAY_CONCURRENCY:=300}"
: "${PHASE10_GATEWAY_CONNECTIONS:=96}"
: "${PHASE10_GATEWAY_DURATION:=30s}"
: "${PHASE10_GATEWAY_TARGET_RPS:=50000}"
: "${PHASE10_GATE_POLICY:=delta-aware}"
: "${PHASE10_GATEWAY_DELTA_RPS_RATIO:=0.20}"
: "${PHASE10_GATEWAY_DELTA_MAX_ERROR_RATE:=0.005}"

: "${PHASE10_PATRONI_LOAD_SECS:=30}"
: "${PHASE10_PATRONI_LEASE_OPS:=1500}"
: "${PHASE10_PATRONI_P99_TARGET_MS:=50}"

SUMMARY_JSON="${RUN_DIR}/phase10-ecosystem-summary.json"

phase10_start_backend_profile() {
  local profile=${1:?profile required}
  export ASTRAD_PROFILE="${profile}"
  export ASTRAD_PROFILE_SAMPLE_SECS=${ASTRAD_PROFILE_SAMPLE_SECS:-5}
  export ASTRAD_PROFILE_MIN_DWELL_SECS=${ASTRAD_PROFILE_MIN_DWELL_SECS:-5}
  export ASTRAD_QOS_TIER0_PREFIXES=${ASTRAD_QOS_TIER0_PREFIXES:-/registry/leases/,/omni/locks/}
  export ASTRAD_QOS_TIER0_SUFFIXES=${ASTRAD_QOS_TIER0_SUFFIXES:-/leader,/lock}
  export ASTRAD_QOS_TIER0_MAX_BATCH_REQUESTS=${ASTRAD_QOS_TIER0_MAX_BATCH_REQUESTS:-32}
  export ASTRAD_QOS_TIER0_MAX_LINGER_US=${ASTRAD_QOS_TIER0_MAX_LINGER_US:-0}
  if [ "${profile}" = "gateway" ]; then
    export ASTRAD_GATEWAY_READ_TICKET_ENABLED=${ASTRAD_GATEWAY_READ_TICKET_ENABLED:-true}
    export ASTRAD_GATEWAY_READ_TICKET_TTL_MS=${ASTRAD_GATEWAY_READ_TICKET_TTL_MS:-100}
    export ASTRAD_GATEWAY_SINGLEFLIGHT_ENABLED=${ASTRAD_GATEWAY_SINGLEFLIGHT_ENABLED:-true}
    export ASTRAD_GATEWAY_SINGLEFLIGHT_MAX_WAITERS=${ASTRAD_GATEWAY_SINGLEFLIGHT_MAX_WAITERS:-16384}
    export ASTRAD_GRPC_MAX_CONCURRENT_STREAMS=${ASTRAD_GRPC_MAX_CONCURRENT_STREAMS:-262144}
  fi
  phase6_stop_backend all || true
  # phase6_start_backend logs to stdout; keep logs visible but off the captured endpoint channel.
  phase6_start_backend astra >&2
  phase6_find_astra_leader 120 1
}

cleanup() {
  phase6_stop_k3s || true
  phase6_stop_backend all || true
}
trap cleanup EXIT

tmp_dir="${RUN_DIR}/phase10-ecosystem"
mkdir -p "${tmp_dir}"

phase6_log "phase10 ecosystem scenario: CNI churn simulation"
leader_ep=$(phase10_start_backend_profile kubernetes)
python3 - <<'PY' "${leader_ep}" "${PHASE10_CNI_CYCLES}" "${PHASE10_CNI_P99_TARGET_MS}" "${tmp_dir}/cni.json"
from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from pathlib import Path

endpoint = sys.argv[1]
cycles = int(sys.argv[2])
p99_target_ms = float(sys.argv[3])
out_path = Path(sys.argv[4])

put_lat = []
del_lat = []
failures = 0

def run_cmd(args: list[str]) -> bool:
    proc = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc.returncode == 0

for i in range(1, cycles + 1):
    key = f"/cilium/ipam/pod-{i:08d}"
    t0 = time.perf_counter()
    ok_put = run_cmd(["etcdctl", "--dial-timeout=1s", "--command-timeout=3s", f"--endpoints={endpoint}", "put", key, f"v-{i}"])
    put_lat.append((time.perf_counter() - t0) * 1000.0)
    if not ok_put:
        failures += 1
    t1 = time.perf_counter()
    ok_del = run_cmd(["etcdctl", "--dial-timeout=1s", "--command-timeout=3s", f"--endpoints={endpoint}", "del", key])
    del_lat.append((time.perf_counter() - t1) * 1000.0)
    if not ok_del:
        failures += 1

raw = subprocess.run(
    ["etcdctl", "--dial-timeout=1s", "--command-timeout=5s", f"--endpoints={endpoint}", "get", "/cilium/ipam/", "--prefix", "--keys-only"],
    check=False,
    capture_output=True,
    text=True,
).stdout
keys_after = len([ln for ln in raw.splitlines() if ln.startswith("/cilium/ipam/")])

def pct(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    arr = sorted(vals)
    if len(arr) == 1:
        return arr[0]
    k = (len(arr) - 1) * q
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return arr[int(k)]
    return arr[f] + (arr[c] - arr[f]) * (k - f)

put_p99 = pct(put_lat, 0.99)
del_p99 = pct(del_lat, 0.99)
result = {
    "cycles": cycles,
    "failures": failures,
    "key_count_after": keys_after,
    "put_p99_ms": put_p99,
    "delete_p99_ms": del_p99,
    "manual_defrag_performed": False,
    "pass": failures == 0 and keys_after == 0 and put_p99 <= p99_target_ms and del_p99 <= p99_target_ms,
}
out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result, indent=2))
PY

phase6_log "phase10 ecosystem scenario: API gateway read storm"
leader_ep=$(phase10_start_backend_profile gateway)
for i in $(seq 1 "${PHASE10_GATEWAY_PRELOAD_KEYS}"); do
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" \
    put "/phase10/gateway/route/${i}" "route-${i}" >/dev/null
done

gateway_range_data="${tmp_dir}/gateway-range-data.json"
python3 - <<'PY' "${gateway_range_data}"
import base64
import json
import sys

payload = {
    "key": base64.b64encode(b"/phase10/gateway/route/1000").decode(),
    "rangeEnd": "",
    "limit": "1",
    "revision": "0",
    "keysOnly": False,
    "countOnly": False,
}
with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump(payload, f)
PY

writer_counts="${tmp_dir}/gateway-writer-counts.txt"
(
  set +e
  put_fail=0
  hb_fail=0
  end=$((SECONDS + ${PHASE10_GATEWAY_DURATION%s}))
  i=1
  while [ "${SECONDS}" -lt "${end}" ]; do
    if ! etcdctl --dial-timeout=1s --command-timeout=2s --endpoints="${leader_ep}" \
      put "/phase10/gateway/write/${i}" "w-${i}" >/dev/null 2>&1; then
      put_fail=$((put_fail + 1))
    fi
    if ! etcdctl --dial-timeout=1s --command-timeout=2s --endpoints="${leader_ep}" \
      put "/registry/leases/gateway-heartbeat" "${i}" >/dev/null 2>&1; then
      hb_fail=$((hb_fail + 1))
    fi
    i=$((i + 1))
  done
  printf '%s %s\n' "${put_fail}" "${hb_fail}" >"${writer_counts}"
) &
writer_pid=$!

gateway_ghz_json="${tmp_dir}/gateway-ghz.json"
ghz --insecure \
  --proto "${SCRIPT_DIR}/proto/rpc.proto" \
  --call etcdserverpb.KV.Range \
  -d @"${gateway_range_data}" \
  -c "${PHASE10_GATEWAY_CONCURRENCY}" \
  --connections "${PHASE10_GATEWAY_CONNECTIONS}" \
  -z "${PHASE10_GATEWAY_DURATION}" \
  --timeout 10s \
  "${leader_ep}" \
  --format json --output "${gateway_ghz_json}"

wait "${writer_pid}" || true
if read -r gw_put_fail gw_hb_fail <"${writer_counts}"; then
  :
else
  gw_put_fail=0
  gw_hb_fail=999999
fi

python3 - <<'PY' \
"${gateway_ghz_json}" \
"${gw_put_fail}" \
"${gw_hb_fail}" \
"${PHASE10_GATEWAY_TARGET_RPS}" \
"${PHASE10_GATE_POLICY}" \
"${PHASE10_GATEWAY_DELTA_RPS_RATIO}" \
"${PHASE10_GATEWAY_DELTA_MAX_ERROR_RATE}" \
"${tmp_dir}/gateway.json"
from __future__ import annotations

import json
import sys
from pathlib import Path

ghz_path = Path(sys.argv[1])
put_fail = int(sys.argv[2])
hb_fail = int(sys.argv[3])
target_rps = float(sys.argv[4])
gate_policy = (sys.argv[5] or "delta-aware").strip().lower()
delta_rps_ratio = float(sys.argv[6])
delta_max_error_rate = float(sys.argv[7])
out_path = Path(sys.argv[8])

obj = json.loads(ghz_path.read_text(encoding="utf-8"))
status = {str(k).lower(): int(v) for k, v in (obj.get("statusCodeDistribution") or {}).items()}
ok = status.get("ok", 0)
count = int(obj.get("count") or 0)
errors = max(0, count - ok)
error_rate = (errors / count) if count > 0 else 1.0
lat = {str(x.get("percentage")): float(x.get("latency") or 0) for x in (obj.get("latencyDistribution") or [])}
p99_ns = lat.get("99")
p99_ms = (p99_ns / 1_000_000.0) if p99_ns is not None else None
rps = float(obj.get("rps") or 0)
delta_rps_floor = target_rps * max(0.0, delta_rps_ratio)

strict_pass = (
    rps >= target_rps
    and errors == 0
    and hb_fail == 0
)
delta_pass = (
    rps >= delta_rps_floor
    and error_rate <= delta_max_error_rate
    and hb_fail == 0
)
if gate_policy == "strict":
    pass_value = strict_pass
elif gate_policy == "hybrid":
    pass_value = strict_pass or delta_pass
else:
    pass_value = delta_pass

result = {
    "count": count,
    "ok": ok,
    "errors": errors,
    "error_rate": error_rate,
    "rps": rps,
    "p99_ms": p99_ms,
    "background_write_failures": put_fail,
    "heartbeat_failures": hb_fail,
    "gate_policy": gate_policy,
    "strict_pass": strict_pass,
    "delta_pass": delta_pass,
    "delta_rps_floor": delta_rps_floor,
    "delta_max_error_rate": delta_max_error_rate,
    "target_rps": target_rps,
    "pass": pass_value,
}
out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result, indent=2))
PY

phase6_log "phase10 ecosystem scenario: Patroni split-brain simulation"
leader_ep=$(phase10_start_backend_profile kubernetes)
(
  set +e
  end=$((SECONDS + PHASE10_PATRONI_LOAD_SECS))
  i=1
  while [ "${SECONDS}" -lt "${end}" ]; do
    etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" \
      put "/phase10/patroni/load/${i}" "load-${i}" >/dev/null 2>&1 || true
    i=$((i + 1))
  done
) &
load_pid=$!

python3 - <<'PY' "${leader_ep}" "${PHASE10_PATRONI_LEASE_OPS}" "${PHASE10_PATRONI_P99_TARGET_MS}" "${tmp_dir}/patroni.json"
from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from pathlib import Path

endpoint = sys.argv[1]
ops = int(sys.argv[2])
p99_target_ms = float(sys.argv[3])
out_path = Path(sys.argv[4])

lat = []
failures = 0
for i in range(1, ops + 1):
    t0 = time.perf_counter()
    proc = subprocess.run(
        [
            "etcdctl",
            "--dial-timeout=1s",
            "--command-timeout=3s",
            f"--endpoints={endpoint}",
            "put",
            "/registry/leases/patroni-leader",
            f"lease-{i}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    lat.append((time.perf_counter() - t0) * 1000.0)
    if proc.returncode != 0:
        failures += 1

arr = sorted(lat)
if not arr:
    p99 = 0.0
else:
    idx = min(len(arr) - 1, int((len(arr) - 1) * 0.99))
    p99 = arr[idx]

result = {
    "ops": ops,
    "failures": failures,
    "lease_update_p99_ms": p99,
    "pass": failures == 0 and p99 <= p99_target_ms,
}
out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result, indent=2))
PY

wait "${load_pid}" || true

python3 - <<'PY' "${tmp_dir}/cni.json" "${tmp_dir}/gateway.json" "${tmp_dir}/patroni.json" "${SUMMARY_JSON}"
from __future__ import annotations

import json
import sys
from pathlib import Path

cni = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
gateway = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
patroni = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
out_path = Path(sys.argv[4])

summary = {
    "cni": cni,
    "gateway": gateway,
    "patroni": patroni,
    "pass": bool(cni.get("pass")) and bool(gateway.get("pass")) and bool(patroni.get("pass")),
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase10 ecosystem summary: ${SUMMARY_JSON}"
