#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase6_require_tools docker etcdctl python3 k3s kubectl

: "${PHASE10_QOS_SWAPOFF_QUICK:=true}"
: "${PHASE10_QOS_SWAPOFF_NODES:=200}"
: "${PHASE10_QOS_SWAPOFF_PODS:=5000}"
: "${PHASE10_QOS_SWAPOFF_CONFIGMAPS:=1000}"
: "${PHASE10_QOS_SWAPOFF_MEM_LIMIT:=450m}"
: "${PHASE10_QOS_SWAPOFF_IOPS:=160}"
: "${PHASE10_QOS_SWAPOFF_BPS:=10485760}"

SUMMARY_JSON="${RUN_DIR}/phase10-qos-swapoff-summary.json"

mapfile -t PHASE10_SWAP_DEVICES < <(swapon --noheadings --show=NAME 2>/dev/null | awk '{print $1}')
SWAP_ENABLED=false
if [ "${#PHASE10_SWAP_DEVICES[@]}" -gt 0 ]; then
  SWAP_ENABLED=true
fi
SWAPPINESS_BEFORE=$(sysctl -n vm.swappiness 2>/dev/null || echo "60")
SWAP_DISABLED=false

cleanup() {
  if [ "${SWAP_DISABLED}" = "true" ] && [ "${SWAP_ENABLED}" = "true" ]; then
    for dev in "${PHASE10_SWAP_DEVICES[@]}"; do
      [ -n "${dev}" ] || continue
      swapon "${dev}" >/dev/null 2>&1 || true
    done
  fi
  if [ -n "${SWAPPINESS_BEFORE:-}" ]; then
    sysctl -w vm.swappiness="${SWAPPINESS_BEFORE}" >/dev/null 2>&1 || true
  fi
  phase6_stop_k3s || true
  phase6_stop_backend all || true
}
trap cleanup EXIT

if [ "${SWAP_ENABLED}" = "true" ]; then
  phase6_log "phase10 qos swapoff scenario: disabling swap devices=${PHASE10_SWAP_DEVICES[*]}"
  swapoff -a
  SWAP_DISABLED=true
fi

export ASTRAD_PROFILE=${ASTRAD_PROFILE:-kubernetes}
export ASTRAD_PROFILE_SAMPLE_SECS=${ASTRAD_PROFILE_SAMPLE_SECS:-5}
export ASTRAD_PROFILE_MIN_DWELL_SECS=${ASTRAD_PROFILE_MIN_DWELL_SECS:-5}
export ASTRAD_QOS_TIER0_PREFIXES=${ASTRAD_QOS_TIER0_PREFIXES:-/registry/leases/,/omni/locks/}
export ASTRAD_QOS_TIER0_SUFFIXES=${ASTRAD_QOS_TIER0_SUFFIXES:-/leader,/lock}
export ASTRAD_QOS_TIER0_MAX_BATCH_REQUESTS=${ASTRAD_QOS_TIER0_MAX_BATCH_REQUESTS:-32}
export ASTRAD_QOS_TIER0_MAX_LINGER_US=${ASTRAD_QOS_TIER0_MAX_LINGER_US:-0}
export PHASE6_ASTRA_MEM_LIMIT=${PHASE6_ASTRA_MEM_LIMIT:-${PHASE10_QOS_SWAPOFF_MEM_LIMIT}}
export PHASE6_ASTRA_NODE_IOPS=${PHASE6_ASTRA_NODE_IOPS:-${PHASE10_QOS_SWAPOFF_IOPS}}
export PHASE6_ASTRA_NODE_BPS=${PHASE6_ASTRA_NODE_BPS:-${PHASE10_QOS_SWAPOFF_BPS}}
export PHASE9_DISABLE_TIERING_FOR_K3S=true
export PHASE9_TELEMETRY_WARMUP_SECS=${PHASE9_TELEMETRY_WARMUP_SECS:-30}

bench_rc=0
if [ "${PHASE10_QOS_SWAPOFF_QUICK}" = "true" ]; then
  set +e
  "${SCRIPT_DIR}/phase6-k3s-benchmark.sh" --backend astra --quick \
    --nodes "${PHASE10_QOS_SWAPOFF_NODES}" \
    --pods "${PHASE10_QOS_SWAPOFF_PODS}" \
    --configmaps "${PHASE10_QOS_SWAPOFF_CONFIGMAPS}"
  bench_rc=$?
  set -e
else
  set +e
  "${SCRIPT_DIR}/phase6-k3s-benchmark.sh" --backend astra \
    --nodes "${PHASE10_QOS_SWAPOFF_NODES}" \
    --pods "${PHASE10_QOS_SWAPOFF_PODS}" \
    --configmaps "${PHASE10_QOS_SWAPOFF_CONFIGMAPS}"
  bench_rc=$?
  set -e
fi

k3s_log="${RUN_DIR}/k3s-astra.log"
leadership_lost=false
if [ -f "${k3s_log}" ] && grep -q "leaderelection lost for k3s" "${k3s_log}"; then
  leadership_lost=true
fi

python3 - <<'PY' \
"${SUMMARY_JSON}" \
"${RUN_DIR}/phase6-k3s-astra-summary.json" \
"${bench_rc}" \
"${SWAP_ENABLED}" \
"${SWAP_DISABLED}" \
"${leadership_lost}" \
"${k3s_log}" \
"${PHASE10_SWAP_DEVICES[*]}"
from __future__ import annotations

import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
astra_summary_path = Path(sys.argv[2])
bench_rc = int(sys.argv[3])
swap_enabled = sys.argv[4].strip().lower() == "true"
swap_disabled = sys.argv[5].strip().lower() == "true"
leadership_lost = sys.argv[6].strip().lower() == "true"
k3s_log = sys.argv[7]
swap_devices = [x for x in sys.argv[8].split() if x]

astra = {}
if astra_summary_path.exists():
    try:
        astra = json.loads(astra_summary_path.read_text(encoding="utf-8"))
    except Exception:
        astra = {}

actual = astra.get("actual") or {}
lat = (astra.get("latency_ms") or {}).get("put") or {}

summary = {
    "bench_rc": bench_rc,
    "swap_enabled_before": swap_enabled,
    "swap_disabled_during_run": swap_disabled,
    "swap_devices": swap_devices,
    "leaderelection_lost": leadership_lost,
    "k3s_log": k3s_log,
    "astra_summary": astra,
    "pass": (
        bench_rc == 0
        and not leadership_lost
        and int(actual.get("pods") or 0) > 0
    ),
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase10 qos swapoff summary: ${SUMMARY_JSON}"
