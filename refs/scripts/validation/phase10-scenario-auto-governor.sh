#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase6_require_tools docker etcdctl curl python3

: "${ASTRAD_PROFILE:=auto}"
: "${ASTRAD_PROFILE_SAMPLE_SECS:=5}"
: "${ASTRAD_PROFILE_MIN_DWELL_SECS:=5}"

: "${PHASE10_AUTO_SEED_KEYS:=2000}"
: "${PHASE10_AUTO_READ_GET_OPS:=6000}"
: "${PHASE10_AUTO_READ_LIST_OPS:=400}"
: "${PHASE10_AUTO_WRITE_OPS:=8000}"
: "${PHASE10_AUTO_WATCH_STREAMS:=24}"
: "${PHASE10_AUTO_WATCH_DURATION_SECS:=20}"
: "${PHASE10_AUTO_SETTLE_SECS:=10}"

SUMMARY_JSON="${RUN_DIR}/phase10-auto-governor-summary.json"
SNAP_DIR="${RUN_DIR}/phase10-auto-governor"
mkdir -p "${SNAP_DIR}"

phase10_metrics_endpoint_for() {
  case "$1" in
    127.0.0.1:2379) echo "127.0.0.1:19479" ;;
    127.0.0.1:32391) echo "127.0.0.1:19480" ;;
    127.0.0.1:32392) echo "127.0.0.1:19481" ;;
    *) return 1 ;;
  esac
}

phase10_capture_snapshot() {
  local phase=${1:?phase required}
  local metrics_ep=${2:?metrics endpoint required}
  local out_file=${3:?output file required}
  local raw="${SNAP_DIR}/${phase}.metrics.txt"

  curl -fsS "http://${metrics_ep}/metrics" >"${raw}"
  python3 - <<'PY' "${raw}" "${phase}" "${out_file}"
import json
import re
import sys
from pathlib import Path

raw_path = Path(sys.argv[1])
phase = sys.argv[2]
out_path = Path(sys.argv[3])
text = raw_path.read_text(encoding="utf-8", errors="ignore")

vals: dict[str, float] = {}
for line in text.splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    parts = line.split()
    if len(parts) != 2:
        continue
    name, value = parts
    try:
        vals[name] = float(value)
    except Exception:
        continue

active = "unknown"
if vals.get("astra_profile_active_kubernetes", 0.0) >= 1.0:
    active = "kubernetes"
elif vals.get("astra_profile_active_omni", 0.0) >= 1.0:
    active = "omni"
elif vals.get("astra_profile_active_gateway", 0.0) >= 1.0:
    active = "gateway"
elif vals.get("astra_profile_active_auto", 0.0) >= 1.0:
    active = "auto"

obj = {
    "phase": phase,
    "active_profile": active,
    "profile_switch_total": vals.get("astra_profile_switch_total"),
    "applied": {
        "put_max_requests": vals.get("astra_profile_applied_put_max_requests"),
        "put_linger_us": vals.get("astra_profile_applied_put_linger_microseconds"),
        "prefetch_entries": vals.get("astra_profile_applied_prefetch_entries"),
        "bg_io_tokens_per_sec": vals.get("astra_profile_applied_bg_io_tokens_per_sec"),
    },
    "request_mix_totals": {
        "get": vals.get("astra_request_get_total"),
        "list": vals.get("astra_request_list_total"),
        "put": vals.get("astra_request_put_total"),
        "delete": vals.get("astra_request_delete_total"),
        "txn": vals.get("astra_request_txn_total"),
        "lease": vals.get("astra_request_lease_total"),
        "watch": vals.get("astra_request_watch_total"),
        "tier0": vals.get("astra_request_tier0_total"),
    },
}
out_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
print(json.dumps(obj, indent=2))
PY
}

cleanup() {
  phase6_stop_k3s || true
  phase6_stop_backend all || true
}
trap cleanup EXIT

phase6_stop_backend all || true
phase6_start_backend astra
leader_ep=$(phase6_find_astra_leader 120 1)
metrics_ep=$(phase10_metrics_endpoint_for "${leader_ep}")
phase6_log "phase10 auto-governor scenario leader=${leader_ep} metrics=${metrics_ep}"

# Seed data for read/list workloads.
for i in $(seq 1 "${PHASE10_AUTO_SEED_KEYS}"); do
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" \
    put "/phase10/auto/k/${i}" "v-${i}" >/dev/null
done

phase10_capture_snapshot "baseline" "${metrics_ep}" "${SNAP_DIR}/baseline.json"

# Phase A: kubernetes-like read/list dominance.
for i in $(seq 1 "${PHASE10_AUTO_READ_GET_OPS}"); do
  key_id=$(( (i % PHASE10_AUTO_SEED_KEYS) + 1 ))
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" \
    get "/phase10/auto/k/${key_id}" >/dev/null
done
for _ in $(seq 1 "${PHASE10_AUTO_READ_LIST_OPS}"); do
  etcdctl --dial-timeout=1s --command-timeout=4s --endpoints="${leader_ep}" \
    get "/phase10/auto/k/" --prefix --keys-only >/dev/null
done
sleep "${PHASE10_AUTO_SETTLE_SECS}"
phase10_capture_snapshot "read_heavy" "${metrics_ep}" "${SNAP_DIR}/read_heavy.json"

# Phase B: omni-like write dominance.
for i in $(seq 1 "${PHASE10_AUTO_WRITE_OPS}"); do
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" \
    put "/phase10/auto/w/${i}" "write-${i}" >/dev/null
done
sleep "${PHASE10_AUTO_SETTLE_SECS}"
phase10_capture_snapshot "write_heavy" "${metrics_ep}" "${SNAP_DIR}/write_heavy.json"

# Phase C: gateway-like watch fan-out.
watch_pids=()
for idx in $(seq 1 "${PHASE10_AUTO_WATCH_STREAMS}"); do
  timeout "${PHASE10_AUTO_WATCH_DURATION_SECS}s" \
    etcdctl --dial-timeout=1s --command-timeout=4s --endpoints="${leader_ep}" \
    watch --prefix "/phase10/auto/watch/" >/dev/null 2>&1 &
  watch_pids+=("$!")
done
watch_end=$((SECONDS + PHASE10_AUTO_WATCH_DURATION_SECS))
widx=1
while [ "${SECONDS}" -lt "${watch_end}" ]; do
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" \
    put "/phase10/auto/watch/${widx}" "watch-${widx}" >/dev/null || true
  widx=$((widx + 1))
done
for pid in "${watch_pids[@]}"; do
  wait "${pid}" || true
done
sleep "${PHASE10_AUTO_SETTLE_SECS}"
phase10_capture_snapshot "watch_heavy" "${metrics_ep}" "${SNAP_DIR}/watch_heavy.json"

python3 - <<'PY' "${SNAP_DIR}" "${SUMMARY_JSON}"
from __future__ import annotations

import json
import sys
from pathlib import Path

snap_dir = Path(sys.argv[1])
out_path = Path(sys.argv[2])

order = ["baseline", "read_heavy", "write_heavy", "watch_heavy"]
snaps = []
for name in order:
    p = snap_dir / f"{name}.json"
    if p.exists():
        snaps.append(json.loads(p.read_text(encoding="utf-8")))

active_profiles = [s.get("active_profile") for s in snaps if s.get("active_profile")]
unique_profiles = sorted({p for p in active_profiles if p != "unknown"})
switch_total = snaps[-1].get("profile_switch_total") if snaps else None
switch_total = int(switch_total) if switch_total is not None else 0

summary = {
    "snapshots": snaps,
    "active_profiles_observed": unique_profiles,
    "profile_switch_total": switch_total,
    "pass": len(unique_profiles) >= 2 and switch_total >= 1,
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase10 auto-governor summary: ${SUMMARY_JSON}"
