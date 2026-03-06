#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase12-common.sh
source "${SCRIPT_DIR}/phase12-common.sh"

phase12_require_tools docker cargo ghz python3 etcdctl
: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra-alpha:phase12-<sha>}"

: "${PHASE12_TOMBSTONE_DURATION_SECS:=86400}"
: "${PHASE12_TOMBSTONE_KEYS:=1000}"
: "${PHASE12_TOMBSTONE_TARGET_RPS:=5000}"
: "${PHASE12_TOMBSTONE_CONCURRENCY:=256}"
: "${PHASE12_TOMBSTONE_PAYLOAD_SIZE:=256}"
: "${PHASE12_TOMBSTONE_SAMPLE_INTERVAL_SECS:=60}"
: "${PHASE12_TOMBSTONE_LIST_DURATION:=20s}"
: "${PHASE12_TOMBSTONE_LIST_CONCURRENCY:=32}"
: "${PHASE12_TOMBSTONE_LIST_CONNECTIONS:=16}"
: "${PHASE12_TOMBSTONE_LIST_P99_REGRESSION_MAX:=1.50}"
: "${PHASE12_TOMBSTONE_DISK_GROWTH_RATIO_MAX:=0.20}"
: "${PHASE12_TOMBSTONE_WAL_SEGMENT_BYTES:=8388608}"
: "${PHASE12_TOMBSTONE_WAL_CHECKPOINT_TRIGGER_BYTES:=134217728}"
: "${PHASE12_TOMBSTONE_WAL_CHECKPOINT_MIN_INTERVAL_SECS:=60}"
: "${PHASE12_TOMBSTONE_RAFT_SNAPSHOT_LOGS_SINCE_LAST:=128}"
: "${PHASE12_TOMBSTONE_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP:=16}"
: "${PHASE12_TOMBSTONE_RAFT_PURGE_BATCH_SIZE:=1024}"

OUT_DIR="${RUN_DIR}/phase12-tombstone-soak"
SUMMARY_JSON="${RUN_DIR}/phase12-tombstone-soak-summary.json"
mkdir -p "${OUT_DIR}"

cleanup() {
  if [ -n "${DISK_SAMPLER_PID:-}" ]; then
    kill "${DISK_SAMPLER_PID}" >/dev/null 2>&1 || true
  fi
  phase6_stop_backend all || true
}
trap cleanup EXIT

export ASTRAD_METRICS_ENABLED=true
export ASTRAD_WAL_SEGMENT_BYTES="${PHASE12_TOMBSTONE_WAL_SEGMENT_BYTES}"
export ASTRAD_WAL_CHECKPOINT_ENABLED=true
export ASTRAD_WAL_CHECKPOINT_TRIGGER_BYTES="${PHASE12_TOMBSTONE_WAL_CHECKPOINT_TRIGGER_BYTES}"
export ASTRAD_WAL_CHECKPOINT_MIN_INTERVAL_SECS="${PHASE12_TOMBSTONE_WAL_CHECKPOINT_MIN_INTERVAL_SECS}"
export ASTRAD_RAFT_SNAPSHOT_LOGS_SINCE_LAST="${PHASE12_TOMBSTONE_RAFT_SNAPSHOT_LOGS_SINCE_LAST}"
export ASTRAD_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP="${PHASE12_TOMBSTONE_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP}"
export ASTRAD_RAFT_PURGE_BATCH_SIZE="${PHASE12_TOMBSTONE_RAFT_PURGE_BATCH_SIZE}"
phase6_log "phase12 tombstone-soak: starting backend"
phase6_start_backend astra
leader_ep=$(phase6_find_astra_leader 120 1)
leader_metrics_port=$(phase12_endpoint_to_metrics_port "${leader_ep}")

range_req="${OUT_DIR}/range-request.json"
phase12_write_range_request_json "${range_req}" "/phase12/tombstone/endpointslice" true

run_list_bench() {
  local out_json=${1:?out json required}
  ghz --insecure \
    --proto "${SCRIPT_DIR}/proto/rpc.proto" \
    --call etcdserverpb.KV.Range \
    --data-file "${range_req}" \
    --duration "${PHASE12_TOMBSTONE_LIST_DURATION}" \
    --concurrency "${PHASE12_TOMBSTONE_LIST_CONCURRENCY}" \
    --connections "${PHASE12_TOMBSTONE_LIST_CONNECTIONS}" \
    --format json \
    "${leader_ep}" >"${out_json}"
}

phase6_log "phase12 tombstone-soak: seeding keyset"
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${leader_ep}" \
    put-storm \
    --prefix /phase12/tombstone/endpointslice \
    --keys "${PHASE12_TOMBSTONE_KEYS}" \
    --payload-size "${PHASE12_TOMBSTONE_PAYLOAD_SIZE}" \
    --duration-secs 20 \
    --concurrency 64 \
    --target-rps "${PHASE12_TOMBSTONE_TARGET_RPS}" \
    --output "${OUT_DIR}/seed-put-summary.json"
) >"${OUT_DIR}/seed-put.log" 2>&1

run_list_bench "${OUT_DIR}/list-before.json"

vol_mount=""
if docker volume inspect phase6_phase6-astra-node1-data >/dev/null 2>&1; then
  vol_mount=$(docker volume inspect phase6_phase6-astra-node1-data --format '{{.Mountpoint}}' 2>/dev/null || true)
fi
if [ -z "${vol_mount}" ]; then
  vol_mount="/var/lib/docker"
fi
wal_path="${vol_mount}/unified-raft.wal"

disk_log="${OUT_DIR}/disk-usage.log"
(
  while true; do
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    bytes=$(du -sb "${vol_mount}" 2>/dev/null | awk '{print $1}')
    metrics_tmp=$(mktemp)
    curl -fsS "http://127.0.0.1:${leader_metrics_port}/metrics" >"${metrics_tmp}" 2>/dev/null || true
    wal_logical=$(awk '$1=="astra_wal_logical_bytes" {print $2}' "${metrics_tmp}" | tail -n1)
    wal_allocated=$(awk '$1=="astra_wal_allocated_bytes" {print $2}' "${metrics_tmp}" | tail -n1)
    wal_checkpoints=$(awk '$1=="astra_wal_checkpoint_total" {print $2}' "${metrics_tmp}" | tail -n1)
    wal_reclaimed=$(awk '$1=="astra_wal_checkpoint_reclaimed_bytes_total" {print $2}' "${metrics_tmp}" | tail -n1)
    rm -f "${metrics_tmp}"
    wal_file_bytes=$(stat -c '%s' "${wal_path}" 2>/dev/null || echo 0)
    wal_file_blocks=$(stat -c '%b' "${wal_path}" 2>/dev/null || echo 0)
    wal_file_allocated=$(( wal_file_blocks * 512 ))
    echo "${ts} ${bytes:-0} ${wal_logical:-0} ${wal_allocated:-0} ${wal_file_bytes:-0} ${wal_file_allocated:-0} ${wal_checkpoints:-0} ${wal_reclaimed:-0}"
    sleep "${PHASE12_TOMBSTONE_SAMPLE_INTERVAL_SECS}"
  done
) >"${disk_log}" 2>/dev/null &
DISK_SAMPLER_PID=$!

phase6_log "phase12 tombstone-soak: running churn duration=${PHASE12_TOMBSTONE_DURATION_SECS}s"
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${leader_ep}" \
    put-storm \
    --prefix /phase12/tombstone/endpointslice \
    --keys "${PHASE12_TOMBSTONE_KEYS}" \
    --payload-size "${PHASE12_TOMBSTONE_PAYLOAD_SIZE}" \
    --duration-secs "${PHASE12_TOMBSTONE_DURATION_SECS}" \
    --concurrency "${PHASE12_TOMBSTONE_CONCURRENCY}" \
    --target-rps "${PHASE12_TOMBSTONE_TARGET_RPS}" \
    --output "${OUT_DIR}/soak-put-summary.json"
) >"${OUT_DIR}/soak-put.log" 2>&1

kill "${DISK_SAMPLER_PID}" >/dev/null 2>&1 || true
wait "${DISK_SAMPLER_PID}" 2>/dev/null || true
unset DISK_SAMPLER_PID

run_list_bench "${OUT_DIR}/list-after.json"

python3 - <<'PY' \
  "${OUT_DIR}/list-before.json" \
  "${OUT_DIR}/list-after.json" \
  "${OUT_DIR}/seed-put-summary.json" \
  "${OUT_DIR}/soak-put-summary.json" \
  "${disk_log}" \
  "${SUMMARY_JSON}" \
  "${leader_ep}" \
  "${PHASE12_TOMBSTONE_DURATION_SECS}" \
  "${PHASE12_TOMBSTONE_LIST_P99_REGRESSION_MAX}" \
  "${PHASE12_TOMBSTONE_DISK_GROWTH_RATIO_MAX}"
from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

before_path = Path(sys.argv[1])
after_path = Path(sys.argv[2])
seed_put_path = Path(sys.argv[3])
soak_put_path = Path(sys.argv[4])
disk_log_path = Path(sys.argv[5])
out_path = Path(sys.argv[6])
leader_ep = sys.argv[7]
duration_secs = int(sys.argv[8])
list_p99_reg_max = float(sys.argv[9])
disk_growth_ratio_max = float(sys.argv[10])

before = json.loads(before_path.read_text(encoding="utf-8")) if before_path.exists() else {}
after = json.loads(after_path.read_text(encoding="utf-8")) if after_path.exists() else {}
seed_put = json.loads(seed_put_path.read_text(encoding="utf-8")) if seed_put_path.exists() else {}
soak_put = json.loads(soak_put_path.read_text(encoding="utf-8")) if soak_put_path.exists() else {}

def ghz_p99_ms(obj: dict) -> float:
    summary = obj.get("summary")
    if not isinstance(summary, dict) or not summary:
        summary = obj
    dist = summary.get("latencyDistribution") or obj.get("latencyDistribution") or []
    for row in dist:
        pct = str(row.get("percentage") or "")
        if pct.startswith("99"):
            return float(row.get("latency") or 0.0) / 1_000_000.0
    return 0.0

def ghz_rps(obj: dict) -> float:
    summary = obj.get("summary")
    if not isinstance(summary, dict) or not summary:
        summary = obj
    return float(summary.get("rps") or obj.get("rps") or 0.0)

before_p99 = ghz_p99_ms(before)
after_p99 = ghz_p99_ms(after)
before_rps = ghz_rps(before)
after_rps = ghz_rps(after)
p99_ratio = (after_p99 / before_p99) if before_p99 > 0 else math.inf

samples = []
if disk_log_path.exists():
    for line in disk_log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.strip().split()
        if len(parts) != 8:
            continue
        try:
            samples.append(
                {
                    "ts": parts[0],
                    "volume_bytes": int(parts[1]),
                    "wal_logical_bytes": int(float(parts[2])),
                    "wal_allocated_bytes": int(float(parts[3])),
                    "wal_file_bytes": int(parts[4]),
                    "wal_file_allocated_bytes": int(parts[5]),
                    "checkpoint_total": int(float(parts[6])),
                    "reclaimed_bytes_total": int(float(parts[7])),
                }
            )
        except Exception:
            pass

start_bytes = samples[0]["volume_bytes"] if samples else 0
end_bytes = samples[-1]["volume_bytes"] if samples else 0
max_bytes = max((row["volume_bytes"] for row in samples), default=0)

wal_allocated_start = samples[0]["wal_allocated_bytes"] if samples else 0
wal_allocated_end = samples[-1]["wal_allocated_bytes"] if samples else 0
wal_allocated_max = max((row["wal_allocated_bytes"] for row in samples), default=0)
wal_logical_start = samples[0]["wal_logical_bytes"] if samples else 0
wal_logical_end = samples[-1]["wal_logical_bytes"] if samples else 0
wal_logical_max = max((row["wal_logical_bytes"] for row in samples), default=0)
wal_checkpoints = samples[-1]["checkpoint_total"] if samples else 0
wal_reclaimed_total = samples[-1]["reclaimed_bytes_total"] if samples else 0

half = len(samples) // 2
first_half_growth = 0
second_half_growth = 0
volume_first_half_growth = 0
volume_second_half_growth = 0
if len(samples) >= 4:
    first_half_growth = (
        max(0, samples[half - 1]["wal_allocated_bytes"] - samples[0]["wal_allocated_bytes"])
        if half > 0
        else 0
    )
    second_half_growth = max(
        0, samples[-1]["wal_allocated_bytes"] - samples[half]["wal_allocated_bytes"]
    )
    volume_first_half_growth = (
        max(0, samples[half - 1]["volume_bytes"] - samples[0]["volume_bytes"])
        if half > 0
        else 0
    )
    volume_second_half_growth = max(
        0, samples[-1]["volume_bytes"] - samples[half]["volume_bytes"]
    )

growth_ratio = (second_half_growth / first_half_growth) if first_half_growth > 0 else 0.0
volume_growth_ratio = (
    volume_second_half_growth / volume_first_half_growth if volume_first_half_growth > 0 else 0.0
)

soak_rps = float(soak_put.get("rps") or 0.0)
soak_ok = int(soak_put.get("ok") or 0)

pass_gate = (
    soak_ok > 0
    and p99_ratio <= list_p99_reg_max
    and growth_ratio <= disk_growth_ratio_max
    and wal_checkpoints > 0
)

summary = {
    "scenario": "tombstone_soak",
    "leader_endpoint": leader_ep,
    "duration_secs": duration_secs,
    "seed_put": seed_put,
    "soak_put": soak_put,
    "list": {
        "before": {
            "rps": before_rps,
            "p99_ms": before_p99,
        },
        "after": {
            "rps": after_rps,
            "p99_ms": after_p99,
        },
        "p99_regression_ratio": p99_ratio,
        "p99_regression_ratio_max": list_p99_reg_max,
    },
    "disk": {
        "sample_count": len(samples),
        "volume_start_bytes": start_bytes,
        "volume_end_bytes": end_bytes,
        "volume_max_bytes": max_bytes,
        "volume_first_half_growth_bytes": volume_first_half_growth,
        "volume_second_half_growth_bytes": volume_second_half_growth,
        "volume_growth_ratio_second_over_first": volume_growth_ratio,
        "wal_logical_start_bytes": wal_logical_start,
        "wal_logical_end_bytes": wal_logical_end,
        "wal_logical_max_bytes": wal_logical_max,
        "wal_allocated_start_bytes": wal_allocated_start,
        "wal_allocated_end_bytes": wal_allocated_end,
        "wal_allocated_max_bytes": wal_allocated_max,
        "first_half_growth_bytes": first_half_growth,
        "second_half_growth_bytes": second_half_growth,
        "growth_ratio_second_over_first": growth_ratio,
        "growth_ratio_max": disk_growth_ratio_max,
        "checkpoint_total": wal_checkpoints,
        "checkpoint_reclaimed_bytes_total": wal_reclaimed_total,
    },
    "notes": [
        "Full 24h objective is supported via PHASE12_TOMBSTONE_DURATION_SECS=86400.",
        "Current run duration may be reduced for practical execution windows; evaluate trend slope accordingly.",
    ],
    "pass": pass_gate,
}

out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase12 tombstone-soak summary: ${SUMMARY_JSON}"
