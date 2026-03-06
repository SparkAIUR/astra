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

SWEEP_ID=${SWEEP_ID:-$(date +%Y%m%d-%H%M%S)}
SWEEP_PREFIX=${SWEEP_PREFIX:-phase4-sweep-${SWEEP_ID}}
SWEEP_CSV="${RESULTS_DIR}/${SWEEP_PREFIX}.csv"
SWEEP_JSON="${RESULTS_DIR}/${SWEEP_PREFIX}.json"

echo "case_id,mode,layout,astra_rps,astra_p99_ns,etcd_rps,phase4_pass,queue_wait_p99_ms,quorum_ack_p99_ms,summary_path" > "${SWEEP_CSV}"

LAST_SUMMARY_PATH=""

append_row() {
  local case_id=$1
  local mode=$2
  local layout=$3
  local summary_path=$4
  python3 - <<'PY' "${case_id}" "${mode}" "${layout}" "${summary_path}" >> "${SWEEP_CSV}"
import json
import pathlib
import sys

case_id, mode, layout, summary_path = sys.argv[1:5]
p = pathlib.Path(summary_path)
obj = json.loads(p.read_text())

astra = obj.get("astra") or {}
etcd = obj.get("etcd") or {}
timeline = obj.get("timeline") or {}

def p99_from_maybe_summary(value):
    if isinstance(value, dict):
        v = value.get("p99_ms", 0)
        if isinstance(v, dict):
            v = v.get("p99_ms", 0)
        try:
            return float(v or 0)
        except Exception:
            return 0.0
    return 0.0

queue_wait = p99_from_maybe_summary(timeline.get("queue_wait_dispatch"))
quorum = p99_from_maybe_summary(timeline.get("quorum_ack_commit"))

def csv(v):
    s = "" if v is None else str(v)
    return '"' + s.replace('"', '""') + '"'

row = [
    case_id,
    mode,
    layout,
    astra.get("rps", 0),
    astra.get("p99_ns", 0),
    etcd.get("rps", 0),
    obj.get("phase4_pass", False),
    queue_wait,
    quorum,
    summary_path,
]
print(",".join(csv(v) for v in row))
PY
}

run_case() {
  local case_id=$1
  local mode=$2
  local layout=$3
  local env_string=${4:-}

  local run_id="${SWEEP_PREFIX}-${case_id}-${mode}"
  local -a env_args
  env_args=(
    "ASTRA_IMAGE=${ASTRA_IMAGE}"
    "ASTRA_RUN_ID=${run_id}"
    "ASTRA_SCENARIO_MODE=${mode}"
  )

  if [ "${layout}" = "mnt" ]; then
    mkdir -p /mnt/astra-node1 /mnt/astra-node2 /mnt/astra-node3
    env_args+=(
      "ASTRA_BLKIO_DEVICE=/dev/vdb"
      "ASTRA_NODE1_DATA=/mnt/astra-node1"
      "ASTRA_NODE2_DATA=/mnt/astra-node2"
      "ASTRA_NODE3_DATA=/mnt/astra-node3"
    )
  else
    env_args+=("ASTRA_BLKIO_DEVICE=${ASTRA_BLKIO_DEVICE:-/dev/vda}")
  fi

  if [ -n "${env_string}" ]; then
    read -r -a extra_env <<< "${env_string}"
    env_args+=("${extra_env[@]}")
  fi

  echo "[sweep] running case=${case_id} mode=${mode} layout=${layout}"
  env "${env_args[@]}" "${SCRIPT_DIR}/phase4-scenario-b.sh"

  LAST_SUMMARY_PATH="${RESULTS_DIR}/phase4-scenario-b-${run_id}-summary.json"
  if [ ! -f "${LAST_SUMMARY_PATH}" ]; then
    echo "missing summary file for case ${case_id}: ${LAST_SUMMARY_PATH}" >&2
    exit 1
  fi

  append_row "${case_id}" "${mode}" "${layout}" "${LAST_SUMMARY_PATH}"
}

run_case "baseline-rootfs" "full" "rootfs"
BASE_ROOTFS_SUMMARY="${LAST_SUMMARY_PATH}"
run_case "baseline-mnt" "full" "mnt"
BASE_MNT_SUMMARY="${LAST_SUMMARY_PATH}"

BEST_LAYOUT=$(python3 - <<'PY' "${BASE_ROOTFS_SUMMARY}" "${BASE_MNT_SUMMARY}"
import json
import pathlib
import sys

rootfs = json.loads(pathlib.Path(sys.argv[1]).read_text())
mnt = json.loads(pathlib.Path(sys.argv[2]).read_text())
rootfs_rps = (rootfs.get("astra") or {}).get("rps", 0) or 0
mnt_rps = (mnt.get("astra") or {}).get("rps", 0) or 0
print("mnt" if mnt_rps > rootfs_rps else "rootfs")
PY
)
echo "[sweep] selected layout=${BEST_LAYOUT}"

declare -a CASE_MATRIX
CASE_MATRIX=(
  "static256|ASTRAD_PUT_ADAPTIVE_ENABLED=false ASTRAD_PUT_BATCH_MIN_REQUESTS=256 ASTRAD_PUT_BATCH_MAX_REQUESTS=256 ASTRAD_PUT_BATCH_MIN_LINGER_US=50 ASTRAD_PUT_BATCH_MAX_LINGER_US=200 ASTRAD_PUT_BATCH_MAX_BYTES=524288"
  "static512|ASTRAD_PUT_ADAPTIVE_ENABLED=false ASTRAD_PUT_BATCH_MIN_REQUESTS=512 ASTRAD_PUT_BATCH_MAX_REQUESTS=512 ASTRAD_PUT_BATCH_MIN_LINGER_US=50 ASTRAD_PUT_BATCH_MAX_LINGER_US=200 ASTRAD_PUT_BATCH_MAX_BYTES=1048576"
  "static1024|ASTRAD_PUT_ADAPTIVE_ENABLED=false ASTRAD_PUT_BATCH_MIN_REQUESTS=1024 ASTRAD_PUT_BATCH_MAX_REQUESTS=1024 ASTRAD_PUT_BATCH_MIN_LINGER_US=100 ASTRAD_PUT_BATCH_MAX_LINGER_US=300 ASTRAD_PUT_BATCH_MAX_BYTES=2097152"
  "adapt_relaxed_1|ASTRAD_PUT_ADAPTIVE_ENABLED=true ASTRAD_PUT_TARGET_QUEUE_DEPTH=2048 ASTRAD_PUT_TARGET_QUEUE_WAIT_P99_MS=600 ASTRAD_PUT_TARGET_QUORUM_ACK_P99_MS=120"
  "adapt_relaxed_2|ASTRAD_PUT_ADAPTIVE_ENABLED=true ASTRAD_PUT_TARGET_QUEUE_DEPTH=4096 ASTRAD_PUT_TARGET_QUEUE_WAIT_P99_MS=900 ASTRAD_PUT_TARGET_QUORUM_ACK_P99_MS=180"
  "wal_high_batch|ASTRAD_WAL_MAX_BATCH_REQUESTS=2000 ASTRAD_WAL_BATCH_MAX_BYTES=16777216 ASTRAD_WAL_MAX_LINGER_US=500"
  "wal_low_linger|ASTRAD_WAL_MAX_BATCH_REQUESTS=500 ASTRAD_WAL_BATCH_MAX_BYTES=4194304 ASTRAD_WAL_MAX_LINGER_US=250"
  "dispatch2|ASTRAD_PUT_DISPATCH_CONCURRENCY=2"
  "dispatch4|ASTRAD_PUT_DISPATCH_CONCURRENCY=4"
  "token_off|ASTRAD_PUT_TOKEN_LANE_ENABLED=false"
  "timeline_sparse|ASTRAD_RAFT_TIMELINE_SAMPLE_RATE=1024"
  "no_blkio_diag|ASTRA_BLKIO_SHAPING=false"
)

for i in "${!CASE_MATRIX[@]}"; do
  IFS='|' read -r case_id case_env <<< "${CASE_MATRIX[$i]}"
  run_case "${case_id}" "astra_only" "${BEST_LAYOUT}" "${case_env}"

  if [ $(( (i + 1) % 4 )) -eq 0 ]; then
    run_case "${case_id}-anchor" "full" "${BEST_LAYOUT}" "${case_env}"
  fi
done

python3 - <<'PY' "${SWEEP_CSV}" "${SWEEP_JSON}"
import csv
import json
import pathlib
import sys

csv_path = pathlib.Path(sys.argv[1])
json_path = pathlib.Path(sys.argv[2])
rows = list(csv.DictReader(csv_path.read_text().splitlines()))

best = None
for row in rows:
    if row["mode"] != "astra_only":
        continue
    try:
        rps = float(row["astra_rps"])
    except Exception:
        rps = 0.0
    if best is None or rps > best[0]:
        best = (rps, row)

payload = {
    "csv": str(csv_path),
    "total_rows": len(rows),
    "best_astra_only": best[1] if best else None,
    "rows": rows,
}
json_path.write_text(json.dumps(payload, indent=2))
print(json.dumps(payload, indent=2))
PY

echo "sweep_csv=${SWEEP_CSV}"
echo "sweep_json=${SWEEP_JSON}"
