#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export RUN_ID=${RUN_ID:-phase12-$(date -u +%Y%m%dT%H%M%SZ)}

# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

RUN_MULTIRAFT=true
RUN_WATCH=true
RUN_LIST=true
RUN_CACHE=true
RUN_CRUCIBLE=true
RUN_TOMBSTONE=true
RUN_CLEANUP=true
SMOKE=false

# Phase12 defaults: avoid ultra-constrained edge profile so directives can probe
# hyperscale behavior instead of low-I/O cgroup throttling artifacts.
export PHASE6_ASTRA_MEM_LIMIT=${PHASE6_ASTRA_MEM_LIMIT:-1024m}
export PHASE6_ASTRA_NODE_IOPS=${PHASE6_ASTRA_NODE_IOPS:-1000}
export PHASE6_ASTRA_NODE_BPS=${PHASE6_ASTRA_NODE_BPS:-52428800}

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke)
      SMOKE=true
      shift
      ;;
    --skip-cleanup)
      RUN_CLEANUP=false
      shift
      ;;
    --skip-multiraft)
      RUN_MULTIRAFT=false
      shift
      ;;
    --skip-watch)
      RUN_WATCH=false
      shift
      ;;
    --skip-list)
      RUN_LIST=false
      shift
      ;;
    --skip-cache)
      RUN_CACHE=false
      shift
      ;;
    --skip-crucible)
      RUN_CRUCIBLE=false
      shift
      ;;
    --skip-tombstone)
      RUN_TOMBSTONE=false
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [ "${SMOKE}" = "true" ]; then
  export PHASE12_MR_TOTAL=${PHASE12_MR_TOTAL:-20000}
  export PHASE12_MR_CONCURRENCY=${PHASE12_MR_CONCURRENCY:-128}
  export PHASE12_WATCHERS=${PHASE12_WATCHERS:-8000}
  export PHASE12_WATCH_STREAMS=${PHASE12_WATCH_STREAMS:-192}
  export PHASE12_WATCH_SETTLE_MS=${PHASE12_WATCH_SETTLE_MS:-30000}
  export PHASE12_WATCH_WRITE_DURATION_SECS=${PHASE12_WATCH_WRITE_DURATION_SECS:-20}
  export PHASE12_WATCH_WRITE_TARGET_RPS=${PHASE12_WATCH_WRITE_TARGET_RPS:-2500}
  export PHASE12_WATCH_WRITE_RPS_MIN=${PHASE12_WATCH_WRITE_RPS_MIN:-1500}
  export PHASE12_LIST_OBJECTS=${PHASE12_LIST_OBJECTS:-20000}
  export PHASE12_LIST_PAYLOAD_SIZE=${PHASE12_LIST_PAYLOAD_SIZE:-4096}
  export PHASE12_LIST_PEAK_RSS_MAX_MB=${PHASE12_LIST_PEAK_RSS_MAX_MB:-256}
  export PHASE12_CACHE_KEYS=${PHASE12_CACHE_KEYS:-4000}
  export PHASE12_CACHE_BENCH_DURATION=${PHASE12_CACHE_BENCH_DURATION:-15s}
  export PHASE12_CRUCIBLE_TARGET_GIB=${PHASE12_CRUCIBLE_TARGET_GIB:-8}
  export PHASE12_CRUCIBLE_PAYLOAD_MIB=${PHASE12_CRUCIBLE_PAYLOAD_MIB:-8}
  export PHASE12_CRUCIBLE_REJOIN_TIMEOUT_SECS=${PHASE12_CRUCIBLE_REJOIN_TIMEOUT_SECS:-300}
  export PHASE12_CRUCIBLE_REJOIN_PASS_SECS=${PHASE12_CRUCIBLE_REJOIN_PASS_SECS:-240}
  export PHASE12_TOMBSTONE_DURATION_SECS=${PHASE12_TOMBSTONE_DURATION_SECS:-900}
  export PHASE12_TOMBSTONE_LIST_P99_REGRESSION_MAX=${PHASE12_TOMBSTONE_LIST_P99_REGRESSION_MAX:-2.0}
fi

phase6_log "phase12 run starting run_id=${RUN_ID} run_dir=${RUN_DIR} smoke=${SMOKE}"
phase6_log "phase12 scenarios: cleanup=${RUN_CLEANUP} multiraft=${RUN_MULTIRAFT} watch=${RUN_WATCH} list=${RUN_LIST} cache=${RUN_CACHE} crucible=${RUN_CRUCIBLE} tombstone=${RUN_TOMBSTONE}"

overall_rc=0

run_step() {
  local name=${1:?name required}
  shift
  phase6_log "phase12 run: scenario=${name} start"
  if "$@"; then
    phase6_log "phase12 run: scenario=${name} done"
  else
    local rc=$?
    phase6_log "phase12 run: scenario=${name} failed rc=${rc}"
    overall_rc=1
  fi
}

if [ "${RUN_CLEANUP}" = "true" ]; then
  run_step cleanup "${SCRIPT_DIR}/phase12-cleanup-host.sh"
fi

if [ "${RUN_MULTIRAFT}" = "true" ]; then
  run_step multiraft "${SCRIPT_DIR}/phase12-scenario-multiraft.sh"
fi
if [ "${RUN_WATCH}" = "true" ]; then
  run_step watch "${SCRIPT_DIR}/phase12-scenario-watch-fanout.sh"
fi
if [ "${RUN_LIST}" = "true" ]; then
  run_step list "${SCRIPT_DIR}/phase12-scenario-list-streaming.sh"
fi
if [ "${RUN_CACHE}" = "true" ]; then
  run_step cache "${SCRIPT_DIR}/phase12-scenario-semantic-cache.sh"
fi
if [ "${RUN_CRUCIBLE}" = "true" ]; then
  run_step crucible "${SCRIPT_DIR}/phase12-scenario-limitless-crucible.sh"
fi
if [ "${RUN_TOMBSTONE}" = "true" ]; then
  run_step tombstone "${SCRIPT_DIR}/phase12-scenario-tombstone-soak.sh"
fi

python3 "${SCRIPT_DIR}/build_phase12_matrix.py" \
  --run-dir "${RUN_DIR}" \
  --out-json "${RUN_DIR}/phase12-matrix.json" \
  --out-csv "${RUN_DIR}/phase12-matrix.csv"

python3 - <<'PY' \
"${RUN_ID}" \
"${RUN_DIR}" \
"${RUN_MULTIRAFT}" \
"${RUN_WATCH}" \
"${RUN_LIST}" \
"${RUN_CACHE}" \
"${RUN_CRUCIBLE}" \
"${RUN_TOMBSTONE}" \
"${SMOKE}"
from __future__ import annotations

import json
import sys
from pathlib import Path

run_id = sys.argv[1]
run_dir = Path(sys.argv[2])
run_multiraft = sys.argv[3].lower() == "true"
run_watch = sys.argv[4].lower() == "true"
run_list = sys.argv[5].lower() == "true"
run_cache = sys.argv[6].lower() == "true"
run_crucible = sys.argv[7].lower() == "true"
run_tombstone = sys.argv[8].lower() == "true"
smoke = sys.argv[9].lower() == "true"


def load(name: str):
    p = run_dir / name
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

index = {
    "run_id": run_id,
    "run_dir": str(run_dir),
    "smoke": smoke,
    "scenarios": {
        "multiraft": load("phase12-multiraft-summary.json") if run_multiraft else None,
        "watch_fanout": load("phase12-watch-fanout-summary.json") if run_watch else None,
        "list_streaming": load("phase12-list-stream-summary.json") if run_list else None,
        "semantic_cache": load("phase12-semantic-cache-summary.json") if run_cache else None,
        "limitless_crucible": load("phase12-limitless-crucible-summary.json") if run_crucible else None,
        "tombstone_soak": load("phase12-tombstone-soak-summary.json") if run_tombstone else None,
    },
    "matrix": load("phase12-matrix.json"),
}
(run_dir / "phase12-run-index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
print(json.dumps(index, indent=2))
PY

phase6_log "phase12 run complete index=${RUN_DIR}/phase12-run-index.json"
exit "${overall_rc}"
