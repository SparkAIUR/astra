#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Phase8 runs use explicit IDs for artifact grouping.
export RUN_ID=${RUN_ID:-phase8-$(date -u +%Y%m%dT%H%M%SZ)}

# Phase8 runtime defaults (all overridable by caller env).
: "${ASTRAD_BG_IO_THROTTLE_ENABLED:=true}"
: "${ASTRAD_BG_IO_TOKENS_PER_SEC:=8192}"
: "${ASTRAD_BG_IO_BURST_TOKENS:=16384}"
: "${ASTRAD_BG_IO_SQE_THROTTLE_ENABLED:=true}"
: "${ASTRAD_BG_IO_SQE_TOKENS_PER_SEC:=1024}"
: "${ASTRAD_BG_IO_SQE_BURST:=2048}"
: "${ASTRAD_BG_IO_MIN_CHUNK_BYTES:=2097152}"
: "${ASTRAD_LSM_MAX_L0_FILES:=8}"
: "${ASTRAD_LSM_STALL_AT_FILES:=6}"
: "${ASTRAD_LSM_STALL_MAX_DELAY_MS:=200}"
: "${ASTRAD_LSM_REJECT_AFTER_MS:=800}"
: "${ASTRAD_LSM_SYNTH_FILE_BYTES:=8388608}"
: "${ASTRAD_LIST_PREFIX_FILTER_ENABLED:=true}"
: "${ASTRAD_LIST_PREFETCH_ENABLED:=true}"
: "${ASTRAD_LIST_PREFETCH_PAGES:=2}"
: "${ASTRAD_LIST_PREFETCH_CACHE_ENTRIES:=4096}"
: "${PHASE8_KUBE_MAX_MUTATING_REQUESTS_INFLIGHT:=4000}"
: "${PHASE8_KUBE_MAX_REQUESTS_INFLIGHT:=8000}"
: "${PHASE8_CONTROLLER_KUBE_API_QPS:=300}"
: "${PHASE8_CONTROLLER_KUBE_API_BURST:=600}"
: "${PHASE8_CONTROLLER_CONCURRENT_DEPLOYMENT_SYNCS:=200}"
: "${PHASE8_CONTROLLER_CONCURRENT_REPLICASET_SYNCS:=200}"
: "${PHASE8_CONTROLLER_CONCURRENT_STATEFULSET_SYNCS:=100}"
: "${PHASE8_CONTROLLER_CONCURRENT_NAMESPACE_SYNCS:=50}"
: "${PHASE8_INJECTION_PARALLELISM:=96}"
: "${PHASE8_HARD_POD_GATE:=false}"

export \
  ASTRAD_BG_IO_THROTTLE_ENABLED \
  ASTRAD_BG_IO_TOKENS_PER_SEC \
  ASTRAD_BG_IO_BURST_TOKENS \
  ASTRAD_BG_IO_SQE_THROTTLE_ENABLED \
  ASTRAD_BG_IO_SQE_TOKENS_PER_SEC \
  ASTRAD_BG_IO_SQE_BURST \
  ASTRAD_BG_IO_MIN_CHUNK_BYTES \
  ASTRAD_LSM_MAX_L0_FILES \
  ASTRAD_LSM_STALL_AT_FILES \
  ASTRAD_LSM_STALL_MAX_DELAY_MS \
  ASTRAD_LSM_REJECT_AFTER_MS \
  ASTRAD_LSM_SYNTH_FILE_BYTES \
  ASTRAD_LIST_PREFIX_FILTER_ENABLED \
  ASTRAD_LIST_PREFETCH_ENABLED \
  ASTRAD_LIST_PREFETCH_PAGES \
  ASTRAD_LIST_PREFETCH_CACHE_ENTRIES \
  PHASE8_KUBE_MAX_MUTATING_REQUESTS_INFLIGHT \
  PHASE8_KUBE_MAX_REQUESTS_INFLIGHT \
  PHASE8_CONTROLLER_KUBE_API_QPS \
  PHASE8_CONTROLLER_KUBE_API_BURST \
  PHASE8_CONTROLLER_CONCURRENT_DEPLOYMENT_SYNCS \
  PHASE8_CONTROLLER_CONCURRENT_REPLICASET_SYNCS \
  PHASE8_CONTROLLER_CONCURRENT_STATEFULSET_SYNCS \
  PHASE8_CONTROLLER_CONCURRENT_NAMESPACE_SYNCS \
  PHASE8_INJECTION_PARALLELISM \
  PHASE8_HARD_POD_GATE

# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase6_log "phase8 integration run starting run_id=${RUN_ID}"
phase6_log "phase8 knobs: l0_max=${ASTRAD_LSM_MAX_L0_FILES} stall_at=${ASTRAD_LSM_STALL_AT_FILES} sqe_tps=${ASTRAD_BG_IO_SQE_TOKENS_PER_SEC} list_prefetch=${ASTRAD_LIST_PREFETCH_ENABLED}/${ASTRAD_LIST_PREFETCH_PAGES}"

"${SCRIPT_DIR}/phase6-run-integration.sh" "$@"

if [ -f "${RUN_DIR}/phase6-k3s-astra-summary.json" ] && [ -f "${RUN_DIR}/phase6-k3s-etcd-summary.json" ]; then
  python3 "${SCRIPT_DIR}/build_phase8_matrix.py" \
    --run-dir "${RUN_DIR}" \
    --out-json "${RUN_DIR}/phase8-k3s-matrix.json" \
    --out-csv "${RUN_DIR}/phase8-k3s-matrix.csv"
fi

python3 - <<'PY' "${RUN_ID}" "${RUN_DIR}"
import json
import pathlib
import sys

run_id = sys.argv[1]
run_dir = pathlib.Path(sys.argv[2])


def load(name):
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
    "phase6_index": load("phase6-run-index.json"),
    "phase8_matrix": load("phase8-k3s-matrix.json"),
    "phase8_matrix_csv": str(run_dir / "phase8-k3s-matrix.csv") if (run_dir / "phase8-k3s-matrix.csv").exists() else None,
}
(run_dir / "phase8-run-index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
print(json.dumps(index, indent=2))
PY

phase6_log "phase8 integration run complete"
phase6_log "index: ${RUN_DIR}/phase8-run-index.json"
