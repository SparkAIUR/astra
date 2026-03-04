#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export RUN_ID=${RUN_ID:-phase10-$(date -u +%Y%m%dT%H%M%SZ)}

# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

RUN_QOS=true
RUN_AUTO=true
RUN_ECOSYSTEM=true
RUN_GRAY=true
SMOKE=false

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke)
      SMOKE=true
      shift
      ;;
    --skip-qos)
      RUN_QOS=false
      shift
      ;;
    --skip-auto)
      RUN_AUTO=false
      shift
      ;;
    --skip-ecosystem)
      RUN_ECOSYSTEM=false
      shift
      ;;
    --skip-gray)
      RUN_GRAY=false
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [ "${SMOKE}" = "true" ]; then
  export PHASE10_QOS_SWAPOFF_QUICK=true
  export PHASE10_QOS_SWAPOFF_NODES=100
  export PHASE10_QOS_SWAPOFF_PODS=1000
  export PHASE10_QOS_SWAPOFF_CONFIGMAPS=300
  export PHASE10_CNI_CYCLES=1200
  export PHASE10_GATEWAY_DURATION=10s
  export PHASE10_GATEWAY_CONCURRENCY=80
  export PHASE10_GATEWAY_CONNECTIONS=32
  export PHASE10_GATEWAY_TARGET_RPS=5000
  export PHASE10_PATRONI_LOAD_SECS=10
  export PHASE10_PATRONI_LEASE_OPS=250
  export PHASE10_GRAY_DURATION=10s
  export PHASE10_GRAY_CONCURRENCY=80
fi

phase6_log "phase10 run starting run_id=${RUN_ID} run_dir=${RUN_DIR} smoke=${SMOKE}"
phase6_log "phase10 scenarios: qos=${RUN_QOS} auto=${RUN_AUTO} ecosystem=${RUN_ECOSYSTEM} gray=${RUN_GRAY}"

if [ "${RUN_QOS}" = "true" ]; then
  "${SCRIPT_DIR}/phase10-scenario-qos-swapoff.sh"
fi
if [ "${RUN_AUTO}" = "true" ]; then
  "${SCRIPT_DIR}/phase10-scenario-auto-governor.sh"
fi
if [ "${RUN_ECOSYSTEM}" = "true" ]; then
  "${SCRIPT_DIR}/phase10-scenario-ecosystem.sh"
fi
if [ "${RUN_GRAY}" = "true" ]; then
  "${SCRIPT_DIR}/phase10-scenario-gray-failure.sh"
fi

python3 "${SCRIPT_DIR}/build_phase10_matrix.py" \
  --run-dir "${RUN_DIR}" \
  --out-json "${RUN_DIR}/phase10-matrix.json" \
  --out-csv "${RUN_DIR}/phase10-matrix.csv"

python3 - <<'PY' \
"${RUN_ID}" \
"${RUN_DIR}" \
"${RUN_QOS}" \
"${RUN_AUTO}" \
"${RUN_ECOSYSTEM}" \
"${RUN_GRAY}" \
"${SMOKE}"
from __future__ import annotations

import json
import sys
from pathlib import Path

run_id = sys.argv[1]
run_dir = Path(sys.argv[2])
run_qos = sys.argv[3].lower() == "true"
run_auto = sys.argv[4].lower() == "true"
run_ecosystem = sys.argv[5].lower() == "true"
run_gray = sys.argv[6].lower() == "true"
smoke = sys.argv[7].lower() == "true"

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
        "qos_swapoff": load("phase10-qos-swapoff-summary.json") if run_qos else None,
        "auto_governor": load("phase10-auto-governor-summary.json") if run_auto else None,
        "ecosystem": load("phase10-ecosystem-summary.json") if run_ecosystem else None,
        "gray_failure": load("phase10-gray-failure-summary.json") if run_gray else None,
    },
    "matrix": load("phase10-matrix.json"),
}
(run_dir / "phase10-run-index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
print(json.dumps(index, indent=2))
PY

phase6_log "phase10 run complete index=${RUN_DIR}/phase10-run-index.json"
