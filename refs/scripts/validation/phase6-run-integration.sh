#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

MODE=full
SMOKE=false
SKIP_BOOTSTRAP=false
RUN_K3S=true
RUN_OMNI=true
RUN_FRICTION=true

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke)
      SMOKE=true
      shift
      ;;
    --k3s-only)
      MODE=k3s-only
      RUN_K3S=true
      RUN_OMNI=false
      RUN_FRICTION=false
      shift
      ;;
    --omni-only)
      MODE=omni-only
      RUN_K3S=false
      RUN_OMNI=true
      RUN_FRICTION=false
      shift
      ;;
    --full)
      MODE=full
      RUN_K3S=true
      RUN_OMNI=true
      RUN_FRICTION=true
      shift
      ;;
    --skip-bootstrap)
      SKIP_BOOTSTRAP=true
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

phase6_log "phase6 integration run starting mode=${MODE} smoke=${SMOKE} run_id=${RUN_ID}"
phase6_log "run directory: ${RUN_DIR}"

if [ "${SKIP_BOOTSTRAP}" != "true" ]; then
  "${SCRIPT_DIR}/bootstrap-phase6-host.sh"
fi

if [ "${RUN_K3S}" = "true" ]; then
  phase6_log "running k3s benchmark suite"
  if [ "${SMOKE}" = "true" ]; then
    QUICK=true "${SCRIPT_DIR}/phase6-k3s-benchmark.sh" --backend both --quick
  else
    "${SCRIPT_DIR}/phase6-k3s-benchmark.sh" --backend both
  fi
fi

if [ "${RUN_OMNI}" = "true" ]; then
  phase6_log "running omni integration suite"
  "${SCRIPT_DIR}/phase6-omni-integration.sh"
fi

if [ "${RUN_FRICTION}" = "true" ]; then
  phase6_log "running friction suite"
  "${SCRIPT_DIR}/phase6-friction.sh"
fi

if [ "${RUN_K3S}" = "true" ]; then
  phase6_log "building phase6 matrix artifacts"
  python3 "${SCRIPT_DIR}/build_phase6_matrix.py" \
    --run-dir "${RUN_DIR}" \
    --out-json "${RUN_DIR}/phase6-k3s-matrix.json" \
    --out-csv "${RUN_DIR}/phase6-k3s-matrix.csv"
fi

python3 - <<'PY' "${RUN_ID}" "${RUN_DIR}" "${MODE}" "${SMOKE}" "${RUN_K3S}" "${RUN_OMNI}" "${RUN_FRICTION}"
import json
import pathlib
import sys

run_id = sys.argv[1]
run_dir = pathlib.Path(sys.argv[2])
mode = sys.argv[3]
smoke = sys.argv[4].lower() == "true"
run_k3s = sys.argv[5].lower() == "true"
run_omni = sys.argv[6].lower() == "true"
run_friction = sys.argv[7].lower() == "true"

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
    "mode": mode,
    "smoke": smoke,
    "artifacts": {
        "k3s_astra": load("phase6-k3s-astra-summary.json") if run_k3s else None,
        "k3s_etcd": load("phase6-k3s-etcd-summary.json") if run_k3s else None,
        "k3s_matrix": load("phase6-k3s-matrix.json") if run_k3s else None,
        "omni": load("phase6-omni-summary.json") if run_omni else None,
        "friction": load("phase6-friction-summary.json") if run_friction else None,
    },
}
(run_dir / "phase6-run-index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
print(json.dumps(index, indent=2))
PY

phase6_log "phase6 integration run complete"
phase6_log "index: ${RUN_DIR}/phase6-run-index.json"
