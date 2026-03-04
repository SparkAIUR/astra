#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export RUN_ID=${RUN_ID:-phase11-$(date -u +%Y%m%dT%H%M%SZ)}

# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

RUN_FORGE=true
RUN_FLEET=true
RUN_DOCS=true
SMOKE=false

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke)
      SMOKE=true
      shift
      ;;
    --skip-forge)
      RUN_FORGE=false
      shift
      ;;
    --skip-fleet)
      RUN_FLEET=false
      shift
      ;;
    --skip-docs)
      RUN_DOCS=false
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [ "${SMOKE}" = "true" ]; then
  export PHASE11_LEGACY_COUNT=${PHASE11_LEGACY_COUNT:-2}
  export PHASE11_LEGACY_MACHINE_KEYS=${PHASE11_LEGACY_MACHINE_KEYS:-20}
  export PHASE11_LEGACY_RBAC_KEYS=${PHASE11_LEGACY_RBAC_KEYS:-8}
  export PHASE11_OMNI_REQUIRE_REAL_MIN=${PHASE11_OMNI_REQUIRE_REAL_MIN:-1}
fi

phase6_log "phase11 run starting run_id=${RUN_ID} run_dir=${RUN_DIR} smoke=${SMOKE}"
phase6_log "phase11 scenarios: forge=${RUN_FORGE} fleet=${RUN_FLEET} docs=${RUN_DOCS}"

if [ "${RUN_FORGE}" = "true" ]; then
  "${SCRIPT_DIR}/phase11-scenario-forge-ux.sh"
fi
if [ "${RUN_FLEET}" = "true" ]; then
  "${SCRIPT_DIR}/phase11-scenario-fleet-cutover.sh"
fi
if [ "${RUN_DOCS}" = "true" ]; then
  "${SCRIPT_DIR}/phase11-scenario-docs-sandbox.sh"
fi

python3 "${SCRIPT_DIR}/build_phase11_matrix.py" \
  --run-dir "${RUN_DIR}" \
  --out-json "${RUN_DIR}/phase11-matrix.json" \
  --out-csv "${RUN_DIR}/phase11-matrix.csv"

python3 - <<'PY' \
"${RUN_ID}" \
"${RUN_DIR}" \
"${RUN_FORGE}" \
"${RUN_FLEET}" \
"${RUN_DOCS}" \
"${SMOKE}"
from __future__ import annotations

import json
import sys
from pathlib import Path

run_id = sys.argv[1]
run_dir = Path(sys.argv[2])
run_forge = sys.argv[3].lower() == "true"
run_fleet = sys.argv[4].lower() == "true"
run_docs = sys.argv[5].lower() == "true"
smoke = sys.argv[6].lower() == "true"

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
        "forge_ux": load("phase11-forge-ux-summary.json") if run_forge else None,
        "fleet_cutover": load("phase11-fleet-cutover-summary.json") if run_fleet else None,
        "docs_sandbox": load("phase11-docs-sandbox-summary.json") if run_docs else None,
    },
    "matrix": load("phase11-matrix.json"),
}
(run_dir / "phase11-run-index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
print(json.dumps(index, indent=2))
PY

phase6_log "phase11 run complete index=${RUN_DIR}/phase11-run-index.json"
