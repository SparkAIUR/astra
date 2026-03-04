#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export RUN_ID=${RUN_ID:-phase8-baseline-$(date -u +%Y%m%dT%H%M%SZ)}
: "${PHASE6_NODE_TARGET:=200}"
: "${PHASE6_POD_TARGET:=5000}"
: "${PHASE6_CONFIGMAP_TARGET:=1000}"

"${SCRIPT_DIR}/phase8-run-all.sh" --k3s-only --smoke "$@"
