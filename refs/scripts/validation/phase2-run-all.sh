#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra-alpha:<tag>}"

"${SCRIPT_DIR}/phase2-scenario-b.sh"
"${SCRIPT_DIR}/phase2-scenario-d.sh"
"${SCRIPT_DIR}/phase2-scenario-e.sh"

echo "phase2 scenarios complete"
