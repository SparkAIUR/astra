#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

"${SCRIPT_DIR}/phase3-scenario-b.sh"
"${SCRIPT_DIR}/phase3-scenario-e.sh"
