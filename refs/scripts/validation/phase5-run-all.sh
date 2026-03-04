#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

"${SCRIPT_DIR}/phase5-scenario-forge-bulkload.sh"
"${SCRIPT_DIR}/phase5-scenario-auth-tenant.sh"
"${SCRIPT_DIR}/phase5-scenario-watch-crucible.sh"
"${SCRIPT_DIR}/phase5-scenario-chaos.sh"
