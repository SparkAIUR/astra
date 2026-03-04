#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)

# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

SUMMARY_JSON="${RUN_DIR}/phase11-docs-sandbox-summary.json"
BUILD_LOG="${RUN_DIR}/phase11-docs-build.log"

required_files=(
  "docs/package.json"
  "docs/.vitepress/config.mjs"
  "docs/index.md"
  "docs/architecture.md"
  "docs/migration-handbook.md"
  "docs/operations.md"
  "docs/disaster-recovery.md"
  "docker-compose.yaml"
  "quickstart.sh"
  "refs/sandbox/grafana/dashboards/astra-overview.json"
)

missing=()
for f in "${required_files[@]}"; do
  if [ ! -f "${REPO_DIR}/${f}" ]; then
    missing+=("${f}")
  fi
done

build_ok=false
build_out="${REPO_DIR}/docs/.vitepress/dist"
if [ ${#missing[@]} -eq 0 ] && command -v npm >/dev/null 2>&1; then
  (
    cd "${REPO_DIR}"
    npm --prefix docs install --no-audit --no-fund >/dev/null
    npm --prefix docs run docs:build
  ) >"${BUILD_LOG}" 2>&1 && build_ok=true || build_ok=false
else
  : >"${BUILD_LOG}"
fi

python3 - <<'PY' \
"${SUMMARY_JSON}" \
"${BUILD_LOG}" \
"${build_out}" \
"${build_ok}" \
"${missing[*]-}"
from __future__ import annotations
import json
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
build_log = sys.argv[2]
build_out = pathlib.Path(sys.argv[3])
build_ok = sys.argv[4].lower() == "true"
missing = [x for x in sys.argv[5].split() if x]

summary = {
    "missing_files": missing,
    "docs_build_log": build_log,
    "docs_build_output": str(build_out),
    "docs_build_ok": build_ok and build_out.exists(),
    "sandbox_assets_ok": len(missing) == 0,
}
summary["pass"] = summary["sandbox_assets_ok"] and summary["docs_build_ok"]
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "phase11 docs/sandbox summary: ${SUMMARY_JSON}"
