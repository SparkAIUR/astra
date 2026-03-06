#!/usr/bin/env bash
set -euo pipefail

SPARKIFY_VERSION="${SPARKIFY_VERSION:-0.2.3}"
RUNTIME_ROOT="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/astra-sparkify-runtime"
RUNTIME_DIR="${RUNTIME_ROOT}/${SPARKIFY_VERSION}"
READY_FILE="${RUNTIME_DIR}/.ready"
WORK_ROOT="${PWD}"
TEMP_NODE_MODULES_LINK="${WORK_ROOT}/node_modules"
CREATED_NODE_MODULES_LINK=0

install_runtime() {
  rm -rf "${RUNTIME_DIR}"
  mkdir -p "${RUNTIME_DIR}"

  npm install --prefix "${RUNTIME_DIR}" --no-save --no-package-lock \
    "sparkify@${SPARKIFY_VERSION}"

  if [[ ! -d "${RUNTIME_DIR}/node_modules/sparkify-template-astro" ]]; then
    local template_version
    template_version="$(
      node -e 'const fs = require("node:fs"); const path = require("node:path"); const runtimeDir = process.argv[1]; const corePkgPath = path.join(runtimeDir, "node_modules", "sparkify-core", "package.json"); const corePkg = JSON.parse(fs.readFileSync(corePkgPath, "utf8")); process.stdout.write(corePkg.dependencies?.["sparkify-template-astro"] ?? "");' \
      "${RUNTIME_DIR}"
    )"
    if [[ -z "${template_version}" ]]; then
      echo "unable to resolve sparkify-template-astro dependency" >&2
      exit 1
    fi
    npm install --prefix "${RUNTIME_DIR}" --no-save --no-package-lock \
      "sparkify-template-astro@${template_version}"
  fi

  touch "${READY_FILE}"
}

if [[ ! -f "${READY_FILE}" ]]; then
  install_runtime
fi

cleanup() {
  if [[ "${CREATED_NODE_MODULES_LINK}" -eq 1 && -L "${TEMP_NODE_MODULES_LINK}" ]]; then
    rm -f "${TEMP_NODE_MODULES_LINK}"
  fi
}

if [[ ! -e "${TEMP_NODE_MODULES_LINK}" ]]; then
  ln -s "${RUNTIME_DIR}/node_modules" "${TEMP_NODE_MODULES_LINK}"
  CREATED_NODE_MODULES_LINK=1
  trap cleanup EXIT
fi

node "${RUNTIME_DIR}/node_modules/sparkify/dist/bin.js" "$@"
