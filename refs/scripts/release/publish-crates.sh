#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: publish-crates.sh [options]

Publish Astra crates to crates.io in dependency order.

Options:
  --version <X.Y.Z>    Expected crate version (default: workspace version)
  --dry-run            Run cargo publish --dry-run only
  --skip-wait          Do not wait for crates.io index propagation between publishes
  --help               Show help
USAGE
}

VERSION=""
DRY_RUN=false
SKIP_WAIT=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version) VERSION=${2:?missing value for --version}; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --skip-wait) SKIP_WAIT=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../../.." && pwd)

if [ -z "${VERSION}" ]; then
  VERSION=$(awk -F'"' '/^version = /{print $2; exit}' "${REPO_ROOT}/Cargo.toml")
fi

if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid version: ${VERSION}" >&2
  exit 1
fi

PACKAGES=(astra-proto astra-core astractl astrad astra-forge)

crate_exists() {
  local pkg=${1:?pkg required}
  (cd /tmp && cargo info "${pkg}" 2>/dev/null | grep -q "version: ${VERSION}")
}

wait_for_crate() {
  local pkg=${1:?pkg required}
  local version=${2:?version required}
  local tries=${3:-60}
  local sleep_secs=${4:-5}
  local n

  for n in $(seq 1 "${tries}"); do
    if (cd /tmp && cargo info "${pkg}" 2>/dev/null | grep -q "version: ${version}"); then
      echo "[publish-crates] ${pkg}@${version} visible on crates.io"
      return 0
    fi
    sleep "${sleep_secs}"
  done

  echo "[publish-crates][warn] timed out waiting for ${pkg}@${version} visibility"
  return 1
}

cd "${REPO_ROOT}"

for idx in "${!PACKAGES[@]}"; do
  pkg=${PACKAGES[$idx]}
  echo "[publish-crates] ${pkg} version=${VERSION} dry_run=${DRY_RUN}"

  if [ "${DRY_RUN}" = "true" ]; then
    case "${pkg}" in
      astra-proto)
        cargo publish -p "${pkg}" --dry-run
        ;;
      astra-core|astractl)
        if crate_exists astra-proto; then
          cargo publish -p "${pkg}" --dry-run
        else
          echo "[publish-crates] astra-proto not on crates.io yet; using cargo package for ${pkg}"
          cargo package -p "${pkg}"
        fi
        ;;
      astrad|astra-forge)
        if crate_exists astra-core; then
          cargo publish -p "${pkg}" --dry-run
        else
          echo "[publish-crates] astra-core not on crates.io yet; using cargo package for ${pkg}"
          cargo package -p "${pkg}"
        fi
        ;;
    esac
  else
    cargo publish -p "${pkg}"
  fi

  # Wait for dependency crates to propagate before publishing dependent crates.
  if [ "${DRY_RUN}" != "true" ] && [ "${SKIP_WAIT}" != "true" ] && [ "${idx}" -lt "$(( ${#PACKAGES[@]} - 1 ))" ]; then
    wait_for_crate "${pkg}" "${VERSION}" || true
  fi
done

echo "[publish-crates] done"
