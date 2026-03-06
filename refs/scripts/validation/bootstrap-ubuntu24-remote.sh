#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bootstrap-ubuntu24-remote.sh root@host-ip [remote-script-args...]

Examples:
  refs/scripts/validation/bootstrap-ubuntu24-remote.sh root@100.118.182.89
  AWSCLI_VERSION=2.27.54 refs/scripts/validation/bootstrap-ubuntu24-remote.sh root@100.118.182.89
  ASTRA_WORKSPACE=/root/astra-phase4 refs/scripts/validation/bootstrap-ubuntu24-remote.sh root@100.118.182.89
EOF
}

if [ "$#" -lt 1 ]; then
  usage
  exit 1
fi

REMOTE_HOST=$1
shift

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REMOTE_SCRIPT="${SCRIPT_DIR}/bootstrap-ubuntu24-host.sh"

if [ ! -f "${REMOTE_SCRIPT}" ]; then
  echo "missing script: ${REMOTE_SCRIPT}" >&2
  exit 1
fi

SSH_OPTS=(
  -o BatchMode=yes
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=10
)

quoted_args=""
for arg in "$@"; do
  quoted_args+=" $(printf '%q' "${arg}")"
done

env_exports=""
for name in ASTRA_WORKSPACE ASTRA_REPO_DIR ASTRA_RESULTS_DIR ASTRA_LOG_DIR ASTRA_BIN_DIR AWSCLI_VERSION GHZ_VERSION GRPCURL_VERSION ETCD_VERSION; do
  if [ -n "${!name:-}" ]; then
    env_exports+="${name}=$(printf '%q' "${!name}") "
  fi
done

echo "[bootstrap-remote] target=${REMOTE_HOST}"
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "${env_exports}bash -s --${quoted_args}" < "${REMOTE_SCRIPT}"
