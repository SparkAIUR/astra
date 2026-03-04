#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: deploy-k3s-single-node-remote.sh [options] [-- on-host-flags...]

Run complete single-node Astra + K3s deployment from local machine to remote host over SSH.

Options:
  --host <user@ip>               Remote SSH target (required)
  --git-url <url>                Git repo to clone/pull (default: https://github.com/SparkAIUR/astra.git)
  --git-ref <ref>                Branch/tag/ref to checkout (default: main)
  --remote-repo-dir <path>       Remote repo path (default: /root/astra-lab/repo)
  --remote-results-dir <path>    Remote results path (default: /root/astra-lab/results)
  --remote-workspace <path>      Remote workspace path (default: dirname(remote-repo-dir))
  --bootstrap-remote             Run bootstrap-ubuntu24-remote.sh (default)
  --no-bootstrap-remote          Skip remote bootstrap
  --pull-kubeconfig              Copy kubeconfig to local machine (default)
  --no-pull-kubeconfig           Do not copy kubeconfig locally
  --local-kubeconfig-path <path> Local kubeconfig output path (default: ~/.kube/astra-k3s-<host>.yaml)
  --ssh-option <opt>             Extra SSH option (repeatable)
  --help                         Show help

Any arguments after `--` are passed to deploy-k3s-single-node.sh on the remote host.

Examples:
  deploy-k3s-single-node-remote.sh --host root@162.209.124.74 -- --disk-device auto --validation full
  deploy-k3s-single-node-remote.sh --host root@vm --git-ref main -- --astra-tag v0.1.0 --validation smoke
USAGE
}

REMOTE_HOST=""
GIT_URL=${GIT_URL:-https://github.com/SparkAIUR/astra.git}
GIT_REF=${GIT_REF:-main}
REMOTE_REPO_DIR=${REMOTE_REPO_DIR:-/root/astra-lab/repo}
REMOTE_RESULTS_DIR=${REMOTE_RESULTS_DIR:-/root/astra-lab/results}
REMOTE_WORKSPACE=${REMOTE_WORKSPACE:-}
BOOTSTRAP_REMOTE=true
PULL_KUBECONFIG=true
LOCAL_KUBECONFIG_PATH=${LOCAL_KUBECONFIG_PATH:-}

EXTRA_SSH_OPTS=()
ON_HOST_ARGS=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --host) REMOTE_HOST=${2:?missing value for --host}; shift 2 ;;
    --git-url) GIT_URL=${2:?missing value for --git-url}; shift 2 ;;
    --git-ref) GIT_REF=${2:?missing value for --git-ref}; shift 2 ;;
    --remote-repo-dir) REMOTE_REPO_DIR=${2:?missing value for --remote-repo-dir}; shift 2 ;;
    --remote-results-dir) REMOTE_RESULTS_DIR=${2:?missing value for --remote-results-dir}; shift 2 ;;
    --remote-workspace) REMOTE_WORKSPACE=${2:?missing value for --remote-workspace}; shift 2 ;;
    --bootstrap-remote) BOOTSTRAP_REMOTE=true; shift ;;
    --no-bootstrap-remote) BOOTSTRAP_REMOTE=false; shift ;;
    --pull-kubeconfig) PULL_KUBECONFIG=true; shift ;;
    --no-pull-kubeconfig) PULL_KUBECONFIG=false; shift ;;
    --local-kubeconfig-path) LOCAL_KUBECONFIG_PATH=${2:?missing value for --local-kubeconfig-path}; shift 2 ;;
    --ssh-option) EXTRA_SSH_OPTS+=("${2:?missing value for --ssh-option}"); shift 2 ;;
    --help|-h) usage; exit 0 ;;
    --)
      shift
      ON_HOST_ARGS=("$@")
      break
      ;;
    *) astra_die "unknown argument: $1" ;;
  esac
done

[ -n "${REMOTE_HOST}" ] || astra_die "--host is required"

if [ -z "${REMOTE_WORKSPACE}" ]; then
  REMOTE_WORKSPACE=$(dirname "${REMOTE_REPO_DIR}")
fi

astra_require_tools ssh scp git python3 || astra_die "missing local prerequisites"

SSH_OPTS=(
  -o BatchMode=yes
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=15
)
if [ "${#EXTRA_SSH_OPTS[@]}" -gt 0 ]; then
  SSH_OPTS+=("${EXTRA_SSH_OPTS[@]}")
fi

BOOTSTRAP_REMOTE_SCRIPT="${SCRIPT_DIR}/../validation/bootstrap-ubuntu24-remote.sh"
if astra_bool_true "${BOOTSTRAP_REMOTE}"; then
  [ -x "${BOOTSTRAP_REMOTE_SCRIPT}" ] || astra_die "missing bootstrap script: ${BOOTSTRAP_REMOTE_SCRIPT}"
  astra_log "bootstrapping remote host: ${REMOTE_HOST}"
  ASTRA_WORKSPACE="${REMOTE_WORKSPACE}" \
  ASTRA_REPO_DIR="${REMOTE_REPO_DIR}" \
  ASTRA_RESULTS_DIR="${REMOTE_RESULTS_DIR}" \
    "${BOOTSTRAP_REMOTE_SCRIPT}" "${REMOTE_HOST}"
else
  astra_log "skipping remote bootstrap"
fi

astra_log "syncing repository on remote host from git"
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" \
  "GIT_URL=$(printf '%q' "${GIT_URL}") GIT_REF=$(printf '%q' "${GIT_REF}") REMOTE_REPO_DIR=$(printf '%q' "${REMOTE_REPO_DIR}") bash -s" <<'EOS'
set -euo pipefail

[ -f /etc/profile ] && . /etc/profile
[ -f /etc/profile.d/astra.sh ] && . /etc/profile.d/astra.sh
[ -f ~/.bashrc ] && . ~/.bashrc

mkdir -p "$(dirname "${REMOTE_REPO_DIR}")"

if [ ! -d "${REMOTE_REPO_DIR}/.git" ]; then
  git clone "${GIT_URL}" "${REMOTE_REPO_DIR}"
fi

git -C "${REMOTE_REPO_DIR}" remote set-url origin "${GIT_URL}"
git -C "${REMOTE_REPO_DIR}" fetch --all --tags --prune
git -C "${REMOTE_REPO_DIR}" checkout "${GIT_REF}"
if git -C "${REMOTE_REPO_DIR}" show-ref --verify --quiet "refs/remotes/origin/${GIT_REF}"; then
  git -C "${REMOTE_REPO_DIR}" reset --hard "origin/${GIT_REF}"
fi
git -C "${REMOTE_REPO_DIR}" clean -fd
printf '[remote-sync] checked out %s\n' "$(git -C "${REMOTE_REPO_DIR}" rev-parse --short HEAD)"
EOS

deploy_args=(
  --repo-dir "${REMOTE_REPO_DIR}"
  --results-dir "${REMOTE_RESULTS_DIR}"
)

if [ "${#ON_HOST_ARGS[@]}" -gt 0 ]; then
  deploy_args+=("${ON_HOST_ARGS[@]}")
fi

quoted_deploy_args=""
for arg in "${deploy_args[@]}"; do
  quoted_deploy_args+=" $(printf '%q' "${arg}")"
done

astra_log "executing remote deployment"
remote_output_file=$(mktemp)
set +e
ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" \
  "REMOTE_REPO_DIR=$(printf '%q' "${REMOTE_REPO_DIR}") bash -s --${quoted_deploy_args}" <<'EOS' | tee "${remote_output_file}"
set -euo pipefail

[ -f /etc/profile ] && . /etc/profile
[ -f /etc/profile.d/astra.sh ] && . /etc/profile.d/astra.sh
[ -f ~/.bashrc ] && . ~/.bashrc

cd "${REMOTE_REPO_DIR}"
refs/scripts/deploy/deploy-k3s-single-node.sh "$@"
EOS
ssh_rc=${PIPESTATUS[0]}
set -e

REMOTE_DEPLOY_SUMMARY_PATH=$(grep '^DEPLOY_SUMMARY_PATH=' "${remote_output_file}" | tail -n 1 | cut -d= -f2- || true)
HOST_PUBLIC_IP=$(grep '^HOST_PUBLIC_IP=' "${remote_output_file}" | tail -n 1 | cut -d= -f2- || true)
REMOTE_KUBECONFIG_PATH=$(grep '^REMOTE_KUBECONFIG_PATH=' "${remote_output_file}" | tail -n 1 | cut -d= -f2- || true)
VALIDATION_SUMMARY_PATH=$(grep '^VALIDATION_SUMMARY_PATH=' "${remote_output_file}" | tail -n 1 | cut -d= -f2- || true)

if [ -z "${HOST_PUBLIC_IP}" ]; then
  HOST_PUBLIC_IP=${REMOTE_HOST##*@}
fi

if [ -z "${REMOTE_KUBECONFIG_PATH}" ]; then
  REMOTE_KUBECONFIG_PATH=/etc/rancher/k3s/k3s.yaml
fi

if [ "${ssh_rc}" -ne 0 ]; then
  astra_error "remote deployment failed (exit=${ssh_rc})"
  rm -f "${remote_output_file}"
  exit "${ssh_rc}"
fi

LOCAL_KUBECONFIG_FINAL=""
if astra_bool_true "${PULL_KUBECONFIG}"; then
  if [ -z "${LOCAL_KUBECONFIG_PATH}" ]; then
    host_label=$(printf '%s' "${REMOTE_HOST##*@}" | tr -c 'A-Za-z0-9._-' '_')
    LOCAL_KUBECONFIG_PATH="${HOME}/.kube/astra-k3s-${host_label}.yaml"
  fi

  mkdir -p "$(dirname "${LOCAL_KUBECONFIG_PATH}")"
  astra_log "copying kubeconfig from remote"
  scp "${SSH_OPTS[@]}" "${REMOTE_HOST}:${REMOTE_KUBECONFIG_PATH}" "${LOCAL_KUBECONFIG_PATH}"

  python3 - <<'PY' "${LOCAL_KUBECONFIG_PATH}" "${HOST_PUBLIC_IP}"
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
host_ip = sys.argv[2]
text = path.read_text(encoding="utf-8", errors="ignore")
text = re.sub(r'^\s*server:\s+https://[^\n]+$', f'    server: https://{host_ip}:6443', text, flags=re.MULTILINE)
path.write_text(text, encoding="utf-8")
PY

  LOCAL_KUBECONFIG_FINAL=${LOCAL_KUBECONFIG_PATH}
fi

rm -f "${remote_output_file}"

printf 'REMOTE_DEPLOY_SUMMARY_PATH=%s\n' "${REMOTE_DEPLOY_SUMMARY_PATH}"
printf 'REMOTE_VALIDATION_SUMMARY_PATH=%s\n' "${VALIDATION_SUMMARY_PATH}"
printf 'HOST_PUBLIC_IP=%s\n' "${HOST_PUBLIC_IP}"
printf 'LOCAL_KUBECONFIG_PATH=%s\n' "${LOCAL_KUBECONFIG_FINAL}"

astra_log "remote deployment completed"
