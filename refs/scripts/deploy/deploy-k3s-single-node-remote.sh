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
  --kubeconfig-server-ip <ip>    Rewrite local kubeconfig server endpoint to this IP (repeatable)
  --cluster-name <name>          Local kubeconfig cluster/context/user name override
  --ssh-option <opt>             Extra SSH option (repeatable)
  --help                         Show help

Any arguments after `--` are passed to deploy-k3s-single-node.sh on the remote host.

Examples:
  deploy-k3s-single-node-remote.sh --host root@162.209.124.74 -- --disk-device auto --validation full
  deploy-k3s-single-node-remote.sh --host root@vm -- --disk-device auto --astra-container-memory-limit 2048M --validation smoke
  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... deploy-k3s-single-node-remote.sh --host root@vm -- --disk-device auto --backup-target external-s3 --s3-endpoint https://<account>.r2.cloudflarestorage.com --s3-bucket astra-prod --validation smoke
  deploy-k3s-single-node-remote.sh --host root@vm --git-ref main -- --astra-tag v0.1.0 --validation smoke
  deploy-k3s-single-node-remote.sh --host root@vm -- --enable-servicelb --snapshotter overlayfs --validation smoke
  deploy-k3s-single-node-remote.sh --host root@100.110.236.66 --cluster-name hplcpc01 --kubeconfig-server-ip 100.110.236.66 --kubeconfig-server-ip 192.168.148.190 -- --host-public-ip 192.168.148.190 --tls-san 100.110.236.66 --node-name hplcpc01
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
CLUSTER_NAME=${CLUSTER_NAME:-}

EXTRA_SSH_OPTS=()
ON_HOST_ARGS=()
KUBECONFIG_SERVER_IPS=()

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
    --kubeconfig-server-ip) KUBECONFIG_SERVER_IPS+=("${2:?missing value for --kubeconfig-server-ip}"); shift 2 ;;
    --cluster-name) CLUSTER_NAME=${2:?missing value for --cluster-name}; shift 2 ;;
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

kubeconfig_label_for_ip() {
  local ip=${1:?ip required}
  local sanitized
  sanitized=$(printf '%s' "${ip}" | tr -c 'A-Za-z0-9._-' '_')

  if [[ "${ip}" == 100.* ]]; then
    printf 'tailscale\n'
    return 0
  fi

  if [[ "${ip}" == 10.* ]] || [[ "${ip}" == 192.168.* ]] || [[ "${ip}" =~ ^172\.(1[6-9]|2[0-9]|3[0-1])\. ]]; then
    printf 'lan\n'
    return 0
  fi

  printf '%s\n' "${sanitized}"
}

rewrite_kubeconfig_local() {
  local path=${1:?path required}
  local server_ip=${2:?server ip required}
  local cluster_name=${3:-}
  python3 - <<'PY' "${path}" "${server_ip}" "${cluster_name}"
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
server_ip = sys.argv[2]
cluster_name = sys.argv[3]
text = path.read_text(encoding="utf-8", errors="ignore")

text = re.sub(
    r"^\s*server:\s+https://[^\n]+$",
    f"    server: https://{server_ip}:6443",
    text,
    flags=re.MULTILINE,
)

if cluster_name:
    replacements = [
        (r"^(\s*current-context:\s*)default\s*$", r"\1" + cluster_name),
        (r"^(\s*(?:-\s*)?name:\s*)default\s*$", r"\1" + cluster_name),
        (r"^(\s*cluster:\s*)default\s*$", r"\1" + cluster_name),
        (r"^(\s*user:\s*)default\s*$", r"\1" + cluster_name),
    ]
    for pattern, repl in replacements:
        text = re.sub(pattern, repl, text, flags=re.MULTILINE)

path.write_text(text, encoding="utf-8")
PY
}

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
LOCAL_KUBECONFIG_PATHS=()
if astra_bool_true "${PULL_KUBECONFIG}"; then
  target_kubeconfig_ips=()
  if [ "${#KUBECONFIG_SERVER_IPS[@]}" -eq 0 ]; then
    target_kubeconfig_ips=("${HOST_PUBLIC_IP}")
  else
    for server_ip in "${KUBECONFIG_SERVER_IPS[@]}"; do
      seen=false
      for existing_ip in "${target_kubeconfig_ips[@]-}"; do
        if [ "${existing_ip}" = "${server_ip}" ]; then
          seen=true
          break
        fi
      done
      if [ "${seen}" = "false" ]; then
        target_kubeconfig_ips+=("${server_ip}")
      fi
    done
  fi

  host_label=$(printf '%s' "${REMOTE_HOST##*@}" | tr -c 'A-Za-z0-9._-' '_')
  base_local_path=${LOCAL_KUBECONFIG_PATH}
  used_local_paths=()

  for server_ip in "${target_kubeconfig_ips[@]}"; do
    label=$(kubeconfig_label_for_ip "${server_ip}")
    sanitized_ip=$(printf '%s' "${server_ip}" | tr -c 'A-Za-z0-9._-' '_')

    out_path=""
    if [ "${#target_kubeconfig_ips[@]}" -eq 1 ]; then
      if [ -n "${base_local_path}" ]; then
        out_path=${base_local_path}
      elif [ -n "${CLUSTER_NAME}" ]; then
        out_path="${HOME}/.kube/${CLUSTER_NAME}.yaml"
      else
        out_path="${HOME}/.kube/astra-k3s-${host_label}.yaml"
      fi
    else
      if [ -n "${base_local_path}" ]; then
        out_dir=$(dirname "${base_local_path}")
        out_file=$(basename "${base_local_path}")
        case "${out_file}" in
          *.yaml)
            out_stem=${out_file%.yaml}
            out_path="${out_dir}/${out_stem}-${label}.yaml"
            ;;
          *)
            out_path="${out_dir}/${out_file}-${label}.yaml"
            ;;
        esac
      elif [ -n "${CLUSTER_NAME}" ]; then
        out_path="${HOME}/.kube/${CLUSTER_NAME}-${label}.yaml"
      else
        out_path="${HOME}/.kube/astra-k3s-${host_label}-${label}.yaml"
      fi
    fi

    for existing_path in "${used_local_paths[@]-}"; do
      if [ "${existing_path}" = "${out_path}" ]; then
        out_path="${out_path%.yaml}-${sanitized_ip}.yaml"
        break
      fi
    done
    used_local_paths+=("${out_path}")

    mkdir -p "$(dirname "${out_path}")"
    astra_log "copying kubeconfig from remote for server=${server_ip} path=${out_path}"
    scp "${SSH_OPTS[@]}" "${REMOTE_HOST}:${REMOTE_KUBECONFIG_PATH}" "${out_path}"
    rewrite_kubeconfig_local "${out_path}" "${server_ip}" "${CLUSTER_NAME}"
    LOCAL_KUBECONFIG_PATHS+=("${out_path}")
  done

  if [ "${#LOCAL_KUBECONFIG_PATHS[@]}" -gt 0 ]; then
    LOCAL_KUBECONFIG_FINAL=${LOCAL_KUBECONFIG_PATHS[0]}
  fi
fi

rm -f "${remote_output_file}"

LOCAL_KUBECONFIG_PATHS_JOINED=""
if [ "${#LOCAL_KUBECONFIG_PATHS[@]}" -gt 0 ]; then
  IFS=,
  LOCAL_KUBECONFIG_PATHS_JOINED="${LOCAL_KUBECONFIG_PATHS[*]}"
  unset IFS
fi

printf 'REMOTE_DEPLOY_SUMMARY_PATH=%s\n' "${REMOTE_DEPLOY_SUMMARY_PATH}"
printf 'REMOTE_VALIDATION_SUMMARY_PATH=%s\n' "${VALIDATION_SUMMARY_PATH}"
printf 'HOST_PUBLIC_IP=%s\n' "${HOST_PUBLIC_IP}"
printf 'LOCAL_KUBECONFIG_PATH=%s\n' "${LOCAL_KUBECONFIG_FINAL}"
printf 'LOCAL_KUBECONFIG_PATHS=%s\n' "${LOCAL_KUBECONFIG_PATHS_JOINED}"

astra_log "remote deployment completed"
