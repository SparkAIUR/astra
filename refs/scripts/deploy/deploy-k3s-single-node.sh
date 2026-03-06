#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib/common.sh"

usage() {
  cat <<'USAGE'
Usage: deploy-k3s-single-node.sh [options]

Complete single-node Astra + K3s deployment on a target host.

Options:
  --astra-image <image>          Full image reference (overrides repo/tag resolution)
  --astra-repo <repo>            Image repo for tag resolution (default: docker.io/halceon/astra)
  --astra-tag <tag>              Explicit tag to use with --astra-repo
  --astra-container-memory-limit <value>
                                 Per-Astra container memory limit for compose startup
                                 (default: 2048M for production single-node deploy)
  --astra-data-root <path>       Astra data root for bind-mounted node state (default: /var/lib/astra)
  --resolve-latest-stable <bool> Resolve latest stable semver tag from Docker Hub (default: true)
  --k3s-version <version>        Optional k3s version pin (default: latest from get.k3s.io)
  --enable-servicelb             Keep the bundled k3s ServiceLB enabled (default: disabled)
  --snapshotter <name>           K3s containerd snapshotter (default: stargz)
  --host-public-ip <ip>          Public IP for K3s TLS SAN / Traefik ingress (default: auto-detect)
  --tls-san <value>              Additional K3s API TLS SAN (repeatable; default: host public IP)
  --node-name <name>             Explicit K3s node name
  --kubelet-arg <arg>            Additional k3s --kubelet-arg value (repeatable)
  --disable-servicelb            Disable bundled k3s ServiceLB (recommended when using MetalLB)
  --disk-device <path|auto>      Disk device to prepare/mount; omit to skip disk setup
  --disk-mount <path>            K3s local storage mount path (default: /var/lib/rancher/k3s/storage)
  --k3s-storage-root <path>      Alias for --disk-mount
  --backup-target <disabled|external-s3>
                                 Off-host backup mode (default: disabled)
  --s3-endpoint <url>            External S3-compatible endpoint for tiering/backups
  --s3-bucket <name>             External S3-compatible bucket name
  --s3-region <value>            External S3-compatible region (default: auto)
  --s3-prefix <prefix>           Live object-tier prefix (default: astra/live)
  --backup-archive-prefix <prefix>
                                 Daily archive prefix for immutable restore points
                                 (default: astra/archive)
  --backup-schedule <calendar>   systemd OnCalendar expression for daily archive job
                                 (default: daily)
  --validation <none|smoke|full> Post-deploy validation mode (default: smoke)
  --readiness-duration <secs>    Readiness test duration seconds (defaults: smoke=180, full=720)
  --repo-dir <path>              Repo path on host (default: /root/astra-lab/repo or ASTRA_REPO_DIR)
  --results-dir <path>           Results directory (default: /root/astra-lab/results or ASTRA_RESULTS_DIR)
  --deploy-run-id <id>           Optional deploy run id override
  --skip-bootstrap               Skip bootstrap-ubuntu24-host.sh
  --skip-phase6-bootstrap        Skip bootstrap-phase6-host.sh
  --non-interactive              Disable prompts; requires --yes for formatting empty devices
  --yes                          Auto-approve destructive confirmation prompts
  --dry-run                      Print planned commands without executing
  --help                         Show help

Examples:
  deploy-k3s-single-node.sh --disk-device auto --validation full
  deploy-k3s-single-node.sh --disk-device auto --astra-container-memory-limit 2048M --validation full
  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... deploy-k3s-single-node.sh --disk-device auto --backup-target external-s3 --s3-endpoint https://<account>.r2.cloudflarestorage.com --s3-bucket astra-prod --validation smoke
  deploy-k3s-single-node.sh --astra-tag v0.1.1-rc1 --host-public-ip 162.209.124.74 --validation smoke
  deploy-k3s-single-node.sh --enable-servicelb --snapshotter overlayfs --validation smoke
  deploy-k3s-single-node.sh --host-public-ip 192.168.148.190 --tls-san 100.110.236.66 --node-name hplcpc01 --kubelet-arg fail-swap-on=false
USAGE
}

ASTRA_IMAGE=""
ASTRA_REPO=${ASTRA_REPO:-docker.io/halceon/astra}
ASTRA_TAG=""
ASTRA_CONTAINER_MEMORY_LIMIT=${ASTRA_CONTAINER_MEMORY_LIMIT:-2048M}
ASTRA_DATA_ROOT=${ASTRA_DATA_ROOT:-/var/lib/astra}
RESOLVE_LATEST_STABLE=${RESOLVE_LATEST_STABLE:-true}
K3S_VERSION=${K3S_VERSION:-}
SERVICE_LB_ENABLED=${SERVICE_LB_ENABLED:-false}
K3S_SNAPSHOTTER=${K3S_SNAPSHOTTER:-stargz}
HOST_PUBLIC_IP=${HOST_PUBLIC_IP:-}
NODE_NAME=${NODE_NAME:-}
TLS_SANS=()
KUBELET_ARGS=()
DISK_DEVICE=${DISK_DEVICE:-}
DISK_MOUNT=${DISK_MOUNT:-/var/lib/rancher/k3s/storage}
BACKUP_TARGET=${BACKUP_TARGET:-disabled}
S3_ENDPOINT=${S3_ENDPOINT:-}
S3_BUCKET=${S3_BUCKET:-}
S3_REGION=${S3_REGION:-auto}
S3_PREFIX=${S3_PREFIX:-astra/live}
BACKUP_ARCHIVE_PREFIX=${BACKUP_ARCHIVE_PREFIX:-astra/archive}
BACKUP_SCHEDULE=${BACKUP_SCHEDULE:-daily}
VALIDATION_MODE=${VALIDATION_MODE:-smoke}
READINESS_DURATION=${READINESS_DURATION:-}
READINESS_DURATION_SET=false
REPO_DIR=${REPO_DIR:-${ASTRA_REPO_DIR:-/root/astra-lab/repo}}
RESULTS_DIR=${RESULTS_DIR:-${ASTRA_RESULTS_DIR:-/root/astra-lab/results}}
DEPLOY_RUN_ID=${DEPLOY_RUN_ID:-deploy-k3s-single-node-$(date -u +%Y%m%dT%H%M%SZ)}
SKIP_BOOTSTRAP=false
SKIP_PHASE6_BOOTSTRAP=false
NON_INTERACTIVE=false
YES_FLAG=false
DRY_RUN=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --astra-image) ASTRA_IMAGE=${2:?missing value for --astra-image}; shift 2 ;;
    --astra-repo) ASTRA_REPO=${2:?missing value for --astra-repo}; shift 2 ;;
    --astra-tag) ASTRA_TAG=${2:?missing value for --astra-tag}; shift 2 ;;
    --astra-container-memory-limit) ASTRA_CONTAINER_MEMORY_LIMIT=${2:?missing value for --astra-container-memory-limit}; shift 2 ;;
    --astra-data-root) ASTRA_DATA_ROOT=${2:?missing value for --astra-data-root}; shift 2 ;;
    --resolve-latest-stable) RESOLVE_LATEST_STABLE=${2:?missing value for --resolve-latest-stable}; shift 2 ;;
    --k3s-version) K3S_VERSION=${2:?missing value for --k3s-version}; shift 2 ;;
    --enable-servicelb) SERVICE_LB_ENABLED=true; shift ;;
    --snapshotter) K3S_SNAPSHOTTER=${2:?missing value for --snapshotter}; shift 2 ;;
    --host-public-ip) HOST_PUBLIC_IP=${2:?missing value for --host-public-ip}; shift 2 ;;
    --tls-san) TLS_SANS+=("${2:?missing value for --tls-san}"); shift 2 ;;
    --node-name) NODE_NAME=${2:?missing value for --node-name}; shift 2 ;;
    --kubelet-arg) KUBELET_ARGS+=("${2:?missing value for --kubelet-arg}"); shift 2 ;;
    --disable-servicelb) SERVICE_LB_ENABLED=false; shift ;;
    --disk-device) DISK_DEVICE=${2:?missing value for --disk-device}; shift 2 ;;
    --disk-mount) DISK_MOUNT=${2:?missing value for --disk-mount}; shift 2 ;;
    --k3s-storage-root) DISK_MOUNT=${2:?missing value for --k3s-storage-root}; shift 2 ;;
    --backup-target) BACKUP_TARGET=${2:?missing value for --backup-target}; shift 2 ;;
    --s3-endpoint) S3_ENDPOINT=${2:?missing value for --s3-endpoint}; shift 2 ;;
    --s3-bucket) S3_BUCKET=${2:?missing value for --s3-bucket}; shift 2 ;;
    --s3-region) S3_REGION=${2:?missing value for --s3-region}; shift 2 ;;
    --s3-prefix) S3_PREFIX=${2:?missing value for --s3-prefix}; shift 2 ;;
    --backup-archive-prefix) BACKUP_ARCHIVE_PREFIX=${2:?missing value for --backup-archive-prefix}; shift 2 ;;
    --backup-schedule) BACKUP_SCHEDULE=${2:?missing value for --backup-schedule}; shift 2 ;;
    --validation) VALIDATION_MODE=${2:?missing value for --validation}; shift 2 ;;
    --readiness-duration) READINESS_DURATION=${2:?missing value for --readiness-duration}; READINESS_DURATION_SET=true; shift 2 ;;
    --repo-dir) REPO_DIR=${2:?missing value for --repo-dir}; shift 2 ;;
    --results-dir) RESULTS_DIR=${2:?missing value for --results-dir}; shift 2 ;;
    --deploy-run-id) DEPLOY_RUN_ID=${2:?missing value for --deploy-run-id}; shift 2 ;;
    --skip-bootstrap) SKIP_BOOTSTRAP=true; shift ;;
    --skip-phase6-bootstrap) SKIP_PHASE6_BOOTSTRAP=true; shift ;;
    --non-interactive) NON_INTERACTIVE=true; shift ;;
    --yes) YES_FLAG=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) astra_die "unknown argument: $1" ;;
  esac
done

case "${VALIDATION_MODE}" in
  none|smoke|full) ;;
  *) astra_die "--validation must be one of: none, smoke, full" ;;
esac

case "${BACKUP_TARGET}" in
  disabled|external-s3) ;;
  *) astra_die "--backup-target must be one of: disabled, external-s3" ;;
esac

if [ -n "${READINESS_DURATION}" ] && ! [[ "${READINESS_DURATION}" =~ ^[0-9]+$ ]]; then
  astra_die "--readiness-duration must be an integer number of seconds"
fi

if [ -z "${K3S_SNAPSHOTTER}" ]; then
  astra_die "--snapshotter must not be empty"
fi

if [ -z "${ASTRA_CONTAINER_MEMORY_LIMIT}" ]; then
  astra_die "--astra-container-memory-limit must not be empty"
fi

if [ -z "${ASTRA_DATA_ROOT}" ]; then
  astra_die "--astra-data-root must not be empty"
fi

if [ -z "${READINESS_DURATION}" ]; then
  case "${VALIDATION_MODE}" in
    smoke) READINESS_DURATION=180 ;;
    full) READINESS_DURATION=720 ;;
    none) READINESS_DURATION=0 ;;
  esac
fi

COMPOSE_PROD_FILE="${REPO_DIR}/refs/scripts/deploy/docker-compose.k3s-single-node.production.yml"
COMPOSE_OBJECT_STORE_FILE="${REPO_DIR}/refs/scripts/deploy/docker-compose.k3s-single-node.object-store.yml"
READINESS_SCRIPT="${REPO_DIR}/refs/scripts/validation/k3s-single-node-readiness.sh"
BOOTSTRAP_HOST_SCRIPT="${REPO_DIR}/refs/scripts/validation/bootstrap-ubuntu24-host.sh"
BOOTSTRAP_PHASE6_SCRIPT="${REPO_DIR}/refs/scripts/validation/bootstrap-phase6-host.sh"
STACK_SCRIPT="${REPO_DIR}/refs/scripts/deploy/astra-single-node-stack.sh"
DATASTORE_READY_SCRIPT="${REPO_DIR}/refs/scripts/deploy/astra-datastore-ready.sh"
BACKUP_ARCHIVE_SCRIPT="${REPO_DIR}/refs/scripts/deploy/astra-archive-tier-backup.sh"
ASTRA_ENV_FILE=/etc/astra/k3s-single-node.env
ASTRA_SERVICE_PATH=/etc/systemd/system/astra-single-node.service
BACKUP_SERVICE_PATH=/etc/systemd/system/astra-tier-archive.service
BACKUP_TIMER_PATH=/etc/systemd/system/astra-tier-archive.timer
K3S_OVERRIDE_PATH=/etc/systemd/system/k3s.service.d/10-astra-datastore.conf
K3S_RUNTIME_CONFIG_FILE="/etc/rancher/k3s/config.yaml.d/90-runtime-overrides.yaml"

DEPLOY_RUN_DIR="${RESULTS_DIR}/${DEPLOY_RUN_ID}"
DEPLOY_SUMMARY_PATH="${DEPLOY_RUN_DIR}/deployment-summary.json"
DEPLOY_LOG_PATH="${DEPLOY_RUN_DIR}/deploy.log"
COMPOSE_PS_PATH="${DEPLOY_RUN_DIR}/compose-ps.txt"

ASTRA_NODE1_DATA_PATH="${ASTRA_DATA_ROOT}/node1"
ASTRA_NODE2_DATA_PATH="${ASTRA_DATA_ROOT}/node2"
ASTRA_NODE3_DATA_PATH="${ASTRA_DATA_ROOT}/node3"

mkdir -p "${DEPLOY_RUN_DIR}"
: > "${DEPLOY_LOG_PATH}"

exec > >(tee -a "${DEPLOY_LOG_PATH}")
exec 2>&1

DEPLOY_STATUS="in_progress"
FAILURE_STEP=""
FAILURE_MESSAGE=""
ASTRA_IMAGE_RESOLVED=""
ASTRA_TAG_RESOLVED=""
HOST_PUBLIC_IP_RESOLVED=""
TLS_SANS_RESOLVED=()
DISK_DEVICE_RESOLVED=""
DISK_SETUP_PERFORMED=false
DATASTORE_ENDPOINTS="http://127.0.0.1:52379,http://127.0.0.1:52391,http://127.0.0.1:52392"
VALIDATION_STATUS="not_run"
VALIDATION_SUMMARY_PATH=""
K3S_VERSION_INSTALLED=""

q() {
  printf '%q' "$1"
}

have_kubelet_arg_key() {
  local key=${1:?key required}
  local existing=""
  for existing in "${KUBELET_ARGS[@]-}"; do
    case "${existing}" in
      "${key}"|${key}=*) return 0 ;;
    esac
  done
  return 1
}

append_default_kubelet_arg() {
  local arg=${1:?arg required}
  local key=${arg%%=*}
  if ! have_kubelet_arg_key "${key}"; then
    KUBELET_ARGS+=("${arg}")
  fi
}

require_nonempty_if() {
  local name=${1:?name required}
  local value=${2-}
  if [ -z "${value}" ]; then
    fail_deploy "preflight" "missing required value: ${name}"
  fi
}

compose_files() {
  printf '%s\n' "${COMPOSE_PROD_FILE}"
  if [ "${BACKUP_TARGET}" = "external-s3" ]; then
    printf '%s\n' "${COMPOSE_OBJECT_STORE_FILE}"
  fi
}

compose_ps_capture() {
  local files=()
  local file=""
  while IFS= read -r file; do
    [ -n "${file}" ] || continue
    files+=(-f "${file}")
  done < <(compose_files)

  (
    cd "${REPO_DIR}" &&
    ASTRA_IMAGE="${ASTRA_IMAGE_RESOLVED:-${ASTRA_IMAGE:-unset}}" \
    ASTRA_CONTAINER_MEMORY_LIMIT="${ASTRA_CONTAINER_MEMORY_LIMIT}" \
    ASTRA_NODE1_DATA="${ASTRA_NODE1_DATA_PATH}" \
    ASTRA_NODE2_DATA="${ASTRA_NODE2_DATA_PATH}" \
    ASTRA_NODE3_DATA="${ASTRA_NODE3_DATA_PATH}" \
      docker compose "${files[@]}" ps
  ) > "${COMPOSE_PS_PATH}" 2>&1 || true
}

write_runtime_env_file() {
  install -d -m 0755 /etc/astra
  cat >"${ASTRA_ENV_FILE}" <<EOF
ASTRA_REPO_DIR=${REPO_DIR}
ASTRA_RESULTS_DIR=${RESULTS_DIR}
ASTRA_IMAGE=${ASTRA_IMAGE_RESOLVED}
ASTRA_CONTAINER_MEMORY_LIMIT=${ASTRA_CONTAINER_MEMORY_LIMIT}
ASTRA_DATASTORE_ENDPOINTS=${DATASTORE_ENDPOINTS}
ASTRA_NODE1_DATA=${ASTRA_NODE1_DATA_PATH}
ASTRA_NODE2_DATA=${ASTRA_NODE2_DATA_PATH}
ASTRA_NODE3_DATA=${ASTRA_NODE3_DATA_PATH}
ASTRA_BACKUP_TARGET=${BACKUP_TARGET}
ASTRA_PROD_COMPOSE_FILE=${COMPOSE_PROD_FILE}
ASTRA_OBJECT_STORE_COMPOSE_FILE=${COMPOSE_OBJECT_STORE_FILE}
ASTRA_BACKUP_ARCHIVE_PREFIX=${BACKUP_ARCHIVE_PREFIX}
ASTRAD_S3_ENDPOINT=${S3_ENDPOINT}
ASTRAD_S3_BUCKET=${S3_BUCKET}
ASTRAD_S3_REGION=${S3_REGION}
ASTRAD_S3_PREFIX=${S3_PREFIX}
EOF
  chmod 0600 "${ASTRA_ENV_FILE}"

  if [ "${BACKUP_TARGET}" = "external-s3" ]; then
    {
      printf 'AWS_ACCESS_KEY_ID=%s\n' "${AWS_ACCESS_KEY_ID}"
      printf 'AWS_SECRET_ACCESS_KEY=%s\n' "${AWS_SECRET_ACCESS_KEY}"
    } >> "${ASTRA_ENV_FILE}"
  fi
}

install_astra_service() {
  cat >"${ASTRA_SERVICE_PATH}" <<EOF
[Unit]
Description=Astra single-node datastore stack
Wants=docker.service network-online.target
After=docker.service network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
EnvironmentFile=-${ASTRA_ENV_FILE}
ExecStart=${STACK_SCRIPT} up
ExecStop=${STACK_SCRIPT} down
TimeoutStartSec=0
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
EOF
}

install_k3s_override() {
  install -d -m 0755 "$(dirname "${K3S_OVERRIDE_PATH}")"
  cat >"${K3S_OVERRIDE_PATH}" <<EOF
[Unit]
Wants=astra-single-node.service
Requires=astra-single-node.service
After=network-online.target docker.service astra-single-node.service

[Service]
EnvironmentFile=-${ASTRA_ENV_FILE}
ExecStartPre=${DATASTORE_READY_SCRIPT}
EOF
}

install_backup_timer() {
  cat >"${BACKUP_SERVICE_PATH}" <<EOF
[Unit]
Description=Archive Astra live object-tier manifest into dated backup prefix
After=network-online.target

[Service]
Type=oneshot
EnvironmentFile=-${ASTRA_ENV_FILE}
ExecStart=${BACKUP_ARCHIVE_SCRIPT}
EOF

  cat >"${BACKUP_TIMER_PATH}" <<EOF
[Unit]
Description=Daily Astra archive backup timer

[Timer]
OnCalendar=${BACKUP_SCHEDULE}
Persistent=true

[Install]
WantedBy=timers.target
EOF
}

fail_deploy() {
  local step=${1:-unknown}
  local message=${2:-failure}
  FAILURE_STEP="${step}"
  FAILURE_MESSAGE="${message}"
  DEPLOY_STATUS="failed"
  astra_error "${step}: ${message}"
  exit 1
}

check_datastore_ready() {
  local key="/astra/deploy/readiness/${DEPLOY_RUN_ID}"
  local endpoints=()
  local endpoint=""

  # Preferred path for environments that expose etcd maintenance RPCs.
  if etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${DATASTORE_ENDPOINTS}" endpoint status -w json >/dev/null 2>&1; then
    return 0
  fi

  # Some Astra profiles omit maintenance RPCs; fall back to direct KV probes.
  IFS=',' read -r -a endpoints <<< "${DATASTORE_ENDPOINTS}"
  for endpoint in "${endpoints[@]-}"; do
    [ -n "${endpoint}" ] || continue
    if ! etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${endpoint}" put "${key}" "${DEPLOY_RUN_ID}" >/dev/null 2>&1; then
      return 1
    fi
    if ! etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${endpoint}" get "${key}" >/dev/null 2>&1; then
      return 1
    fi
  done

  return 0
}

write_k3s_runtime_overrides() {
  local path=${1:?config path required}
  local snapshotter=${2:?snapshotter required}
  local servicelb_enabled=${3:-false}

  astra_log "writing k3s runtime overrides path=${path} snapshotter=${snapshotter} servicelb_enabled=${servicelb_enabled}"

  if astra_bool_true "${DRY_RUN}"; then
    printf '[astra-deploy][%s][dry-run] write %s with snapshotter=%s servicelb_enabled=%s\n' \
      "$(astra_ts)" "${path}" "${snapshotter}" "${servicelb_enabled}"
    return 0
  fi

  install -d -m 0755 "$(dirname "${path}")"
  {
    printf '# managed by deploy-k3s-single-node.sh\n'
    printf 'snapshotter: %s\n' "${snapshotter}"
    if ! astra_bool_true "${servicelb_enabled}"; then
      printf 'disable+:\n'
      printf '  - servicelb\n'
    fi
  } > "${path}"
}

write_summary() {
  local exit_code=${1:-1}
  local node_total=0
  local node_ready=0
  local kube_system_unhealthy=0
  local tls_sans_json
  local kubelet_args_json

  if command -v k3s >/dev/null 2>&1; then
    K3S_VERSION_INSTALLED=$(k3s --version 2>/dev/null | head -n 1 || true)
  fi

  if [ -f /etc/rancher/k3s/k3s.yaml ] && command -v kubectl >/dev/null 2>&1; then
    export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
    node_total=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ' || echo 0)
    node_ready=$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 ~ /Ready/ {c++} END{print c+0}' || echo 0)
    kube_system_unhealthy=$(kubectl -n kube-system get pods --no-headers 2>/dev/null | awk '$3 !~ /(Running|Completed)/ {c++} END{print c+0}' || echo 0)
  fi

  if [ -f "${COMPOSE_PROD_FILE}" ] && ! astra_bool_true "${DRY_RUN}"; then
    compose_ps_capture
  fi

  tls_sans_json=$(printf '%s\n' "${TLS_SANS_RESOLVED[@]-}" | python3 -c 'import json,sys; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))')
  kubelet_args_json=$(printf '%s\n' "${KUBELET_ARGS[@]-}" | python3 -c 'import json,sys; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))')

  python3 - <<'PY' "${DEPLOY_SUMMARY_PATH}" "${DEPLOY_RUN_ID}" "${exit_code}" "${DEPLOY_STATUS}" \
    "${FAILURE_STEP}" "${FAILURE_MESSAGE}" "${ASTRA_IMAGE_RESOLVED}" "${ASTRA_TAG_RESOLVED}" \
    "${ASTRA_REPO}" "${ASTRA_CONTAINER_MEMORY_LIMIT}" "${ASTRA_DATA_ROOT}" "${HOST_PUBLIC_IP_RESOLVED}" "${DISK_DEVICE}" "${DISK_DEVICE_RESOLVED}" \
    "${DISK_MOUNT}" "${DISK_SETUP_PERFORMED}" "${DATASTORE_ENDPOINTS}" "${VALIDATION_MODE}" \
    "${READINESS_DURATION}" "${VALIDATION_STATUS}" "${VALIDATION_SUMMARY_PATH}" "${K3S_VERSION}" \
    "${K3S_VERSION_INSTALLED}" "${REPO_DIR}" "${RESULTS_DIR}" "${DEPLOY_LOG_PATH}" \
    "${COMPOSE_PS_PATH}" "${node_total}" "${node_ready}" "${kube_system_unhealthy}" \
    "${NODE_NAME}" "${tls_sans_json}" "${kubelet_args_json}" "${BACKUP_TARGET}" "${S3_BUCKET}" \
    "${S3_PREFIX}" "${BACKUP_ARCHIVE_PREFIX}" "${SERVICE_LB_ENABLED}" "${K3S_SNAPSHOTTER}" \
    "${K3S_RUNTIME_CONFIG_FILE}"
import json
import pathlib
import sys

(
    summary_path,
    run_id,
    exit_code,
    status,
    failure_step,
    failure_message,
    astra_image,
    astra_tag,
    astra_repo,
    astra_container_memory_limit,
    astra_data_root,
    host_public_ip,
    disk_device_requested,
    disk_device_resolved,
    disk_mount,
    disk_setup_performed,
    datastore_endpoints,
    validation_mode,
    readiness_duration,
    validation_status,
    validation_summary_path,
    k3s_version_requested,
    k3s_version_installed,
    repo_dir,
    results_dir,
    deploy_log_path,
    compose_ps_path,
    node_total,
    node_ready,
    kube_system_unhealthy,
    node_name,
    tls_sans_json,
    kubelet_args_json,
    backup_target,
    s3_bucket,
    s3_prefix,
    backup_archive_prefix,
    servicelb_enabled,
    k3s_snapshotter,
    k3s_runtime_config_path,
) = sys.argv[1:]

summary = {
    "run_id": run_id,
    "status": status,
    "exit_code": int(exit_code),
    "failure": {
        "step": failure_step,
        "message": failure_message,
    },
    "astra": {
        "image": astra_image,
        "repo": astra_repo,
        "tag": astra_tag,
        "container_memory_limit": astra_container_memory_limit,
        "data_root": astra_data_root,
        "datastore_endpoints": datastore_endpoints,
    },
    "k3s": {
        "requested_version": k3s_version_requested,
        "installed_version": k3s_version_installed,
        "node_name": node_name,
        "kubelet_args": json.loads(kubelet_args_json),
        "kubeconfig_path": "/etc/rancher/k3s/k3s.yaml",
        "runtime_overrides": {
            "servicelb_enabled": servicelb_enabled.lower() in {"1", "true", "yes", "on"},
            "snapshotter": k3s_snapshotter,
            "config_path": k3s_runtime_config_path,
        },
    },
    "network": {
        "host_public_ip": host_public_ip,
        "tls_sans": json.loads(tls_sans_json),
    },
    "disk": {
        "requested": disk_device_requested,
        "resolved": disk_device_resolved,
        "mount": disk_mount,
        "setup_performed": disk_setup_performed.lower() in {"1", "true", "yes", "on"},
    },
    "validation": {
        "mode": validation_mode,
        "duration_secs": int(readiness_duration),
        "status": validation_status,
        "summary_path": validation_summary_path,
    },
    "cluster": {
        "node_total": int(node_total),
        "node_ready": int(node_ready),
        "kube_system_unhealthy_pods": int(kube_system_unhealthy),
    },
    "artifacts": {
        "deploy_log": deploy_log_path,
        "compose_ps": compose_ps_path,
        "results_dir": results_dir,
    },
    "inputs": {
        "repo_dir": repo_dir,
        "backup_target": backup_target,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "backup_archive_prefix": backup_archive_prefix,
        "servicelb_enabled": servicelb_enabled.lower() in {"1", "true", "yes", "on"},
        "snapshotter": k3s_snapshotter,
        "k3s_runtime_config_path": k3s_runtime_config_path,
    },
}

path = pathlib.Path(summary_path)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
PY
}

on_exit() {
  local exit_code=$?
  if [ "${DEPLOY_STATUS}" = "in_progress" ]; then
    if [ "${exit_code}" -eq 0 ]; then
      DEPLOY_STATUS="success"
    else
      DEPLOY_STATUS="failed"
    fi
  fi

  write_summary "${exit_code}" || true

  printf 'DEPLOY_SUMMARY_PATH=%s\n' "${DEPLOY_SUMMARY_PATH}"
  printf 'HOST_PUBLIC_IP=%s\n' "${HOST_PUBLIC_IP_RESOLVED}"
  printf 'REMOTE_KUBECONFIG_PATH=%s\n' "/etc/rancher/k3s/k3s.yaml"
  printf 'VALIDATION_SUMMARY_PATH=%s\n' "${VALIDATION_SUMMARY_PATH}"
}
trap on_exit EXIT

if ! astra_bool_true "${DRY_RUN}" && [ "${EUID:-$(id -u)}" -ne 0 ]; then
  fail_deploy "preflight" "this script must run as root"
fi

if [ ! -d "${REPO_DIR}" ]; then
  fail_deploy "preflight" "repo directory not found: ${REPO_DIR}"
fi

if ! astra_bool_true "${DRY_RUN}" && ! astra_is_ubuntu_2404_amd64; then
  fail_deploy "preflight" "expected Ubuntu 24.04 amd64 host"
fi

astra_log "starting single-node deployment run_id=${DEPLOY_RUN_ID} validation=${VALIDATION_MODE} dry_run=${DRY_RUN}"
astra_log "repo_dir=${REPO_DIR} results_dir=${RESULTS_DIR}"

if ! astra_bool_true "${SKIP_BOOTSTRAP}"; then
  [ -x "${BOOTSTRAP_HOST_SCRIPT}" ] || fail_deploy "bootstrap" "missing script: ${BOOTSTRAP_HOST_SCRIPT}"
  astra_log "running host bootstrap"
  astra_run "${DRY_RUN}" "${BOOTSTRAP_HOST_SCRIPT}" \
    --workspace "$(dirname "${REPO_DIR}")" \
    --repo-dir "${REPO_DIR}" \
    --results-dir "${RESULTS_DIR}" || fail_deploy "bootstrap" "host bootstrap failed"
else
  astra_log "skipping host bootstrap"
fi

if ! astra_bool_true "${SKIP_PHASE6_BOOTSTRAP}"; then
  [ -x "${BOOTSTRAP_PHASE6_SCRIPT}" ] || fail_deploy "bootstrap" "missing script: ${BOOTSTRAP_PHASE6_SCRIPT}"
  astra_log "running phase6 host bootstrap"
  astra_run "${DRY_RUN}" "${BOOTSTRAP_PHASE6_SCRIPT}" || fail_deploy "bootstrap" "phase6 bootstrap failed"
else
  astra_log "skipping phase6 host bootstrap"
fi

if [ -z "${HOST_PUBLIC_IP}" ]; then
  HOST_PUBLIC_IP_RESOLVED=$(astra_detect_public_ip)
else
  HOST_PUBLIC_IP_RESOLVED=${HOST_PUBLIC_IP}
fi

if [ -z "${HOST_PUBLIC_IP_RESOLVED}" ]; then
  fail_deploy "network" "failed to resolve host public ip; set --host-public-ip"
fi

if [ "${BACKUP_TARGET}" = "external-s3" ]; then
  require_nonempty_if "--s3-endpoint" "${S3_ENDPOINT}"
  require_nonempty_if "--s3-bucket" "${S3_BUCKET}"
  if ! astra_bool_true "${DRY_RUN}"; then
    require_nonempty_if "AWS_ACCESS_KEY_ID" "${AWS_ACCESS_KEY_ID:-}"
    require_nonempty_if "AWS_SECRET_ACCESS_KEY" "${AWS_SECRET_ACCESS_KEY:-}"
  fi
fi

if [ "${#TLS_SANS[@]}" -eq 0 ]; then
  TLS_SANS_RESOLVED=("${HOST_PUBLIC_IP_RESOLVED}")
else
  for san in "${TLS_SANS[@]}"; do
    local_seen=false
    for existing in "${TLS_SANS_RESOLVED[@]-}"; do
      if [ "${existing}" = "${san}" ]; then
        local_seen=true
        break
      fi
    done
    if [ "${local_seen}" = "false" ]; then
      TLS_SANS_RESOLVED+=("${san}")
    fi
  done
fi

if [ -n "${ASTRA_IMAGE}" ]; then
  ASTRA_IMAGE_RESOLVED=${ASTRA_IMAGE}
elif [ -n "${ASTRA_TAG}" ]; then
  ASTRA_IMAGE_RESOLVED="${ASTRA_REPO}:${ASTRA_TAG}"
elif astra_bool_true "${RESOLVE_LATEST_STABLE}"; then
  astra_log "resolving latest stable tag for ${ASTRA_REPO}"
  ASTRA_TAG_RESOLVED=$(astra_resolve_latest_stable_tag "${ASTRA_REPO}" || true)
  if [ -z "${ASTRA_TAG_RESOLVED}" ]; then
    fail_deploy "image" "failed to resolve latest stable tag for ${ASTRA_REPO}"
  fi
  ASTRA_IMAGE_RESOLVED="${ASTRA_REPO}:${ASTRA_TAG_RESOLVED}"
else
  fail_deploy "image" "no image specified; set --astra-image or --astra-tag"
fi

if [ -z "${ASTRA_TAG_RESOLVED}" ] && [ -n "${ASTRA_IMAGE_RESOLVED##*:}" ]; then
  ASTRA_TAG_RESOLVED=${ASTRA_IMAGE_RESOLVED##*:}
fi

astra_log "using astra image: ${ASTRA_IMAGE_RESOLVED}"

if [ -n "${DISK_DEVICE}" ]; then
  DISK_DEVICE_RESOLVED=${DISK_DEVICE}
  if [ "${DISK_DEVICE}" = "auto" ]; then
    DISK_DEVICE_RESOLVED=$(astra_find_auto_disk_device)
    if [ -z "${DISK_DEVICE_RESOLVED}" ]; then
      fail_deploy "disk" "auto disk detection found no unmounted empty candidate"
    fi
    astra_log "auto-detected disk device: ${DISK_DEVICE_RESOLVED}"
  fi

  astra_prepare_disk "${DISK_DEVICE_RESOLVED}" "${DISK_MOUNT}" "${DRY_RUN}" "${NON_INTERACTIVE}" "${YES_FLAG}" \
    || fail_deploy "disk" "disk preparation failed"
  DISK_SETUP_PERFORMED=true
else
  astra_log "disk setup skipped (no --disk-device provided)"
fi

[ -f "${COMPOSE_PROD_FILE}" ] || fail_deploy "compose" "missing file: ${COMPOSE_PROD_FILE}"
[ -x "${STACK_SCRIPT}" ] || fail_deploy "compose" "missing script: ${STACK_SCRIPT}"
[ -x "${DATASTORE_READY_SCRIPT}" ] || fail_deploy "datastore" "missing script: ${DATASTORE_READY_SCRIPT}"
if [ "${BACKUP_TARGET}" = "external-s3" ]; then
  [ -f "${COMPOSE_OBJECT_STORE_FILE}" ] || fail_deploy "compose" "missing file: ${COMPOSE_OBJECT_STORE_FILE}"
  [ -x "${BACKUP_ARCHIVE_SCRIPT}" ] || fail_deploy "backup" "missing script: ${BACKUP_ARCHIVE_SCRIPT}"
fi

append_default_kubelet_arg "fail-swap-on=false"
append_default_kubelet_arg "system-reserved=cpu=500m,memory=1Gi,ephemeral-storage=1Gi"
append_default_kubelet_arg "kube-reserved=cpu=250m,memory=512Mi,ephemeral-storage=1Gi"
append_default_kubelet_arg "eviction-hard=memory.available<750Mi,nodefs.available<10%,imagefs.available<15%"
append_default_kubelet_arg "image-gc-high-threshold=70"
append_default_kubelet_arg "image-gc-low-threshold=50"

astra_run "${DRY_RUN}" mkdir -p \
  "${ASTRA_DATA_ROOT}" \
  "${ASTRA_NODE1_DATA_PATH}" \
  "${ASTRA_NODE2_DATA_PATH}" \
  "${ASTRA_NODE3_DATA_PATH}"

if ! astra_bool_true "${DRY_RUN}"; then
  write_runtime_env_file
  install_astra_service
  install_k3s_override
  if [ "${BACKUP_TARGET}" = "external-s3" ]; then
    install_backup_timer
  fi
  systemctl daemon-reload
fi

astra_log "starting astra compose stack"
astra_run "${DRY_RUN}" systemctl enable --now astra-single-node.service || fail_deploy "compose" "failed to start astra-single-node service"

if ! astra_bool_true "${DRY_RUN}"; then
  compose_ps_capture

  if grep -Eq '(0\.0\.0\.0|\[::\]):(52379|52391|52392|19479|19480|19481)' "${COMPOSE_PS_PATH}"; then
    fail_deploy "compose" "detected non-localhost bindings in astra production ports"
  fi

  if ! grep -q '127.0.0.1:52379->2379/tcp' "${COMPOSE_PS_PATH}"; then
    fail_deploy "compose" "expected localhost binding missing for astra-node1 (52379)"
  fi
fi

if ! astra_bool_true "${DRY_RUN}"; then
  astra_log "waiting for datastore endpoints"
  "${DATASTORE_READY_SCRIPT}" || fail_deploy "datastore" "astra endpoints did not become ready"

  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${DATASTORE_ENDPOINTS}" \
    put /astra/deploy/probe "${DEPLOY_RUN_ID}" >/dev/null \
    || fail_deploy "datastore" "put probe failed"
fi

write_k3s_runtime_overrides "${K3S_RUNTIME_CONFIG_FILE}" "${K3S_SNAPSHOTTER}" "${SERVICE_LB_ENABLED}" \
  || fail_deploy "k3s-config" "failed to write k3s runtime overrides"

if [ -n "${K3S_VERSION}" ]; then
  astra_log "installing/upgrading k3s to ${K3S_VERSION}"
else
  astra_log "installing/upgrading k3s to latest"
fi

install_exec="server --write-kubeconfig-mode 644 --node-external-ip ${HOST_PUBLIC_IP_RESOLVED}"
for san in "${TLS_SANS_RESOLVED[@]-}"; do
  install_exec+=" --tls-san ${san}"
done
if [ -n "${NODE_NAME}" ]; then
  install_exec+=" --node-name ${NODE_NAME}"
fi
for kubelet_arg in "${KUBELET_ARGS[@]-}"; do
  install_exec+=" --kubelet-arg ${kubelet_arg}"
done
install_exec+=" --default-local-storage-path ${DISK_MOUNT} --datastore-endpoint '${DATASTORE_ENDPOINTS}'"
if [ -n "${K3S_VERSION}" ]; then
  k3s_install_cmd="curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$(q "${K3S_VERSION}") INSTALL_K3S_EXEC=$(q "${install_exec}") sh -"
else
  k3s_install_cmd="curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC=$(q "${install_exec}") sh -"
fi

astra_run_shell "${DRY_RUN}" "${k3s_install_cmd}" || fail_deploy "k3s-install" "k3s installation failed"

if ! astra_bool_true "${DRY_RUN}"; then
  systemctl daemon-reload
  systemctl restart k3s
  astra_retry 120 2 systemctl is-active --quiet k3s || fail_deploy "k3s-start" "k3s service not active"

  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  astra_retry 120 2 kubectl --request-timeout=5s get --raw=/readyz >/dev/null || fail_deploy "k3s-ready" "k3s API /readyz did not become ready"

  if ! kubectl get nodes --no-headers 2>/dev/null | awk '$2 ~ /Ready/ {ready++} END{exit ready<1}'; then
    fail_deploy "k3s-ready" "no Ready nodes detected"
  fi

  if [ "${BACKUP_TARGET}" = "external-s3" ]; then
    systemctl enable --now astra-tier-archive.timer
  fi
fi

case "${VALIDATION_MODE}" in
  none)
    VALIDATION_STATUS="skipped"
    astra_log "validation skipped (--validation none)"
    ;;
  smoke|full)
    [ -x "${READINESS_SCRIPT}" ] || fail_deploy "validation" "missing script: ${READINESS_SCRIPT}"

    local_run_id="single-node-readiness-$(date -u +%Y%m%dT%H%M%SZ)"
    VALIDATION_SUMMARY_PATH="${RESULTS_DIR}/${local_run_id}/single-node-readiness-summary.json"

    if [ "${VALIDATION_MODE}" = "smoke" ] && [ "${READINESS_DURATION_SET}" = "false" ]; then
      READINESS_DURATION=180
    fi

    astra_log "running readiness validation mode=${VALIDATION_MODE} duration=${READINESS_DURATION}s"
    if [ "${VALIDATION_MODE}" = "smoke" ]; then
      validation_cmd="cd $(q "${REPO_DIR}") && RUN_ID=$(q "${local_run_id}") RESULTS_DIR=$(q "${RESULTS_DIR}") INGRESS_URL=$(q "http://${HOST_PUBLIC_IP_RESOLVED}/astra-ready") DURATION_SECS=$(q "${READINESS_DURATION}") CONFIGMAP_POOL=100 CHURN_WORKERS=2 LIST_WORKERS=1 INGRESS_WORKERS=2 PVC_SIZE=1Gi $(q "${READINESS_SCRIPT}")"
    else
      validation_cmd="cd $(q "${REPO_DIR}") && RUN_ID=$(q "${local_run_id}") RESULTS_DIR=$(q "${RESULTS_DIR}") INGRESS_URL=$(q "http://${HOST_PUBLIC_IP_RESOLVED}/astra-ready") DURATION_SECS=$(q "${READINESS_DURATION}") $(q "${READINESS_SCRIPT}")"
    fi

    if ! astra_run_shell "${DRY_RUN}" "${validation_cmd}"; then
      VALIDATION_STATUS="failed"
      fail_deploy "validation" "readiness script failed"
    fi

    if [ ! -f "${VALIDATION_SUMMARY_PATH}" ] && ! astra_bool_true "${DRY_RUN}"; then
      VALIDATION_STATUS="failed"
      fail_deploy "validation" "expected summary missing: ${VALIDATION_SUMMARY_PATH}"
    fi

    if ! astra_bool_true "${DRY_RUN}" && kubectl get ns astra-readiness >/dev/null 2>&1; then
      astra_warn "validation namespace astra-readiness still exists"
    fi

    VALIDATION_STATUS="passed"
    ;;
esac

DEPLOY_STATUS="success"
astra_log "deployment completed successfully"
