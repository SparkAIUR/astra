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
  --resolve-latest-stable <bool> Resolve latest stable semver tag from Docker Hub (default: true)
  --k3s-version <version>        Optional k3s version pin (default: latest from get.k3s.io)
  --host-public-ip <ip>          Public IP for K3s TLS SAN / Traefik ingress (default: auto-detect)
  --disk-device <path|auto>      Disk device to prepare/mount; omit to skip disk setup
  --disk-mount <path>            K3s local storage mount path (default: /var/lib/rancher/k3s/storage)
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
  deploy-k3s-single-node.sh --astra-tag v0.1.0 --host-public-ip 162.209.124.74 --validation smoke
USAGE
}

ASTRA_IMAGE=""
ASTRA_REPO=${ASTRA_REPO:-docker.io/halceon/astra}
ASTRA_TAG=""
RESOLVE_LATEST_STABLE=${RESOLVE_LATEST_STABLE:-true}
K3S_VERSION=${K3S_VERSION:-}
HOST_PUBLIC_IP=${HOST_PUBLIC_IP:-}
DISK_DEVICE=${DISK_DEVICE:-}
DISK_MOUNT=${DISK_MOUNT:-/var/lib/rancher/k3s/storage}
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
    --resolve-latest-stable) RESOLVE_LATEST_STABLE=${2:?missing value for --resolve-latest-stable}; shift 2 ;;
    --k3s-version) K3S_VERSION=${2:?missing value for --k3s-version}; shift 2 ;;
    --host-public-ip) HOST_PUBLIC_IP=${2:?missing value for --host-public-ip}; shift 2 ;;
    --disk-device) DISK_DEVICE=${2:?missing value for --disk-device}; shift 2 ;;
    --disk-mount) DISK_MOUNT=${2:?missing value for --disk-mount}; shift 2 ;;
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

if [ -n "${READINESS_DURATION}" ] && ! [[ "${READINESS_DURATION}" =~ ^[0-9]+$ ]]; then
  astra_die "--readiness-duration must be an integer number of seconds"
fi

if [ -z "${READINESS_DURATION}" ]; then
  case "${VALIDATION_MODE}" in
    smoke) READINESS_DURATION=180 ;;
    full) READINESS_DURATION=720 ;;
    none) READINESS_DURATION=0 ;;
  esac
fi

COMPOSE_IMAGE_FILE="${REPO_DIR}/refs/scripts/validation/docker-compose.image.yml"
COMPOSE_SINGLE_NODE_FILE="${REPO_DIR}/refs/scripts/validation/docker-compose.k3s-single-node.yml"
READINESS_SCRIPT="${REPO_DIR}/refs/scripts/validation/k3s-single-node-readiness.sh"
BOOTSTRAP_HOST_SCRIPT="${REPO_DIR}/refs/scripts/validation/bootstrap-ubuntu24-host.sh"
BOOTSTRAP_PHASE6_SCRIPT="${REPO_DIR}/refs/scripts/validation/bootstrap-phase6-host.sh"

DEPLOY_RUN_DIR="${RESULTS_DIR}/${DEPLOY_RUN_ID}"
DEPLOY_SUMMARY_PATH="${DEPLOY_RUN_DIR}/deployment-summary.json"
DEPLOY_LOG_PATH="${DEPLOY_RUN_DIR}/deploy.log"
COMPOSE_PS_PATH="${DEPLOY_RUN_DIR}/compose-ps.txt"

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
DISK_DEVICE_RESOLVED=""
DISK_SETUP_PERFORMED=false
DATASTORE_ENDPOINTS="http://127.0.0.1:52379,http://127.0.0.1:52391,http://127.0.0.1:52392"
VALIDATION_STATUS="not_run"
VALIDATION_SUMMARY_PATH=""
K3S_VERSION_INSTALLED=""

q() {
  printf '%q' "$1"
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

write_summary() {
  local exit_code=${1:-1}
  local node_total=0
  local node_ready=0
  local kube_system_unhealthy=0

  if command -v k3s >/dev/null 2>&1; then
    K3S_VERSION_INSTALLED=$(k3s --version 2>/dev/null | head -n 1 || true)
  fi

  if [ -f /etc/rancher/k3s/k3s.yaml ] && command -v kubectl >/dev/null 2>&1; then
    export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
    node_total=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ' || echo 0)
    node_ready=$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 ~ /Ready/ {c++} END{print c+0}' || echo 0)
    kube_system_unhealthy=$(kubectl -n kube-system get pods --no-headers 2>/dev/null | awk '$3 !~ /(Running|Completed)/ {c++} END{print c+0}' || echo 0)
  fi

  if [ -f "${COMPOSE_IMAGE_FILE}" ] && [ -f "${COMPOSE_SINGLE_NODE_FILE}" ]; then
    (
      cd "${REPO_DIR}" &&
      ASTRA_IMAGE="${ASTRA_IMAGE_RESOLVED:-${ASTRA_IMAGE:-unset}}" docker compose \
        -f "${COMPOSE_IMAGE_FILE}" \
        -f "${COMPOSE_SINGLE_NODE_FILE}" \
        ps
    ) > "${COMPOSE_PS_PATH}" 2>&1 || true
  fi

  python3 - <<'PY' "${DEPLOY_SUMMARY_PATH}" "${DEPLOY_RUN_ID}" "${exit_code}" "${DEPLOY_STATUS}" \
    "${FAILURE_STEP}" "${FAILURE_MESSAGE}" "${ASTRA_IMAGE_RESOLVED}" "${ASTRA_TAG_RESOLVED}" \
    "${ASTRA_REPO}" "${HOST_PUBLIC_IP_RESOLVED}" "${DISK_DEVICE}" "${DISK_DEVICE_RESOLVED}" \
    "${DISK_MOUNT}" "${DISK_SETUP_PERFORMED}" "${DATASTORE_ENDPOINTS}" "${VALIDATION_MODE}" \
    "${READINESS_DURATION}" "${VALIDATION_STATUS}" "${VALIDATION_SUMMARY_PATH}" "${K3S_VERSION}" \
    "${K3S_VERSION_INSTALLED}" "${REPO_DIR}" "${RESULTS_DIR}" "${DEPLOY_LOG_PATH}" \
    "${COMPOSE_PS_PATH}" "${node_total}" "${node_ready}" "${kube_system_unhealthy}"
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
        "datastore_endpoints": datastore_endpoints,
    },
    "k3s": {
        "requested_version": k3s_version_requested,
        "installed_version": k3s_version_installed,
        "kubeconfig_path": "/etc/rancher/k3s/k3s.yaml",
    },
    "network": {
        "host_public_ip": host_public_ip,
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

[ -f "${COMPOSE_IMAGE_FILE}" ] || fail_deploy "compose" "missing file: ${COMPOSE_IMAGE_FILE}"
[ -f "${COMPOSE_SINGLE_NODE_FILE}" ] || fail_deploy "compose" "missing file: ${COMPOSE_SINGLE_NODE_FILE}"

astra_log "starting astra compose stack"
compose_up_cmd="cd $(q "${REPO_DIR}") && ASTRA_IMAGE=$(q "${ASTRA_IMAGE_RESOLVED}") docker compose -f $(q "${COMPOSE_IMAGE_FILE}") -f $(q "${COMPOSE_SINGLE_NODE_FILE}") up -d --force-recreate minio minio-init astra-node1 astra-node2 astra-node3"
astra_run_shell "${DRY_RUN}" "${compose_up_cmd}" || fail_deploy "compose" "failed to start compose stack"

if ! astra_bool_true "${DRY_RUN}"; then
  (
    cd "${REPO_DIR}" &&
    ASTRA_IMAGE="${ASTRA_IMAGE_RESOLVED}" docker compose \
      -f "${COMPOSE_IMAGE_FILE}" \
      -f "${COMPOSE_SINGLE_NODE_FILE}" \
      ps
  ) | tee "${COMPOSE_PS_PATH}" >/dev/null

  if grep -Eq '(0\.0\.0\.0|\[::\]):(2379|32391|32392|9000|9001)' "${COMPOSE_PS_PATH}"; then
    fail_deploy "compose" "detected non-localhost datastore/minio bindings in compose ports"
  fi

  if ! grep -q '127.0.0.1:52379->2379/tcp' "${COMPOSE_PS_PATH}"; then
    fail_deploy "compose" "expected localhost binding missing for astra-node1 (52379)"
  fi
fi

if ! astra_bool_true "${DRY_RUN}"; then
  astra_log "waiting for datastore endpoints"
  astra_retry 120 2 etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${DATASTORE_ENDPOINTS}" endpoint status -w json >/dev/null \
    || fail_deploy "datastore" "astra endpoints did not become ready"

  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${DATASTORE_ENDPOINTS}" \
    put /astra/deploy/probe "${DEPLOY_RUN_ID}" >/dev/null \
    || fail_deploy "datastore" "put probe failed"
fi

if [ -n "${K3S_VERSION}" ]; then
  astra_log "installing/upgrading k3s to ${K3S_VERSION}"
else
  astra_log "installing/upgrading k3s to latest"
fi

install_exec="server --write-kubeconfig-mode 644 --node-external-ip ${HOST_PUBLIC_IP_RESOLVED} --tls-san ${HOST_PUBLIC_IP_RESOLVED} --default-local-storage-path ${DISK_MOUNT} --datastore-endpoint '${DATASTORE_ENDPOINTS}'"
if [ -n "${K3S_VERSION}" ]; then
  k3s_install_cmd="curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$(q "${K3S_VERSION}") INSTALL_K3S_EXEC=$(q "${install_exec}") sh -"
else
  k3s_install_cmd="curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC=$(q "${install_exec}") sh -"
fi

astra_run_shell "${DRY_RUN}" "${k3s_install_cmd}" || fail_deploy "k3s-install" "k3s installation failed"

if ! astra_bool_true "${DRY_RUN}"; then
  astra_retry 120 2 systemctl is-active --quiet k3s || fail_deploy "k3s-start" "k3s service not active"

  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  astra_retry 120 2 kubectl --request-timeout=5s get --raw=/readyz >/dev/null || fail_deploy "k3s-ready" "k3s API /readyz did not become ready"

  if ! kubectl get nodes --no-headers 2>/dev/null | awk '$2 ~ /Ready/ {ready++} END{exit ready<1}'; then
    fail_deploy "k3s-ready" "no Ready nodes detected"
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
