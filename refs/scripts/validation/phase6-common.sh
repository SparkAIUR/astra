#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
RESULTS_DIR=${RESULTS_DIR:-${SCRIPT_DIR}/results}
RUN_ID=${RUN_ID:-phase6-$(date -u +%Y%m%dT%H%M%SZ)}
RUN_DIR=${RUN_DIR:-${RESULTS_DIR}/${RUN_ID}}
RUNTIME_DIR=${RUNTIME_DIR:-${RUN_DIR}/runtime}
COMPOSE_FILE=${COMPOSE_FILE:-${SCRIPT_DIR}/docker-compose.phase6.yml}

mkdir -p "${RESULTS_DIR}" "${RUN_DIR}" "${RUNTIME_DIR}"

phase6_log() {
  printf '[phase6][%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

phase6_require_tools() {
  local missing=0
  for t in "$@"; do
    if ! command -v "$t" >/dev/null 2>&1; then
      phase6_log "missing required tool: ${t}"
      missing=1
    fi
  done
  if [ "${missing}" -ne 0 ]; then
    return 1
  fi
}

phase6_compose() {
  docker compose --project-directory "${REPO_DIR}" -f "${COMPOSE_FILE}" "$@"
}

phase6_wait_for_tcp() {
  local endpoint=${1:?endpoint required}
  local attempts=${2:-90}
  local delay=${3:-1}
  local host=${endpoint%%:*}
  local port=${endpoint##*:}
  local i
  for i in $(seq 1 "${attempts}"); do
    if timeout 1 bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${delay}"
  done
  return 1
}

phase6_detect_blkio_device() {
  if [ -n "${ASTRA_BLKIO_DEVICE:-}" ]; then
    echo "${ASTRA_BLKIO_DEVICE}"
    return 0
  fi

  local disk
  disk=$(lsblk -ndo NAME,TYPE | awk '$2=="disk"{print "/dev/"$1; exit}')
  if [ -z "${disk}" ]; then
    disk="/dev/vda"
  fi
  echo "${disk}"
}

phase6_find_astra_leader() {
  local attempts=${1:-90}
  local delay=${2:-1}
  local ep1="127.0.0.1:${PHASE6_ASTRA_NODE1_PORT:-2379}"
  local ep2="127.0.0.1:${PHASE6_ASTRA_NODE2_PORT:-32391}"
  local ep3="127.0.0.1:${PHASE6_ASTRA_NODE3_PORT:-32392}"
  local eps=("${ep1}" "${ep2}" "${ep3}")
  local i

  for i in $(seq 1 "${attempts}"); do
    local ep
    for ep in "${eps[@]}"; do
      local out
      if out=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${ep}" --write-out=json put "/phase6/probe/leader" "${RUN_ID}" 2>/dev/null); then
        local member_id
        member_id=$(python3 - <<'PY' "${out}"
import json
import sys

try:
    obj = json.loads(sys.argv[1])
except Exception:
    print("")
    raise SystemExit(0)

header = obj.get("header") or {}
mid = header.get("member_id")
if mid is None:
    mid = header.get("memberId")
print("" if mid is None else str(mid))
PY
)
        case "${member_id}" in
          1) echo "${ep1}"; return 0 ;;
          2) echo "${ep2}"; return 0 ;;
          3) echo "${ep3}"; return 0 ;;
          *) echo "${ep}"; return 0 ;;
        esac
      fi
    done
    sleep "${delay}"
  done

  return 1
}

phase6_start_backend() {
  local backend=${1:?backend required}
  export ASTRA_BLKIO_DEVICE=${ASTRA_BLKIO_DEVICE:-$(phase6_detect_blkio_device)}

  case "${backend}" in
    astra)
      phase6_log "starting astra backend stack"
      phase6_compose up -d minio minio-init astra-node1 astra-node2 astra-node3
      phase6_wait_for_tcp "127.0.0.1:${PHASE6_ASTRA_NODE1_PORT:-2379}" 120 1
      phase6_wait_for_tcp "127.0.0.1:${PHASE6_ASTRA_NODE2_PORT:-32391}" 120 1
      phase6_wait_for_tcp "127.0.0.1:${PHASE6_ASTRA_NODE3_PORT:-32392}" 120 1
      local leader_ep
      if [ "${ASTRAD_AUTH_ENABLED:-false}" = "true" ]; then
        leader_ep="127.0.0.1:${PHASE6_ASTRA_NODE1_PORT:-2379}"
      else
        leader_ep=$(phase6_find_astra_leader 120 1) || {
          phase6_log "failed to detect writable Astra endpoint"
          return 1
        }
      fi
      export PHASE6_BACKEND_MODE=astra
      export PHASE6_BACKEND_ENDPOINT="${leader_ep}"
      local ds_ep="http://127.0.0.1:${PHASE6_ASTRA_NODE1_PORT:-2379},http://127.0.0.1:${PHASE6_ASTRA_NODE2_PORT:-32391},http://127.0.0.1:${PHASE6_ASTRA_NODE3_PORT:-32392}"
      export PHASE6_BACKEND_DATASTORE_ENDPOINT="${ds_ep}"
      phase6_log "astra backend endpoint=${PHASE6_BACKEND_ENDPOINT}"
      ;;
    etcd)
      phase6_log "starting etcd backend stack"
      phase6_compose up -d etcd
      phase6_wait_for_tcp "127.0.0.1:${PHASE6_ETCD_PORT:-22379}" 120 1
      local ep="127.0.0.1:${PHASE6_ETCD_PORT:-22379}"
      etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${ep}" put "/phase6/probe/etcd" "${RUN_ID}" >/dev/null
      export PHASE6_BACKEND_MODE=etcd
      export PHASE6_BACKEND_ENDPOINT="${ep}"
      export PHASE6_BACKEND_DATASTORE_ENDPOINT="http://${ep}"
      phase6_log "etcd backend endpoint=${PHASE6_BACKEND_ENDPOINT}"
      ;;
    *)
      phase6_log "unknown backend: ${backend}"
      return 1
      ;;
  esac
}

phase6_stop_backend() {
  local backend=${1:-all}
  case "${backend}" in
    astra)
      phase6_compose rm -sf astra-node1 astra-node2 astra-node3 minio minio-init >/dev/null 2>&1 || true
      ;;
    etcd)
      phase6_compose rm -sf etcd >/dev/null 2>&1 || true
      ;;
    all)
      phase6_compose down -v >/dev/null 2>&1 || true
      ;;
  esac
}

phase6_start_k3s() {
  local datastore_endpoint=${1:?datastore endpoint required}
  local tag=${2:?tag required}
  local https_port=${PHASE6_K3S_HTTPS_PORT:-6443}
  local cluster_cidr=${PHASE6_K3S_CLUSTER_CIDR:-10.42.0.0/12}
  local service_cidr=${PHASE6_K3S_SERVICE_CIDR:-10.96.0.0/16}
  local node_cidr_mask=${PHASE6_K3S_NODE_CIDR_MASK_IPV4:-16}
  local nofile_limit=${PHASE6_K3S_NOFILE:-1048576}
  local kube_max_mutating_inflight=${PHASE9_KUBE_MAX_MUTATING_REQUESTS_INFLIGHT:-${PHASE8_KUBE_MAX_MUTATING_REQUESTS_INFLIGHT:-1000}}
  local kube_max_inflight=${PHASE9_KUBE_MAX_REQUESTS_INFLIGHT:-${PHASE8_KUBE_MAX_REQUESTS_INFLIGHT:-3000}}
  local controller_qps=${PHASE9_CONTROLLER_KUBE_API_QPS:-${PHASE8_CONTROLLER_KUBE_API_QPS:-300}}
  local controller_burst=${PHASE9_CONTROLLER_KUBE_API_BURST:-${PHASE8_CONTROLLER_KUBE_API_BURST:-600}}
  local concurrent_deployment_syncs=${PHASE9_CONTROLLER_CONCURRENT_DEPLOYMENT_SYNCS:-${PHASE8_CONTROLLER_CONCURRENT_DEPLOYMENT_SYNCS:-200}}
  local concurrent_replicaset_syncs=${PHASE9_CONTROLLER_CONCURRENT_REPLICASET_SYNCS:-${PHASE8_CONTROLLER_CONCURRENT_REPLICASET_SYNCS:-200}}
  local concurrent_statefulset_syncs=${PHASE9_CONTROLLER_CONCURRENT_STATEFULSET_SYNCS:-${PHASE8_CONTROLLER_CONCURRENT_STATEFULSET_SYNCS:-100}}
  local concurrent_namespace_syncs=${PHASE9_CONTROLLER_CONCURRENT_NAMESPACE_SYNCS:-${PHASE8_CONTROLLER_CONCURRENT_NAMESPACE_SYNCS:-50}}
  local disable_apf=${PHASE9_KUBE_DISABLE_APF:-true}
  local kubeconfig_qps=${PHASE9_KUBECONFIG_QPS:-1000}
  local kubeconfig_burst=${PHASE9_KUBECONFIG_BURST:-2000}
  local data_dir="${RUNTIME_DIR}/k3s-${tag}"
  local kubeconfig="${RUNTIME_DIR}/kubeconfig-${tag}.yaml"
  local log_file="${RUN_DIR}/k3s-${tag}.log"
  local pid_file="${RUNTIME_DIR}/k3s-${tag}.pid"

  phase6_stop_k3s || true
  systemctl stop k3s >/dev/null 2>&1 || true

  rm -rf "${data_dir}" "${kubeconfig}" "${pid_file}"
  mkdir -p "${data_dir}"

  phase6_log "starting k3s tag=${tag} datastore=${datastore_endpoint}"
  phase6_log "k3s api inflight tuning: max_mutating=${kube_max_mutating_inflight} max_requests=${kube_max_inflight} controller_qps=${controller_qps} controller_burst=${controller_burst} disable_apf=${disable_apf}"
  ulimit -n "${nofile_limit}" >/dev/null 2>&1 || true
  local k3s_args=(
    server
    --write-kubeconfig "${kubeconfig}"
    --write-kubeconfig-mode 644
    --https-listen-port "${https_port}"
    --disable traefik
    --disable servicelb
    --disable local-storage
    --disable metrics-server
    --disable-cloud-controller
    --node-name "phase6-${tag}"
    --cluster-cidr "${cluster_cidr}"
    --service-cidr "${service_cidr}"
    --kube-controller-manager-arg "node-cidr-mask-size-ipv4=${node_cidr_mask}"
    --kube-apiserver-arg "max-mutating-requests-inflight=${kube_max_mutating_inflight}"
    --kube-apiserver-arg "max-requests-inflight=${kube_max_inflight}"
    --kube-controller-manager-arg "kube-api-qps=${controller_qps}"
    --kube-controller-manager-arg "kube-api-burst=${controller_burst}"
    --kube-controller-manager-arg "concurrent-deployment-syncs=${concurrent_deployment_syncs}"
    --kube-controller-manager-arg "concurrent-replicaset-syncs=${concurrent_replicaset_syncs}"
    --kube-controller-manager-arg "concurrent-statefulset-syncs=${concurrent_statefulset_syncs}"
    --kube-controller-manager-arg "concurrent-namespace-syncs=${concurrent_namespace_syncs}"
    --data-dir "${data_dir}"
    --datastore-endpoint "${datastore_endpoint}"
  )
  if [ "${disable_apf}" = "true" ]; then
    k3s_args+=(--kube-apiserver-arg "enable-priority-and-fairness=false")
  fi
  K3S_KUBECONFIG_MODE=644 k3s "${k3s_args[@]}" >"${log_file}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pid_file}"

  export KUBECONFIG="${kubeconfig}"
  local i
  for i in $(seq 1 120); do
    if kubectl --request-timeout=5s get --raw=/readyz >/dev/null 2>&1; then
      python3 - <<'PY' "${kubeconfig}" "${kubeconfig_qps}" "${kubeconfig_burst}"
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
qps = sys.argv[2]
burst = sys.argv[3]
lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
lines = [ln for ln in lines if not ln.startswith("qps:") and not ln.startswith("burst:")]
lines.extend([f"qps: {qps}", f"burst: {burst}"])
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
      phase6_log "k3s kubeconfig tuning applied: qps=${kubeconfig_qps} burst=${kubeconfig_burst}"
      phase6_log "k3s ready tag=${tag}"
      return 0
    fi
    sleep 1
  done

  phase6_log "k3s failed to become ready; log=${log_file}"
  return 1
}

phase6_stop_k3s() {
  local pid_file
  for pid_file in "${RUNTIME_DIR}"/k3s-*.pid; do
    [ -f "${pid_file}" ] || continue
    local pid
    pid=$(cat "${pid_file}" 2>/dev/null || true)
    if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 1
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${pid_file}"
  done
  pkill -f 'k3s server' >/dev/null 2>&1 || true
  pkill -f '/run/k3s/containerd/containerd.sock' >/dev/null 2>&1 || true
  pkill -f '/var/lib/rancher/k3s/.*/containerd-shim-runc-v2' >/dev/null 2>&1 || true
  rm -rf /run/k3s >/dev/null 2>&1 || true

  # Ensure old CNI allocation state does not leak across backend runs.
  for iface in cni0 flannel.1; do
    ip link delete "${iface}" >/dev/null 2>&1 || true
  done
  for cni_dir in /var/lib/cni/networks/cni0 /var/lib/cni/networks/flannel /var/lib/cni/networks/flannel.1; do
    if [ -d "${cni_dir}" ]; then
      find "${cni_dir}" -mindepth 1 -maxdepth 1 -type f -delete >/dev/null 2>&1 || true
    fi
  done
}

phase6_start_telemetry() {
  local tag=${1:?tag required}
  local tele_dir="${RUN_DIR}/telemetry-${tag}"
  local pid_file="${tele_dir}/pids.txt"
  mkdir -p "${tele_dir}"
  : >"${pid_file}"

  iostat -dx 1 >"${tele_dir}/iostat.log" 2>&1 &
  echo "$!" >>"${pid_file}"
  vmstat 1 >"${tele_dir}/vmstat.log" 2>&1 &
  echo "$!" >>"${pid_file}"
  pidstat -dur 1 >"${tele_dir}/pidstat.log" 2>&1 &
  echo "$!" >>"${pid_file}"
  (
    while true; do
      date -u +%Y-%m-%dT%H:%M:%SZ
      docker stats --no-stream --format 'container={{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} net={{.NetIO}} block={{.BlockIO}}'
      sleep 1
    done
  ) >"${tele_dir}/docker-stats.log" 2>&1 &
  echo "$!" >>"${pid_file}"

  export PHASE6_TELEMETRY_DIR="${tele_dir}"
}

phase6_stop_telemetry() {
  local tele_dir=${1:-${PHASE6_TELEMETRY_DIR:-}}
  [ -n "${tele_dir}" ] || return 0
  local pid_file="${tele_dir}/pids.txt"
  [ -f "${pid_file}" ] || return 0
  while IFS= read -r pid; do
    [ -n "${pid}" ] || continue
    kill "${pid}" >/dev/null 2>&1 || true
  done <"${pid_file}"
}
