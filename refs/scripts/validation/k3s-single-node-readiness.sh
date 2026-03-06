#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RESULTS_DIR=${RESULTS_DIR:-${ASTRA_RESULTS_DIR:-${SCRIPT_DIR}/results}}
RUN_ID=${RUN_ID:-single-node-readiness-$(date -u +%Y%m%dT%H%M%SZ)}
RUN_DIR=${RUN_DIR:-${RESULTS_DIR}/${RUN_ID}}
NAMESPACE=${NAMESPACE:-astra-readiness}
DURATION_SECS=${DURATION_SECS:-720}
CONFIGMAP_POOL=${CONFIGMAP_POOL:-300}
CHURN_WORKERS=${CHURN_WORKERS:-4}
LIST_WORKERS=${LIST_WORKERS:-2}
INGRESS_WORKERS=${INGRESS_WORKERS:-4}
INGRESS_URL=${INGRESS_URL:-http://162.209.124.74/astra-ready}
PVC_SIZE=${PVC_SIZE:-4Gi}
KEEP_RESOURCES=${KEEP_RESOURCES:-false}

mkdir -p "${RUN_DIR}"
SUMMARY_JSON="${RUN_DIR}/single-node-readiness-summary.json"

log() {
  printf '[single-node-readiness][%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_tools() {
  local missing=0
  local t
  for t in kubectl curl python3; do
    if ! command -v "${t}" >/dev/null 2>&1; then
      log "missing tool: ${t}"
      missing=1
    fi
  done
  if [ "${missing}" -ne 0 ]; then
    return 1
  fi
}

wait_for_ready_pod() {
  local ns=${1:?namespace required}
  local label=${2:?label selector required}

  local found=false
  local i
  for i in $(seq 1 60); do
    if kubectl -n "${ns}" get pods -l "${label}" --no-headers 2>/dev/null | grep -q .; then
      found=true
      break
    fi
    sleep 2
  done

  if [ "${found}" != "true" ]; then
    log "timed out waiting for pod selector=${label} in namespace=${ns}"
    return 1
  fi

  kubectl -n "${ns}" wait --for=condition=Ready "pod" -l "${label}" --timeout=180s >/dev/null
}

cleanup_namespace() {
  if [ "${KEEP_RESOURCES}" = "true" ]; then
    log "KEEP_RESOURCES=true, skipping cleanup for namespace ${NAMESPACE}"
    return 0
  fi

  log "cleaning namespace ${NAMESPACE}"
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  local i
  for i in $(seq 1 120); do
    if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
      log "namespace ${NAMESPACE} removed"
      return 0
    fi
    sleep 2
  done
  log "namespace ${NAMESPACE} still terminating after timeout"
  return 1
}

terminate_workers() {
  local pid_file=${1:?pid file required}
  [ -f "${pid_file}" ] || return 0
  while IFS= read -r pid; do
    [ -n "${pid}" ] || continue
    kill "${pid}" >/dev/null 2>&1 || true
  done <"${pid_file}"
}

churn_worker() {
  local id=${1:?worker id required}
  local end_epoch=${2:?end epoch required}
  local stats_file=${3:?stats file required}
  local ok=0
  local fail=0
  local i=0
  while [ "$(date +%s)" -lt "${end_epoch}" ]; do
    local key=$(( (i % CONFIGMAP_POOL) + 1 ))
    if kubectl --request-timeout=6s -n "${NAMESPACE}" patch "configmap/cm-${key}" --type merge \
      -p "{\"data\":{\"value\":\"w${id}-$(date +%s%N)\"}}" >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi
    i=$((i + 1))
  done
  printf '%s %s\n' "${ok}" "${fail}" >"${stats_file}"
}

list_worker() {
  local end_epoch=${1:?end epoch required}
  local stats_file=${2:?stats file required}
  local ok=0
  local fail=0
  while [ "$(date +%s)" -lt "${end_epoch}" ]; do
    if kubectl --request-timeout=8s -n "${NAMESPACE}" get configmaps --chunk-size=200 -o json >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi
  done
  printf '%s %s\n' "${ok}" "${fail}" >"${stats_file}"
}

ingress_worker() {
  local end_epoch=${1:?end epoch required}
  local stats_file=${2:?stats file required}
  local ok=0
  local fail=0
  while [ "$(date +%s)" -lt "${end_epoch}" ]; do
    if curl -fsS --max-time 3 "${INGRESS_URL}" >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi
  done
  printf '%s %s\n' "${ok}" "${fail}" >"${stats_file}"
}

io_worker() {
  local end_epoch=${1:?end epoch required}
  local stats_file=${2:?stats file required}
  local ok=0
  local fail=0
  while [ "$(date +%s)" -lt "${end_epoch}" ]; do
    if kubectl --request-timeout=10s -n "${NAMESPACE}" exec io-probe -- sh -ec \
      'ts=$(date +%s%N); echo "${ts}" >> /data/io.log; tail -n 1 /data/io.log >/dev/null' >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi
    sleep 1
  done
  printf '%s %s\n' "${ok}" "${fail}" >"${stats_file}"
}

sum_stats() {
  local pattern=${1:?glob pattern required}
  local total_ok=0
  local total_fail=0
  local f
  for f in ${pattern}; do
    [ -f "${f}" ] || continue
    local ok fail
    read -r ok fail <"${f}" || true
    ok=${ok:-0}
    fail=${fail:-0}
    total_ok=$((total_ok + ok))
    total_fail=$((total_fail + fail))
  done
  printf '%s %s\n' "${total_ok}" "${total_fail}"
}

main() {
  require_tools

  log "preparing namespace ${NAMESPACE}"
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  log "deploying ingress workload"
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: apps/v1
kind: Deployment
metadata:
  name: whoami
  namespace: ${NAMESPACE}
spec:
  replicas: 2
  selector:
    matchLabels:
      app: whoami
  template:
    metadata:
      labels:
        app: whoami
    spec:
      containers:
      - name: whoami
        image: traefik/whoami:v1.10.3
        ports:
        - containerPort: 80
---
apiVersion: v1
kind: Service
metadata:
  name: whoami
  namespace: ${NAMESPACE}
spec:
  selector:
    app: whoami
  ports:
  - name: http
    port: 80
    targetPort: 80
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: whoami
  namespace: ${NAMESPACE}
spec:
  ingressClassName: traefik
  rules:
  - http:
      paths:
      - path: /astra-ready
        pathType: Prefix
        backend:
          service:
            name: whoami
            port:
              number: 80
EOF

  kubectl -n "${NAMESPACE}" rollout status deployment/whoami --timeout=180s >/dev/null
  wait_for_ready_pod "${NAMESPACE}" "app=whoami"

  log "deploying local-path PVC IO probe"
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: io-pvc
  namespace: ${NAMESPACE}
spec:
  accessModes:
  - ReadWriteOnce
  storageClassName: local-path
  resources:
    requests:
      storage: ${PVC_SIZE}
---
apiVersion: v1
kind: Pod
metadata:
  name: io-probe
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Always
  containers:
  - name: io-probe
    image: busybox:1.36
    command: ["sh", "-ec", "sleep 86400"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: io-pvc
EOF
  local pvc_bound=false
  local attempt
  for attempt in $(seq 1 60); do
    if [ "$(kubectl -n "${NAMESPACE}" get pvc/io-pvc -o jsonpath='{.status.phase}' 2>/dev/null || true)" = "Bound" ]; then
      pvc_bound=true
      break
    fi
    sleep 5
  done
  if [ "${pvc_bound}" != "true" ]; then
    log "io-pvc did not bind in time; dumping diagnostics"
    kubectl -n "${NAMESPACE}" get pvc io-pvc -o wide || true
    kubectl -n "${NAMESPACE}" describe pvc io-pvc || true
    kubectl -n "${NAMESPACE}" describe pod io-probe || true
    return 1
  fi
  kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod/io-probe --timeout=300s >/dev/null
  kubectl -n "${NAMESPACE}" exec io-probe -- sh -ec 'echo ready > /data/probe.txt; grep -q ready /data/probe.txt' >/dev/null

  log "pre-loading ${CONFIGMAP_POOL} configmaps"
  local configmap_yaml="${RUN_DIR}/configmaps-seed.yaml"
  python3 - <<'PY' "${NAMESPACE}" "${CONFIGMAP_POOL}" >"${configmap_yaml}"
import sys

ns = sys.argv[1]
count = int(sys.argv[2])
for i in range(1, count + 1):
    print("---")
    print("apiVersion: v1")
    print("kind: ConfigMap")
    print("metadata:")
    print(f"  name: cm-{i}")
    print(f"  namespace: {ns}")
    print("data:")
    print(f"  value: seed-{i}")
PY
  kubectl apply -f "${configmap_yaml}" >/dev/null

  log "starting stress workers duration_secs=${DURATION_SECS}"
  local end_epoch
  end_epoch=$(( $(date +%s) + DURATION_SECS ))
  local pid_file="${RUN_DIR}/worker-pids.txt"
  : >"${pid_file}"

  local i
  for i in $(seq 1 "${CHURN_WORKERS}"); do
    churn_worker "${i}" "${end_epoch}" "${RUN_DIR}/churn-${i}.txt" &
    echo "$!" >>"${pid_file}"
  done
  for i in $(seq 1 "${LIST_WORKERS}"); do
    list_worker "${end_epoch}" "${RUN_DIR}/list-${i}.txt" &
    echo "$!" >>"${pid_file}"
  done
  for i in $(seq 1 "${INGRESS_WORKERS}"); do
    ingress_worker "${end_epoch}" "${RUN_DIR}/ingress-${i}.txt" &
    echo "$!" >>"${pid_file}"
  done
  io_worker "${end_epoch}" "${RUN_DIR}/io.txt" &
  echo "$!" >>"${pid_file}"

  while [ "$(date +%s)" -lt "${end_epoch}" ]; do
    sleep 5
  done

  terminate_workers "${pid_file}"
  wait || true

  local churn_ok churn_fail list_ok list_fail ingress_ok ingress_fail io_ok io_fail
  read -r churn_ok churn_fail < <(sum_stats "${RUN_DIR}/churn-*.txt")
  read -r list_ok list_fail < <(sum_stats "${RUN_DIR}/list-*.txt")
  read -r ingress_ok ingress_fail < <(sum_stats "${RUN_DIR}/ingress-*.txt")
  read -r io_ok io_fail < <(sum_stats "${RUN_DIR}/io.txt")

  local node_total node_ready kube_system_unhealthy
  node_total=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
  node_ready=$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 ~ /Ready/ {c++} END{print c+0}')
  kube_system_unhealthy=$(kubectl -n kube-system get pods --no-headers 2>/dev/null | awk '$3 !~ /(Running|Completed)/ {c++} END{print c+0}')

  python3 - <<'PY' \
"${SUMMARY_JSON}" "${RUN_ID}" "${NAMESPACE}" "${DURATION_SECS}" \
"${INGRESS_URL}" "${churn_ok}" "${churn_fail}" "${list_ok}" "${list_fail}" \
"${ingress_ok}" "${ingress_fail}" "${io_ok}" "${io_fail}" \
"${node_total}" "${node_ready}" "${kube_system_unhealthy}"
import json
import sys

summary_path = sys.argv[1]
run_id = sys.argv[2]
namespace = sys.argv[3]
duration_secs = int(sys.argv[4])
ingress_url = sys.argv[5]
churn_ok, churn_fail = int(sys.argv[6]), int(sys.argv[7])
list_ok, list_fail = int(sys.argv[8]), int(sys.argv[9])
ing_ok, ing_fail = int(sys.argv[10]), int(sys.argv[11])
io_ok, io_fail = int(sys.argv[12]), int(sys.argv[13])
node_total, node_ready = int(sys.argv[14]), int(sys.argv[15])
kube_unhealthy = int(sys.argv[16])

def ratio(ok: int, fail: int) -> float:
    total = ok + fail
    if total <= 0:
        return 1.0
    return fail / total

summary = {
    "run_id": run_id,
    "namespace": namespace,
    "duration_secs": duration_secs,
    "ingress_url": ingress_url,
    "workloads": {
        "configmap_churn": {"ok": churn_ok, "fail": churn_fail, "error_rate": ratio(churn_ok, churn_fail)},
        "api_list": {"ok": list_ok, "fail": list_fail, "error_rate": ratio(list_ok, list_fail)},
        "ingress_http": {"ok": ing_ok, "fail": ing_fail, "error_rate": ratio(ing_ok, ing_fail)},
        "pvc_io": {"ok": io_ok, "fail": io_fail, "error_rate": ratio(io_ok, io_fail)},
    },
    "cluster": {
        "node_total": node_total,
        "node_ready": node_ready,
        "kube_system_unhealthy_pods": kube_unhealthy,
    },
}
summary["pass"] = (
    node_total > 0
    and node_total == node_ready
    and kube_unhealthy == 0
    and summary["workloads"]["ingress_http"]["error_rate"] <= 0.03
    and summary["workloads"]["configmap_churn"]["error_rate"] <= 0.05
    and summary["workloads"]["api_list"]["error_rate"] <= 0.05
    and summary["workloads"]["pvc_io"]["error_rate"] <= 0.02
)

with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)
print(json.dumps(summary, indent=2))
PY

  cleanup_namespace
  log "summary: ${SUMMARY_JSON}"
}

trap 'terminate_workers "${RUN_DIR}/worker-pids.txt" >/dev/null 2>&1 || true' EXIT

main "$@"
