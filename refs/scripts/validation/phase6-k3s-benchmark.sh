#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

BACKEND_MODE=${BACKEND_MODE:-both}
QUICK=${QUICK:-false}
NODES=${PHASE6_NODE_TARGET:-1000}
PODS=${PHASE6_POD_TARGET:-50000}
CONFIGMAPS=${PHASE6_CONFIGMAP_TARGET:-10000}
PUT_SAMPLES=${PHASE6_PUT_SAMPLES:-200}
GET_SAMPLES=${PHASE6_GET_SAMPLES:-200}
LIST_SAMPLES=${PHASE6_LIST_SAMPLES:-50}
HEARTBEAT_OPS=${PHASE6_HEARTBEAT_OPS:-2000}
PARALLELISM=${PHASE6_PARALLELISM:-${PHASE9_INJECTION_PARALLELISM:-${PHASE8_INJECTION_PARALLELISM:-96}}}
HARD_POD_GATE=${PHASE9_HARD_POD_GATE:-${PHASE8_HARD_POD_GATE:-false}}
POD_INFLATION_MODE=${PHASE9_POD_INFLATION_MODE:-deployment}
API_TIMEOUT=${PHASE6_API_REQUEST_TIMEOUT:-5s}
TELEMETRY_WARMUP_SECS=${PHASE9_TELEMETRY_WARMUP_SECS:-90}
DISABLE_TIERING_FOR_K3S=${PHASE9_DISABLE_TIERING_FOR_K3S:-false}
ADAPTIVE_IOWAIT_ENABLED=${PHASE9_ADAPTIVE_IOWAIT_ENABLED:-false}
ADAPTIVE_IOWAIT_TARGET=${PHASE9_ADAPTIVE_IOWAIT_TARGET:-8}
ADAPTIVE_IOWAIT_MAX=${PHASE9_ADAPTIVE_IOWAIT_MAX:-12}
ADAPTIVE_MIN_SLEEP_MS=${PHASE9_ADAPTIVE_MIN_SLEEP_MS:-0}
ADAPTIVE_MAX_SLEEP_MS=${PHASE9_ADAPTIVE_MAX_SLEEP_MS:-250}
ADAPTIVE_STEP_MS=${PHASE9_ADAPTIVE_STEP_MS:-25}
ADAPTIVE_POD_CHUNK_MIN=${PHASE9_POD_MANIFEST_CHUNK_MIN:-250}
ADAPTIVE_POD_CHUNK_MAX=${PHASE9_POD_MANIFEST_CHUNK_MAX:-2000}
NODES_QUICK_MAX=200
PODS_QUICK_MAX=5000
CONFIGMAPS_QUICK_MAX=1000

min_int() {
  local a=${1:?}
  local b=${2:?}
  if [ "${a}" -le "${b}" ]; then
    echo "${a}"
  else
    echo "${b}"
  fi
}

max_int() {
  local a=${1:?}
  local b=${2:?}
  if [ "${a}" -ge "${b}" ]; then
    echo "${a}"
  else
    echo "${b}"
  fi
}

clamp_int() {
  local v=${1:?}
  local lo=${2:?}
  local hi=${3:?}
  if [ "${v}" -lt "${lo}" ]; then
    echo "${lo}"
    return
  fi
  if [ "${v}" -gt "${hi}" ]; then
    echo "${hi}"
    return
  fi
  echo "${v}"
}

phase9_latest_vmstat_triplet() {
  local tele_dir=${PHASE6_TELEMETRY_DIR:-}
  local vmstat_log="${tele_dir}/vmstat.log"
  if [ -z "${tele_dir}" ] || [ ! -f "${vmstat_log}" ]; then
    echo "0 0 0"
    return
  fi

  awk 'NF>=17 && $1!="procs" && $1!="r" && $1!="b"{si=$7;so=$8;wa=$16} END{printf "%d %d %d\n", int(wa+0), int(si+0), int(so+0)}' "${vmstat_log}"
}

phase9_sleep_ms() {
  local ms=${1:-0}
  if [ "${ms}" -le 0 ]; then
    return 0
  fi
  sleep "$(python3 - <<'PY' "${ms}"
import sys
ms = max(0, int(sys.argv[1]))
print(f"{ms/1000:.3f}")
PY
)"
}

phase9_adaptive_tick() {
  local current_chunk=${1:?current chunk required}
  local current_sleep_ms=${2:?current sleep required}

  local next_chunk=${current_chunk}
  local next_sleep_ms=${current_sleep_ms}

  local snapshot
  snapshot=$(phase9_latest_vmstat_triplet)
  local wa si so
  wa=$(echo "${snapshot}" | awk '{print int($1)}')
  si=$(echo "${snapshot}" | awk '{print int($2)}')
  so=$(echo "${snapshot}" | awk '{print int($3)}')

  if [ "${ADAPTIVE_IOWAIT_ENABLED}" = "true" ]; then
    if [ "${si}" -gt 0 ] || [ "${so}" -gt 0 ] || [ "${wa}" -gt "${ADAPTIVE_IOWAIT_MAX}" ]; then
      next_sleep_ms=$(clamp_int $((current_sleep_ms + ADAPTIVE_STEP_MS)) "${ADAPTIVE_MIN_SLEEP_MS}" "${ADAPTIVE_MAX_SLEEP_MS}")
      next_chunk=$(max_int "${ADAPTIVE_POD_CHUNK_MIN}" $((current_chunk / 2)))
    elif [ "${wa}" -gt "${ADAPTIVE_IOWAIT_TARGET}" ]; then
      next_sleep_ms=$(clamp_int $((current_sleep_ms + ADAPTIVE_STEP_MS)) "${ADAPTIVE_MIN_SLEEP_MS}" "${ADAPTIVE_MAX_SLEEP_MS}")
      next_chunk=$(max_int "${ADAPTIVE_POD_CHUNK_MIN}" $((current_chunk - 250)))
    elif [ "${wa}" -lt $((ADAPTIVE_IOWAIT_TARGET - 2)) ]; then
      next_sleep_ms=$(clamp_int $((current_sleep_ms - ADAPTIVE_STEP_MS)) "${ADAPTIVE_MIN_SLEEP_MS}" "${ADAPTIVE_MAX_SLEEP_MS}")
      next_chunk=$(min_int "${ADAPTIVE_POD_CHUNK_MAX}" $((current_chunk + 250)))
    fi
  fi

  next_chunk=$(clamp_int "${next_chunk}" "${ADAPTIVE_POD_CHUNK_MIN}" "${ADAPTIVE_POD_CHUNK_MAX}")
  next_sleep_ms=$(clamp_int "${next_sleep_ms}" "${ADAPTIVE_MIN_SLEEP_MS}" "${ADAPTIVE_MAX_SLEEP_MS}")
  echo "${next_chunk} ${next_sleep_ms} ${wa} ${si} ${so}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --backend)
      BACKEND_MODE=${2:?missing value for --backend}
      shift 2
      ;;
    --quick)
      QUICK=true
      shift
      ;;
    --nodes)
      NODES=${2:?missing value for --nodes}
      shift 2
      ;;
    --pods)
      PODS=${2:?missing value for --pods}
      shift 2
      ;;
    --configmaps)
      CONFIGMAPS=${2:?missing value for --configmaps}
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [ "${QUICK}" = "true" ]; then
  NODES=$(min_int "${NODES}" "${NODES_QUICK_MAX}")
  PODS=$(min_int "${PODS}" "${PODS_QUICK_MAX}")
  CONFIGMAPS=$(min_int "${CONFIGMAPS}" "${CONFIGMAPS_QUICK_MAX}")
  PUT_SAMPLES=80
  GET_SAMPLES=80
  LIST_SAMPLES=20
  HEARTBEAT_OPS=400
fi

phase6_require_tools docker etcdctl jq python3 kubectl k3s iostat pidstat vmstat

install_kwok_components() {
  local kwok_manifest=${KWOK_MANIFEST_PATH:-/usr/local/share/kwok.yaml}
  if [ -f "${kwok_manifest}" ]; then
    kubectl apply -f "${kwok_manifest}" >/dev/null 2>&1 || true
  elif command -v kwokctl >/dev/null 2>&1; then
    local tag
    tag=$(kwokctl version 2>/dev/null | awk '/Version:/{print $2; exit}')
    if [ -n "${tag}" ]; then
      kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${tag}/kwok.yaml" >/dev/null 2>&1 || true
    fi
  fi
  kubectl -n kube-system wait --for=condition=Available deployment/kwok-controller --timeout=180s >/dev/null 2>&1 || true
}

inflate_nodes() {
  local count=${1:?count required}
  local nodes_yaml="${RUNTIME_DIR}/phase6-nodes-${count}.yaml"
  python3 - <<'PY' "${count}" >"${nodes_yaml}"
import sys

count = int(sys.argv[1])
for i in range(1, count + 1):
    name = f"kwok-node-{i:04d}"
    print("---")
    print("apiVersion: v1")
    print("kind: Node")
    print("metadata:")
    print(f"  name: {name}")
    print("  labels:")
    print("    kwok.x-k8s.io/node: \"true\"")
    print("    kubernetes.io/os: linux")
    print("    kubernetes.io/arch: amd64")
    print("spec:")
    print("  taints: []")
PY
  if ! kubectl --request-timeout=120s apply --validate=false -f "${nodes_yaml}" >/dev/null 2>&1; then
    phase6_log "node inflation apply failed; continuing with partial node set"
  fi
}

inflate_pods_deployment() {
  local count=${1:?count required}
  kubectl create namespace phase6-churn --dry-run=client -o yaml | kubectl apply --validate=false -f - >/dev/null 2>&1 || true
  kubectl -n phase6-churn create deployment phase6-storm --image=registry.k8s.io/pause:3.9 --replicas=1 --dry-run=client -o yaml | kubectl apply --validate=false -f - >/dev/null 2>&1 || true
  if ! kubectl --request-timeout=30s -n phase6-churn scale deployment/phase6-storm --replicas="${count}" >/dev/null 2>&1; then
    phase6_log "pod scale request failed; continuing"
  fi
}

inflate_pods_manifest() {
  local count=${1:?count required}
  kubectl create namespace phase6-churn --dry-run=client -o yaml | kubectl apply --validate=false -f - >/dev/null 2>&1 || true
  local chunk_size=${PHASE9_POD_MANIFEST_CHUNK_SIZE:-${ADAPTIVE_POD_CHUNK_MAX}}
  chunk_size=$(clamp_int "${chunk_size}" "${ADAPTIVE_POD_CHUNK_MIN}" "${ADAPTIVE_POD_CHUNK_MAX}")
  local sleep_ms=${ADAPTIVE_MIN_SLEEP_MS}
  local node_count=${NODES:-1}
  if [ "${node_count}" -lt 1 ]; then
    node_count=1
  fi
  local start=1
  while [ "${start}" -le "${count}" ]; do
    if [ "${sleep_ms}" -gt 0 ]; then
      phase9_sleep_ms "${sleep_ms}"
    fi
    local end=$((start + chunk_size - 1))
    if [ "${end}" -gt "${count}" ]; then
      end=${count}
    fi
    local pods_yaml="${RUNTIME_DIR}/phase6-pods-${start}-${end}.yaml"
    python3 - <<'PY' "${start}" "${end}" "${node_count}" >"${pods_yaml}"
import sys

start = int(sys.argv[1])
end = int(sys.argv[2])
node_count = max(1, int(sys.argv[3]))
for i in range(start, end + 1):
    name = f"phase6-pod-{i:05d}"
    node = f"kwok-node-{((i - 1) % node_count) + 1:04d}"
    print("---")
    print("apiVersion: v1")
    print("kind: Pod")
    print("metadata:")
    print(f"  name: {name}")
    print("  namespace: phase6-churn")
    print("  labels:")
    print("    kwok.x-k8s.io/pod: \"true\"")
    print("spec:")
    print(f"  nodeName: {node}")
    print("  containers:")
    print("  - name: pause")
    print("    image: registry.k8s.io/pause:3.9")
PY
    if ! kubectl --request-timeout=180s apply --validate=false -f "${pods_yaml}" >/dev/null 2>&1; then
      phase6_log "pod manifest chunk apply failed range=${start}-${end}; continuing with partial pod set"
    fi
    local adaptive
    adaptive=$(phase9_adaptive_tick "${chunk_size}" "${sleep_ms}")
    chunk_size=$(echo "${adaptive}" | awk '{print $1}')
    sleep_ms=$(echo "${adaptive}" | awk '{print $2}')
    local wa si so
    wa=$(echo "${adaptive}" | awk '{print $3}')
    si=$(echo "${adaptive}" | awk '{print $4}')
    so=$(echo "${adaptive}" | awk '{print $5}')
    phase6_log "adaptive pod inflation: range=${start}-${end} chunk_next=${chunk_size} sleep_ms_next=${sleep_ms} wa=${wa} si=${si} so=${so}"
    start=$((end + 1))
  done
}

inflate_pods() {
  local count=${1:?count required}
  case "${POD_INFLATION_MODE}" in
    deployment)
      inflate_pods_deployment "${count}"
      ;;
    manifest)
      inflate_pods_manifest "${count}"
      ;;
    *)
      phase6_log "unknown pod inflation mode=${POD_INFLATION_MODE}, falling back to deployment"
      inflate_pods_deployment "${count}"
      ;;
  esac
}

create_configmaps() {
  local count=${1:?count required}
  kubectl create namespace phase6-scan --dry-run=client -o yaml | kubectl apply --validate=false -f - >/dev/null 2>&1 || true
  local chunk_size=${PHASE9_CONFIGMAP_CHUNK_SIZE:-500}
  chunk_size=$(max_int "${chunk_size}" 50)
  local start=1
  local sleep_ms=${ADAPTIVE_MIN_SLEEP_MS}
  while [ "${start}" -le "${count}" ]; do
    if [ "${sleep_ms}" -gt 0 ]; then
      phase9_sleep_ms "${sleep_ms}"
    fi
    local end=$((start + chunk_size - 1))
    if [ "${end}" -gt "${count}" ]; then
      end=${count}
    fi
    local cms_yaml="${RUNTIME_DIR}/phase6-configmaps-${start}-${end}.yaml"
    python3 - <<'PY' "${start}" "${end}" >"${cms_yaml}"
import sys

start = int(sys.argv[1])
end = int(sys.argv[2])
for i in range(start, end + 1):
    print("---")
    print("apiVersion: v1")
    print("kind: ConfigMap")
    print("metadata:")
    print(f"  name: cm-{i}")
    print("  namespace: phase6-scan")
    print("data:")
    print(f"  value: \"v-{i}\"")
PY
    if ! kubectl --request-timeout=300s apply --validate=false -f "${cms_yaml}" >/dev/null 2>&1; then
      phase6_log "configmap chunk apply failed range=${start}-${end}; continuing with partial configmap set"
    fi
    local adaptive
    adaptive=$(phase9_adaptive_tick "${chunk_size}" "${sleep_ms}")
    local next_sleep_ms
    next_sleep_ms=$(echo "${adaptive}" | awk '{print $2}')
    local wa si so
    wa=$(echo "${adaptive}" | awk '{print $3}')
    si=$(echo "${adaptive}" | awk '{print $4}')
    so=$(echo "${adaptive}" | awk '{print $5}')
    sleep_ms=$(clamp_int "${next_sleep_ms}" "${ADAPTIVE_MIN_SLEEP_MS}" "${ADAPTIVE_MAX_SLEEP_MS}")
    phase6_log "adaptive configmap apply: range=${start}-${end} chunk=${chunk_size} sleep_ms_next=${sleep_ms} wa=${wa} si=${si} so=${so}"
    start=$((end + 1))
  done
}

run_heartbeat_workload() {
  local ops=${1:?ops required}
  local nspaces=32
  local i
  for i in $(seq 1 "${ops}"); do
    local id=$(( (i % nspaces) + 1 ))
    # Under heavy API pressure, transient apiserver resets can occur. Keep heartbeat
    # pressure generation moving instead of aborting the whole benchmark pass.
    if ! cat <<EOF | kubectl --request-timeout=10s apply --validate=false -f - >/dev/null 2>&1
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: phase6-heartbeat-${id}
  namespace: kube-node-lease
spec:
  holderIdentity: "kwok-${id}"
  leaseDurationSeconds: 40
EOF
    then
      continue
    fi
  done
}

measure_ms() {
  local start_ns end_ns
  start_ns=$(date +%s%N)
  # Keep benchmark flow alive under transient API/server failures and encode failure
  # as a high-latency sample so tail percentiles reflect the outage.
  if "$@" >/dev/null 2>&1; then
    :
  else
    end_ns=$(date +%s%N)
    echo "${PHASE6_FAILED_OP_LATENCY_MS:-60000}"
    return 0
  fi
  end_ns=$(date +%s%N)
  echo $(( (end_ns - start_ns) / 1000000 ))
}

collect_latency_samples() {
  local outfile=${1:?outfile required}
  local iterations=${2:?iterations required}
  local mode=${3:?mode required}
  local i
  : >"${outfile}"
  for i in $(seq 1 "${iterations}"); do
    local id=$(( (i % CONFIGMAPS) + 1 ))
    case "${mode}" in
      put)
        printf '%s\n' "$(measure_ms kubectl --request-timeout="${API_TIMEOUT}" -n phase6-scan patch configmap "cm-${id}" --type merge -p "{\"data\":{\"value\":\"u-${i}\"}}")" >>"${outfile}"
        ;;
      get)
        printf '%s\n' "$(measure_ms kubectl --request-timeout="${API_TIMEOUT}" -n phase6-scan get configmap "cm-${id}" -o json)" >>"${outfile}"
        ;;
      list)
        printf '%s\n' "$(measure_ms kubectl --request-timeout="${API_TIMEOUT}" -n phase6-scan get configmaps --chunk-size=500 -o json)" >>"${outfile}"
        ;;
      *)
        echo "unknown sample mode: ${mode}" >&2
        return 1
        ;;
    esac
  done
}

backend_summary() {
  local backend=${1:?backend required}
  local telemetry_dir=${2:?telemetry dir required}
  local put_samples=${3:?put samples required}
  local get_samples=${4:?get samples required}
  local list_samples=${5:?list samples required}
  local churn_ms=${6:?churn ms required}
  local heartbeat_ms=${7:?heartbeat ms required}
  local cold_scan_ms=${8:?cold scan ms required}
  local out_json=${9:?out json required}
  local pod_mode=${10:?pod mode required}
  local telemetry_warmup_secs=${11:?warmup required}

  local actual_nodes actual_pods actual_cms
  actual_nodes=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
  actual_pods=$(kubectl -n phase6-churn get pods --no-headers 2>/dev/null | wc -l | tr -d ' ')
  actual_cms=$(kubectl -n phase6-scan get configmaps --no-headers 2>/dev/null | wc -l | tr -d ' ')

  python3 - <<'PY' \
"${backend}" "${RUN_ID}" "${NODES}" "${PODS}" "${CONFIGMAPS}" \
"${actual_nodes}" "${actual_pods}" "${actual_cms}" \
"${put_samples}" "${get_samples}" "${list_samples}" \
"${churn_ms}" "${heartbeat_ms}" "${cold_scan_ms}" \
"${telemetry_dir}" "${out_json}" "${pod_mode}" "${telemetry_warmup_secs}"
import json
import math
import pathlib
import re
import statistics
import sys

backend, run_id = sys.argv[1], sys.argv[2]
target_nodes, target_pods, target_cms = map(int, sys.argv[3:6])
actual_nodes, actual_pods, actual_cms = map(int, sys.argv[6:9])
put_path = pathlib.Path(sys.argv[9])
get_path = pathlib.Path(sys.argv[10])
list_path = pathlib.Path(sys.argv[11])
churn_ms = int(sys.argv[12])
heartbeat_ms = int(sys.argv[13])
cold_scan_ms = int(sys.argv[14])
telemetry_dir = pathlib.Path(sys.argv[15])
out_path = pathlib.Path(sys.argv[16])
pod_mode = sys.argv[17]
warmup_secs = max(0, int(sys.argv[18]))

def load_samples(path: pathlib.Path):
    vals = []
    if not path.exists():
        return vals
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            vals.append(float(line))
        except Exception:
            pass
    return vals

def pct(vals, q):
    if not vals:
        return None
    vals = sorted(vals)
    if len(vals) == 1:
        return vals[0]
    k = (len(vals) - 1) * q
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return vals[int(k)]
    return vals[f] + (vals[c] - vals[f]) * (k - f)

def summarize(vals):
    return {
        "samples": len(vals),
        "p50_ms": pct(vals, 0.50),
        "p90_ms": pct(vals, 0.90),
        "p99_ms": pct(vals, 0.99),
        "mean_ms": (statistics.fmean(vals) if vals else None),
    }

put_vals = load_samples(put_path)
get_vals = load_samples(get_path)
list_vals = load_samples(list_path)

peak_iops = None
peak_iops_post = None
peak_iops_post_device = None
device_peaks_overall = {}
device_peaks_post = {}
iostat_intervals_total = 0
iostat_log = telemetry_dir / "iostat.log"
if iostat_log.exists():
    iostat_interval = -1
    for raw in iostat_log.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if not parts:
            continue
        if parts[0].lower() == "device":
            iostat_interval += 1
            iostat_intervals_total = max(iostat_intervals_total, iostat_interval + 1)
            continue
        if len(parts) < 6:
            continue
        token0 = parts[0].lower()
        if token0 in {"linux", "avg-cpu:"} or token0.startswith("%"):
            continue
        if token0.replace(".", "", 1).isdigit():
            continue
        try:
            rs = float(parts[1])
            ws = float(parts[2])
            iops = rs + ws
            device = parts[0]
            prev = device_peaks_overall.get(device)
            device_peaks_overall[device] = iops if prev is None else max(prev, iops)
            peak_iops = iops if peak_iops is None else max(peak_iops, iops)
            if iostat_interval >= warmup_secs:
                prev_post = device_peaks_post.get(device)
                device_peaks_post[device] = iops if prev_post is None else max(prev_post, iops)
                if peak_iops_post is None or iops > peak_iops_post:
                    peak_iops_post = iops
                    peak_iops_post_device = device
        except Exception:
            continue

iowait_vals = []
iowait_vals_post = []
vmstat_samples_total = 0
vmstat_samples_post_warmup = 0
swap_samples_total = 0
swap_samples_post_warmup = 0
swap_si_total = 0.0
swap_so_total = 0.0
swap_si_post_warmup = 0.0
swap_so_post_warmup = 0.0
vmstat_log = telemetry_dir / "vmstat.log"
if vmstat_log.exists():
    for line in vmstat_log.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.split()
        if len(parts) < 17:
            continue
        if parts[0] == "procs" or parts[0] == "r" or parts[0] == "b":
            continue
        try:
            vmstat_samples_total += 1
            si = float(parts[6])
            so = float(parts[7])
            iowait = float(parts[15])
            iowait_vals.append(iowait)
            if si > 0.0 or so > 0.0:
                swap_samples_total += 1
                swap_si_total += si
                swap_so_total += so
            if (vmstat_samples_total - 1) >= warmup_secs:
                vmstat_samples_post_warmup += 1
                iowait_vals_post.append(iowait)
                if si > 0.0 or so > 0.0:
                    swap_samples_post_warmup += 1
                    swap_si_post_warmup += si
                    swap_so_post_warmup += so
        except Exception:
            continue

peak_mem_mb = None
docker_stats = telemetry_dir / "docker-stats.log"
if docker_stats.exists():
    mem_pattern = re.compile(r"mem=([0-9.]+)([KMG]i?)B", re.IGNORECASE)
    for line in docker_stats.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = mem_pattern.search(line)
        if not m:
            continue
        num = float(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith("k"):
            mb = num / 1024.0
        elif unit.startswith("g"):
            mb = num * 1024.0
        else:
            mb = num
        peak_mem_mb = mb if peak_mem_mb is None else max(peak_mem_mb, mb)

caveats = []
if actual_nodes < target_nodes:
    caveats.append(f"node inflation below target ({actual_nodes}/{target_nodes})")
if actual_pods < target_pods:
    caveats.append(f"pod inflation below target ({actual_pods}/{target_pods})")
if actual_cms < target_cms:
    caveats.append(f"configmap set below target ({actual_cms}/{target_cms})")

summary = {
    "backend": backend,
    "run_id": run_id,
    "target": {
        "nodes": target_nodes,
        "pods": target_pods,
        "configmaps": target_cms,
    },
    "actual": {
        "nodes": actual_nodes,
        "pods": actual_pods,
        "configmaps": actual_cms,
    },
    "workloads": {
        "churn_scale_ms": churn_ms,
        "heartbeat_ms": heartbeat_ms,
        "cold_scan_first_ms": cold_scan_ms,
        "pod_inflation_mode": pod_mode,
    },
    "latency_ms": {
        "put": summarize(put_vals),
        "get": summarize(get_vals),
        "list": summarize(list_vals),
    },
    "resource": {
        "peak_container_mem_mb": peak_mem_mb,
        "disk_iops_peak": peak_iops,
        "cpu_iowait_p99": pct(iowait_vals, 0.99) if iowait_vals else None,
    },
    "resource_post_warmup": {
        "warmup_secs": warmup_secs,
        "disk_iops_peak": peak_iops_post,
        "disk_iops_peak_device": peak_iops_post_device,
        "cpu_iowait_p99": pct(iowait_vals_post, 0.99) if iowait_vals_post else None,
        "samples": {
            "iostat_intervals_total": iostat_intervals_total,
            "iostat_intervals_post_warmup": max(0, iostat_intervals_total - warmup_secs),
            "vmstat_total": vmstat_samples_total,
            "vmstat_post_warmup": vmstat_samples_post_warmup,
        },
    },
    "resource_device_peaks": {
        "overall": dict(sorted(device_peaks_overall.items())),
        "post_warmup": dict(sorted(device_peaks_post.items())),
    },
    "resource_swap_events": {
        "overall": {
            "samples_with_swap": swap_samples_total,
            "si_total": swap_si_total,
            "so_total": swap_so_total,
        },
        "post_warmup": {
            "samples_with_swap": swap_samples_post_warmup,
            "si_total": swap_si_post_warmup,
            "so_total": swap_so_post_warmup,
        },
    },
    "pass": (len(caveats) == 0),
    "caveats": caveats,
    "telemetry_dir": str(telemetry_dir),
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY
}

run_backend() {
  local backend=${1:?backend required}
  local backend_run_dir="${RUN_DIR}/k3s-${backend}"
  local telemetry_dir="${backend_run_dir}/telemetry"
  local put_file="${backend_run_dir}/put-lat-ms.txt"
  local get_file="${backend_run_dir}/get-lat-ms.txt"
  local list_file="${backend_run_dir}/list-lat-ms.txt"
  local summary_file="${RUN_DIR}/phase6-k3s-${backend}-summary.json"
  local tiering_restore_mode=unset
  local tiering_restore_value=
  local tiering_override_applied=false
  mkdir -p "${backend_run_dir}" "${telemetry_dir}"

  if [ "${backend}" = "astra" ] && [ "${DISABLE_TIERING_FOR_K3S}" = "true" ]; then
    if [ "${ASTRAD_TIERING_INTERVAL_SECS+x}" = "x" ]; then
      tiering_restore_mode=set
      tiering_restore_value=${ASTRAD_TIERING_INTERVAL_SECS}
    fi
    export ASTRAD_TIERING_INTERVAL_SECS=${PHASE9_K3S_TIERING_INTERVAL_SECS:-86400}
    tiering_override_applied=true
    phase6_log "k3s benchmark tiering override enabled backend=${backend} interval_secs=${ASTRAD_TIERING_INTERVAL_SECS}"
  fi

  phase6_stop_k3s || true
  phase6_stop_backend all || true
  phase6_start_backend "${backend}"

  phase6_start_telemetry "k3s-${backend}"
  phase6_start_k3s "${PHASE6_BACKEND_DATASTORE_ENDPOINT}" "${backend}"
  install_kwok_components || true
  if [ "${TELEMETRY_WARMUP_SECS}" -gt 0 ]; then
    phase6_log "telemetry warmup backend=${backend} seconds=${TELEMETRY_WARMUP_SECS}"
    sleep "${TELEMETRY_WARMUP_SECS}"
  fi

  phase6_log "inflating cluster backend=${backend} nodes=${NODES} pods=${PODS} configmaps=${CONFIGMAPS}"
  inflate_nodes "${NODES}"
  local churn_ms
  churn_ms=$(measure_ms inflate_pods "${PODS}")
  create_configmaps "${CONFIGMAPS}"

  local heartbeat_ms
  heartbeat_ms=$(measure_ms run_heartbeat_workload "${HEARTBEAT_OPS}")

  local cold_scan_ms
  cold_scan_ms=$(measure_ms kubectl --request-timeout="${API_TIMEOUT}" -n phase6-scan get configmaps --chunk-size=500 -o json)

  collect_latency_samples "${put_file}" "${PUT_SAMPLES}" put
  collect_latency_samples "${get_file}" "${GET_SAMPLES}" get
  collect_latency_samples "${list_file}" "${LIST_SAMPLES}" list

  phase6_stop_telemetry "${PHASE6_TELEMETRY_DIR}"
  cp -r "${PHASE6_TELEMETRY_DIR}/." "${telemetry_dir}/" || true

  backend_summary \
    "${backend}" \
    "${telemetry_dir}" \
    "${put_file}" \
    "${get_file}" \
    "${list_file}" \
    "${churn_ms}" \
    "${heartbeat_ms}" \
    "${cold_scan_ms}" \
    "${summary_file}" \
    "${POD_INFLATION_MODE}" \
    "${TELEMETRY_WARMUP_SECS}"

  if [ "${HARD_POD_GATE}" = "true" ]; then
    python3 - <<'PY' "${summary_file}" "${PODS}" "${backend}"
import json
import sys

summary_path, target_pods, backend = sys.argv[1], int(sys.argv[2]), sys.argv[3]
with open(summary_path, "r", encoding="utf-8") as f:
    obj = json.load(f)
actual = int((obj.get("actual") or {}).get("pods") or 0)
if actual < target_pods:
    raise SystemExit(
        f"hard pod gate failed for {backend}: actual_pods={actual} target_pods={target_pods}"
    )
print(f"hard pod gate passed for {backend}: actual_pods={actual} target_pods={target_pods}")
PY
  fi

  phase6_log "backend summary written: ${summary_file}"
  phase6_stop_k3s || true
  phase6_stop_backend all || true

  if [ "${tiering_override_applied}" = "true" ]; then
    if [ "${tiering_restore_mode}" = "set" ]; then
      export ASTRAD_TIERING_INTERVAL_SECS=${tiering_restore_value}
    else
      unset ASTRAD_TIERING_INTERVAL_SECS
    fi
  fi
}

main() {
  local backends=()
  case "${BACKEND_MODE}" in
    astra) backends=(astra) ;;
    etcd) backends=(etcd) ;;
    both) backends=(astra etcd) ;;
    *)
      echo "invalid --backend: ${BACKEND_MODE}" >&2
      exit 1
      ;;
  esac

  phase6_log "starting k3s benchmark run_id=${RUN_ID} backends=${backends[*]} quick=${QUICK}"
  local backend
  for backend in "${backends[@]}"; do
    run_backend "${backend}"
  done
}

main "$@"
