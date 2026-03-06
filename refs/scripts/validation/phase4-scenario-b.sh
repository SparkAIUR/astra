#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase2-common.sh
source "${SCRIPT_DIR}/phase2-common.sh"

ensure_tools

if [ -z "${ASTRA_IMAGE:-}" ]; then
  echo "ASTRA_IMAGE is required, e.g. halceon/astra-alpha:<tag>" >&2
  exit 1
fi

export ASTRA_BLKIO_DEVICE=${ASTRA_BLKIO_DEVICE:-/dev/vda}
export ASTRAD_WAL_MAX_BATCH_REQUESTS=${ASTRAD_WAL_MAX_BATCH_REQUESTS:-1000}
export ASTRAD_WAL_BATCH_MAX_BYTES=${ASTRAD_WAL_BATCH_MAX_BYTES:-8388608}
export ASTRAD_WAL_MAX_LINGER_US=${ASTRAD_WAL_MAX_LINGER_US:-2000}
export ASTRAD_WAL_PENDING_LIMIT=${ASTRAD_WAL_PENDING_LIMIT:-2000}
export ASTRAD_WAL_SEGMENT_BYTES=${ASTRAD_WAL_SEGMENT_BYTES:-67108864}
export ASTRAD_WAL_IO_ENGINE=${ASTRAD_WAL_IO_ENGINE:-posix}
export ASTRAD_PUT_BATCH_MAX_REQUESTS=${ASTRAD_PUT_BATCH_MAX_REQUESTS:-256}
export ASTRAD_PUT_BATCH_MIN_REQUESTS=${ASTRAD_PUT_BATCH_MIN_REQUESTS:-16}
export ASTRAD_PUT_BATCH_MAX_LINGER_US=${ASTRAD_PUT_BATCH_MAX_LINGER_US:-2000}
export ASTRAD_PUT_BATCH_MIN_LINGER_US=${ASTRAD_PUT_BATCH_MIN_LINGER_US:-50}
export ASTRAD_PUT_BATCH_MAX_BYTES=${ASTRAD_PUT_BATCH_MAX_BYTES:-262144}
export ASTRAD_PUT_BATCH_PENDING_LIMIT=${ASTRAD_PUT_BATCH_PENDING_LIMIT:-10000}
export ASTRAD_PUT_ADAPTIVE_ENABLED=${ASTRAD_PUT_ADAPTIVE_ENABLED:-true}
export ASTRAD_PUT_ADAPTIVE_MODE=${ASTRAD_PUT_ADAPTIVE_MODE:-queue_backlog_drain}
export ASTRAD_PUT_ADAPTIVE_MIN_REQUEST_FLOOR=${ASTRAD_PUT_ADAPTIVE_MIN_REQUEST_FLOOR:-128}
export ASTRAD_PUT_DISPATCH_CONCURRENCY=${ASTRAD_PUT_DISPATCH_CONCURRENCY:-1}
export ASTRAD_PUT_TARGET_QUEUE_DEPTH=${ASTRAD_PUT_TARGET_QUEUE_DEPTH:-512}
export ASTRAD_PUT_P99_BUDGET_MS=${ASTRAD_PUT_P99_BUDGET_MS:-550}
export ASTRAD_PUT_TARGET_QUEUE_WAIT_P99_MS=${ASTRAD_PUT_TARGET_QUEUE_WAIT_P99_MS:-120}
export ASTRAD_PUT_TARGET_QUORUM_ACK_P99_MS=${ASTRAD_PUT_TARGET_QUORUM_ACK_P99_MS:-300}
export ASTRAD_PUT_TOKEN_LANE_ENABLED=${ASTRAD_PUT_TOKEN_LANE_ENABLED:-true}
export ASTRAD_PUT_TOKEN_DICT_MAX_ENTRIES=${ASTRAD_PUT_TOKEN_DICT_MAX_ENTRIES:-4096}
export ASTRAD_PUT_TOKEN_MIN_REUSE=${ASTRAD_PUT_TOKEN_MIN_REUSE:-2}
export ASTRAD_RAFT_TIMELINE_ENABLED=${ASTRAD_RAFT_TIMELINE_ENABLED:-true}
export ASTRAD_RAFT_TIMELINE_SAMPLE_RATE=${ASTRAD_RAFT_TIMELINE_SAMPLE_RATE:-64}
export ASTRAD_RAFT_ELECTION_TIMEOUT_MIN_MS=${ASTRAD_RAFT_ELECTION_TIMEOUT_MIN_MS:-2500}
export ASTRAD_RAFT_ELECTION_TIMEOUT_MAX_MS=${ASTRAD_RAFT_ELECTION_TIMEOUT_MAX_MS:-5000}
export ASTRAD_RAFT_HEARTBEAT_INTERVAL_MS=${ASTRAD_RAFT_HEARTBEAT_INTERVAL_MS:-350}
export ASTRAD_RAFT_MAX_PAYLOAD_ENTRIES=${ASTRAD_RAFT_MAX_PAYLOAD_ENTRIES:-5000}
export ASTRAD_RAFT_REPLICATION_LAG_THRESHOLD=${ASTRAD_RAFT_REPLICATION_LAG_THRESHOLD:-2048}
export ASTRAD_METRICS_ENABLED=${ASTRAD_METRICS_ENABLED:-true}
export ASTRAD_METRICS_ADDR=${ASTRAD_METRICS_ADDR:-0.0.0.0:9479}
export ASTRAD_SST_TARGET_BYTES=${ASTRAD_SST_TARGET_BYTES:-67108864}
ASTRA_BLKIO_SHAPING=${ASTRA_BLKIO_SHAPING:-true}

ASTRA_SCENARIO_MODE=${ASTRA_SCENARIO_MODE:-full}
if [ "${ASTRA_SCENARIO_MODE}" != "full" ] && [ "${ASTRA_SCENARIO_MODE}" != "astra_only" ]; then
  echo "ASTRA_SCENARIO_MODE must be one of: full, astra_only" >&2
  exit 1
fi

ASTRA_RUN_ID=${ASTRA_RUN_ID:-}
if [ -n "${ASTRA_RUN_ID}" ]; then
  ARTIFACT_PREFIX="phase4-scenario-b-${ASTRA_RUN_ID}"
else
  ARTIFACT_PREFIX="phase4-scenario-b"
fi

ASTRA_JSON="${RESULTS_DIR}/${ARTIFACT_PREFIX}-astra.json"
ETCD_JSON="${RESULTS_DIR}/${ARTIFACT_PREFIX}-etcd.json"
SUMMARY_JSON="${RESULTS_DIR}/${ARTIFACT_PREFIX}-summary.json"
GHZ_DATA_JSON="${RESULTS_DIR}/${ARTIFACT_PREFIX}-put-data.json"
ASTRA_LOG_TXT="${RESULTS_DIR}/${ARTIFACT_PREFIX}-astra-logs.txt"
ASTRA_METRICS_TXT="${RESULTS_DIR}/${ARTIFACT_PREFIX}-metrics.txt"

if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
  compose_blkio down -v || true
else
  compose down -v || true
fi
docker rm -f phase4-etcd >/dev/null 2>&1 || true
if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
  compose_blkio pull
  compose_blkio up -d
else
  compose pull
  compose up -d
fi

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
    compose_blkio logs --tail 120
  else
    compose logs --tail 120
  fi
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase4/b/leader_probe/${ASTRA_RUN_ID:-default}" "$(date +%s%N)" 90) || {
  echo "failed to find writable Astra endpoint" >&2
  if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
    compose_blkio logs --tail 120
  else
    compose logs --tail 120
  fi
  exit 2
}

echo "astra_writable_endpoint=${leader_ep}"
leader_service="unknown"
leader_container="unknown"
if leader_meta=$(endpoint_to_container "${leader_ep}" 2>/dev/null); then
  read -r leader_container leader_service <<<"${leader_meta}"
fi
leader_metrics_ep=$(endpoint_to_metrics_endpoint "${leader_ep}" 2>/dev/null || true)
echo "astra_leader_service=${leader_service}"
echo "astra_leader_metrics_endpoint=${leader_metrics_ep:-unavailable}"

python3 - <<PY
import base64
import json
payload = {
    "key": base64.b64encode(b"/bench/key").decode(),
    "value": base64.b64encode(b"x" * 4096).decode(),
    "lease": 0,
    "ignoreValue": False,
    "ignoreLease": False,
}
with open("${GHZ_DATA_JSON}", "w", encoding="utf-8") as f:
    json.dump(payload, f)
PY

ghz --insecure \
  --proto "${SCRIPT_DIR}/proto/rpc.proto" \
  --call etcdserverpb.KV.Put \
  -d @"${GHZ_DATA_JSON}" \
  -c 200 -n 50000 --connections 64 \
  --timeout 30s \
  "${leader_ep}" \
  --format json --output "${ASTRA_JSON}"

if [ -n "${leader_metrics_ep:-}" ]; then
  curl -fsS "http://${leader_metrics_ep}/metrics" > "${ASTRA_METRICS_TXT}" || true
fi

if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
  compose_blkio logs --no-color astra-node1 astra-node2 astra-node3 > "${ASTRA_LOG_TXT}" || true
else
  compose logs --no-color astra-node1 astra-node2 astra-node3 > "${ASTRA_LOG_TXT}" || true
fi

if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
  node1_id=$(compose_blkio ps -aq astra-node1 || true)
  node2_id=$(compose_blkio ps -aq astra-node2 || true)
  node3_id=$(compose_blkio ps -aq astra-node3 || true)
else
  node1_id=$(compose ps -aq astra-node1 || true)
  node2_id=$(compose ps -aq astra-node2 || true)
  node3_id=$(compose ps -aq astra-node3 || true)
fi
NODE1_OOM=$( [ -n "${node1_id}" ] && docker inspect -f '{{.State.OOMKilled}}' "${node1_id}" || echo "unknown")
NODE2_OOM=$( [ -n "${node2_id}" ] && docker inspect -f '{{.State.OOMKilled}}' "${node2_id}" || echo "unknown")
NODE3_OOM=$( [ -n "${node3_id}" ] && docker inspect -f '{{.State.OOMKilled}}' "${node3_id}" || echo "unknown")

if [ "${ASTRA_SCENARIO_MODE}" = "full" ]; then
  if [ "${ASTRA_BLKIO_SHAPING}" = "true" ]; then
    docker run -d --name phase4-etcd \
      --device-write-iops "${ASTRA_BLKIO_DEVICE}:150" \
      --device-write-bps "${ASTRA_BLKIO_DEVICE}:10485760" \
      -p 42379:2379 -p 42380:2380 \
      quay.io/coreos/etcd:v3.5.17 \
      /usr/local/bin/etcd \
        --name s1 \
        --data-dir /etcd-data \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls http://0.0.0.0:2379 \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls http://0.0.0.0:2380 \
        --initial-cluster s1=http://0.0.0.0:2380 \
        --initial-cluster-state new
  else
    docker run -d --name phase4-etcd \
      -p 42379:2379 -p 42380:2380 \
      quay.io/coreos/etcd:v3.5.17 \
      /usr/local/bin/etcd \
        --name s1 \
        --data-dir /etcd-data \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls http://0.0.0.0:2379 \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls http://0.0.0.0:2380 \
        --initial-cluster s1=http://0.0.0.0:2380 \
        --initial-cluster-state new
  fi

  sleep 6

  ghz --insecure \
    --proto "${SCRIPT_DIR}/proto/rpc.proto" \
    --call etcdserverpb.KV.Put \
    -d @"${GHZ_DATA_JSON}" \
    -c 200 -n 50000 --connections 64 \
    --timeout 30s \
    127.0.0.1:42379 \
    --format json --output "${ETCD_JSON}"
else
  printf '{}' > "${ETCD_JSON}"
fi

NODE1_OOM="${NODE1_OOM}" \
NODE2_OOM="${NODE2_OOM}" \
NODE3_OOM="${NODE3_OOM}" \
LEADER_EP="${leader_ep}" \
LEADER_SERVICE="${leader_service}" \
LEADER_CONTAINER="${leader_container}" \
LEADER_METRICS_EP="${leader_metrics_ep:-}" \
ASTRA_METRICS_TXT="${ASTRA_METRICS_TXT}" \
SCENARIO_MODE="${ASTRA_SCENARIO_MODE}" \
BLKIO_SHAPING="${ASTRA_BLKIO_SHAPING}" \
python3 - <<PY
import json
import os
import re
from pathlib import Path

P99_NS_TARGET = 150_000_000
RPS_TARGET = 1500
TOTAL = 50000

def status_count(dist, name):
    normalized = {}
    for key, value in dist.items():
        normalized[key.lower()] = normalized.get(key.lower(), 0) + value
    return normalized.get(name.lower(), 0)

def summary(path):
    p = Path(path)
    if not p.exists():
        return None
    d = json.loads(p.read_text())
    lat = {str(x["percentage"]): x["latency"] for x in d.get("latencyDistribution", [])}
    status = d.get("statusCodeDistribution", {})
    ok = status_count(status, "OK")
    deadline = status_count(status, "DeadlineExceeded")
    exhausted = status_count(status, "ResourceExhausted")
    unavailable = status_count(status, "Unavailable")
    return {
        "count": d.get("count", 0),
        "ok": ok,
        "resource_exhausted": exhausted,
        "deadline_exceeded": deadline,
        "unavailable": unavailable,
        "other_errors": max(0, d.get("count", 0) - ok - exhausted - deadline - unavailable),
        "rps": d.get("rps", 0),
        "avg_ns": d.get("average", 0),
        "p50_ns": lat.get("50"),
        "p90_ns": lat.get("90"),
        "p99_ns": lat.get("99"),
    }

astra = summary("${ASTRA_JSON}")
etcd = summary("${ETCD_JSON}") if os.environ.get("SCENARIO_MODE", "full") == "full" else None
logs = Path("${ASTRA_LOG_TXT}").read_text(encoding="utf-8", errors="ignore")
logs = re.sub(r"\\x1b\\[[0-9;]*m", "", logs)
metrics_path_value = os.environ.get("ASTRA_METRICS_TXT", "")
metrics_path = Path(metrics_path_value) if metrics_path_value else None
metrics_text = ""
if metrics_path and metrics_path.exists() and metrics_path.is_file():
    metrics_text = metrics_path.read_text(encoding="utf-8", errors="ignore")

vectorized_lines = [
    line
    for line in logs.splitlines()
    if "wal vectorized append flush complete" in line
]
append_entries_values = []
fdatasync_values = []
for line in vectorized_lines:
    append_match = re.search(r"append_entries=(\\d+)", line)
    if append_match:
        append_entries_values.append(int(append_match.group(1)))
    sync_match = re.search(r"fdatasync_calls=(\\d+)", line)
    if sync_match:
        fdatasync_values.append(int(sync_match.group(1)))

max_append_entries_logged = max(append_entries_values) if append_entries_values else 0
append_gt50 = max_append_entries_logged >= 50
single_sync_per_array = bool(vectorized_lines) and fdatasync_values and all(v == 1 for v in fdatasync_values)

timeline_lines = [line for line in logs.splitlines() if "raft timeline" in line]
timeline_stages = {}
queue_wait_p50 = []
queue_wait_p90 = []
queue_wait_p99 = []
queue_wait_avg = []
queue_wait_max = []
quorum_ack_since_submit = []
for line in timeline_lines:
    stage_match = re.search(r'stage="?([a-zA-Z0-9_]+)"?', line)
    if not stage_match:
        continue
    stage = stage_match.group(1)
    stage_obj = timeline_stages.setdefault(stage, {"count": 0, "durations_ms": []})
    stage_obj["count"] += 1
    duration_match = (
        re.search(r"write_duration_ms=(\\d+)", line)
        or re.search(r"elapsed_ms=(\\d+)", line)
        or re.search(r"since_submit_ms=(\\d+)", line)
    )
    if duration_match:
        stage_obj["durations_ms"].append(int(duration_match.group(1)))
    p50 = re.search(r"queue_wait_p50_ms=(\\d+)", line)
    if p50:
        queue_wait_p50.append(int(p50.group(1)))
    p90 = re.search(r"queue_wait_p90_ms=(\\d+)", line)
    if p90:
        queue_wait_p90.append(int(p90.group(1)))
    p99 = re.search(r"queue_wait_p99_ms=(\\d+)", line)
    if p99:
        queue_wait_p99.append(int(p99.group(1)))
    avg = re.search(r"queue_wait_avg_ms=(\\d+)", line)
    if avg:
        queue_wait_avg.append(int(avg.group(1)))
    mx = re.search(r"max_queue_wait_ms=(\\d+)", line)
    if mx:
        queue_wait_max.append(int(mx.group(1)))
    if stage == "quorum_ack_commit_advanced":
        q = re.search(r"since_submit_ms=(\\d+)", line)
        if q:
            quorum_ack_since_submit.append(int(q.group(1)))

for stage, obj in timeline_stages.items():
    if obj["durations_ms"]:
        vals = sorted(obj["durations_ms"])
        obj["p50_ms"] = vals[len(vals) // 2]
        obj["p90_ms"] = vals[min(len(vals) - 1, int(len(vals) * 0.9))]
        obj["p99_ms"] = vals[min(len(vals) - 1, int(len(vals) * 0.99))]
    del obj["durations_ms"]

def summarize_ms(values):
    if not values:
        return {"count": 0}
    vals = sorted(values)
    return {
        "count": len(vals),
        "avg_ms": sum(vals) // len(vals),
        "p50_ms": vals[len(vals) // 2],
        "p90_ms": vals[min(len(vals) - 1, int(len(vals) * 0.9))],
        "p99_ms": vals[min(len(vals) - 1, int(len(vals) * 0.99))],
        "max_ms": vals[-1],
    }

def parse_prometheus_metrics(text):
    if not text:
        return {}
    wanted = {
        "astra_put_batches_total",
        "astra_put_batch_requests_total",
        "astra_put_queue_wait_seconds_count",
        "astra_put_queue_wait_seconds_sum",
        "astra_put_raft_client_write_seconds_count",
        "astra_put_raft_client_write_seconds_sum",
        "astra_put_quorum_ack_seconds_count",
        "astra_put_quorum_ack_seconds_sum",
    }
    out = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 2:
            continue
        name, value = parts
        if name in wanted:
            try:
                out[name] = float(value)
            except ValueError:
                pass
    return out

prom_metrics = parse_prometheus_metrics(metrics_text)

oom = {
    "astra-node1": os.environ.get("NODE1_OOM", "unknown"),
    "astra-node2": os.environ.get("NODE2_OOM", "unknown"),
    "astra-node3": os.environ.get("NODE3_OOM", "unknown"),
}
no_oom = all(v.lower() == "false" for v in oom.values())

throughput_ok = (astra["rps"] or 0) > RPS_TARGET
p99_ok = (astra["p99_ns"] or 10**18) < P99_NS_TARGET
deadline_ok = astra["deadline_exceeded"] == 0
resource_exhausted_ok = astra["resource_exhausted"] == 0
strict_ok = astra["ok"] == TOTAL
queue_wait_telemetry_present = len(queue_wait_p99) > 0
quorum_ack_telemetry_present = (
    "quorum_ack_commit_advanced" in timeline_stages and timeline_stages["quorum_ack_commit_advanced"].get("count", 0) > 0
)
metrics_scrape_ok = bool(prom_metrics)

phase4_pass = (
    throughput_ok
    and p99_ok
    and deadline_ok
    and resource_exhausted_ok
    and strict_ok
    and no_oom
    and append_gt50
    and single_sync_per_array
)

out = {
    "scenario_mode": os.environ.get("SCENARIO_MODE", "full"),
    "blkio_shaping": os.environ.get("BLKIO_SHAPING", "true"),
    "astra": astra,
    "etcd": etcd,
    "targets": {
        "rps_gt": RPS_TARGET,
        "p99_ns_lt": P99_NS_TARGET,
        "deadline_exceeded_eq": 0,
        "resource_exhausted_eq": 0,
        "ok_eq": TOTAL,
        "oom_killed_eq": False,
    },
    "checks": {
        "throughput_ok": throughput_ok,
        "p99_ok": p99_ok,
        "deadline_ok": deadline_ok,
        "resource_exhausted_ok": resource_exhausted_ok,
        "strict_ok_50000": strict_ok,
        "no_oom": no_oom,
        "telemetry_append_gt50": append_gt50,
        "telemetry_single_sync_per_array": single_sync_per_array,
        "telemetry_queue_wait_percentiles": queue_wait_telemetry_present,
        "telemetry_quorum_ack_stage": quorum_ack_telemetry_present,
        "metrics_scrape_ok": metrics_scrape_ok,
        "etcd_skipped": os.environ.get("SCENARIO_MODE", "full") != "full",
        "max_append_entries_logged": max_append_entries_logged,
        "vectorized_flush_log_lines": len(vectorized_lines),
        "timeline_event_lines": len(timeline_lines),
    },
    "leader": {
        "endpoint": os.environ.get("LEADER_EP", ""),
        "service": os.environ.get("LEADER_SERVICE", ""),
        "container": os.environ.get("LEADER_CONTAINER", ""),
        "metrics_endpoint": os.environ.get("LEADER_METRICS_EP", ""),
    },
    "timeline": {
        "stages": timeline_stages,
        "queue_wait_dispatch": {
            "avg_ms": summarize_ms(queue_wait_avg),
            "p50_ms": summarize_ms(queue_wait_p50),
            "p90_ms": summarize_ms(queue_wait_p90),
            "p99_ms": summarize_ms(queue_wait_p99),
            "max_ms": summarize_ms(queue_wait_max),
        },
        "quorum_ack_commit": summarize_ms(quorum_ack_since_submit),
    },
    "prom_metrics": prom_metrics,
    "metrics_file": str(metrics_path) if metrics_path and metrics_text else "",
    "oom_killed": oom,
    "phase4_pass": phase4_pass,
    "winner_rps": (
        "astra"
        if etcd is None or (astra["rps"] or 0) > (etcd["rps"] or 0)
        else "etcd"
    ),
    "logs_file": "${ASTRA_LOG_TXT}",
}
Path("${SUMMARY_JSON}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY

docker rm -f phase4-etcd >/dev/null 2>&1 || true

echo "scenario_b_summary=${SUMMARY_JSON}"
