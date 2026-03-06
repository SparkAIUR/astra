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
export ASTRAD_WAL_IO_ENGINE=${ASTRAD_WAL_IO_ENGINE:-io_uring}
export ASTRAD_SST_TARGET_BYTES=${ASTRAD_SST_TARGET_BYTES:-67108864}

ASTRA_JSON="${RESULTS_DIR}/phase3-scenario-b-astra.json"
ETCD_JSON="${RESULTS_DIR}/phase3-scenario-b-etcd.json"
SUMMARY_JSON="${RESULTS_DIR}/phase3-scenario-b-summary.json"
GHZ_DATA_JSON="${RESULTS_DIR}/phase3-scenario-b-put-data.json"
ASTRA_LOG_TXT="${RESULTS_DIR}/phase3-scenario-b-astra-logs.txt"

compose_blkio down -v || true
docker rm -f phase3-etcd >/dev/null 2>&1 || true
compose_blkio pull
compose_blkio up -d

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  compose_blkio logs --tail 120
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase3/b/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable Astra endpoint" >&2
  compose_blkio logs --tail 120
  exit 2
}

echo "astra_writable_endpoint=${leader_ep}"

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

compose_blkio logs --no-color astra-node1 astra-node2 astra-node3 > "${ASTRA_LOG_TXT}" || true

docker run -d --name phase3-etcd \
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

sleep 6

ghz --insecure \
  --proto "${SCRIPT_DIR}/proto/rpc.proto" \
  --call etcdserverpb.KV.Put \
  -d @"${GHZ_DATA_JSON}" \
  -c 200 -n 50000 --connections 64 \
  --timeout 30s \
  127.0.0.1:42379 \
  --format json --output "${ETCD_JSON}"

python3 - <<PY
import json
import re
from pathlib import Path

P99_NS_TARGET = 300_000_000
RPS_TARGET = 1500
TOTAL = 50000

def status_count(dist, name):
    normalized = {}
    for key, value in dist.items():
        normalized[key.lower()] = normalized.get(key.lower(), 0) + value
    return normalized.get(name.lower(), 0)

def summary(path):
    d = json.loads(Path(path).read_text())
    lat = {str(x["percentage"]): x["latency"] for x in d.get("latencyDistribution", [])}
    status = d.get("statusCodeDistribution", {})
    ok = status_count(status, "OK")
    deadline = status_count(status, "DeadlineExceeded")
    exhausted = status_count(status, "ResourceExhausted")
    unavailable = status_count(status, "Unavailable")
    return {
        "count": d.get("count"),
        "ok": ok,
        "resource_exhausted": exhausted,
        "deadline_exceeded": deadline,
        "unavailable": unavailable,
        "other_errors": max(0, d.get("count", 0) - ok - exhausted - deadline),
        "rps": d.get("rps"),
        "avg_ns": d.get("average"),
        "p50_ns": lat.get("50"),
        "p90_ns": lat.get("90"),
        "p99_ns": lat.get("99"),
    }

astra = summary("${ASTRA_JSON}")
etcd = summary("${ETCD_JSON}")
logs = Path("${ASTRA_LOG_TXT}").read_text(encoding="utf-8", errors="ignore")
logs = re.sub(r"\\x1b\\[[0-9;]*m", "", logs)

batch_vals = [
    int(m.group(1))
    for m in re.finditer(r"batch_requests=(\\d+)", logs)
]
max_batch_requests_logged = max(batch_vals) if batch_vals else 0

has_fallocate = "wal fallocate preallocation complete" in logs
has_io_uring = "wal io engine selected: io_uring" in logs
has_batch_gt50 = max_batch_requests_logged >= 50

throughput_ok = (astra["rps"] or 0) > RPS_TARGET
p99_ok = (astra["p99_ns"] or 10**18) < P99_NS_TARGET
deadline_ok = astra["deadline_exceeded"] == 0
strict_ok = astra["ok"] == TOTAL
fail_fast_ok = (
    astra["ok"] + astra["resource_exhausted"] == TOTAL
    and astra["other_errors"] == 0
    and astra["resource_exhausted"] > 0
)
hybrid_success_ok = strict_ok or fail_fast_ok
phase3_pass = throughput_ok and p99_ok and deadline_ok and hybrid_success_ok

out = {
    "astra": astra,
    "etcd": etcd,
    "targets": {
        "rps_gt": RPS_TARGET,
        "p99_ns_lt": P99_NS_TARGET,
        "deadline_exceeded_eq": 0,
    },
    "checks": {
        "throughput_ok": throughput_ok,
        "p99_ok": p99_ok,
        "deadline_ok": deadline_ok,
        "strict_ok_50000": strict_ok,
        "fail_fast_ok": fail_fast_ok,
        "hybrid_success_ok": hybrid_success_ok,
        "telemetry_fallocate": has_fallocate,
        "telemetry_io_uring": has_io_uring,
        "telemetry_batch_gt50": has_batch_gt50,
        "max_batch_requests_logged": max_batch_requests_logged,
    },
    "phase3_pass": phase3_pass,
    "winner_rps": "astra" if (astra["rps"] or 0) > (etcd["rps"] or 0) else "etcd",
    "logs_file": "${ASTRA_LOG_TXT}",
}
Path("${SUMMARY_JSON}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY

docker rm -f phase3-etcd >/dev/null 2>&1 || true

echo "scenario_b_summary=${SUMMARY_JSON}"
