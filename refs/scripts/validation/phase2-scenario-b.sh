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
export ASTRAD_WAL_BATCH_MAX_ENTRIES=${ASTRAD_WAL_BATCH_MAX_ENTRIES:-2048}
export ASTRAD_WAL_BATCH_MAX_BYTES=${ASTRAD_WAL_BATCH_MAX_BYTES:-4194304}
export ASTRAD_WAL_FLUSH_INTERVAL_US=${ASTRAD_WAL_FLUSH_INTERVAL_US:-1000}

ASTRA_JSON="${RESULTS_DIR}/phase2-scenario-b-astra.json"
ETCD_JSON="${RESULTS_DIR}/phase2-scenario-b-etcd.json"
SUMMARY_JSON="${RESULTS_DIR}/phase2-scenario-b-summary.json"
GHZ_DATA_JSON="${RESULTS_DIR}/phase2-scenario-b-put-data.json"

compose_blkio down -v || true
docker rm -f phase2-etcd >/dev/null 2>&1 || true
compose_blkio pull
compose_blkio up -d

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  compose_blkio logs --tail 120
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase2/b/leader_probe" "$(date +%s%N)" 90) || {
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

docker run -d --name phase2-etcd \
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
from pathlib import Path

def summary(path):
    d = json.loads(Path(path).read_text())
    lat = {str(x["percentage"]): x["latency"] for x in d.get("latencyDistribution", [])}
    return {
        "count": d.get("count"),
        "ok": d.get("statusCodeDistribution", {}).get("OK", 0),
        "rps": d.get("rps"),
        "avg_ns": d.get("average"),
        "p50_ns": lat.get("50"),
        "p90_ns": lat.get("90"),
        "p99_ns": lat.get("99"),
    }

astra = summary("${ASTRA_JSON}")
etcd = summary("${ETCD_JSON}")
out = {
    "astra": astra,
    "etcd": etcd,
    "winner_rps": "astra" if (astra["rps"] or 0) > (etcd["rps"] or 0) else "etcd",
}
Path("${SUMMARY_JSON}").write_text(json.dumps(out, indent=2))
print(json.dumps(out, indent=2))
PY

docker rm -f phase2-etcd >/dev/null 2>&1 || true

echo "scenario_b_summary=${SUMMARY_JSON}"
