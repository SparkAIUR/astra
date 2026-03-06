#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase12-common.sh
source "${SCRIPT_DIR}/phase12-common.sh"

phase12_require_tools docker cargo python3 etcdctl curl
: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra-alpha:phase12-<sha>}"

: "${PHASE12_CRUCIBLE_TARGET_GIB:=100}"
: "${PHASE12_CRUCIBLE_PAYLOAD_MIB:=15}"
: "${PHASE12_CRUCIBLE_DISK_SAFETY_RATIO:=0.75}"
: "${PHASE12_CRUCIBLE_PUT_COUNT_OVERRIDE:=0}"
: "${PHASE12_CRUCIBLE_REJOIN_TIMEOUT_SECS:=900}"
: "${PHASE12_CRUCIBLE_REJOIN_PASS_SECS:=600}"
: "${PHASE12_CRUCIBLE_LEADER_CPU_MAX:=85}"
: "${PHASE12_CRUCIBLE_PUT_TIMEOUT_SECS:=180}"
: "${ASTRAD_LARGE_VALUE_MODE:=tiered}"
: "${ASTRAD_LARGE_VALUE_THRESHOLD_BYTES:=1048576}"
: "${ASTRAD_LARGE_VALUE_UPLOAD_CHUNK_BYTES:=1048576}"
: "${ASTRAD_LARGE_VALUE_UPLOAD_TIMEOUT_SECS:=180}"
: "${ASTRAD_LARGE_VALUE_HYDRATE_CACHE_MAX_BYTES:=67108864}"
: "${ASTRAD_RAFT_SNAPSHOT_MAX_CHUNK_BYTES:=16777216}"
: "${ASTRAD_RAFT_SNAPSHOT_LOGS_SINCE_LAST:=128}"
: "${ASTRAD_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP:=32}"
: "${ASTRAD_RAFT_PURGE_BATCH_SIZE:=1024}"

OUT_DIR="${RUN_DIR}/phase12-limitless-crucible"
SUMMARY_JSON="${RUN_DIR}/phase12-limitless-crucible-summary.json"
mkdir -p "${OUT_DIR}"

cleanup() {
  if [ -n "${RECOVERY_SAMPLER_PID:-}" ]; then
    kill "${RECOVERY_SAMPLER_PID}" >/dev/null 2>&1 || true
  fi
  phase6_stop_backend all || true
}
trap cleanup EXIT

export ASTRAD_GRPC_MAX_DECODING_MESSAGE_BYTES=$((64 * 1024 * 1024))
export ASTRAD_GRPC_MAX_ENCODING_MESSAGE_BYTES=$((64 * 1024 * 1024))
export ASTRAD_LARGE_VALUE_MODE
export ASTRAD_LARGE_VALUE_THRESHOLD_BYTES
export ASTRAD_LARGE_VALUE_UPLOAD_CHUNK_BYTES
export ASTRAD_LARGE_VALUE_UPLOAD_TIMEOUT_SECS
export ASTRAD_LARGE_VALUE_HYDRATE_CACHE_MAX_BYTES
export ASTRAD_RAFT_SNAPSHOT_MAX_CHUNK_BYTES
export ASTRAD_RAFT_SNAPSHOT_LOGS_SINCE_LAST
export ASTRAD_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP
export ASTRAD_RAFT_PURGE_BATCH_SIZE
export PHASE6_ASTRA_MEM_LIMIT=${PHASE6_ASTRA_MEM_LIMIT:-1200m}
export ASTRAD_METRICS_ENABLED=true

phase6_log "phase12 limitless-crucible: starting backend"
phase6_start_backend astra
leader_ep=$(phase6_find_astra_leader 120 1)
leader_container=$(phase12_endpoint_to_container "${leader_ep}")

payload_bytes=$((PHASE12_CRUCIBLE_PAYLOAD_MIB * 1024 * 1024))
requested_target_bytes=$((PHASE12_CRUCIBLE_TARGET_GIB * 1024 * 1024 * 1024))
avail_bytes=$(df --output=avail -B1 / | tail -n 1 | tr -d ' ')
max_safe_bytes=$(python3 - <<'PY' "${avail_bytes}" "${PHASE12_CRUCIBLE_DISK_SAFETY_RATIO}"
import math
import sys
avail = int(float(sys.argv[1]))
ratio = float(sys.argv[2])
print(int(math.floor(avail * ratio)))
PY
)

actual_target_bytes=${requested_target_bytes}
truncated=false
if [ "${actual_target_bytes}" -gt "${max_safe_bytes}" ]; then
  actual_target_bytes=${max_safe_bytes}
  truncated=true
fi
if [ "${actual_target_bytes}" -lt "${payload_bytes}" ]; then
  phase6_log "phase12 limitless-crucible: insufficient disk budget for a single payload"
  actual_target_bytes=${payload_bytes}
fi
put_count=$((actual_target_bytes / payload_bytes))
if [ "${put_count}" -lt 1 ]; then
  put_count=1
fi
if [ "${PHASE12_CRUCIBLE_PUT_COUNT_OVERRIDE}" -gt 0 ]; then
  put_count=${PHASE12_CRUCIBLE_PUT_COUNT_OVERRIDE}
  actual_target_bytes=$((put_count * payload_bytes))
  truncated=true
fi

phase6_log "phase12 limitless-crucible: requested_gib=${PHASE12_CRUCIBLE_TARGET_GIB} actual_bytes=${actual_target_bytes} payload_bytes=${payload_bytes} puts=${put_count}"

blast_log="${OUT_DIR}/blast.log"
start_ts=$(date -u +%s)
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${leader_ep}" \
    blast \
    --prefix /phase12/crucible/blob \
    --count "${put_count}" \
    --payload-size "${payload_bytes}" \
    --put-timeout-secs "${PHASE12_CRUCIBLE_PUT_TIMEOUT_SECS}"
) >"${blast_log}" 2>&1
end_ts=$(date -u +%s)
ingest_secs=$((end_ts - start_ts))

recovery_log="${OUT_DIR}/recovery-stats.log"
(
  while true; do
    printf '%s ' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker stats --no-stream --format '{{.CPUPerc}}|{{.NetIO}}|{{.BlockIO}}' "${leader_container}"
    sleep 1
  done
) >"${recovery_log}" 2>/dev/null &
RECOVERY_SAMPLER_PID=$!

phase6_log "phase12 limitless-crucible: killing node3 and wiping local disk"
docker rm -f phase6-astra-node3-1 >/dev/null 2>&1 || true
docker volume rm -f phase6_phase6-astra-node3-data >/dev/null 2>&1 || true
phase6_compose up -d astra-node3 >/dev/null

rejoin_secs=-1
for i in $(seq 1 "${PHASE12_CRUCIBLE_REJOIN_TIMEOUT_SECS}"); do
  if etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="127.0.0.1:${PHASE6_ASTRA_NODE3_PORT:-32392}" get "/phase12/crucible/rejoin-probe" --write-out=json >"${OUT_DIR}/node3-status.json" 2>/dev/null; then
    rejoin_secs=${i}
    break
  fi
  sleep 1
done

docker ps -a >"${OUT_DIR}/docker-ps.txt" 2>&1 || true
docker inspect phase6-astra-node3-1 >"${OUT_DIR}/node3.inspect.json" 2>&1 || true
docker logs --tail 400 phase6-astra-node3-1 >"${OUT_DIR}/node3.log" 2>&1 || true
docker logs --tail 400 "${leader_container}" >"${OUT_DIR}/leader.log" 2>&1 || true
leader_metrics_port=$(phase12_endpoint_to_metrics_port "${leader_ep}")
curl -fsS "http://127.0.0.1:${leader_metrics_port}/metrics" >"${OUT_DIR}/leader.metrics.txt" 2>&1 || true
curl -fsS "http://127.0.0.1:${PHASE6_ASTRA_NODE3_METRICS_PORT:-19481}/metrics" >"${OUT_DIR}/node3.metrics.txt" 2>&1 || true

kill "${RECOVERY_SAMPLER_PID}" >/dev/null 2>&1 || true
wait "${RECOVERY_SAMPLER_PID}" 2>/dev/null || true
unset RECOVERY_SAMPLER_PID

minio_usage_file="${OUT_DIR}/minio-usage.txt"
docker run --rm --network phase6_default --entrypoint /bin/sh minio/mc:latest \
  -c "mc alias set m http://minio:9000 minioadmin minioadmin >/dev/null && mc du --versions --recursive m/astra-tier" \
  >"${minio_usage_file}" 2>&1 || true

python3 - <<'PY' \
  "${blast_log}" \
  "${recovery_log}" \
  "${minio_usage_file}" \
  "${SUMMARY_JSON}" \
  "${leader_ep}" \
  "${put_count}" \
  "${payload_bytes}" \
  "${requested_target_bytes}" \
  "${actual_target_bytes}" \
  "${truncated}" \
  "${ingest_secs}" \
  "${rejoin_secs}" \
  "${PHASE12_CRUCIBLE_REJOIN_PASS_SECS}" \
  "${PHASE12_CRUCIBLE_LEADER_CPU_MAX}"
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

blast_log = Path(sys.argv[1])
recovery_log = Path(sys.argv[2])
minio_usage_file = Path(sys.argv[3])
out_path = Path(sys.argv[4])
leader_ep = sys.argv[5]
put_count = int(sys.argv[6])
payload_bytes = int(sys.argv[7])
requested_target_bytes = int(sys.argv[8])
actual_target_bytes = int(sys.argv[9])
truncated = sys.argv[10].strip().lower() in {"1", "true", "yes"}
ingest_secs = int(sys.argv[11])
rejoin_secs = int(sys.argv[12])
rejoin_pass_secs = float(sys.argv[13])
leader_cpu_max = float(sys.argv[14])

blast_text = blast_log.read_text(encoding="utf-8", errors="ignore") if blast_log.exists() else ""
m = re.search(r"blast\s+success=(\d+)\s+exhausted=(\d+)\s+last_error=(.*)$", blast_text, re.MULTILINE)
if m:
    success = int(m.group(1))
    exhausted = int(m.group(2))
    last_error = m.group(3).strip()
else:
    success = 0
    exhausted = 0
    last_error = ""

cpu_vals = []
net_tx_vals = []
if recovery_log.exists():
    for line in recovery_log.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.strip().split(" ", 1)
        if len(parts) != 2:
            continue
        payload = parts[1]
        chunks = payload.split("|")
        if len(chunks) < 2:
            continue
        try:
            cpu_vals.append(float(chunks[0].strip().rstrip("%")))
        except Exception:
            pass
        net = chunks[1].strip()
        if "/" in net:
            tx = net.split("/")[-1].strip()
            n = re.match(r"^([0-9eE+\-.]+)\s*([kKmMgGtTpP]?B)$", tx)
            if n:
                v = float(n.group(1))
                unit = n.group(2).upper()
                scale = {
                    "B": 1,
                    "KB": 1024,
                    "MB": 1024**2,
                    "GB": 1024**3,
                    "TB": 1024**4,
                    "PB": 1024**5,
                }.get(unit, 1)
                net_tx_vals.append(v * scale)

leader_cpu_peak = max(cpu_vals) if cpu_vals else 0.0
leader_net_tx_delta_mb = 0.0
if len(net_tx_vals) >= 2:
    leader_net_tx_delta_mb = max(0.0, (net_tx_vals[-1] - net_tx_vals[0]) / (1024.0 * 1024.0))

minio_usage_text = minio_usage_file.read_text(encoding="utf-8", errors="ignore") if minio_usage_file.exists() else ""

ingested_bytes = success * payload_bytes
ingested_gib = ingested_bytes / float(1024**3)

pass_gate = (
    success > 0
    and rejoin_secs > 0
    and rejoin_secs <= rejoin_pass_secs
    and leader_cpu_peak <= leader_cpu_max
)

summary = {
    "scenario": "limitless_crucible",
    "leader_endpoint": leader_ep,
    "target": {
        "requested_bytes": requested_target_bytes,
        "actual_bytes": actual_target_bytes,
        "truncated_by_disk_budget": truncated,
        "payload_bytes": payload_bytes,
        "put_count": put_count,
    },
    "ingest": {
        "success": success,
        "exhausted": exhausted,
        "last_error": last_error,
        "duration_secs": ingest_secs,
        "ingested_bytes": ingested_bytes,
        "ingested_gib": ingested_gib,
    },
    "recovery": {
        "node3_rejoin_secs": rejoin_secs,
        "rejoin_pass_secs": rejoin_pass_secs,
        "leader_cpu_peak_percent": leader_cpu_peak,
        "leader_cpu_max_percent": leader_cpu_max,
        "leader_net_tx_delta_mb": leader_net_tx_delta_mb,
    },
    "minio_usage": {
        "raw": minio_usage_text.strip(),
    },
    "pass": pass_gate,
}

out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase12 limitless-crucible summary: ${SUMMARY_JSON}"
