#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase12-common.sh
source "${SCRIPT_DIR}/phase12-common.sh"

phase12_require_tools docker cargo python3 curl
: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra-alpha:phase12-<sha>}"

: "${PHASE12_LIST_OBJECTS:=150000}"
: "${PHASE12_LIST_PAYLOAD_SIZE:=10240}"
: "${PHASE12_LIST_BLAST_CONCURRENCY:=64}"
: "${PHASE12_LIST_PAGE_BYTES:=1048576}"
: "${PHASE12_LIST_PEAK_RSS_MAX_MB:=50}"
: "${PHASE12_LIST_GRPC_MAX_BYTES:=268435456}"
: "${PHASE12_LIST_PUT_TIMEOUT_SECS:=120}"
: "${PHASE12_LIST_LARGE_VALUE_THRESHOLD_BYTES:=4096}"
: "${PHASE12_LIST_HYDRATE_CACHE_MAX_BYTES:=4194304}"

OUT_DIR="${RUN_DIR}/phase12-list-stream"
SUMMARY_JSON="${RUN_DIR}/phase12-list-stream-summary.json"
mkdir -p "${OUT_DIR}"

cleanup() {
  if [ -n "${RSS_SAMPLER_PID:-}" ]; then
    kill "${RSS_SAMPLER_PID}" >/dev/null 2>&1 || true
  fi
  phase6_stop_backend all || true
}
trap cleanup EXIT

export ASTRAD_LIST_STREAM_ENABLED=true
export ASTRAD_LIST_STREAM_CHUNK_BYTES="${PHASE12_LIST_PAGE_BYTES}"
export ASTRAD_GRPC_MAX_DECODING_MESSAGE_BYTES="${PHASE12_LIST_GRPC_MAX_BYTES}"
export ASTRAD_GRPC_MAX_ENCODING_MESSAGE_BYTES="${PHASE12_LIST_GRPC_MAX_BYTES}"
export ASTRAD_METRICS_ENABLED=true
export ASTRAD_LARGE_VALUE_MODE=tiered
export ASTRAD_LARGE_VALUE_THRESHOLD_BYTES="${PHASE12_LIST_LARGE_VALUE_THRESHOLD_BYTES}"
export ASTRAD_LARGE_VALUE_UPLOAD_TIMEOUT_SECS="${PHASE12_LIST_PUT_TIMEOUT_SECS}"
export ASTRAD_LARGE_VALUE_HYDRATE_CACHE_MAX_BYTES="${PHASE12_LIST_HYDRATE_CACHE_MAX_BYTES}"

phase6_log "phase12 list-stream: starting backend"
phase6_start_backend astra
ingest_leader_ep=$(phase6_find_astra_leader 120 1)
phase6_log "phase12 list-stream: ingest leader endpoint=${ingest_leader_ep}"

blast_log="${OUT_DIR}/blast.log"
blast_exit_code=0
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${ingest_leader_ep}" \
    blast \
    --prefix /phase12/list/obj \
    --count "${PHASE12_LIST_OBJECTS}" \
    --payload-size "${PHASE12_LIST_PAYLOAD_SIZE}" \
    --concurrency "${PHASE12_LIST_BLAST_CONCURRENCY}" \
    --put-timeout-secs "${PHASE12_LIST_PUT_TIMEOUT_SECS}"
) >"${blast_log}" 2>&1 || blast_exit_code=$?

stream_leader_ep=$(phase6_find_astra_leader 60 1 || true)
if [ -z "${stream_leader_ep}" ]; then
  stream_leader_ep="${ingest_leader_ep}"
fi
phase6_log "phase12 list-stream: stream leader endpoint=${stream_leader_ep}"

leader_container=$(phase12_endpoint_to_container "${stream_leader_ep}")
leader_metrics_port=$(phase12_endpoint_to_metrics_port "${stream_leader_ep}")
if [ -z "${leader_container}" ] || [ -z "${leader_metrics_port}" ]; then
  phase6_log "phase12 list-stream: failed to map leader container/metrics"
  exit 1
fi

leader_pid=$(docker inspect --format '{{.State.Pid}}' "${leader_container}")
rss_log="${OUT_DIR}/rss.log"
(
  while true; do
    printf '%s ' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    awk '/VmRSS:/ {print $2}' "/proc/${leader_pid}/status" 2>/dev/null || echo 0
    sleep 0.2
  done
) >"${rss_log}" 2>/dev/null &
RSS_SAMPLER_PID=$!

stream_log="${OUT_DIR}/stream-list.log"
stream_exit_code=0
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${stream_leader_ep}" \
    stream-list /phase12/list/obj --prefix \
    --page-size-bytes "${PHASE12_LIST_PAGE_BYTES}" \
    --limit 0
) >"${stream_log}" 2>&1 || stream_exit_code=$?

kill "${RSS_SAMPLER_PID}" >/dev/null 2>&1 || true
wait "${RSS_SAMPLER_PID}" 2>/dev/null || true
unset RSS_SAMPLER_PID

metrics_file="${OUT_DIR}/leader.metrics.txt"
curl -fsS "http://127.0.0.1:${leader_metrics_port}/metrics" >"${metrics_file}" || true

python3 - <<'PY' \
  "${blast_log}" \
  "${stream_log}" \
  "${rss_log}" \
  "${metrics_file}" \
  "${SUMMARY_JSON}" \
  "${PHASE12_LIST_OBJECTS}" \
  "${PHASE12_LIST_PAYLOAD_SIZE}" \
  "${PHASE12_LIST_PEAK_RSS_MAX_MB}" \
  "${ingest_leader_ep}" \
  "${stream_leader_ep}" \
  "${blast_exit_code}" \
  "${stream_exit_code}" \
  "${OUT_DIR}"
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

blast_log = Path(sys.argv[1])
stream_log = Path(sys.argv[2])
rss_log = Path(sys.argv[3])
metrics_path = Path(sys.argv[4])
out_path = Path(sys.argv[5])
objects_target = int(sys.argv[6])
payload_size = int(sys.argv[7])
peak_rss_max_mb = float(sys.argv[8])
ingest_leader_ep = sys.argv[9]
stream_leader_ep = sys.argv[10]
blast_exit_code = int(sys.argv[11])
stream_exit_code = int(sys.argv[12])
out_dir = Path(sys.argv[13])

blast_text = blast_log.read_text(encoding="utf-8", errors="ignore") if blast_log.exists() else ""
m = re.search(r"blast\s+success=(\d+)\s+exhausted=(\d+)\s+last_error=(.*)$", blast_text, re.MULTILINE)
if m:
    blast_success = int(m.group(1))
    blast_exhausted = int(m.group(2))
    blast_last_error = m.group(3).strip()
else:
    blast_success = 0
    blast_exhausted = 0
    blast_last_error = ""

stream_text = stream_log.read_text(encoding="utf-8", errors="ignore") if stream_log.exists() else ""
page_lines = [ln for ln in stream_text.splitlines() if ln.startswith("page=")]
m2 = re.search(r"stream_list\s+pages=(\d+)\s+kvs_total=(\d+)", stream_text)
if m2:
    pages = int(m2.group(1))
    kvs_total = int(m2.group(2))
else:
    pages = len(page_lines)
    kvs_total = 0

rss_kb = []
if rss_log.exists():
    for line in rss_log.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        try:
            rss_kb.append(int(parts[1]))
        except Exception:
            pass
peak_rss_mb = (max(rss_kb) / 1024.0) if rss_kb else 0.0
rss_start_mb = (rss_kb[0] / 1024.0) if rss_kb else 0.0
peak_rss_delta_mb = max(0.0, peak_rss_mb - rss_start_mb)

metrics_text = metrics_path.read_text(encoding="utf-8", errors="ignore") if metrics_path.exists() else ""
def metric_scalar(name: str) -> float:
    m = re.search(rf"^{re.escape(name)}\s+([0-9eE+\-.]+)$", metrics_text, re.MULTILINE)
    return float(m.group(1)) if m else 0.0

stream_chunks = metric_scalar("astra_list_stream_chunks_total")
stream_bytes = metric_scalar("astra_list_stream_bytes_total")
hydrated_values = metric_scalar("astra_list_stream_hydrated_values_total")
hydrated_bytes = metric_scalar("astra_list_stream_hydrated_bytes_total")

estimated_payload_gib = (blast_success * payload_size) / float(1024**3)

pass_gate = (
    blast_success >= objects_target
    and kvs_total >= objects_target
    and pages > 0
    and stream_chunks > 0
    and hydrated_values > 0
    and peak_rss_delta_mb <= peak_rss_max_mb
)

summary = {
    "scenario": "list_streaming",
    "leader_endpoint": stream_leader_ep,
    "ingest_leader_endpoint": ingest_leader_ep,
    "target": {
        "objects": objects_target,
        "payload_size_bytes": payload_size,
        "estimated_payload_gib": (objects_target * payload_size) / float(1024**3),
        "peak_rss_max_mb": peak_rss_max_mb,
    },
    "ingest": {
        "blast_exit_code": blast_exit_code,
        "blast_success": blast_success,
        "blast_exhausted": blast_exhausted,
        "blast_last_error": blast_last_error,
        "estimated_ingested_payload_gib": estimated_payload_gib,
    },
    "stream": {
        "stream_exit_code": stream_exit_code,
        "pages": pages,
        "kvs_total": kvs_total,
        "metrics_chunks_total": stream_chunks,
        "metrics_bytes_total": stream_bytes,
        "metrics_hydrated_values_total": hydrated_values,
        "metrics_hydrated_bytes_total": hydrated_bytes,
    },
    "memory": {
        "rss_start_mb": rss_start_mb,
        "peak_rss_mb": peak_rss_mb,
        "peak_rss_delta_mb": peak_rss_delta_mb,
    },
    "artifacts": {
        "blast_log": str(blast_log),
        "stream_log": str(stream_log),
        "rss_log": str(rss_log),
        "metrics": str(metrics_path),
        "out_dir": str(out_dir),
    },
    "pass": pass_gate,
}

out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase12 list-stream summary: ${SUMMARY_JSON}"
