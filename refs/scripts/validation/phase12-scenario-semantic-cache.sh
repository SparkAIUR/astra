#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase12-common.sh
source "${SCRIPT_DIR}/phase12-common.sh"

phase12_require_tools docker cargo ghz python3 curl
: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra-alpha:phase12-<sha>}"

: "${PHASE12_CACHE_KEYS:=10000}"
: "${PHASE12_CACHE_PAYLOAD_SIZE:=8192}"
: "${PHASE12_CACHE_BENCH_DURATION:=30s}"
: "${PHASE12_CACHE_BENCH_CONCURRENCY:=256}"
: "${PHASE12_CACHE_BENCH_CONNECTIONS:=64}"
: "${PHASE12_CACHE_EXPECT_RPS_GAIN:=1.10}"
: "${PHASE12_CACHE_EXPECT_P99_GAIN:=1.05}"
: "${PHASE12_CACHE_PUT_TIMEOUT_SECS:=120}"
: "${PHASE12_CACHE_LARGE_VALUE_THRESHOLD_BYTES:=4096}"
: "${PHASE12_CACHE_HYDRATE_CACHE_MAX_BYTES:=4096}"

OUT_DIR="${RUN_DIR}/phase12-semantic-cache"
SUMMARY_JSON="${RUN_DIR}/phase12-semantic-cache-summary.json"
mkdir -p "${OUT_DIR}"

cleanup() {
  phase6_stop_backend all || true
}
trap cleanup EXIT

range_req="${OUT_DIR}/get-request.json"
phase12_write_range_request_json "${range_req}" "/registry/secrets/default/phase12-secret/42" false

run_case() {
  local mode=${1:?mode required}
  local cache_enabled=${2:?cache flag required}

  phase6_stop_backend all || true
  export ASTRAD_SEMANTIC_CACHE_ENABLED="${cache_enabled}"
  export ASTRAD_SEMANTIC_CACHE_PREFIXES="/registry/secrets/,/registry/configmaps/"
  export ASTRAD_SEMANTIC_CACHE_MAX_ENTRIES="${PHASE12_CACHE_KEYS}"
  export ASTRAD_SEMANTIC_CACHE_MAX_BYTES=$((PHASE12_CACHE_KEYS * PHASE12_CACHE_PAYLOAD_SIZE * 2))
  export ASTRAD_METRICS_ENABLED=true
  export ASTRAD_LARGE_VALUE_MODE=tiered
  export ASTRAD_LARGE_VALUE_THRESHOLD_BYTES="${PHASE12_CACHE_LARGE_VALUE_THRESHOLD_BYTES}"
  export ASTRAD_LARGE_VALUE_UPLOAD_TIMEOUT_SECS="${PHASE12_CACHE_PUT_TIMEOUT_SECS}"
  export ASTRAD_LARGE_VALUE_HYDRATE_CACHE_MAX_BYTES="${PHASE12_CACHE_HYDRATE_CACHE_MAX_BYTES}"

  phase6_log "phase12 semantic-cache: starting mode=${mode} cache=${cache_enabled}"
  phase6_start_backend astra
  local leader_ep
  leader_ep=$(phase6_find_astra_leader 120 1)
  local leader_container
  leader_container=$(phase12_endpoint_to_container "${leader_ep}")
  local leader_metrics_port
  leader_metrics_port=$(phase12_endpoint_to_metrics_port "${leader_ep}")
  local minio_container=phase6-minio-1

  local blast_log="${OUT_DIR}/${mode}-blast.log"
  (
    cd "${REPO_DIR}"
    cargo run --quiet --bin astractl -- \
      --endpoint "http://${leader_ep}" \
      blast \
      --prefix /registry/secrets/default/phase12-secret \
      --count "${PHASE12_CACHE_KEYS}" \
      --payload-size "${PHASE12_CACHE_PAYLOAD_SIZE}" \
      --put-timeout-secs "${PHASE12_CACHE_PUT_TIMEOUT_SECS}"
  ) >"${blast_log}" 2>&1

  local pid
  pid=$(docker inspect --format '{{.State.Pid}}' "${leader_container}")
  local rss_after_seed_kb
  rss_after_seed_kb=$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)

  local leader_block_before
  leader_block_before=$(docker stats --no-stream --format '{{.BlockIO}}' "${leader_container}" || true)
  local minio_block_before
  minio_block_before=$(docker stats --no-stream --format '{{.BlockIO}}' "${minio_container}" || true)

  local ghz_out="${OUT_DIR}/${mode}-get.json"
  ghz --insecure \
    --proto "${SCRIPT_DIR}/proto/rpc.proto" \
    --call etcdserverpb.KV.Range \
    --data-file "${range_req}" \
    --duration "${PHASE12_CACHE_BENCH_DURATION}" \
    --concurrency "${PHASE12_CACHE_BENCH_CONCURRENCY}" \
    --connections "${PHASE12_CACHE_BENCH_CONNECTIONS}" \
    --format json \
    "${leader_ep}" >"${ghz_out}"

  local rss_after_kb
  rss_after_kb=$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)

  local leader_block_after
  leader_block_after=$(docker stats --no-stream --format '{{.BlockIO}}' "${leader_container}" || true)
  local minio_block_after
  minio_block_after=$(docker stats --no-stream --format '{{.BlockIO}}' "${minio_container}" || true)

  local metrics_file="${OUT_DIR}/${mode}.metrics.txt"
  curl -fsS "http://127.0.0.1:${leader_metrics_port}/metrics" >"${metrics_file}" || true

  python3 - <<'PY' \
    "${ghz_out}" \
    "${metrics_file}" \
    "${blast_log}" \
    "${OUT_DIR}/${mode}-summary.json" \
    "${mode}" \
    "${leader_ep}" \
    "${rss_after_seed_kb}" \
    "${rss_after_kb}" \
    "${leader_block_before}" \
    "${leader_block_after}" \
    "${minio_block_before}" \
    "${minio_block_after}"
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ghz_path = Path(sys.argv[1])
metrics_path = Path(sys.argv[2])
blast_log = Path(sys.argv[3])
out_path = Path(sys.argv[4])
mode = sys.argv[5]
leader_ep = sys.argv[6]
rss_after_seed_kb = int(float(sys.argv[7]))
rss_after_kb = int(float(sys.argv[8]))
leader_block_before = sys.argv[9]
leader_block_after = sys.argv[10]
minio_block_before = sys.argv[11]
minio_block_after = sys.argv[12]

ghz = json.loads(ghz_path.read_text(encoding="utf-8")) if ghz_path.exists() else {}
summary = ghz.get("summary")
if not isinstance(summary, dict) or not summary:
    summary = ghz
lat_dist = summary.get("latencyDistribution") or ghz.get("latencyDistribution") or []

def pxx_ms(pct: str) -> float:
    for row in lat_dist:
        p = str(row.get("percentage") or "")
        if p.startswith(pct):
            return float(row.get("latency") or 0.0) / 1_000_000.0
    return 0.0

metrics_text = metrics_path.read_text(encoding="utf-8", errors="ignore") if metrics_path.exists() else ""
def metric_scalar(name: str) -> float:
    m = re.search(rf"^{re.escape(name)}\s+([0-9eE+\-.]+)$", metrics_text, re.MULTILINE)
    return float(m.group(1)) if m else 0.0

blast_text = blast_log.read_text(encoding="utf-8", errors="ignore") if blast_log.exists() else ""
m_blast = re.search(r"blast\s+success=(\d+)\s+exhausted=(\d+)\s+last_error=(.*)$", blast_text, re.MULTILINE)
blast_success = int(m_blast.group(1)) if m_blast else 0
blast_exhausted = int(m_blast.group(2)) if m_blast else 0

units = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
    "PB": 1024**5,
}

def parse_block_io(raw: str) -> tuple[float, float]:
    # e.g. "1.23MB / 5.67MB"
    if not raw or "/" not in raw:
        return (0.0, 0.0)
    parts = [x.strip() for x in raw.split("/")]
    if len(parts) != 2:
        return (0.0, 0.0)
    out = []
    for token in parts:
        m = re.match(r"^([0-9eE+\-.]+)\s*([kKmMgGtTpP]?B)$", token)
        if not m:
            out.append(0.0)
            continue
        out.append(float(m.group(1)) * units.get(m.group(2).upper(), 1))
    return (out[0], out[1])

leader_read_before, leader_write_before = parse_block_io(leader_block_before)
leader_read_after, leader_write_after = parse_block_io(leader_block_after)
minio_read_before, minio_write_before = parse_block_io(minio_block_before)
minio_read_after, minio_write_after = parse_block_io(minio_block_after)

case_summary = {
    "mode": mode,
    "leader_endpoint": leader_ep,
    "ingest": {
        "blast_success": blast_success,
        "blast_exhausted": blast_exhausted,
    },
    "ghz": {
        "rps": float(summary.get("rps") or ghz.get("rps") or 0.0),
        "count": float(summary.get("count") or ghz.get("count") or 0.0),
        "p50_ms": pxx_ms("50"),
        "p95_ms": pxx_ms("95"),
        "p99_ms": pxx_ms("99"),
    },
    "semantic_cache_metrics": {
        "hits": metric_scalar("astra_semantic_cache_hits_total"),
        "misses": metric_scalar("astra_semantic_cache_misses_total"),
    },
    "large_value_metrics": {
        "hydrate_cache_hits": metric_scalar("astra_large_value_hydrate_cache_hits_total"),
        "hydrate_cache_misses": metric_scalar("astra_large_value_hydrate_cache_misses_total"),
    },
    "memory": {
        "rss_after_seed_mb": rss_after_seed_kb / 1024.0,
        "rss_after_mb": rss_after_kb / 1024.0,
        "rss_delta_mb": max(0.0, (rss_after_kb - rss_after_seed_kb) / 1024.0),
    },
    "block_io": {
        "leader_read_delta_mb": max(0.0, (leader_read_after - leader_read_before) / (1024.0 * 1024.0)),
        "leader_write_delta_mb": max(0.0, (leader_write_after - leader_write_before) / (1024.0 * 1024.0)),
        "minio_read_delta_mb": max(0.0, (minio_read_after - minio_read_before) / (1024.0 * 1024.0)),
        "minio_write_delta_mb": max(0.0, (minio_write_after - minio_write_before) / (1024.0 * 1024.0)),
    },
    "artifacts": {
        "ghz": str(ghz_path),
        "metrics": str(metrics_path),
        "blast_log": str(blast_log),
    },
}
out_path.write_text(json.dumps(case_summary, indent=2), encoding="utf-8")
print(json.dumps(case_summary, indent=2))
PY

  phase6_stop_backend all || true
}

run_case off false
run_case on true

python3 - <<'PY' \
  "${OUT_DIR}/off-summary.json" \
  "${OUT_DIR}/on-summary.json" \
  "${SUMMARY_JSON}" \
  "${PHASE12_CACHE_EXPECT_RPS_GAIN}" \
  "${PHASE12_CACHE_EXPECT_P99_GAIN}"
from __future__ import annotations

import json
import sys
from pathlib import Path

off = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
on = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
out_path = Path(sys.argv[3])
expected_rps_gain = float(sys.argv[4])
expected_p99_gain = float(sys.argv[5])

off_rps = float((off.get("ghz") or {}).get("rps") or 0.0)
on_rps = float((on.get("ghz") or {}).get("rps") or 0.0)
off_p99 = float((off.get("ghz") or {}).get("p99_ms") or 0.0)
on_p99 = float((on.get("ghz") or {}).get("p99_ms") or 0.0)

rps_gain = (on_rps / off_rps) if off_rps > 0 else 0.0
p99_improve = (off_p99 / on_p99) if on_p99 > 0 else 0.0
hit_total = float((on.get("semantic_cache_metrics") or {}).get("hits") or 0.0)
miss_total = float((on.get("semantic_cache_metrics") or {}).get("misses") or 0.0)
off_hydrate_misses = float((off.get("large_value_metrics") or {}).get("hydrate_cache_misses") or 0.0)
on_hydrate_misses = float((on.get("large_value_metrics") or {}).get("hydrate_cache_misses") or 0.0)

summary = {
    "scenario": "semantic_cache",
    "off": off,
    "on": on,
    "delta": {
        "rps_gain": rps_gain,
        "p99_improvement_ratio": p99_improve,
        "cache_hits": hit_total,
        "cache_misses": miss_total,
        "rss_seed_overhead_mb": float((on.get("memory") or {}).get("rss_after_seed_mb") or 0.0)
        - float((off.get("memory") or {}).get("rss_after_seed_mb") or 0.0),
        "rss_runtime_overhead_mb": float((on.get("memory") or {}).get("rss_delta_mb") or 0.0)
        - float((off.get("memory") or {}).get("rss_delta_mb") or 0.0),
        "hydrate_miss_reduction": off_hydrate_misses - on_hydrate_misses,
        "minio_read_io_delta_mb": float((off.get("block_io") or {}).get("minio_read_delta_mb") or 0.0)
        - float((on.get("block_io") or {}).get("minio_read_delta_mb") or 0.0),
    },
    "thresholds": {
        "rps_gain_min": expected_rps_gain,
        "p99_improvement_min": expected_p99_gain,
    },
    "notes": [
        "Both modes force large-value tiering so GETs traverse the object tier unless the semantic hot-cache serves the request directly.",
        "The on-case blast warms the semantic cache across the full 10k keyset via put-path refresh.",
    ],
}
summary["pass"] = (
    rps_gain >= expected_rps_gain
    and p99_improve >= expected_p99_gain
    and hit_total > 0
    and (off_hydrate_misses - on_hydrate_misses) > 0
)

out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase12 semantic-cache summary: ${SUMMARY_JSON}"
