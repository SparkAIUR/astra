#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase12-common.sh
source "${SCRIPT_DIR}/phase12-common.sh"

phase12_require_tools docker etcdctl cargo python3 curl
: "${ASTRA_IMAGE:?ASTRA_IMAGE is required, e.g. halceon/astra:phase12-<sha>}"

: "${PHASE12_WATCHERS:=50000}"
: "${PHASE12_WATCH_STREAMS:=512}"
: "${PHASE12_WATCH_CREATE_TIMEOUT_SECS:=360}"
: "${PHASE12_WATCH_SETTLE_MS:=90000}"
: "${PHASE12_WATCH_PAYLOAD_SIZE:=256}"
: "${PHASE12_WATCH_WRITE_DURATION_SECS:=60}"
: "${PHASE12_WATCH_WRITE_KEYS:=1024}"
: "${PHASE12_WATCH_WRITE_CONCURRENCY:=192}"
: "${PHASE12_WATCH_WRITE_TARGET_RPS:=5000}"
: "${PHASE12_WATCH_LAG_P99_PASS_MS:=250}"
: "${PHASE12_WATCH_WRITE_RPS_MIN:=3500}"
: "${PHASE12_WATCH_CREATE_RATIO_MIN:=0.98}"
: "${ASTRAD_WATCH_DISPATCH_WORKERS:=8}"
: "${ASTRAD_WATCH_STREAM_QUEUE_DEPTH:=256}"
: "${ASTRAD_WATCH_SLOW_CANCEL_GRACE_MS:=1500}"
: "${ASTRAD_WATCH_EMIT_BATCH_MAX:=256}"
: "${ASTRAD_WATCH_LAGGED_POLICY:=resync_current}"
: "${ASTRAD_WATCH_LAGGED_RESYNC_LIMIT:=8}"
: "${PHASE12_WATCH_USE_ALL_READY_FOLLOWERS:=false}"
: "${PHASE12_WATCH_FORCE_ENTRY_ENDPOINT:=}"

OUT_DIR="${RUN_DIR}/phase12-watch-fanout"
SUMMARY_JSON="${RUN_DIR}/phase12-watch-fanout-summary.json"
mkdir -p "${OUT_DIR}"

cleanup() {
  if [ -n "${CPU_SAMPLER_PID:-}" ]; then
    kill "${CPU_SAMPLER_PID}" >/dev/null 2>&1 || true
  fi
  if [ -n "${WATCH_JOB_PID:-}" ]; then
    kill "${WATCH_JOB_PID}" >/dev/null 2>&1 || true
  fi
  phase6_stop_backend all || true
}
trap cleanup EXIT

export ASTRAD_WATCH_ACCEPT_ROLE=follower_only
export ASTRAD_WATCH_REDIRECT_HINT="127.0.0.1:${PHASE6_ASTRA_NODE2_PORT:-32391}"
export ASTRAD_WATCH_DISPATCH_WORKERS
export ASTRAD_WATCH_STREAM_QUEUE_DEPTH
export ASTRAD_WATCH_SLOW_CANCEL_GRACE_MS
export ASTRAD_WATCH_EMIT_BATCH_MAX
export ASTRAD_WATCH_LAGGED_POLICY
export ASTRAD_WATCH_LAGGED_RESYNC_LIMIT
export ASTRAD_METRICS_ENABLED=true

phase6_log "phase12 watch-fanout: starting backend with follower-only watch policy"
phase6_start_backend astra
leader_ep=$(phase6_find_astra_leader 120 1)
all_eps=("127.0.0.1:${PHASE6_ASTRA_NODE1_PORT:-2379}" "127.0.0.1:${PHASE6_ASTRA_NODE2_PORT:-32391}" "127.0.0.1:${PHASE6_ASTRA_NODE3_PORT:-32392}")

phase12_watch_health_probe() {
  local endpoint=${1:?endpoint required}
  etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${endpoint}" get "/phase6/probe/leader" >/dev/null 2>&1
}

phase12_watch_transport_ready() {
  local endpoint=${1:?endpoint required}
  timeout 10 cargo run --quiet --bin astractl -- \
    --endpoint "http://${endpoint}" \
    get /phase12/watch/preflight >/dev/null 2>&1
}

phase12_watch_role_probe_is_leader() {
  local endpoint=${1:?endpoint required}
  local role_log="${OUT_DIR}/role-probe-${endpoint##*:}.log"
  if timeout 8 cargo run --quiet --bin astractl -- \
    --endpoint "http://${endpoint}" \
    watch /phase12/watch/role-probe --prefix >"${role_log}" 2>&1; then
    return 1
  fi
  grep -Eqi 'delegated|failed precondition|retry against' "${role_log}"
}

derived_leader=""
for ep in "${all_eps[@]}"; do
  if ! phase6_wait_for_tcp "${ep}" 5 1; then
    continue
  fi
  if ! phase12_watch_transport_ready "${ep}"; then
    continue
  fi
  if phase12_watch_role_probe_is_leader "${ep}"; then
    derived_leader="${ep}"
    break
  fi
done
if [ -n "${derived_leader}" ]; then
  leader_ep="${derived_leader}"
fi
phase6_log "phase12 watch-fanout: resolved leader endpoint=${leader_ep}"

followers=()
for ep in "${all_eps[@]}"; do
  if [ "${ep}" != "${leader_ep}" ]; then
    followers+=("${ep}")
  fi
done
if [ "${#followers[@]}" -eq 0 ]; then
  phase6_log "phase12 watch-fanout: failed to identify follower endpoints"
  exit 1
fi

healthy_followers=()
for _ in $(seq 1 120); do
  healthy_followers=()
  for ep in "${followers[@]}"; do
    if ! phase6_wait_for_tcp "${ep}" 2 1; then
      continue
    fi
    if phase12_watch_health_probe "${ep}"; then
      healthy_followers+=("${ep}")
    fi
  done
  if [ "${#healthy_followers[@]}" -gt 0 ]; then
    break
  fi
  sleep 1
done
if [ "${#healthy_followers[@]}" -eq 0 ]; then
  phase6_log "phase12 watch-fanout: no healthy follower endpoint available"
  exit 1
fi

ready_followers=()
for ep in "${healthy_followers[@]}"; do
  if phase12_watch_transport_ready "${ep}" && phase12_watch_transport_ready "${ep}"; then
    ready_followers+=("${ep}")
  fi
done
if [ "${#ready_followers[@]}" -eq 0 ]; then
  phase6_log "phase12 watch-fanout: no transport-ready follower endpoint available"
  exit 1
fi
phase6_log "phase12 watch-fanout: leader=${leader_ep} followers=${followers[*]} ready_followers=${ready_followers[*]}"

best_follower=""
best_score=-1
for ep in "${ready_followers[@]}"; do
  score=0
  for _ in $(seq 1 5); do
    if phase12_watch_transport_ready "${ep}"; then
      score=$((score + 1))
    fi
    sleep 1
  done
  if [ "${score}" -gt "${best_score}" ]; then
    best_score=${score}
    best_follower="${ep}"
  elif [ "${score}" -eq "${best_score}" ] && [ "${ep##*:}" != "${PHASE6_ASTRA_NODE1_PORT:-2379}" ]; then
    # Prefer non-node1 endpoints when scores are tied: node1 host-port is often most collision-prone.
    best_follower="${ep}"
  fi
done
if [ -z "${best_follower}" ]; then
  phase6_log "phase12 watch-fanout: unable to select stable follower endpoint"
  exit 1
fi

watch_entry_ep="${best_follower}"
if [ -n "${PHASE12_WATCH_FORCE_ENTRY_ENDPOINT}" ]; then
  for ep in "${ready_followers[@]}"; do
    if [ "${ep}" = "${PHASE12_WATCH_FORCE_ENTRY_ENDPOINT}" ]; then
      watch_entry_ep="${ep}"
      break
    fi
  done
fi
if [ "${PHASE12_WATCH_USE_ALL_READY_FOLLOWERS}" = "true" ]; then
  follower_csv=$(IFS=,; echo "${ready_followers[*]}")
else
  follower_csv="${watch_entry_ep}"
fi
phase6_log "phase12 watch-fanout: selected watch entry=${watch_entry_ep} follower_csv=${follower_csv} stable_score=${best_score}/5"

leader_container=$(phase12_endpoint_to_container "${leader_ep}")
if [ -z "${leader_container}" ]; then
  phase6_log "phase12 watch-fanout: unable to map leader container for ${leader_ep}"
  exit 1
fi

deny_log="${OUT_DIR}/leader-watch-deny.log"
if timeout 8 cargo run --quiet --bin astractl -- \
  --endpoint "http://${leader_ep}" \
  watch /phase12/watch/deny --prefix >"${deny_log}" 2>&1; then
  leader_watch_denied=false
else
  if grep -Eqi 'delegated|failed precondition|retry against' "${deny_log}"; then
    leader_watch_denied=true
  else
    leader_watch_denied=false
  fi
fi

ulimit -n 262144 >/dev/null 2>&1 || true

cpu_log="${OUT_DIR}/leader-cpu-net.log"
(
  while true; do
    printf '%s ' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker stats --no-stream --format '{{.CPUPerc}}|{{.NetIO}}|{{.BlockIO}}' "${leader_container}"
    sleep 1
  done
) >"${cpu_log}" 2>/dev/null &
CPU_SAMPLER_PID=$!

watch_summary="${OUT_DIR}/watch-crucible-summary.json"
watch_log="${OUT_DIR}/watch-crucible.log"
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${watch_entry_ep}" \
    watch-crucible /phase12/watch/ns --prefix \
    --watchers "${PHASE12_WATCHERS}" \
    --streams "${PHASE12_WATCH_STREAMS}" \
    --updates 1 \
    --payload-size "${PHASE12_WATCH_PAYLOAD_SIZE}" \
    --settle-ms "${PHASE12_WATCH_SETTLE_MS}" \
    --create-timeout-secs "${PHASE12_WATCH_CREATE_TIMEOUT_SECS}" \
    --watch-endpoints "${follower_csv}" \
    --output "${watch_summary}"
) >"${watch_log}" 2>&1 &
WATCH_JOB_PID=$!

sleep 5

put_summary="${OUT_DIR}/put-storm-summary.json"
(
  cd "${REPO_DIR}"
  cargo run --quiet --bin astractl -- \
    --endpoint "http://${leader_ep}" \
    put-storm \
    --prefix /phase12/watch/write \
    --keys "${PHASE12_WATCH_WRITE_KEYS}" \
    --payload-size 256 \
    --duration-secs "${PHASE12_WATCH_WRITE_DURATION_SECS}" \
    --concurrency "${PHASE12_WATCH_WRITE_CONCURRENCY}" \
    --target-rps "${PHASE12_WATCH_WRITE_TARGET_RPS}" \
    --output "${put_summary}"
) >"${OUT_DIR}/put-storm.log" 2>&1

wait "${WATCH_JOB_PID}" || true
unset WATCH_JOB_PID

for ep in "${ready_followers[@]}"; do
  cname=$(phase12_endpoint_to_container "${ep}")
  if [ -n "${cname}" ]; then
    docker logs --tail 200 "${cname}" >"${OUT_DIR}/${cname}.log" 2>&1 || true
    docker inspect "${cname}" >"${OUT_DIR}/${cname}.inspect.json" 2>&1 || true
  fi
done

if [ ! -f "${watch_summary}" ]; then
  phase6_log "phase12 watch-fanout: watch summary missing; capturing container diagnostics"
  docker ps -a >"${OUT_DIR}/docker-ps.txt" 2>&1 || true
  for ep in "${ready_followers[@]}"; do
    cname=$(phase12_endpoint_to_container "${ep}")
    if [ -n "${cname}" ]; then
      docker logs --tail 200 "${cname}" >"${OUT_DIR}/${cname}.log" 2>&1 || true
    fi
  done
fi

kill "${CPU_SAMPLER_PID}" >/dev/null 2>&1 || true
wait "${CPU_SAMPLER_PID}" 2>/dev/null || true
unset CPU_SAMPLER_PID

leader_metrics_port=$(phase12_endpoint_to_metrics_port "${leader_ep}")
leader_metrics="${OUT_DIR}/leader.metrics.txt"
curl -fsS "http://127.0.0.1:${leader_metrics_port}/metrics" >"${leader_metrics}" || true

follower_metrics_files=()
for ep in "${followers[@]}"; do
  mport=$(phase12_endpoint_to_metrics_port "${ep}")
  mf="${OUT_DIR}/follower-${mport}.metrics.txt"
  curl -fsS "http://127.0.0.1:${mport}/metrics" >"${mf}" || true
  follower_metrics_files+=("${mf}")
done

python3 - <<'PY' \
  "${watch_summary}" \
  "${put_summary}" \
  "${cpu_log}" \
  "${leader_metrics}" \
  "${SUMMARY_JSON}" \
  "${leader_ep}" \
  "${watch_entry_ep}" \
  "${follower_csv}" \
  "${PHASE12_WATCHERS}" \
  "${PHASE12_WATCH_LAG_P99_PASS_MS}" \
  "${PHASE12_WATCH_WRITE_RPS_MIN}" \
  "${PHASE12_WATCH_CREATE_RATIO_MIN}" \
  "${leader_watch_denied}" \
  "${OUT_DIR}" \
  "${follower_metrics_files[@]}"
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

watch_path = Path(sys.argv[1])
put_path = Path(sys.argv[2])
cpu_path = Path(sys.argv[3])
leader_metrics_path = Path(sys.argv[4])
out_path = Path(sys.argv[5])
leader_ep = sys.argv[6]
watch_entry_ep = sys.argv[7]
followers_csv = sys.argv[8]
watch_target = int(sys.argv[9])
lag_p99_pass_ms = float(sys.argv[10])
write_rps_min = float(sys.argv[11])
create_ratio_min = float(sys.argv[12])
leader_watch_denied = sys.argv[13].strip().lower() in {"1", "true", "yes"}
out_dir = Path(sys.argv[14])
follower_metrics_paths = [Path(p) for p in sys.argv[15:] if p]

watch = json.loads(watch_path.read_text(encoding="utf-8")) if watch_path.exists() else {}
put = json.loads(put_path.read_text(encoding="utf-8")) if put_path.exists() else {}

cpu_values = []
net_tx_bytes = []
if cpu_path.exists():
    for line in cpu_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.strip().split(" ", 1)
        if len(parts) != 2:
            continue
        payload = parts[1]
        chunks = payload.split("|")
        if len(chunks) < 2:
            continue
        cpu_raw = chunks[0].strip().rstrip("%")
        try:
            cpu_values.append(float(cpu_raw))
        except Exception:
            pass
        net = chunks[1].strip()
        # format: "12.3kB / 45.6MB"
        if "/" in net:
            tx = net.split("/")[-1].strip()
            m = re.match(r"^([0-9eE+\-.]+)\s*([kKmMgGtTpP]?B)$", tx)
            if m:
                v = float(m.group(1))
                unit = m.group(2).upper()
                scale = {
                    "B": 1,
                    "KB": 1024,
                    "MB": 1024**2,
                    "GB": 1024**3,
                    "TB": 1024**4,
                    "PB": 1024**5,
                }.get(unit, 1)
                net_tx_bytes.append(v * scale)

leader_cpu_peak = max(cpu_values) if cpu_values else 0.0
leader_cpu_p99 = 0.0
if cpu_values:
    s = sorted(cpu_values)
    leader_cpu_p99 = s[max(0, min(len(s) - 1, int(round((len(s) - 1) * 0.99))))]
leader_net_tx_delta_mb = 0.0
if len(net_tx_bytes) >= 2:
    leader_net_tx_delta_mb = max(0.0, (net_tx_bytes[-1] - net_tx_bytes[0]) / (1024.0 * 1024.0))

def metric_scalar(text: str, metric: str) -> float:
    m = re.search(rf"^{re.escape(metric)}\s+([0-9eE+\-.]+)$", text, re.MULTILINE)
    return float(m.group(1)) if m else 0.0

def histogram_p99_ms(text: str, base: str) -> float:
    rows = []
    pat = re.compile(
        rf"^{re.escape(base)}_bucket\{{[^}}]*le=\"([^\"]+)\"[^}}]*\}}\s+([0-9eE+\-.]+)$",
        re.MULTILINE,
    )
    for m in pat.finditer(text):
        le_raw = m.group(1)
        if le_raw == "+Inf":
            le = float("inf")
        else:
            le = float(le_raw)
        rows.append((le, float(m.group(2))))
    if not rows:
        return 0.0
    rows.sort(key=lambda x: x[0])
    total = rows[-1][1]
    if total <= 0:
        return 0.0
    want = total * 0.99
    for le, count in rows:
        if count >= want:
            if le == float("inf"):
                finite = [v for v, _ in rows if v != float("inf")]
                le = finite[-1] if finite else 0.0
            return max(0.0, le) * 1000.0
    return 0.0

leader_metrics_text = leader_metrics_path.read_text(encoding="utf-8", errors="ignore") if leader_metrics_path.exists() else ""
leader_reject = metric_scalar(leader_metrics_text, "astra_watch_delegate_reject_total")
leader_accept = metric_scalar(leader_metrics_text, "astra_watch_delegate_accept_total")

combined = {}
for fp in follower_metrics_paths:
    text = fp.read_text(encoding="utf-8", errors="ignore") if fp.exists() else ""
    for m in re.finditer(
        r'^astra_watch_emit_lag_seconds_bucket\{[^}]*le="([^"]+)"[^}]*\}\s+([0-9eE+\-.]+)$',
        text,
        re.MULTILINE,
    ):
        le = m.group(1)
        combined[le] = combined.get(le, 0.0) + float(m.group(2))

lag_p99_ms = 0.0
if combined:
    pairs = []
    for le_raw, count in combined.items():
        le = float("inf") if le_raw == "+Inf" else float(le_raw)
        pairs.append((le, count))
    pairs.sort(key=lambda x: x[0])
    total = pairs[-1][1]
    if total > 0:
        want = total * 0.99
        for le, count in pairs:
            if count >= want:
                if le == float("inf"):
                    finite = [v for v, _ in pairs if v != float("inf")]
                    le = finite[-1] if finite else 0.0
                lag_p99_ms = max(0.0, le) * 1000.0
                break

watchers_created = int(watch.get("watchers_created") or 0)
watch_stream_errors = int(watch.get("stream_errors") or 0)
watch_events = int(watch.get("events_received") or 0)
write_rps = float(put.get("rps") or 0.0)
write_p99_ms = float(((put.get("latency_ms") or {}).get("p99_approx")) or 0.0)

create_ratio = (watchers_created / watch_target) if watch_target > 0 else 0.0
pass_gate = (
    leader_watch_denied
    and create_ratio >= create_ratio_min
    and watch_stream_errors == 0
    and write_rps >= write_rps_min
    and lag_p99_ms <= lag_p99_pass_ms
)

summary = {
    "scenario": "watch_fanout",
    "leader_endpoint": leader_ep,
    "watch_entry_endpoint": watch_entry_ep,
    "follower_endpoints": [x for x in followers_csv.split(",") if x],
    "leader_watch_denied": leader_watch_denied,
    "watch": {
        "target": watch_target,
        "created": watchers_created,
        "create_ratio": create_ratio,
        "stream_errors": watch_stream_errors,
        "events_received": watch_events,
    },
    "write_load": {
        "target_rps": float(put.get("target_rps") or 0.0),
        "achieved_rps": write_rps,
        "p99_approx_ms": write_p99_ms,
    },
    "leader_cpu": {
        "peak_percent": leader_cpu_peak,
        "p99_percent": leader_cpu_p99,
    },
    "leader_network": {
        "tx_delta_mb": leader_net_tx_delta_mb,
    },
    "delegation_metrics": {
        "leader_reject_total": leader_reject,
        "leader_accept_total": leader_accept,
    },
    "lag": {
        "p99_ms": lag_p99_ms,
        "p99_pass_ms": lag_p99_pass_ms,
    },
    "thresholds": {
        "create_ratio_min": create_ratio_min,
        "write_rps_min": write_rps_min,
        "lag_p99_ms_max": lag_p99_pass_ms,
    },
    "artifacts": {
        "watch_summary": str(watch_path),
        "put_summary": str(put_path),
        "cpu_log": str(cpu_path),
        "leader_metrics": str(leader_metrics_path),
        "out_dir": str(out_dir),
    },
    "pass": pass_gate,
}

out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase12 watch-fanout summary: ${SUMMARY_JSON}"
