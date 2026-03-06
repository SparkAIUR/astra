#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

phase12_require_tools() {
  phase6_require_tools "$@"
}

phase12_endpoint_to_node() {
  local endpoint=${1:?endpoint required}
  local port=${endpoint##*:}
  local node1_port=${PHASE6_ASTRA_NODE1_PORT:-2379}
  local node2_port=${PHASE6_ASTRA_NODE2_PORT:-32391}
  local node3_port=${PHASE6_ASTRA_NODE3_PORT:-32392}
  case "${port}" in
    "${node1_port}") echo "node1" ;;
    "${node2_port}") echo "node2" ;;
    "${node3_port}") echo "node3" ;;
    *) echo "unknown" ;;
  esac
}

phase12_endpoint_to_metrics_port() {
  local endpoint=${1:?endpoint required}
  local node
  node=$(phase12_endpoint_to_node "${endpoint}")
  local node1_metrics=${PHASE6_ASTRA_NODE1_METRICS_PORT:-19479}
  local node2_metrics=${PHASE6_ASTRA_NODE2_METRICS_PORT:-19480}
  local node3_metrics=${PHASE6_ASTRA_NODE3_METRICS_PORT:-19481}
  case "${node}" in
    node1) echo "${node1_metrics}" ;;
    node2) echo "${node2_metrics}" ;;
    node3) echo "${node3_metrics}" ;;
    *) echo "" ;;
  esac
}

phase12_endpoint_to_container() {
  local endpoint=${1:?endpoint required}
  local node
  node=$(phase12_endpoint_to_node "${endpoint}")
  case "${node}" in
    node1) echo "phase6-astra-node1-1" ;;
    node2) echo "phase6-astra-node2-1" ;;
    node3) echo "phase6-astra-node3-1" ;;
    *) echo "" ;;
  esac
}

phase12_write_put_request_json() {
  local out_file=${1:?output file required}
  local key=${2:?key required}
  local value=${3:?value required}
  python3 - <<'PY' "${out_file}" "${key}" "${value}"
import base64
import json
import sys

out_path = sys.argv[1]
key = sys.argv[2].encode()
value = sys.argv[3].encode()
obj = {
    "key": base64.b64encode(key).decode(),
    "value": base64.b64encode(value).decode(),
    "lease": 0,
    "prevKv": False,
    "ignoreValue": False,
    "ignoreLease": False,
}
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(obj, f)
PY
}

phase12_write_range_request_json() {
  local out_file=${1:?output file required}
  local key=${2:?key required}
  local prefix=${3:-true}
  python3 - <<'PY' "${out_file}" "${key}" "${prefix}"
import base64
import json
import sys

out_path = sys.argv[1]
key = sys.argv[2].encode()
prefix = sys.argv[3].strip().lower() in {"1", "true", "yes"}

if prefix:
    arr = bytearray(key)
    i = len(arr) - 1
    while i >= 0 and arr[i] == 0xFF:
        i -= 1
    if i < 0:
        range_end = b"\x00"
    else:
        arr[i] += 1
        range_end = bytes(arr[: i + 1])
else:
    range_end = b""

obj = {
    "key": base64.b64encode(key).decode(),
    "rangeEnd": base64.b64encode(range_end).decode(),
    "limit": "0",
    "revision": "0",
    "keysOnly": False,
    "countOnly": False,
}
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(obj, f)
PY
}

phase12_ghz_field() {
  local in_file=${1:?ghz output file required}
  local field=${2:?field required}
  python3 - <<'PY' "${in_file}" "${field}"
import json
import sys
from pathlib import Path

obj = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
summary = obj.get("summary")
if not isinstance(summary, dict) or not summary:
    summary = obj
latency = summary.get("latencyDistribution") or obj.get("latencyDistribution") or []
field = sys.argv[2].strip().lower()
if field == "rps":
    print(float(summary.get("rps") or obj.get("rps") or 0.0))
elif field in {"count", "total", "ok", "error", "errors"}:
    print(float(summary.get(field) or obj.get(field) or 0.0))
elif field in {"p99_ms", "p95_ms", "p50_ms"}:
    wanted = field.split("_")[0].lstrip("p")
    val = 0.0
    for row in latency:
        per = str(row.get("percentage") or "")
        if per.startswith(wanted + ""):
            try:
                val = float(row.get("latency") or 0.0) / 1_000_000.0
            except Exception:
                val = 0.0
            break
    print(val)
else:
    print(0.0)
PY
}

phase12_metric_scalar() {
  local metrics_file=${1:?metrics file required}
  local metric=${2:?metric required}
  python3 - <<'PY' "${metrics_file}" "${metric}"
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
name = sys.argv[2].strip()
pat = re.compile(rf"^{re.escape(name)}\\s+([0-9eE+\\-\\.]+)$", re.MULTILINE)
m = pat.search(text)
if not m:
    print(0.0)
else:
    try:
        print(float(m.group(1)))
    except Exception:
        print(0.0)
PY
}

phase12_histogram_quantile_ms() {
  local metrics_file=${1:?metrics file required}
  local metric_base=${2:?metric base required}
  local quantile=${3:?quantile required}
  python3 - <<'PY' "${metrics_file}" "${metric_base}" "${quantile}"
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
base = sys.argv[2].strip()
q = float(sys.argv[3])

bucket_pat = re.compile(
    rf"^{re.escape(base)}_bucket\\{{[^}}]*le=\"([^\"]+)\"[^}}]*\\}}\\s+([0-9eE+\\-\\.]+)$",
    re.MULTILINE,
)
buckets = []
for m in bucket_pat.finditer(text):
    le = m.group(1)
    count = float(m.group(2))
    if le == "+Inf":
        ub = float("inf")
    else:
        ub = float(le)
    buckets.append((ub, count))
if not buckets:
    print(0.0)
    raise SystemExit(0)

buckets.sort(key=lambda x: x[0])
total = buckets[-1][1]
if total <= 0:
    print(0.0)
    raise SystemExit(0)

want = total * q
for ub, c in buckets:
    if c >= want:
        if ub == float("inf"):
            # If we only hit +Inf, report previous finite bound if present.
            finite = [x for x, _ in buckets if x != float("inf")]
            ub = finite[-1] if finite else 0.0
        print(max(0.0, ub) * 1000.0)
        raise SystemExit(0)

print(0.0)
PY
}
