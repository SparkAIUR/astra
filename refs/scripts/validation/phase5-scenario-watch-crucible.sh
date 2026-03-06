#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
# shellcheck source=phase2-common.sh
source "${SCRIPT_DIR}/phase2-common.sh"

ensure_tools

if [ -z "${ASTRA_IMAGE:-}" ]; then
  echo "ASTRA_IMAGE is required, e.g. halceon/astra:<tag>" >&2
  exit 1
fi

WATCHERS=${ASTRA_PHASE5_WATCHERS:-10000}
STREAMS=${ASTRA_PHASE5_WATCH_STREAMS:-64}
UPDATES=${ASTRA_PHASE5_WATCH_UPDATES:-1}
PAYLOAD_SIZE=${ASTRA_PHASE5_WATCH_PAYLOAD_SIZE:-256}
SETTLE_MS=${ASTRA_PHASE5_WATCH_SETTLE_MS:-2000}
CREATE_TIMEOUT_SECS=${ASTRA_PHASE5_WATCH_CREATE_TIMEOUT_SECS:-180}
SUMMARY_JSON="${RESULTS_DIR}/phase5-scenario-watch-crucible-summary.json"

compose down -v || true
if [ "${ASTRA_SKIP_PULL:-false}" != "true" ]; then
  compose pull
fi
compose up -d

wait_for_ports 90 1 || {
  echo "timed out waiting for Astra ports" >&2
  compose logs --tail 120
  exit 2
}

leader_ep=$(find_writable_endpoint "/phase5/watch/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable Astra endpoint" >&2
  compose logs --tail 120
  exit 2
}
other_eps=$(other_endpoints "${leader_ep}" || true)
watch_endpoints="${leader_ep}"
for ep in ${other_eps}; do
  watch_endpoints="${watch_endpoints},${ep}"
done

ulimit -n 65535 || true

(cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- \
  --endpoint "http://${leader_ep}" \
  watch-crucible /phase5/watch/ns --prefix \
  --watchers "${WATCHERS}" \
  --streams "${STREAMS}" \
  --updates "${UPDATES}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --settle-ms "${SETTLE_MS}" \
  --create-timeout-secs "${CREATE_TIMEOUT_SECS}" \
  --watch-endpoints "${watch_endpoints}" \
  --output "${SUMMARY_JSON}")

python3 - <<'PY' "${SUMMARY_JSON}" "${WATCHERS}" "${UPDATES}"
import json
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
watchers = int(sys.argv[2])
updates = int(sys.argv[3])
summary = json.loads(summary_path.read_text(encoding="utf-8"))

events_received = int(summary.get("events_received", 0))
created = int(summary.get("watchers_created", 0))
errors = int(summary.get("stream_errors", 0))
expected_min = watchers * max(1, updates)
summary["expected_min_events"] = expected_min
summary["pass"] = created >= watchers and errors == 0 and events_received >= expected_min

summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "phase5 watch crucible summary: ${SUMMARY_JSON}"
