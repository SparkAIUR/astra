#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "${SCRIPT_DIR}/../../.." && pwd)
# shellcheck source=phase2-common.sh
source "${SCRIPT_DIR}/phase2-common.sh"

ensure_tools

if [ -z "${ASTRA_IMAGE:-}" ]; then
  echo "ASTRA_IMAGE is required, e.g. halceon/astra-alpha:<tag>" >&2
  exit 1
fi

export ASTRAD_AUTH_ENABLED=${ASTRAD_AUTH_ENABLED:-false}
export ASTRAD_TENANT_VIRTUALIZATION_ENABLED=${ASTRAD_TENANT_VIRTUALIZATION_ENABLED:-false}

SUMMARY_JSON="${RESULTS_DIR}/phase5-scenario-forge-bulkload-summary.json"
BUNDLE_DIR="${RESULTS_DIR}/phase5-forge-bundle"
INPUT_JSONL="${RESULTS_DIR}/phase5-forge-input.jsonl"
WASM_WAT="${RESULTS_DIR}/phase5-forge-identity.wat"

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

leader_ep=$(find_writable_endpoint "/phase5/forge/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable Astra endpoint" >&2
  compose logs --tail 120
  exit 2
}

leader_info=$(endpoint_to_container "${leader_ep}" || true)
if [ -z "${leader_info}" ]; then
  echo "failed to map leader endpoint to container: ${leader_ep}" >&2
  exit 2
fi
read -r leader_container _ <<<"${leader_info}"

python3 - <<'PY' "${INPUT_JSONL}"
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rows = []
for i in range(1000):
    rows.append(
        {
            "key": f"/phase5/forge/k-{i:04d}",
            "value": f"value-{i:04d}",
            "lease": 0,
            "create_revision": i + 1,
            "mod_revision": i + 1,
            "version": 1,
        }
    )

with out.open("w", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row))
        f.write("\n")
PY

cat > "${WASM_WAT}" <<'WAT'
(module
  (memory (export "memory") 1)
  (func $alloc (export "alloc") (param $len i32) (result i32)
    (i32.const 0))
  (func (export "dealloc") (param i32 i32))
  (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))))
)
WAT

rm -rf "${BUNDLE_DIR}"
mkdir -p "${BUNDLE_DIR}"

(cd "${REPO_DIR}" && cargo run --quiet --bin astra-forge -- compile \
  --input "${INPUT_JSONL}" \
  --input-format jsonl \
  --out-dir "${BUNDLE_DIR}" \
  --chunk-target-bytes 65536 \
  --wasm "${WASM_WAT}")

container_bundle_dir=/tmp/phase5-forge-bundle
container_manifest="${container_bundle_dir}/manifest.json"
docker exec "${leader_container}" rm -rf "${container_bundle_dir}" >/dev/null
docker exec "${leader_container}" mkdir -p "${container_bundle_dir}" >/dev/null
docker cp "${BUNDLE_DIR}/." "${leader_container}:${container_bundle_dir}/"

(cd "${REPO_DIR}" && cargo run --quiet --bin astra-forge -- bulk-load \
  --endpoint "http://${leader_ep}" \
  --manifest "${container_manifest}" \
  --tenant-id "tenant-a" \
  --wait)

probe_value=$(etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${leader_ep}" get /phase5/forge/k-0042 --print-value-only 2>/dev/null || true)

python3 - <<'PY' "${BUNDLE_DIR}/manifest.json" "${probe_value}" "${SUMMARY_JSON}"
import json
import pathlib
import sys

manifest_path = pathlib.Path(sys.argv[1])
probe_value = sys.argv[2]
summary_path = pathlib.Path(sys.argv[3])

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
chunks = manifest.get("chunks", [])
records = sum(int(c.get("records", 0)) for c in chunks)
summary = {
    "manifest_path": str(manifest_path),
    "chunk_count": len(chunks),
    "records_total": records,
    "probe_value": probe_value,
    "pass": len(chunks) > 0 and records == 1000 and probe_value == "value-0042",
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "phase5 forge summary: ${SUMMARY_JSON}"
