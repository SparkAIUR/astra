#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

LEGACY_COUNT=${PHASE11_LEGACY_COUNT:-5}
OMNI_IMAGE=${PHASE11_OMNI_IMAGE:-ghcr.io/siderolabs/omni:latest}
OMNI_BOOT_WAIT_SECS=${PHASE11_OMNI_BOOT_WAIT_SECS:-60}
OMNI_REQUIRE_REAL_MIN=${PHASE11_OMNI_REQUIRE_REAL_MIN:-4}
OMNI_OIDC_PROVIDER_URL=${PHASE11_OMNI_OIDC_PROVIDER_URL:-https://accounts.google.com}
OMNI_OIDC_CLIENT_ID=${PHASE11_OMNI_OIDC_CLIENT_ID:-phase11-dev-client}
OMNI_OIDC_CLIENT_SECRET=${PHASE11_OMNI_OIDC_CLIENT_SECRET:-phase11-dev-secret}
OMNI_KEY_FILE=${PHASE11_OMNI_KEY_FILE:-${RUN_DIR}/phase11-omni.asc}

RUN_FLEET_DIR="${RUN_DIR}/phase11-fleet-cutover"
SUMMARY_JSON="${RUN_DIR}/phase11-fleet-cutover-summary.json"
FORGE_SUMMARY_JSON="${RUN_DIR}/phase11-forge-ux-summary.json"
mkdir -p "${RUN_FLEET_DIR}"

phase6_require_tools docker etcdctl jq python3 gpg

if [ -z "${PHASE6_BACKEND_ENDPOINT:-}" ]; then
  if detected=$(phase6_find_astra_leader 30 1); then
    export PHASE6_BACKEND_ENDPOINT="${detected}"
  else
    export PHASE6_BACKEND_ENDPOINT="127.0.0.1:${PHASE6_ASTRA_NODE1_PORT:-2379}"
  fi
fi
phase6_wait_for_tcp "${PHASE6_BACKEND_ENDPOINT}" 30 1 || {
  echo "astra backend endpoint unavailable: ${PHASE6_BACKEND_ENDPOINT}" >&2
  exit 1
}
phase6_log "phase11 fleet: backend endpoint=${PHASE6_BACKEND_ENDPOINT}"

cleanup_omni() {
  local i
  for i in $(seq 1 "${LEGACY_COUNT}"); do
    docker rm -f "phase11-omni-${i}" >/dev/null 2>&1 || true
  done
}
trap cleanup_omni EXIT

if [ ! -f "${FORGE_SUMMARY_JSON}" ]; then
  echo "missing forge summary: ${FORGE_SUMMARY_JSON}" >&2
  exit 1
fi

tenants=$(python3 - <<'PY' "${FORGE_SUMMARY_JSON}"
import json
import sys
from pathlib import Path
obj = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for item in obj.get("tenants", []):
    t = item.get("tenant_id")
    if t:
        print(t)
PY
)

if [ -z "${tenants}" ]; then
  echo "no tenants found in ${FORGE_SUMMARY_JSON}" >&2
  exit 1
fi

if [ ! -e /dev/net/tun ]; then
  echo "/dev/net/tun missing; cannot run real omni containers" >&2
  exit 1
fi

if [ ! -s "${OMNI_KEY_FILE}" ]; then
  phase6_log "phase11 fleet: generating omni private key ${OMNI_KEY_FILE}"
  tmp_home=$(mktemp -d)
  gpg --batch --homedir "${tmp_home}" --pinentry-mode loopback --passphrase '' \
    --quick-gen-key 'phase11-omni <phase11@astra.local>' rsa3072 sign 0 >/dev/null 2>&1
  fpr=$(gpg --homedir "${tmp_home}" --list-secret-keys --with-colons | awk -F: '$1=="fpr"{print $10; exit}')
  gpg --homedir "${tmp_home}" --armor --export-secret-keys "${fpr}" >"${OMNI_KEY_FILE}"
  rm -rf "${tmp_home}"
fi

before_counts=()
region_values_before=()
for tenant in ${tenants}; do
  prefix="/__tenant/${tenant}/"
  count=$(etcdctl --dial-timeout=2s --command-timeout=8s --endpoints="${PHASE6_BACKEND_ENDPOINT}" get "${prefix}" --prefix --keys-only \
    | awk '/^\/__tenant\// {c++} END {print c+0}')
  before_counts+=("${count}")
  region_key="/__tenant/${tenant}//omni/shared/region"
  region_val=$(etcdctl --dial-timeout=2s --command-timeout=8s --endpoints="${PHASE6_BACKEND_ENDPOINT}" get "${region_key}" --print-value-only 2>/dev/null || true)
  region_values_before+=("${region_val}")
done

boot_ok=()
external_ok=()
running_ok=()
log_paths=()

idx=0
for tenant in ${tenants}; do
  idx=$((idx + 1))
  name="phase11-omni-${idx}"
  bind_port=$((18100 + idx))
  machine_api_port=$((18200 + idx))
  k8s_proxy_port=$((18300 + idx))
  wg_port=$((18400 + idx))
  metrics_port=$((18500 + idx))
  data_dir="${RUN_FLEET_DIR}/${name}-data"
  log_file="${RUN_FLEET_DIR}/${name}.log"
  mkdir -p "${data_dir}"

  phase6_log "phase11 fleet: booting ${name} tenant=${tenant}"
  docker rm -f "${name}" >/dev/null 2>&1 || true
  if docker run -d --name "${name}" \
    --network host \
    --cap-add NET_ADMIN \
    --device /dev/net/tun \
    -v "${data_dir}:/var/lib/omni" \
    -v "${OMNI_KEY_FILE}:${OMNI_KEY_FILE}:ro" \
    "${OMNI_IMAGE}" \
    --name="${name}" \
    --storage-kind=etcd \
    --etcd-embedded=false \
    --etcd-endpoints="http://${PHASE6_BACKEND_ENDPOINT}" \
    --etcd-client-cert-path= \
    --etcd-client-key-path= \
    --etcd-ca-path= \
    --sqlite-storage-path=/var/lib/omni/omni.sqlite \
    --private-key-source="file://${OMNI_KEY_FILE}" \
    --auth-oidc-enabled=true \
    --auth-oidc-provider-url="${OMNI_OIDC_PROVIDER_URL}" \
    --auth-oidc-client-id="${OMNI_OIDC_CLIENT_ID}" \
    --auth-oidc-client-secret="${OMNI_OIDC_CLIENT_SECRET}" \
    --account-id="11111111-1111-1111-1111-$(printf '%012d' "${idx}")" \
    --bind-addr="0.0.0.0:${bind_port}" \
    --advertised-api-url="https://127.0.0.1:${bind_port}" \
    --machine-api-bind-addr="0.0.0.0:${machine_api_port}" \
    --k8s-proxy-bind-addr="0.0.0.0:${k8s_proxy_port}" \
    --siderolink-wireguard-bind-addr="127.0.0.1:${wg_port}" \
    --siderolink-wireguard-advertised-addr="127.0.0.1:${wg_port}" \
    --metrics-bind-addr="0.0.0.0:${metrics_port}" \
    >/dev/null 2>&1; then

    ok=false
    ext=false
    running=false
    for _ in $(seq 1 "${OMNI_BOOT_WAIT_SECS}"); do
      docker logs "${name}" >"${log_file}" 2>&1 || true
      if docker inspect -f '{{.State.Running}}' "${name}" 2>/dev/null | grep -q true; then
        running=true
      fi
      if grep -q 'starting etcd client' "${log_file}"; then
        ok=true
      fi
      if grep -q 'starting etcd client' "${log_file}" && ! grep -q 'starting embedded etcd server' "${log_file}"; then
        ext=true
      fi
      if [ "${ok}" = "true" ] && [ "${ext}" = "true" ]; then
        break
      fi
      sleep 1
    done
    boot_ok+=("${ok}")
    external_ok+=("${ext}")
    running_ok+=("${running}")
  else
    boot_ok+=("false")
    external_ok+=("false")
    running_ok+=("false")
    docker logs "${name}" >"${log_file}" 2>&1 || true
  fi
  log_paths+=("${log_file}")
done

after_counts=()
region_values_after=()
for tenant in ${tenants}; do
  prefix="/__tenant/${tenant}/"
  count=$(etcdctl --dial-timeout=2s --command-timeout=8s --endpoints="${PHASE6_BACKEND_ENDPOINT}" get "${prefix}" --prefix --keys-only \
    | awk '/^\/__tenant\// {c++} END {print c+0}')
  after_counts+=("${count}")
  region_key="/__tenant/${tenant}//omni/shared/region"
  region_val=$(etcdctl --dial-timeout=2s --command-timeout=8s --endpoints="${PHASE6_BACKEND_ENDPOINT}" get "${region_key}" --print-value-only 2>/dev/null || true)
  region_values_after+=("${region_val}")
done

python3 - <<'PY' \
"${SUMMARY_JSON}" \
"${PHASE6_BACKEND_ENDPOINT}" \
"${LEGACY_COUNT}" \
"${OMNI_REQUIRE_REAL_MIN}" \
"${tenants}" \
"${before_counts[*]}" \
"${after_counts[*]}" \
"${region_values_before[*]}" \
"${region_values_after[*]}" \
"${boot_ok[*]}" \
"${external_ok[*]}" \
"${running_ok[*]}" \
"${log_paths[*]}"
from __future__ import annotations
import json
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
backend = sys.argv[2]
legacy_count = int(sys.argv[3])
require_real_min = int(sys.argv[4])
tenants = sys.argv[5].split()
before_counts = [int(x) for x in sys.argv[6].split()]
after_counts = [int(x) for x in sys.argv[7].split()]
region_before = sys.argv[8].split()
region_after = sys.argv[9].split()
boot_ok = [x.lower() == "true" for x in sys.argv[10].split()]
external_ok = [x.lower() == "true" for x in sys.argv[11].split()]
running_ok = [x.lower() == "true" for x in sys.argv[12].split()]
log_paths = sys.argv[13].split()

instances = []
for i, tenant in enumerate(tenants):
    instances.append(
        {
            "tenant_id": tenant,
            "boot_ok": boot_ok[i] if i < len(boot_ok) else False,
            "external_etcd": external_ok[i] if i < len(external_ok) else False,
            "running": running_ok[i] if i < len(running_ok) else False,
            "log": log_paths[i] if i < len(log_paths) else "",
        }
    )

tenant_checks = []
for i, tenant in enumerate(tenants):
    b = before_counts[i] if i < len(before_counts) else 0
    a = after_counts[i] if i < len(after_counts) else 0
    rb = region_before[i] if i < len(region_before) else ""
    ra = region_after[i] if i < len(region_after) else ""
    tenant_checks.append(
        {
            "tenant_id": tenant,
            "records_before": b,
            "records_after": a,
            "no_data_loss": a >= b,
            "region_marker_before": rb,
            "region_marker_after": ra,
            "marker_preserved": rb != "" and rb == ra,
        }
    )

real_boot_count = sum(1 for item in instances if item["boot_ok"] and item["external_etcd"])
all_no_loss = all(item["no_data_loss"] for item in tenant_checks)
all_markers_ok = all(item["marker_preserved"] for item in tenant_checks)

summary = {
    "backend_endpoint": backend,
    "legacy_count": legacy_count,
    "required_real_boot_min": require_real_min,
    "real_boot_count": real_boot_count,
    "instances": instances,
    "tenant_checks": tenant_checks,
    "pass": real_boot_count >= require_real_min and all_no_loss and all_markers_ok,
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "phase11 fleet summary: ${SUMMARY_JSON}"
