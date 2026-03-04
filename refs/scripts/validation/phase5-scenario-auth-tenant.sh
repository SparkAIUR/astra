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

SECRET=${ASTRA_PHASE5_HS256_SECRET:-phase5-dev-secret}
export ASTRAD_AUTH_ENABLED=true
export ASTRAD_AUTH_JWT_HS256_SECRET="${SECRET}"
export ASTRAD_TENANT_VIRTUALIZATION_ENABLED=true
export ASTRAD_AUTH_TENANT_CLAIM=${ASTRAD_AUTH_TENANT_CLAIM:-tenant_id}

SUMMARY_JSON="${RESULTS_DIR}/phase5-scenario-auth-tenant-summary.json"

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

gen_token() {
  local tenant=$1
  local sub=$2
  python3 - <<'PY' "${SECRET}" "${tenant}" "${sub}"
import base64
import hashlib
import hmac
import json
import sys
import time

secret = sys.argv[1].encode("utf-8")
tenant = sys.argv[2]
subject = sys.argv[3]
header = {"alg": "HS256", "typ": "JWT"}
claims = {
    "sub": subject,
    "tenant_id": tenant,
    "iat": int(time.time()),
    "exp": int(time.time()) + 3600,
}

def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

head = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
body = b64url(json.dumps(claims, separators=(",", ":")).encode("utf-8"))
sig = hmac.new(secret, f"{head}.{body}".encode("utf-8"), hashlib.sha256).digest()
print(f"{head}.{body}.{b64url(sig)}")
PY
}

token_a=$(gen_token "tenant-a" "phase5-a")
token_b=$(gen_token "tenant-b" "phase5-b")

find_writable_endpoint_with_token() {
  local token=$1
  local key=${2:-/phase5/auth/leader_probe}
  local value=${3:-"$(date +%s%N)"}
  local loops=${4:-60}
  local eps=(127.0.0.1:2379 127.0.0.1:32391 127.0.0.1:32392)

  local i ep
  for ((i = 1; i <= loops; i++)); do
    for ep in "${eps[@]}"; do
      if (cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- \
        --endpoint "http://${ep}" \
        --bearer-token "${token}" \
        put "${key}" "${value}" >/dev/null 2>&1); then
        echo "${ep}"
        return 0
      fi
    done
    sleep 1
  done
  return 1
}

leader_ep=$(find_writable_endpoint_with_token "${token_a}" "/phase5/auth/leader_probe" "$(date +%s%N)" 90) || {
  echo "failed to find writable Astra endpoint with token auth" >&2
  compose logs --tail 120
  exit 2
}

(cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- \
  --endpoint "http://${leader_ep}" \
  --bearer-token "${token_a}" \
  put /phase5/auth/shared "value-a")

(cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- \
  --endpoint "http://${leader_ep}" \
  --bearer-token "${token_b}" \
  put /phase5/auth/shared "value-b")

value_a=$( (cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- \
  --endpoint "http://${leader_ep}" \
  --bearer-token "${token_a}" \
  get /phase5/auth/shared) | awk -F= '/^\/phase5\/auth\/shared=/{print $2}' | tail -n1 )
value_b=$( (cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- \
  --endpoint "http://${leader_ep}" \
  --bearer-token "${token_b}" \
  get /phase5/auth/shared) | awk -F= '/^\/phase5\/auth\/shared=/{print $2}' | tail -n1 )

unauth_status=0
if (cd "${REPO_DIR}" && cargo run --quiet --bin astractl -- --endpoint "http://${leader_ep}" get /phase5/auth/shared >/dev/null 2>&1); then
  unauth_status=0
else
  unauth_status=1
fi

python3 - <<'PY' "${SUMMARY_JSON}" "${value_a}" "${value_b}" "${unauth_status}"
import json
import pathlib
import sys

summary_path = pathlib.Path(sys.argv[1])
value_a = sys.argv[2]
value_b = sys.argv[3]
unauth_failed = sys.argv[4] == "1"
summary = {
    "tenant_a_value": value_a,
    "tenant_b_value": value_b,
    "unauthenticated_request_rejected": unauth_failed,
    "pass": value_a == "value-a" and value_b == "value-b" and unauth_failed,
}
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "phase5 auth tenant summary: ${SUMMARY_JSON}"
