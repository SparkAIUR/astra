#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

FRICTION_KEYS=${PHASE6_FRICTION_KEYS:-20000}
JWT_SAMPLES=${PHASE6_JWT_SAMPLES:-200}
SUMMARY_JSON="${RUN_DIR}/phase6-friction-summary.json"
WORK_DIR="${RUN_DIR}/friction"

mkdir -p "${WORK_DIR}"
phase6_require_tools docker etcdctl jq python3 cargo

measure_ms() {
  local start_ns end_ns
  start_ns=$(date +%s%N)
  "$@" >/dev/null
  end_ns=$(date +%s%N)
  echo $(( (end_ns - start_ns) / 1000000 ))
}

seed_keys() {
  local endpoint=${1:?endpoint required}
  local prefix=${2:?prefix required}
  local count=${3:?count required}
  seq 1 "${count}" | xargs -I{} -P 32 sh -c \
    'etcdctl --dial-timeout=1s --command-timeout=4s --endpoints="'"${endpoint}"'" put "'"${prefix}"'/k-{}" "v-{}" >/dev/null'
}

scan_case() {
  local backend=${1:?backend required}
  local prefix="/phase6/friction/${backend}"
  local out_json="${WORK_DIR}/scan-${backend}.json"

  phase6_stop_backend all || true
  phase6_start_backend "${backend}"
  local ep="${PHASE6_BACKEND_ENDPOINT}"

  seed_keys "${ep}" "${prefix}" "${FRICTION_KEYS}"
  local hot_list_ms
  hot_list_ms=$(measure_ms etcdctl --dial-timeout=2s --command-timeout=20s --endpoints="${ep}" get "${prefix}/" --prefix --keys-only)
  local rev
  rev=$(etcdctl --dial-timeout=2s --command-timeout=5s --endpoints="${ep}" get "${prefix}/k-1" -w json | jq -r '.header.revision')
  etcdctl --dial-timeout=2s --command-timeout=5s --endpoints="${ep}" compact "${rev}" >/dev/null 2>&1 || true

  if [ "${backend}" = "astra" ]; then
    phase6_stop_backend astra
    phase6_start_backend astra
    ep="${PHASE6_BACKEND_ENDPOINT}"
  else
    phase6_stop_backend etcd
    phase6_start_backend etcd
    ep="${PHASE6_BACKEND_ENDPOINT}"
  fi

  local cold_list_ms
  cold_list_ms=$(measure_ms etcdctl --dial-timeout=2s --command-timeout=30s --endpoints="${ep}" get "${prefix}/" --prefix --keys-only)

  python3 - <<'PY' "${backend}" "${FRICTION_KEYS}" "${hot_list_ms}" "${cold_list_ms}" "${out_json}"
import json
import pathlib
import sys

backend = sys.argv[1]
keys = int(sys.argv[2])
hot_ms = int(sys.argv[3])
cold_ms = int(sys.argv[4])
out_path = pathlib.Path(sys.argv[5])

obj = {
    "backend": backend,
    "keys": keys,
    "hot_list_ms": hot_ms,
    "cold_list_ms": cold_ms,
    "cold_minus_hot_ms": cold_ms - hot_ms,
}
out_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
print(json.dumps(obj, indent=2))
PY

  phase6_stop_backend all || true
}

build_jwt() {
  local secret=${1:?secret required}
  local tenant=${2:?tenant required}
  python3 - <<'PY' "${secret}" "${tenant}"
import base64
import hashlib
import hmac
import json
import time
import sys

secret = sys.argv[1].encode()
tenant = sys.argv[2]
header = {"alg": "HS256", "typ": "JWT"}
payload = {
    "sub": "phase6-friction",
    "tenant_id": tenant,
    "iat": int(time.time()),
    "exp": int(time.time()) + 3600,
}

def b64(data):
    return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).rstrip(b"=")

head = b64(header)
body = b64(payload)
signing_input = head + b"." + body
sig = base64.urlsafe_b64encode(hmac.new(secret, signing_input, hashlib.sha256).digest()).rstrip(b"=")
print((signing_input + b"." + sig).decode())
PY
}

jwt_overhead_case() {
  local secret="phase6-jwt-secret"
  local tenant="phase6-tenant"
  local noauth_ms auth_ms
  local out_json="${WORK_DIR}/jwt-overhead.json"
  local ctl="${REPO_DIR}/target/debug/astractl"

  (cd "${REPO_DIR}" && cargo build -q -p astractl)

  export ASTRAD_AUTH_ENABLED=false
  export ASTRAD_TENANT_VIRTUALIZATION_ENABLED=false
  phase6_stop_backend all || true
  phase6_start_backend astra
  local ep="${PHASE6_BACKEND_ENDPOINT}"
  noauth_ms=$(measure_ms bash -c '
    for i in $(seq 1 '"${JWT_SAMPLES}"'); do
      "'"${ctl}"'" --endpoint "http://'"${ep}"'" put "/phase6/jwt/noauth/k-${i}" "v-${i}" >/dev/null
    done
  ')
  phase6_stop_backend all || true

  export ASTRAD_AUTH_ENABLED=true
  export ASTRAD_AUTH_JWT_HS256_SECRET="${secret}"
  export ASTRAD_TENANT_VIRTUALIZATION_ENABLED=true
  phase6_start_backend astra
  ep="${PHASE6_BACKEND_ENDPOINT}"
  local token
  token=$(build_jwt "${secret}" "${tenant}")
  auth_ms=$(measure_ms bash -c '
    for i in $(seq 1 '"${JWT_SAMPLES}"'); do
      "'"${ctl}"'" --endpoint "http://'"${ep}"'" --bearer-token "'"${token}"'" put "/phase6/jwt/auth/k-${i}" "v-${i}" >/dev/null
    done
  ')
  phase6_stop_backend all || true

  unset ASTRAD_AUTH_ENABLED ASTRAD_AUTH_JWT_HS256_SECRET ASTRAD_TENANT_VIRTUALIZATION_ENABLED

  python3 - <<'PY' "${JWT_SAMPLES}" "${noauth_ms}" "${auth_ms}" "${out_json}"
import json
import pathlib
import sys

samples = int(sys.argv[1])
noauth_ms = int(sys.argv[2])
auth_ms = int(sys.argv[3])
out_path = pathlib.Path(sys.argv[4])

obj = {
    "samples": samples,
    "noauth_total_ms": noauth_ms,
    "auth_total_ms": auth_ms,
    "per_request_noauth_ms": noauth_ms / samples,
    "per_request_auth_ms": auth_ms / samples,
    "jwt_overhead_per_request_ms": (auth_ms - noauth_ms) / samples,
}
out_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
print(json.dumps(obj, indent=2))
PY
}

s3_cold_case() {
  local out_json="${WORK_DIR}/s3-cold-read.json"
  export ASTRAD_TIERING_INTERVAL_SECS=5
  phase6_stop_backend all || true
  phase6_start_backend astra
  local ep="${PHASE6_BACKEND_ENDPOINT}"
  seed_keys "${ep}" "/phase6/friction/s3" 5000
  sleep 12
  local hot_get_ms
  hot_get_ms=$(measure_ms etcdctl --dial-timeout=2s --command-timeout=10s --endpoints="${ep}" get "/phase6/friction/s3/k-42" --print-value-only)
  phase6_stop_backend astra
  phase6_start_backend astra
  ep="${PHASE6_BACKEND_ENDPOINT}"
  local cold_get_ms
  cold_get_ms=$(measure_ms etcdctl --dial-timeout=2s --command-timeout=20s --endpoints="${ep}" get "/phase6/friction/s3/k-42" --print-value-only)
  phase6_stop_backend all || true
  unset ASTRAD_TIERING_INTERVAL_SECS

  python3 - <<'PY' "${hot_get_ms}" "${cold_get_ms}" "${out_json}"
import json
import pathlib
import sys

hot_ms = int(sys.argv[1])
cold_ms = int(sys.argv[2])
out_path = pathlib.Path(sys.argv[3])
obj = {
    "hot_get_ms": hot_ms,
    "cold_get_ms": cold_ms,
    "cold_minus_hot_ms": cold_ms - hot_ms,
}
out_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
print(json.dumps(obj, indent=2))
PY
}

scan_case astra
scan_case etcd
jwt_overhead_case
s3_cold_case

python3 - <<'PY' "${WORK_DIR}/scan-astra.json" "${WORK_DIR}/scan-etcd.json" "${WORK_DIR}/jwt-overhead.json" "${WORK_DIR}/s3-cold-read.json" "${SUMMARY_JSON}"
import json
import pathlib
import sys

paths = [pathlib.Path(p) for p in sys.argv[1:5]]
out_path = pathlib.Path(sys.argv[5])
data = []
for p in paths:
    try:
        data.append(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        data.append({})

summary = {
    "range_scan": {
        "astra": data[0],
        "etcd": data[1],
    },
    "jwt_overhead": data[2],
    "s3_cold_read": data[3],
    "pass": bool(data[0] and data[1] and data[2] and data[3]),
}
out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

phase6_log "friction summary: ${SUMMARY_JSON}"
