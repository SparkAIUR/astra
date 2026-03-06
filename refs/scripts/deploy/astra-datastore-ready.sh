#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=${ASTRA_SINGLE_NODE_ENV_FILE:-/etc/astra/k3s-single-node.env}
if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
fi

ENDPOINTS=${ASTRA_DATASTORE_ENDPOINTS:-http://127.0.0.1:52379,http://127.0.0.1:52391,http://127.0.0.1:52392}
ATTEMPTS=${ASTRA_READY_ATTEMPTS:-60}
DELAY_SECS=${ASTRA_READY_DELAY_SECS:-2}
CHECK_KEY=${ASTRA_READY_CHECK_KEY:-/astra/system/ready}

check_once() {
  local endpoints=()
  local endpoint=""

  if etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${ENDPOINTS}" endpoint status -w json >/dev/null 2>&1; then
    return 0
  fi

  IFS=',' read -r -a endpoints <<< "${ENDPOINTS}"
  for endpoint in "${endpoints[@]-}"; do
    [ -n "${endpoint}" ] || continue
    if ! etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${endpoint}" \
      put "${CHECK_KEY}" "ready" >/dev/null 2>&1; then
      return 1
    fi
    if ! etcdctl --dial-timeout=1s --command-timeout=3s --endpoints="${endpoint}" \
      get "${CHECK_KEY}" >/dev/null 2>&1; then
      return 1
    fi
  done
}

for _ in $(seq 1 "${ATTEMPTS}"); do
  if check_once; then
    exit 0
  fi
  sleep "${DELAY_SECS}"
done

printf 'astra datastore did not become ready after %s attempts\n' "${ATTEMPTS}" >&2
exit 1
