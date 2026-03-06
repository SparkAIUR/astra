#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: fetch-keycloak-token.sh --issuer <issuer-url> --client-id <id> --client-secret <secret> [options]

Options:
  --issuer <value>         OIDC issuer URL, for example https://keycloak.example.net/realms/omni
  --token-url <value>      Explicit token endpoint override
  --client-id <value>      Keycloak client ID
  --client-secret <value>  Keycloak client secret
  --scope <value>          Optional space-delimited scope list
  --claim <name>           Optional claim to print to stderr after decoding the token
  --help                   Show help
USAGE
}

ISSUER=""
TOKEN_URL=""
CLIENT_ID=""
CLIENT_SECRET=""
SCOPE=""
CLAIM_TO_PRINT=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --issuer) ISSUER=${2:?missing value for --issuer}; shift 2 ;;
    --token-url) TOKEN_URL=${2:?missing value for --token-url}; shift 2 ;;
    --client-id) CLIENT_ID=${2:?missing value for --client-id}; shift 2 ;;
    --client-secret) CLIENT_SECRET=${2:?missing value for --client-secret}; shift 2 ;;
    --scope) SCOPE=${2:?missing value for --scope}; shift 2 ;;
    --claim) CLAIM_TO_PRINT=${2:?missing value for --claim}; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

[ -n "${CLIENT_ID}" ] || { echo "--client-id is required" >&2; exit 1; }
[ -n "${CLIENT_SECRET}" ] || { echo "--client-secret is required" >&2; exit 1; }

if [ -z "${TOKEN_URL}" ]; then
  [ -n "${ISSUER}" ] || { echo "--issuer or --token-url is required" >&2; exit 1; }
  TOKEN_URL=$(python3 - <<'PY' "${ISSUER}"
import json
import sys
import urllib.request

issuer = sys.argv[1].rstrip("/")
req = urllib.request.Request(
    f"{issuer}/.well-known/openid-configuration",
    headers={"User-Agent": "Mozilla/5.0"},
)
with urllib.request.urlopen(req, timeout=10) as resp:
    data = json.load(resp)
print(data["token_endpoint"])
PY
)
fi

response=$(curl --fail --silent --show-error --location \
  --header 'content-type: application/x-www-form-urlencoded' \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id="${CLIENT_ID}" \
  --data-urlencode client_secret="${CLIENT_SECRET}" \
  ${SCOPE:+--data-urlencode scope="${SCOPE}"} \
  "${TOKEN_URL}")

token=$(printf '%s' "${response}" | jq -r '.access_token')
[ -n "${token}" ] && [ "${token}" != "null" ] || {
  echo "token endpoint did not return access_token" >&2
  exit 1
}

if [ -n "${CLAIM_TO_PRINT}" ]; then
  python3 - <<'PY' "${token}" "${CLAIM_TO_PRINT}" >&2
import base64
import json
import sys

token, claim = sys.argv[1:]
parts = token.split(".")
if len(parts) < 2:
    raise SystemExit("invalid JWT")
payload = parts[1] + "=" * (-len(parts[1]) % 4)
data = json.loads(base64.urlsafe_b64decode(payload.encode("utf-8")))
print(f"{claim}={data.get(claim)!r}")
PY
fi

printf '%s\n' "${token}"
