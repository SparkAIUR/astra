#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=${ASTRA_SINGLE_NODE_ENV_FILE:-/etc/astra/k3s-single-node.env}
if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
fi

require() {
  local name=${1:?name required}
  local value=${2-}
  if [ -z "${value}" ]; then
    printf 'missing required setting: %s\n' "${name}" >&2
    exit 1
  fi
}

require ASTRA_BACKUP_TARGET "${ASTRA_BACKUP_TARGET:-}"
[ "${ASTRA_BACKUP_TARGET}" = "external-s3" ] || {
  printf 'ASTRA_BACKUP_TARGET=%s does not require archive job\n' "${ASTRA_BACKUP_TARGET}" >&2
  exit 0
}

require ASTRAD_S3_ENDPOINT "${ASTRAD_S3_ENDPOINT:-}"
require ASTRAD_S3_BUCKET "${ASTRAD_S3_BUCKET:-}"
require ASTRAD_S3_PREFIX "${ASTRAD_S3_PREFIX:-}"
require ASTRA_BACKUP_ARCHIVE_PREFIX "${ASTRA_BACKUP_ARCHIVE_PREFIX:-}"
require AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID:-}"
require AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY:-}"

command -v aws >/dev/null 2>&1 || {
  printf 'aws CLI is required\n' >&2
  exit 1
}
command -v jq >/dev/null 2>&1 || {
  printf 'jq is required\n' >&2
  exit 1
}

AWS_REGION=${ASTRAD_S3_REGION:-auto}
export AWS_DEFAULT_REGION="${AWS_REGION}"

RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DATE=${ASTRA_BACKUP_DATE_OVERRIDE:-$(date -u +%Y-%m-%d)}
LIVE_BASE="${ASTRAD_S3_PREFIX%/}/cluster-1"
ARCHIVE_BASE="${ASTRA_BACKUP_ARCHIVE_PREFIX%/}/${RUN_DATE}/cluster-1"
MANIFEST_KEY="${LIVE_BASE}/manifest.json"
STATUS_DIR=${ASTRA_RESULTS_DIR:-/root/astra-lab/results}/backups
STATUS_PATH="${STATUS_DIR}/archive-${RUN_TS}.json"

mkdir -p "${STATUS_DIR}"
tmpdir=$(mktemp -d)
trap 'rm -rf "${tmpdir}"' EXIT

aws_base=(aws --no-cli-pager --endpoint-url "${ASTRAD_S3_ENDPOINT}")

"${aws_base[@]}" s3api get-object \
  --bucket "${ASTRAD_S3_BUCKET}" \
  --key "${MANIFEST_KEY}" \
  "${tmpdir}/manifest.json" >/dev/null

revision=$(jq -r '.revision' "${tmpdir}/manifest.json")
compact_revision=$(jq -r '.compact_revision' "${tmpdir}/manifest.json")
chunk_count=$(jq -r '.chunks | length' "${tmpdir}/manifest.json")

jq -r '.chunks[].key' "${tmpdir}/manifest.json" | while IFS= read -r chunk_key; do
  [ -n "${chunk_key}" ] || continue
  "${aws_base[@]}" s3api copy-object \
    --bucket "${ASTRAD_S3_BUCKET}" \
    --copy-source "${ASTRAD_S3_BUCKET}/${LIVE_BASE}/${chunk_key}" \
    --key "${ARCHIVE_BASE}/${chunk_key}" >/dev/null
done

"${aws_base[@]}" s3 cp \
  "${tmpdir}/manifest.json" \
  "s3://${ASTRAD_S3_BUCKET}/${ARCHIVE_BASE}/manifest.json" >/dev/null

cat >"${tmpdir}/latest.json" <<EOF
{
  "ts": "${RUN_TS}",
  "date": "${RUN_DATE}",
  "bucket": "${ASTRAD_S3_BUCKET}",
  "live_prefix": "${LIVE_BASE}",
  "archive_prefix": "${ARCHIVE_BASE}",
  "manifest_key": "${ARCHIVE_BASE}/manifest.json",
  "revision": ${revision},
  "compact_revision": ${compact_revision},
  "chunk_count": ${chunk_count}
}
EOF

"${aws_base[@]}" s3 cp \
  "${tmpdir}/latest.json" \
  "s3://${ASTRAD_S3_BUCKET}/${ASTRA_BACKUP_ARCHIVE_PREFIX%/}/latest.json" >/dev/null

cp "${tmpdir}/latest.json" "${STATUS_PATH}"
printf '%s\n' "${STATUS_PATH}"
