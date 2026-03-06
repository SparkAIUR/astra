#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_FILE=${ASTRA_SINGLE_NODE_ENV_FILE:-/etc/astra/k3s-single-node.env}

if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
fi

usage() {
  cat <<'USAGE'
Usage: astra-single-node-stack.sh <up|down|ps|config>
USAGE
}

ACTION=${1:-}
[ -n "${ACTION}" ] || {
  usage
  exit 1
}

REPO_DIR=${ASTRA_REPO_DIR:-/root/astra-lab/repo}
ASTRA_IMAGE=${ASTRA_IMAGE:-}
ASTRA_CONTAINER_MEMORY_LIMIT=${ASTRA_CONTAINER_MEMORY_LIMIT:-2048M}
ASTRA_NODE1_DATA=${ASTRA_NODE1_DATA:-/var/lib/astra/node1}
ASTRA_NODE2_DATA=${ASTRA_NODE2_DATA:-/var/lib/astra/node2}
ASTRA_NODE3_DATA=${ASTRA_NODE3_DATA:-/var/lib/astra/node3}
ASTRA_BACKUP_TARGET=${ASTRA_BACKUP_TARGET:-disabled}
ASTRA_PROD_COMPOSE_FILE=${ASTRA_PROD_COMPOSE_FILE:-${REPO_DIR}/refs/scripts/deploy/docker-compose.k3s-single-node.production.yml}
ASTRA_OBJECT_STORE_COMPOSE_FILE=${ASTRA_OBJECT_STORE_COMPOSE_FILE:-${REPO_DIR}/refs/scripts/deploy/docker-compose.k3s-single-node.object-store.yml}

[ -n "${ASTRA_IMAGE}" ] || {
  printf 'ASTRA_IMAGE must be set via %s or environment\n' "${ENV_FILE}" >&2
  exit 1
}

compose() {
  local files=(
    -f "${ASTRA_PROD_COMPOSE_FILE}"
  )
  if [ "${ASTRA_BACKUP_TARGET}" != "disabled" ] && [ "${ASTRA_BACKUP_TARGET}" != "none" ]; then
    files+=(-f "${ASTRA_OBJECT_STORE_COMPOSE_FILE}")
  fi

  (
    cd "${REPO_DIR}" &&
    export ASTRA_IMAGE \
      ASTRA_CONTAINER_MEMORY_LIMIT \
      ASTRA_NODE1_DATA \
      ASTRA_NODE2_DATA \
      ASTRA_NODE3_DATA \
      ASTRA_BACKUP_TARGET \
      ASTRAD_S3_ENDPOINT \
      ASTRAD_S3_BUCKET \
      ASTRAD_S3_REGION \
      ASTRAD_S3_PREFIX \
      AWS_ACCESS_KEY_ID \
      AWS_SECRET_ACCESS_KEY
    docker compose "${files[@]}" "$@"
  )
}

case "${ACTION}" in
  up)
    compose up -d --remove-orphans --force-recreate astra-node1 astra-node2 astra-node3
    ;;
  down)
    compose down
    ;;
  ps)
    compose ps
    ;;
  config)
    compose config
    ;;
  *)
    usage
    exit 1
    ;;
esac
