#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=phase6-common.sh
source "${SCRIPT_DIR}/phase6-common.sh"

: "${PHASE12_CLEANUP_AGGRESSIVE:=false}"
: "${PHASE12_CLEANUP_PRUNE_IMAGES:=false}"
: "${PHASE12_CLEAN_TARGET:=false}"
: "${PHASE12_CLEAN_RESULTS:=false}"
: "${PHASE12_CLEAN_ALL_RESULTS:=false}"

phase6_log "phase12 cleanup: stopping local validation stacks"
phase6_stop_k3s || true
phase6_stop_backend all || true

phase6_log "phase12 cleanup: removing stale validation containers"
docker ps -aq --filter 'name=phase' | xargs -r docker rm -f >/dev/null 2>&1 || true
docker ps -aq --filter 'name=astra-node' | xargs -r docker rm -f >/dev/null 2>&1 || true
docker ps -aq --filter 'name=minio' | xargs -r docker rm -f >/dev/null 2>&1 || true
docker ps -aq --filter 'name=etcd' | xargs -r docker rm -f >/dev/null 2>&1 || true

phase6_log "phase12 cleanup: removing stale validation volumes"
docker volume ls -q | grep -E '(phase|astra|k3s|minio|etcd)' | xargs -r docker volume rm -f >/dev/null 2>&1 || true

phase6_log "phase12 cleanup: removing stale validation networks"
docker network ls --format '{{.Name}}' | grep -E '^(phase|astra|k3s)' | xargs -r docker network rm >/dev/null 2>&1 || true

if [ "${PHASE12_CLEANUP_AGGRESSIVE}" = "true" ]; then
  phase6_log "phase12 cleanup: aggressive docker prune enabled"
  docker container prune -f >/dev/null 2>&1 || true
  docker volume prune -f >/dev/null 2>&1 || true
  docker builder prune -af >/dev/null 2>&1 || true
  if [ "${PHASE12_CLEANUP_PRUNE_IMAGES}" = "true" ]; then
    docker image prune -af >/dev/null 2>&1 || true
  fi
fi

if [ "${PHASE12_CLEAN_RESULTS}" = "true" ]; then
  phase6_log "phase12 cleanup: reclaiming old phase12 result dirs"
  find "${RESULTS_DIR}" -maxdepth 1 -type d -name 'phase12-*' -mtime +2 -print -exec rm -rf {} + || true
  if [ "${PHASE12_CLEAN_ALL_RESULTS}" = "true" ]; then
    phase6_log "phase12 cleanup: removing all prior validation result bundles"
    rm -rf "${RESULTS_DIR}"/* >/dev/null 2>&1 || true
  fi
fi

if [ "${PHASE12_CLEAN_TARGET}" = "true" ] && [ -d "${REPO_DIR}/target" ]; then
  phase6_log "phase12 cleanup: removing Cargo target cache at ${REPO_DIR}/target"
  rm -rf "${REPO_DIR}/target" >/dev/null 2>&1 || true
fi

phase6_log "phase12 cleanup complete"
