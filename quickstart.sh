#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMPOSE_FILE=${COMPOSE_FILE:-${ROOT_DIR}/docker-compose.yaml}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose plugin is required" >&2
  exit 1
fi

echo "[quickstart] bringing up Astra sandbox stack"
docker compose -f "${COMPOSE_FILE}" up -d \
  minio minio-init astra-node1 astra-node2 astra-node3 traffic-gen prometheus grafana

echo "[quickstart] waiting for Astra endpoint 127.0.0.1:2379"
for _ in $(seq 1 60); do
  if timeout 1 bash -c '</dev/tcp/127.0.0.1/2379' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[quickstart] running smoke put/get through etcd API"
docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --dial-timeout=2s --command-timeout=5s --endpoints=http://127.0.0.1:2379 \
  put /quickstart/hello astra >/dev/null

docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --dial-timeout=2s --command-timeout=5s --endpoints=http://127.0.0.1:2379 \
  get /quickstart/hello

cat <<MSG

Astra sandbox is ready.
- etcd endpoint:    http://127.0.0.1:2379
- MinIO API:        http://127.0.0.1:9000
- MinIO Console:    http://127.0.0.1:9001
- Prometheus:       http://127.0.0.1:9090
- Grafana:          http://127.0.0.1:3000 (admin/admin by default)

To stop:
  docker compose -f ${COMPOSE_FILE} down -v
MSG
