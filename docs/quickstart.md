---
title: Quickstart
summary: Run a local 3-node Astra cluster with MinIO, Prometheus, and Grafana in under a minute.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - quickstart.sh
  - docker-compose.yaml
related_artifacts:
  - refs/sandbox/prometheus/prometheus.yml
  - refs/sandbox/grafana/dashboards/astra-overview.json
---

# Quickstart

## Prerequisites

- Docker Engine with Docker Compose plugin.
- Ports available: `2379`, `32391`, `32392`, `9000`, `9001`, `9090`, `3000`.

## Start the Sandbox

```bash
./quickstart.sh
```

The script brings up:

- 3-node Astra cluster.
- MinIO object store.
- traffic generator.
- Prometheus and Grafana with pre-provisioned dashboard.

## Validate Basic KV Operations

```bash
docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --endpoints=http://127.0.0.1:2379 put /quickstart/hello astra

docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --endpoints=http://127.0.0.1:2379 get /quickstart/hello
```

## Observe Runtime Health

- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000` (default `admin/admin`)

Focus first on queue-wait, quorum-ack, write-latency, and profile metrics in the bundled dashboard.

## Stop and Reset

```bash
docker compose -f docker-compose.yaml down -v
```
