---
title: Deploy Locally with Docker Compose
summary: Local Astra deployment layout and service behavior using the repository compose stack.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - docker-compose.yaml
  - quickstart.sh
related_artifacts:
  - refs/sandbox/prometheus/prometheus.yml
---

# Deploy Locally with Docker Compose

The repository compose stack provisions:

- `astra-node1`, `astra-node2`, `astra-node3`
- `minio`, `minio-init`
- `traffic-gen`
- `prometheus`, `grafana`

## Start

```bash
docker compose -f docker-compose.yaml up -d
```

## Confirm Node Endpoints

- node1: `127.0.0.1:2379`
- node2: `127.0.0.1:32391`
- node3: `127.0.0.1:32392`

## Stop

```bash
docker compose -f docker-compose.yaml down -v
```
