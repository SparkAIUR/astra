---
title: Docker Compose Reference
summary: Service and environment model for repository compose deployment.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - docker-compose.yml
related_artifacts:
  - quickstart.sh
---

# Docker Compose Reference

## Core Services

- `astra-node1`, `astra-node2`, `astra-node3`
- `minio`, `minio-init`
- `traffic-gen`
- `prometheus`, `grafana`

## Primary Environment Surface

`x-astra-env` defines the canonical local runtime defaults including:

- tiering config,
- write batching controls,
- QoS controls,
- profile and telemetry controls,
- auth and tenanting controls.

For parameter-by-parameter env reference, use [Environment Variables](./env-vars).
