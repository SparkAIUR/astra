---
title: Deploy Astra + K3s (Single Node and Cluster)
summary: Run K3s with Astra as the external datastore on one host or across a production cluster.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - refs/scripts/validation/phase6-common.sh
  - docker-compose.yml
related_artifacts:
  - docs/guides/migration-k3s.md
  - docs/operations/deploy-production.md
---

# Deploy Astra + K3s (Single Node and Cluster)

This guide shows two deployment patterns:

- Single-node lab: Astra and K3s on the same host.
- Cluster deployment: multi-node Astra backend with multi-node K3s control plane/workers.

## Prerequisites

- Linux host(s) with Docker and `k3s`, `kubectl`, `etcdctl` available.
- Network reachability from all K3s server nodes to all Astra client endpoints.
- Astra image published as stable semver tags (`vX.Y.Z`) in:
  - `docker.io/halceon/astra:{tag}`
  - `docker.io/nudevco/astra:{tag}`

## Topology A: Single-Node Lab

### 1. Start Astra locally

From repo root:

```bash
docker compose up -d minio minio-init astra-node1 astra-node2 astra-node3
```

Check datastore health:

```bash
etcdctl --endpoints=http://127.0.0.1:2379 endpoint status -w table
```

### 2. Install/start K3s with Astra datastore endpoint

Use Astra node client endpoints exposed by compose:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server \
    --write-kubeconfig-mode 644 \
    --disable traefik \
    --disable servicelb \
    --datastore-endpoint 'http://127.0.0.1:2379,http://127.0.0.1:32391,http://127.0.0.1:32392'" \
  sh -
```

### 3. Validate cluster + datastore path

```bash
kubectl get nodes
kubectl create ns astra-k3s-smoke
kubectl -n astra-k3s-smoke create configmap smoke --from-literal=ok=true
kubectl -n astra-k3s-smoke get configmap smoke -o yaml
```

Optional direct datastore probe:

```bash
etcdctl --endpoints=http://127.0.0.1:2379 get /registry/configmaps/astra-k3s-smoke/smoke --prefix --keys-only
```

## Topology B: Cluster Deployment

### 1. Deploy Astra backend (recommended: 3 nodes)

Deploy 3 Astra nodes with stable raft/client addresses and persistent disks. Every K3s server must be able to reach all Astra client endpoints.

Example endpoint list used by K3s:

```text
http://astra1.example.net:2379,http://astra2.example.net:2379,http://astra3.example.net:2379
```

### 2. Bootstrap first K3s server node

```bash
export K3S_TOKEN="<shared-cluster-token>"
export ASTRA_DATASTORE="http://astra1.example.net:2379,http://astra2.example.net:2379,http://astra3.example.net:2379"

curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server \
    --token ${K3S_TOKEN} \
    --datastore-endpoint '${ASTRA_DATASTORE}' \
    --write-kubeconfig-mode 644" \
  sh -
```

### 3. Join additional K3s server nodes

Run on each additional control-plane node:

```bash
export K3S_TOKEN="<shared-cluster-token>"
export K3S_URL="https://<first-server-ip>:6443"
export ASTRA_DATASTORE="http://astra1.example.net:2379,http://astra2.example.net:2379,http://astra3.example.net:2379"

curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server \
    --server ${K3S_URL} \
    --token ${K3S_TOKEN} \
    --datastore-endpoint '${ASTRA_DATASTORE}'" \
  sh -
```

### 4. Join worker nodes

```bash
export K3S_TOKEN="<shared-cluster-token>"
export K3S_URL="https://<first-server-ip>:6443"

curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="agent --server ${K3S_URL} --token ${K3S_TOKEN}" \
  sh -
```

### 5. Validate HA behavior

- Confirm all server/agent nodes are `Ready`.
- Run workload smoke tests (configmaps, deployments, service accounts).
- Restart one Astra node and verify K3s API remains available.

## Production Notes

- Prefer dedicated Astra nodes; avoid collocating heavy workloads with Astra in production.
- Use TLS and auth controls for Astra client access paths.
- Keep Astra profile/tuning explicit (`ASTRAD_PROFILE`, queue/quorum budgets) and track with Prometheus/Grafana.
- Revalidate with the phase harness when changing datastore tuning:
  - `refs/scripts/validation/phase6-k3s-benchmark.sh`

## Rollback

- Keep last-known-good datastore endpoint config and K3s token material.
- Before large rollout waves, snapshot Astra and validate restore path.
