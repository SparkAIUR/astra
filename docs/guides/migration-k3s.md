---
title: Migrate K3s Control Planes to Astra
summary: Practical migration and validation flow for K3s control-plane datastores.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
- refs/scripts/validation/phase6-k3s-benchmark.sh
- refs/scripts/validation/phase9-run-all.sh
related_artifacts: []
---

# Migrate K3s Control Planes to Astra

## Recommended Approach

1. Stand up Astra cluster and validate basic etcd API operations.
2. Point a non-production K3s control-plane at Astra datastore endpoint.
3. Replay representative workload and observe API/lease/list behavior.
4. Compare against etcd baseline using the same harness profile.
5. Promote only after passing workload-specific parity and SLO gates.

For deployment commands and topology layouts, use [Deploy Astra + K3s](./deploy-k3s-with-astra).

## Validation Priorities

- Lease lane stability under churn.
- LIST latency parity under high object cardinality.
- Resource profile (iowait, disk IOPS) under sustained reconcile loops.

## Rollout Guidance

- Start with canary control-plane environments.
- Keep explicit rollback path to prior datastore until SLO confidence is established.
