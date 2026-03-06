---
title: Generic etcd to Astra Migration
summary: Generic migration checklist for systems currently using etcd APIs.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-proto/proto/rpc.proto
  - crates/astra-forge/src/main.rs
related_artifacts:
  - refs/scripts/validation/phase2-scenario-b.sh
---

# Generic etcd to Astra Migration

## Pre-Migration

- Inventory client API usage (KV, Watch, Lease, Txn).
- Identify revision-sensitive consumers and compaction assumptions.
- Establish baseline latency and error budgets on current etcd deployment.

## Migration Execution

- Use `astra-forge compile` or `astra-forge converge` depending on source format and tenancy strategy.
- Bulk-load into Astra with checksum verification.
- Shift clients using staged endpoint rollout.

## Post-Migration Validation

- Key/value parity checks.
- Watch stream correctness and cancellation behavior.
- Lease grant/revoke/keepalive correctness.
- Error-budget adherence under representative load.
