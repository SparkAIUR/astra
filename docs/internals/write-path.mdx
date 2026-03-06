---
title: Write Path
summary: Request flow from KV put/txn ingress through batching, raft commit, and apply.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astrad/src/main.rs
  - crates/astra-core/src/raft.rs
related_artifacts:
  - docs/internals/raft-timeline.md
---

# Write Path

## Stages

1. gRPC ingress (`KV.Put` or `Txn`) in `astrad`.
2. Tenant-aware key encoding (optional virtualization).
3. Lane selection (Tier-0 vs normal).
4. Batch packing and adaptive linger/request sizing.
5. Raft client write and quorum commit.
6. State-machine apply into `KvStore`.
7. Watch event publication.

## Key Controls

- `ASTRAD_PUT_BATCH_*`
- `ASTRAD_PUT_TARGET_*`
- `ASTRAD_QOS_TIER0_*`

## Failure Semantics

- Backpressure before OOM.
- Resource exhaustion surfaced to clients when memory/queue limits are exceeded.
