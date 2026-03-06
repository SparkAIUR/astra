---
title: Read Path
summary: Range/list read execution, isolation controls, and revision-aware behavior.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astrad/src/main.rs
  - crates/astra-core/src/store.rs
related_artifacts:
  - docs/reference/api/etcd-kv-watch-lease.md
---

# Read Path

Read operations resolve through `KvStore` range logic with:

- key range boundaries,
- optional prefix filtering,
- revision filters,
- prefetch and cache controls.

## Critical Behavior

- Compaction windows can reject stale revision reads.
- Read isolation settings help prevent write storms from collapsing read latencies.
