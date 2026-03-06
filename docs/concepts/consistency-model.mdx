---
title: Consistency Model
summary: Astra's CP behavior, Raft-backed write semantics, and read consistency expectations.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/raft.rs
- crates/astra-core/src/store.rs
related_artifacts: []
---

# Consistency Model

Astra follows strict CP semantics:

- Writes commit through Raft quorum.
- Compaction is revision-aware.
- Partition behavior preserves consistency over availability for conflicting writes.

## Key Guarantees

- Monotonic revisions across committed operations.
- Lease and lock lanes protected by semantic QoS under pressure.
- No local-only write acknowledgment that bypasses quorum.

## Client Guidance

- Use revision-aware reads when your control-plane logic requires explicit ordering.
- Treat compaction errors as expected signals to refresh stale read windows.
