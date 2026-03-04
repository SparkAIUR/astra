---
title: Control-Plane Compatibility
summary: Scope and boundaries of Astra's etcd compatibility for control-plane workloads.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-proto/proto/rpc.proto
- crates/astrad/src/main.rs
related_artifacts: []
---

# Control-Plane Compatibility

Astra exposes etcd-compatible gRPC surfaces for control-plane critical operations, including:

- `KV` service: `Range`, `Put`, `DeleteRange`, `Txn`, `Compact`.
- `Watch` service: streaming watch create/cancel/progress.
- `Lease` service: grant, revoke, keepalive, TTL, list.

## Compatibility Model

- Goal: production-safe compatibility for Kubernetes/Omni style control-plane usage.
- Non-goal: strict byte-for-byte parity for every historical etcd edge behavior.
- Contract: prioritize correctness, consistency, and liveness under constrained I/O.

## Operational Expectation

When migrating clients from etcd to Astra:

- Use validation harnesses to confirm workload-specific parity.
- Treat behavior outside validated scenario scope as compatibility testing candidates.
