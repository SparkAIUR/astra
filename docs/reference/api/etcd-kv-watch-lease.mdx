---
title: etcd KV, Watch, and Lease API
summary: etcd-compatible gRPC service surfaces exposed by Astra.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-proto/proto/rpc.proto
related_artifacts:
  - docs/reference/api/internal-raft.md
---

# etcd KV, Watch, and Lease API

Astra implements the following etcd-compatible services:

- `KV`
- `Watch`
- `Lease`

For canonical RPC request/response fields, use the generated proto contract in `crates/astra-proto/proto/rpc.proto`.

## RPC Surface

### KV Service

| RPC | Purpose |
| --- | --- |
| `Range` | Point read and prefix LIST reads. |
| `Put` | Key write with optional lease association. |
| `DeleteRange` | Point delete or range delete. |
| `Txn` | Compare-and-swap and conditional operations. |
| `Compact` | Logical compaction signal. |

### Watch Service

| RPC | Purpose |
| --- | --- |
| `Watch(stream WatchRequest)` | Bidirectional stream for create/cancel/progress watch control and event delivery. |

### Lease Service

| RPC | Purpose |
| --- | --- |
| `LeaseGrant` | Create lease ID/TTL binding. |
| `LeaseRevoke` | Revoke lease and detach keys. |
| `LeaseKeepAlive(stream LeaseKeepAliveRequest)` | Lease heartbeat stream. |
| `LeaseTimeToLive` | Inspect TTL state and keys. |
| `LeaseLeases` | List current leases. |

## Compatibility Notes

- Focus is control-plane compatibility for validated scenarios.
- Unsupported historical edge semantics should be validated case-by-case before migration.
