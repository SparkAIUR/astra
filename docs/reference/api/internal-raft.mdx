---
title: Internal Raft API
summary: Internal Raft transport RPC contract used between Astra peers.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-proto/proto/raft.proto
related_artifacts:
  - crates/astra-core/src/raft.rs
---

# Internal Raft API

Service: `astraraftpb.InternalRaft`

Methods:

| RPC | Purpose |
| --- | --- |
| `AppendEntries` | Replication and heartbeat transport between peers. |
| `Vote` | Election request/response transport. |
| `InstallSnapshot` | Snapshot transfer transport. |

Each RPC uses `RaftBytes` with one field:

- `payload: bytes` (opaque openraft transport envelope)

## Operational Constraints

- This API is peer-only and must not be exposed as a public client surface.
- Payload compatibility is tied to Astra/openraft runtime versions in the same cluster.
- Authentication/authorization for peer plane is expected to be handled by network boundary controls.

This surface is internal and not intended for external client integration.
