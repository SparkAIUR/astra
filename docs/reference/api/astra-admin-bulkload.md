---
title: Astra Admin Bulkload API
summary: Admin RPCs for manifest-based bulk-load submission and job status queries.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-proto/proto/admin.proto
related_artifacts:
  - crates/astra-forge/src/main.rs
---

# Astra Admin Bulkload API

Service: `astraadminpb.AstraAdmin`

Methods:

- `BulkLoad(BulkLoadRequest) -> BulkLoadResponse`
- `GetBulkLoadJob(GetBulkLoadJobRequest) -> GetBulkLoadJobResponse`

## BulkLoadRequest Fields

| Field | Type | Notes |
| --- | --- | --- |
| `tenant_id` | `string` | Target tenant namespace for imported keys. |
| `manifest_source` | `string` | Manifest URI/path. Supports local and `s3://` sources. |
| `manifest_checksum` | `string` | Optional checksum gate for manifest integrity. |
| `dry_run` | `bool` | Validates manifest and planning without mutating state. |
| `allow_overwrite` | `bool` | Allows replacement semantics for existing keys. |

## BulkLoadResponse Fields

| Field | Type | Notes |
| --- | --- | --- |
| `job_id` | `string` | Server-assigned job identifier. |
| `accepted` | `bool` | Whether request was accepted for execution. |
| `message` | `string` | Status/error detail. |

## GetBulkLoadJobResponse Fields

| Field | Type | Notes |
| --- | --- | --- |
| `job_id` | `string` | Requested job identifier. |
| `status` | `string` | Job state (`pending`, `running`, `completed`, `failed`). |
| `message` | `string` | Additional result text. |
| `records_total` | `uint64` | Planned record count. |
| `records_applied` | `uint64` | Applied record count so far. |
| `started_at_unix_ms` | `uint64` | Start timestamp epoch ms. |
| `finished_at_unix_ms` | `uint64` | Completion timestamp epoch ms. |

## Expected Use

Primary client path is `astra-forge` converge/compile + bulk-load workflow.
