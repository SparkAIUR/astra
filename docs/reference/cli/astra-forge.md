---
title: astra-forge CLI
summary: Migration and bulk-load toolkit for compiling data bundles and converging multi-source tenants.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-forge/src/main.rs
related_artifacts:
  - docs/guides/migration-omni.md
  - docs/reference/api/astra-admin-bulkload.md
---

# astra-forge CLI

`astra-forge` compiles legacy data into Astra tier bundles and drives bulk-load workflows.

## Usage

```bash
astra-forge <command> [options]
```

## Commands

- `compile`: build chunked manifest artifacts from a source input.
- `bulk-load`: submit an existing manifest to Astra admin API.
- `converge`: end-to-end multi-source migration (`extract -> transform -> package -> upload -> bulk-load`).

## Input Formats

- `auto`: infer from source path and metadata.
- `jsonl`: newline-delimited key/value records.
- `endpoint`: live endpoint extraction.
- `db_snapshot`: etcd snapshot ingestion.

## Converge Example

```bash
astra-forge converge \
  --source omni-us.snap --tenant-id tenant-us \
  --source omni-eu.snap --tenant-id tenant-eu \
  --dest s3://astra-tier/omni/ \
  --endpoint http://127.0.0.1:9000 \
  --region us-east-1 \
  --astra-endpoint http://127.0.0.1:2379 \
  --allow-overwrite
```

## Operational Notes

- `--tenant-id` cardinality must match `--source` cardinality.
- `converge` supports optional Wasm transform stage via `--apply-wasm`.
- S3 destination and admin endpoint failures are surfaced per-source in converge summary output.
