---
title: Migrate Sidero Omni to Astra
summary: End-to-end migration from isolated embedded Omni stores to a shared Astra
  backend.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-forge/src/main.rs
- refs/scripts/validation/phase11-scenario-forge-ux.sh
- refs/scripts/validation/phase11-scenario-fleet-cutover.sh
related_artifacts: []
---

# Migrate Sidero Omni to Astra

## Migration Pattern

1. Extract each legacy embedded snapshot.
2. Converge all snapshots with `astra-forge` into S3/MinIO-backed manifests.
3. Bulk-load manifests into live Astra.
4. Restart Omni in external-etcd mode against Astra endpoint.
5. Validate no-data-loss and tenant isolation.

## Converge Command

```bash
astra-forge converge \
  --source omni-1.snap --tenant-id tenant-01 \
  --source omni-2.snap --tenant-id tenant-02 \
  --source omni-3.snap --tenant-id tenant-03 \
  --source omni-4.snap --tenant-id tenant-04 \
  --source omni-5.snap --tenant-id tenant-05 \
  --apply-wasm transformations/identity.wat \
  --dest s3://astra-tier/omni-global/ \
  --endpoint http://minio.sidero.local:9000 \
  --astra-endpoint http://127.0.0.1:2379
```

## Validation Checklist

- Per-tenant key counts match expected pre-cutover counts.
- Per-tenant region/marker keys are preserved.
- Omni instances boot in external etcd mode (no embedded etcd startup).

## Reference Evidence

Phase 11 non-smoke validation demonstrates a 5-to-1 converged migration with parity and successful stateless Omni cutover.
