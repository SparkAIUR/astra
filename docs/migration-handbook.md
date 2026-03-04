---
title: Migration Handbook (Legacy Entry)
summary: Legacy migration index retained for compatibility; canonical migration guides live under /guides.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-forge/src/main.rs
  - refs/scripts/validation/phase11-scenario-fleet-cutover.sh
related_artifacts:
  - docs/guides/migration-omni.md
  - docs/guides/migration-k3s.md
  - docs/guides/migration-etcd-generic.md
---

# Migration Handbook

Canonical migration guides:

- [Migrate Omni](./guides/migration-omni)
- [Migrate K3s](./guides/migration-k3s)
- [Migrate Generic etcd](./guides/migration-etcd-generic)
- [Disaster Recovery](./guides/disaster-recovery)

This guide covers multi-tenant migration from siloed embedded control-plane stores into a shared Astra backend.

## 1. Extract Legacy Snapshots

Export each legacy silo snapshot:

```bash
etcdctl --endpoints=127.0.0.1:2379 snapshot save omni-us-east.snap
```

## 2. Converge with `astra-forge`

Use one command to transform, package, upload, and bulk-load multiple sources:

```bash
astra-forge converge \
  --source omni-us-east.snap --tenant-id tenant-us-east \
  --source omni-eu-west.snap --tenant-id tenant-eu-west \
  --source omni-apac.snap --tenant-id tenant-apac \
  --apply-wasm transformations/identity.wat \
  --dest s3://astra-tier/omni-global/ \
  --endpoint http://minio.sidero.local:9000 \
  --astra-endpoint http://127.0.0.1:2379
```

What the command does:

- Reads each source (jsonl, endpoint, or `.db` snapshot).
- Applies optional Wasm transformation.
- Injects tenant-scoped key prefixes.
- Builds chunked SST bundles and uploads to object storage.
- Submits bulk-load jobs to live Astra with checksum verification.

## 3. Validate Data Integrity

Check expected keys under each virtual keyspace prefix:

```bash
etcdctl --endpoints=127.0.0.1:2379 get /__tenant/tenant-us-east/ --prefix --keys-only
```

## 4. Cut Over Stateful Components

Restart consumers in stateless mode pointing to the disaggregated endpoint.
Use per-tenant identity routing so overlapping key names remain isolated.
