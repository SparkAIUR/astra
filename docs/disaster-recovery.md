---
title: Disaster Recovery (Legacy Entry)
summary: Legacy DR page retained for compatibility; canonical DR runbook is under /guides.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - docs/guides/disaster-recovery.md
related_artifacts:
  - docs/guides/disaster-recovery.md
---

# Disaster Recovery

Canonical DR runbook:

- [Guides: Disaster Recovery](./guides/disaster-recovery)

This runbook covers full-cluster recovery from object storage.

## Crater Event Objective

Recover service with preserved revision history and tenant keyspaces from S3/MinIO manifests.

## Steps

1. Provision replacement Astra nodes.
2. Point `ASTRAD_S3_ENDPOINT`, `ASTRAD_S3_BUCKET`, and `ASTRAD_S3_PREFIX` to the snapshot tier.
3. Start Astra nodes with empty local data directories.
4. Astra restores from `manifest.json` and referenced chunks at startup.
5. Verify revision and key counts before re-admitting writers.

## Verification Commands

```bash
etcdctl --endpoints=127.0.0.1:2379 endpoint status -w table
etcdctl --endpoints=127.0.0.1:2379 get /__tenant/ --prefix --keys-only
```

## Recovery SLOs

- Time-to-first-read should be bounded by manifest + hot chunk replay.
- No key loss across tenant prefixes.
- Write admission resumes only after quorum and consistency checks pass.
