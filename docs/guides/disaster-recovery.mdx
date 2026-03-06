---
title: Disaster Recovery Guide
summary: Recover Astra clusters from object-tier manifests after a crater event.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/tiering.rs
- refs/scripts/validation/phase2-scenario-e.sh
related_artifacts: []
---

# Disaster Recovery Guide

## Objective

Restore read availability and consistency from S3/MinIO manifests and chunks with minimal time-to-first-read.

## Recovery Workflow

1. Provision replacement Astra nodes.
2. Configure object-tier settings (`ASTRAD_S3_ENDPOINT`, `ASTRAD_S3_BUCKET`, `ASTRAD_S3_PREFIX`).
3. Start nodes with clean local data directories.
4. Verify manifest/chunk retrieval and revision restoration.
5. Re-enable write traffic after quorum and key-parity checks.

## Verification Commands

```bash
etcdctl --endpoints=http://127.0.0.1:2379 endpoint status -w table
etcdctl --endpoints=http://127.0.0.1:2379 get /__tenant/ --prefix --keys-only
```

## Failure Modes to Watch

- Missing object chunks.
- Checksum mismatch.
- Partial node restore with divergent revisions.
