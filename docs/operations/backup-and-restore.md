---
title: Backup and Restore
summary: Backup strategy and restore workflows for Astra local and object-tier state.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/tiering.rs
- refs/scripts/validation/phase2-scenario-e.sh
related_artifacts: []
---

# Backup and Restore

## Backup Model

Astra continuously publishes compacted chunk bundles and manifests to object storage.

## Backup Verification

- Confirm manifest presence and chunk count.
- Validate checksums and object accessibility.
- Record latest stable manifest key per cluster.

## Restore Model

- Start with clean local node data.
- Restore from latest validated manifest.
- Validate revision and key parity before admitting writes.
