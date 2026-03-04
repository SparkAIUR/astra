---
title: Deploy in Production
summary: Production deployment checklist for Astra clusters.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/config.rs
- docker-compose.yml
related_artifacts: []
---

# Deploy in Production

## Baseline Requirements

- 3+ nodes for quorum.
- Stable network between raft peers.
- Object storage target for tiering/recovery manifests.
- Centralized metrics collection.

## Configuration Priorities

- Explicit raft peer topology and advertise addresses.
- Memory caps and backpressure thresholds.
- Profile selection (`kubernetes`, `omni`, `gateway`, `auto`).
- Auth and tenant virtualization policy.

## Operational Guardrails

- Never disable durability-critical paths for benchmark gains.
- Run non-smoke validation scripts for candidate tuning profiles.
- Keep rollback playbooks tested and current.
