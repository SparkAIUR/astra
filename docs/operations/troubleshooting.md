---
title: Troubleshooting
summary: Diagnose common Astra runtime, migration, and operational failures.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astrad/src/main.rs
- refs/scripts/validation
related_artifacts: []
---

# Troubleshooting

## High Queue Wait, Normal Quorum Ack

Likely dispatch/backlog pressure before raft commit.

Actions:

- inspect put batch queue depth metrics,
- tune put batching and adaptive queue targets,
- verify incoming request burst shape.

## High Quorum Ack

Likely replication/commit path bottleneck.

Actions:

- inspect raft timeline telemetry,
- check peer health/network,
- confirm follower lag behavior.

## Migration Parity Mismatch

Actions:

- verify tenant-ID mapping in `astra-forge converge`,
- inspect source snapshot extraction,
- validate checksum and manifest upload path.

## Docs Build Fails in Validation

If `docs_build_ok=false` in phase11 docs scenario, verify host has Node.js and npm installed.
