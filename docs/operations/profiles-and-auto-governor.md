---
title: Profiles and Auto-Governor
summary: Runtime profile system and adaptive controls for workload-specific behavior.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/config.rs
- crates/astrad/src/main.rs
related_artifacts: []
---

# Profiles and Auto-Governor

## Supported Profiles

- `kubernetes`
- `omni`
- `gateway`
- `auto`

## Auto Mode Behavior

`auto` samples request mix and applies runtime tuning to:

- normal put lane batching,
- list prefetch bounds,
- background IO token rates.

## Telemetry to Monitor

- `astra_profile_active`
- `astra_profile_switch_total`
- queue-wait and quorum-ack histograms

Use dwell and sample settings to avoid oscillation in mixed workloads.
