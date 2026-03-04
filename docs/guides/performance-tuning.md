---
title: Performance Tuning Guide
summary: Tune Astra using queue-stage telemetry, profile controls, and workload-specific
  budgets.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/config.rs
- crates/astra-core/src/metrics.rs
- crates/astrad/src/main.rs
related_artifacts: []
---

# Performance Tuning Guide

## Tuning Principle

Tune by stage, not by aggregate latency only:

- queue wait,
- raft append/write,
- quorum ack,
- apply latency.

## High-Value Controls

- Put batching (`ASTRAD_PUT_BATCH_*`).
- Adaptive controller targets (`ASTRAD_PUT_TARGET_*`).
- QoS Tier-0 classification (`ASTRAD_QOS_TIER0_*`).
- Profile mode (`ASTRAD_PROFILE`).
- Background IO token budgets (`ASTRAD_BG_IO_*`).

## Recommended Process

1. Baseline with fixed workload and capture stage metrics.
2. Adjust one control family at a time.
3. Re-run validation harness and compare against prior run ID.
4. Promote only if improvements hold under non-smoke scenarios.
