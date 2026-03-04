---
title: Raft Timeline Telemetry
summary: Raft stage timing instrumentation for queue/append/replication/quorum/apply
  analysis.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/raft.rs
- crates/astra-core/src/metrics.rs
related_artifacts: []
---

# Raft Timeline Telemetry

Astra instruments write lifecycle timings to isolate bottlenecks.

## Typical Stages

- enqueue
- dispatch / append
- replicate
- quorum ack
- apply

## Usage

Use timeline and histogram metrics to classify latency inflation before changing tuning knobs.
