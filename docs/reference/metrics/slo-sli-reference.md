---
title: SLI and SLO Reference
summary: Recommended service indicators and objective framing for Astra operations.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-core/src/metrics.rs
related_artifacts:
  - docs/reference/metrics/metric-catalog.md
---

# SLI and SLO Reference

## Core SLIs

- write queue-wait p99
- quorum-ack p99
- write success/error rates
- lease lane service stability
- profile switch rate and oscillation behavior

## SLO Framing Guidance

- Define lane-specific objectives (Tier-0 vs normal).
- Separate correctness gates from performance gates.
- Use run-ID-backed benchmark evidence for any public claim.
