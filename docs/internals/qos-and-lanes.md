---
title: QoS and Lanes
summary: Semantic QoS classification and dual-lane write behavior.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
- crates/astra-core/src/config.rs
- crates/astrad/src/main.rs
related_artifacts: []
---

# QoS and Lanes

Astra can classify lease/lock lanes as Tier-0 and route them through priority handling.

## Benefits

- protects control-plane liveness during normal-lane saturation,
- limits lease lock contention regressions under stress.

## Controls

- Tier-0 prefixes and suffixes.
- Tier-0 batch sizing and linger.
