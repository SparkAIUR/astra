---
title: Monitoring with Prometheus and Grafana
summary: Monitor Astra queue stages, raft timings, and profile shifts with bundled telemetry.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - refs/sandbox/prometheus/prometheus.yml
  - refs/sandbox/grafana/dashboards/astra-overview.json
  - crates/astra-core/src/metrics.rs
related_artifacts:
  - docker-compose.yaml
---

# Monitoring with Prometheus and Grafana

## Core Panels to Track

- Put queue wait distribution.
- Quorum ack distribution.
- Batch sizes and lane depth.
- Profile switch counts.
- Tier-0 enqueue/dispatch counters.

## Alerting Starting Points

- Sustained queue-wait p99 growth.
- Sudden quorum-ack inflation.
- Tier-0 queue depth not draining.
- Repeated profile oscillation.
