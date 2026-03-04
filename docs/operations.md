---
title: Operations (Legacy Entry)
summary: Legacy operations entrypoint retained for compatibility; canonical runbooks live under /operations.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - docker-compose.yml
  - crates/astra-core/src/config.rs
  - crates/astra-core/src/metrics.rs
related_artifacts:
  - docs/operations/deploy-production.md
  - docs/operations/profiles-and-auto-governor.md
  - docs/operations/monitoring-prometheus-grafana.md
---

# Operations & Auto-Tuning

Canonical operations runbooks:

- [Deploy Production](./operations/deploy-production)
- [Profiles and Auto-Governor](./operations/profiles-and-auto-governor)
- [Monitoring](./operations/monitoring-prometheus-grafana)
- [Troubleshooting](./operations/troubleshooting)
- [FAQ](./operations/faq)

## Runtime Profiles

Astra supports profile-directed behavior with optional auto-governor switching.

- `kubernetes`
- `omni`
- `gateway`
- `auto`

Set profile via:

```bash
export ASTRAD_PROFILE=auto
```

## Key Tuning Controls

- Put lane batching and linger caps.
- Queue-depth and p99 budget targets.
- Background IO token bucket rates.
- List prefetch and cache bounds.
- Tier-0 prefix/suffix classification.

## Metrics to Watch

- `astra_put_batch_queue_wait_seconds_*`
- `astra_put_batch_write_seconds_*`
- `astra_put_batch_quorum_ack_seconds_*`
- `astra_profile_switch_total`
- `astra_qos_tier0_enqueued_total`
- `astra_qos_tier0_dispatched_total`

## Prometheus + Grafana

The sandbox ships with pre-provisioned Prometheus and Grafana assets.
Use the included dashboard JSON to inspect queue wait, write latency, and profile switches in real time.

## Practical Guardrails

- Keep Tier-0 lane thresholds conservative for lease workloads.
- Tune by stage telemetry (queue wait vs quorum ack), not aggregate p99 alone.
- Validate any profile changes with the phase validation harness before rollout.
