---
title: The Book of Astra
summary: Canonical knowledge base for learning, operating, and contributing to Astra.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
- README.md
related_artifacts: []
---

# The Book of Astra

Astra is a disaggregated control-plane datastore with etcd-compatible APIs, multi-tenant virtualization, and performance guardrails designed for edge and cloud-native control planes.

## Who This Site Is For

- Platform operators migrating from embedded etcd deployments.
- SREs running Astra in production.
- Contributors extending Astra runtime, APIs, and validation harnesses.

## Learning Paths

- Operator path:
  - [Quickstart](./quickstart)
  - [Migrate Omni](./guides/migration-omni)
  - [Deploy Production](./operations/deploy-production)
  - [Monitoring](./operations/monitoring-prometheus-grafana)
  - [Troubleshooting](./operations/troubleshooting)
- Contributor path:
  - [Architecture Overview](./internals/architecture-overview)
  - [Write Path](./internals/write-path)
  - [Raft Timeline](./internals/raft-timeline)
  - [Validation Harness](./internals/testing-and-validation-harness)
  - [Contributing](./contribute/contributing)

## Documentation Rules

- Stability and behavior claims must be evidence-backed.
- Every page maps to source-of-truth code or validated artifacts.
- Historical phase results are archived and explicitly marked as snapshots.
