# BENCHMARKS

This file is generated from `docs/research/data/benchmarks.json`.

Last updated: `2026-03-05`

## Astra vs etcd snapshots

| Phase | Scenario | Status | Astra | etcd |
| --- | --- | --- | --- | --- |
| Phase 4 | 150 IOPS survival, wal_low_linger tuned run | PASS | 7267.42 RPS, 47.52 ms p99, 50k/50k writes, no OOM | 980.17 RPS on the same run shape |
| Phase 6 | K3s external datastore benchmark | PASS with cardinality caveat | PUT 335.25 ms p99, GET 312.17 ms p99, LIST 2952.64 ms p99, 1001 nodes / 17506 pods / 10001 configmaps | PUT 158.11 ms p99, GET 143.06 ms p99, LIST 2728.32 ms p99, 1001 nodes / 16113 pods / 10001 configmaps |
| Phase 9 | RC2 K3s acceptance matrix | PASS | PUT 151.13 ms p99, GET 117.06 ms p99, LIST 268.02 ms p99, peak disk IOPS 30.0, iowait p99 5.0% | PUT 106.01 ms p99, GET 108.01 ms p99, LIST 259.00 ms p99, peak disk IOPS 22.0, iowait p99 16.5% |

## Feature validations

- **Phase 10 - QoS, governor, ecosystem, and gray-failure suite**: Gateway storm reached 13447.49 RPS with 0.0253% error rate; Patroni lease updates stayed at 33.02 ms p99; gray-failure delta gate passed with stable leader behavior.
- **Phase 11 - Migration DX and Omni fleet cutover**: 5 sources converged with 5/5 tenant parity matches; 5/5 real Omni external-etcd boots passed with no key loss.
- **Phase 12 - Directive closure run**: 50k watch fanout passed at 4995.20 write RPS and 150 ms lag p99; semantic cache showed 1.299x RPS gain and 1.292x p99 improvement; multi-raft prefix sharding reached 2.009x scale gain.

## Regeneration

```bash
uv run --project refs/scripts python refs/scripts/public/generate_public_reports.py
```

## Sources

- `Sanitized benchmark summaries curated from internal validation runs`
- `Public research pages under docs/research/`
- `Benchmark methodology and scenario notes maintained in this repository`
