---
title: astractl CLI
summary: Developer and operator command-line client for etcd-compatible and Astra admin APIs.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astractl/src/main.rs
related_artifacts:
  - docs/reference/api/etcd-kv-watch-lease.md
  - docs/reference/api/astra-admin-bulkload.md
---

# astractl CLI

`astractl` is a thin operational client for Astra endpoints.

## Usage

```bash
astractl --endpoint http://127.0.0.1:2379 [--bearer-token TOKEN] <command> [args]
```

## Global Flags

- `--endpoint`: target API endpoint (default `http://127.0.0.1:2379`).
- `--bearer-token`: optional JWT bearer token for auth-enabled clusters.

## Commands

- `put <key> <value>`: write a key/value.
- `get <key> [--prefix]`: read one key or prefix set.
- `watch <key> [--prefix] [--from-revision N]`: stream watch events.
- `blast [--prefix /blast] [--count N] [--payload-size bytes]`: PUT load generator.
- `bulk-load <manifest> [--tenant-id id] [--manifest-checksum crc32c] [--dry-run] [--allow-overwrite]`: submit bulk-load job.
- `bulk-load-job <job-id>`: poll job status.
- `watch-crucible <key> [--prefix] [--watchers N] [--streams N] [--updates N] [--payload-size bytes] [--settle-ms N] [--create-timeout-secs N] [--watch-endpoints csv] [--output path]`: high-fanout watch test harness.

## Typical Flows

```bash
# Put + get
astractl put /demo/key value
astractl get /demo/key

# Prefix query
astractl get /registry/ --prefix

# Watch
astractl watch /registry/ --prefix --from-revision 0
```
