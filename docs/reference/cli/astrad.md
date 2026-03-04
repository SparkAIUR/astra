---
title: astrad CLI
summary: Runtime server entrypoint flags and startup behaviors.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astrad/src/main.rs
  - crates/astra-core/src/config.rs
related_artifacts:
  - docs/reference/config/env-vars.md
  - docker-compose.yml
---

# astrad CLI

`astrad` is the Astra server process. Most runtime behavior is controlled by environment variables in [Environment Variables](../config/env-vars).

## Usage

```bash
astrad [OPTIONS]
```

## Options

- `--node-id <u64>`: overrides `ASTRAD_NODE_ID`.
- `--peers <csv>`: overrides `ASTRAD_PEERS`.
- `--client-addr <host:port>`: overrides `ASTRAD_CLIENT_ADDR`.
- `--raft-addr <host:port>`: overrides `ASTRAD_RAFT_ADDR`.
- `--profile <name>`: overrides `ASTRAD_PROFILE` (`kubernetes|omni|gateway|auto`).

## Startup Notes

- CLI flags are optional overrides; baseline config comes from `AstraConfig::from_env()`.
- `--profile` is parsed with validation and falls back to `auto` when omitted.
- Node identity and peer topology must be stable across restarts for safe raft membership.

## Example

```bash
ASTRAD_NODE_ID=1 ASTRAD_PEERS=1@node1:2380,2@node2:2380,3@node3:2380 \
ASTRAD_CLIENT_ADDR=0.0.0.0:2379 ASTRAD_RAFT_ADDR=0.0.0.0:2380 \
astrad --profile kubernetes
```
