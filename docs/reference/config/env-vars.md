---
title: Environment Variables
summary: Runtime configuration surface for astrad sourced from AstraConfig::from_env.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astra-core/src/config.rs
related_artifacts:
  - docker-compose.yml
---

# Environment Variables

This page is generated from `crates/astra-core/src/config.rs`.

Legend:
- `Field`: target `AstraConfig` field or local setting variable.
- `Default`: literal default expression used when env var is unset.

## Auth and Tenanting

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_AUTH_AUDIENCE` | `auth_audience` | `(unset)` | `opt_env` |
| `ASTRAD_AUTH_ENABLED` | `auth_enabled` | `false` | `parse_env` |
| `ASTRAD_AUTH_ISSUER` | `auth_issuer` | `(unset)` | `opt_env` |
| `ASTRAD_AUTH_JWKS_URL` | `auth_jwks_url` | `(unset)` | `opt_env` |
| `ASTRAD_AUTH_JWT_HS256_SECRET` | `auth_jwt_hs256_secret` | `(unset)` | `opt_env` |
| `ASTRAD_AUTH_TENANT_CLAIM` | `auth_tenant_claim` | `tenant_id` | `env_var` |
| `ASTRAD_TENANT_VIRTUALIZATION_ENABLED` | `tenant_virtualization_enabled` | `auth_enabled` | `parse_env` |

## Background IO and Tiering

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_BG_IO_BURST_TOKENS` | `bg_io_burst_tokens` | `16384` | `parse_env` |
| `ASTRAD_BG_IO_MAX_CHUNK_BYTES` | `bg_io_max_chunk_bytes` | `256 * 1024` | `parse_env` |
| `ASTRAD_BG_IO_MIN_CHUNK_BYTES` | `bg_io_min_chunk_bytes` | `256 * 1024` | `parse_env` |
| `ASTRAD_BG_IO_SQE_BURST` | `bg_io_sqe_burst_tokens` | `2048` | `parse_env` |
| `ASTRAD_BG_IO_SQE_THROTTLE_ENABLED` | `bg_io_sqe_throttle_enabled` | `true` | `parse_env` |
| `ASTRAD_BG_IO_SQE_TOKENS_PER_SEC` | `bg_io_sqe_tokens_per_sec` | `1024` | `parse_env` |
| `ASTRAD_BG_IO_THROTTLE_ENABLED` | `bg_io_throttle_enabled` | `false` | `parse_env` |
| `ASTRAD_BG_IO_TOKENS_PER_SEC` | `bg_io_tokens_per_sec` | `8192` | `parse_env` |

## Batching, QoS, and Adaptation

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_PUT_ADAPTIVE_ENABLED` | `put_adaptive_enabled` | `true` | `parse_env` |
| `ASTRAD_PUT_ADAPTIVE_MIN_REQUEST_FLOOR` | `put_adaptive_min_request_floor` | `128` | `parse_env` |
| `ASTRAD_PUT_ADAPTIVE_MODE` | `put_adaptive_mode` | `(unset)` | `env_var` |
| `ASTRAD_PUT_BATCH_MAX_BYTES` | `put_batch_max_bytes` | `262144` | `parse_env` |
| `ASTRAD_PUT_BATCH_MAX_LINGER_US` | `put_batch_max_linger_us` | `2000` | `parse_env` |
| `ASTRAD_PUT_BATCH_MAX_REQUESTS` | `put_batch_max_requests` | `256` | `parse_env` |
| `ASTRAD_PUT_BATCH_MIN_LINGER_US` | `put_batch_min_linger_us` | `50` | `parse_env` |
| `ASTRAD_PUT_BATCH_MIN_REQUESTS` | `put_batch_min_requests` | `16` | `parse_env` |
| `ASTRAD_PUT_BATCH_PENDING_LIMIT` | `put_batch_pending_limit` | `10000` | `parse_env` |
| `ASTRAD_PUT_DISPATCH_CONCURRENCY` | `put_dispatch_concurrency` | `1` | `parse_env` |
| `ASTRAD_PUT_P99_BUDGET_MS` | `put_p99_budget_ms` | `550` | `parse_env` |
| `ASTRAD_PUT_TARGET_QUEUE_DEPTH` | `put_target_queue_depth` | `512` | `parse_env` |
| `ASTRAD_PUT_TARGET_QUEUE_WAIT_P99_MS` | `put_target_queue_wait_p99_ms` | `120` | `parse_env` |
| `ASTRAD_PUT_TARGET_QUORUM_ACK_P99_MS` | `put_target_quorum_ack_p99_ms` | `300` | `parse_env` |
| `ASTRAD_PUT_TOKEN_DICT_MAX_ENTRIES` | `put_token_dict_max_entries` | `4096` | `parse_env` |
| `ASTRAD_PUT_TOKEN_LANE_ENABLED` | `put_token_lane_enabled` | `true` | `parse_env` |
| `ASTRAD_PUT_TOKEN_MIN_REUSE` | `put_token_min_reuse` | `2` | `parse_env` |
| `ASTRAD_QOS_TIER0_MAX_BATCH_REQUESTS` | `qos_tier0_max_batch_requests` | `32` | `parse_env` |
| `ASTRAD_QOS_TIER0_MAX_LINGER_US` | `qos_tier0_max_linger_us` | `0` | `parse_env` |
| `ASTRAD_QOS_TIER0_PREFIXES` | `qos_tier0_prefixes` | `&["/registry/leases/", "/omni/locks/"],` | `parse_csv_bytes` |
| `ASTRAD_QOS_TIER0_SUFFIXES` | `qos_tier0_suffixes` | `&["/leader", "/lock"]` | `parse_csv_bytes` |

## Chaos Testing

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_CHAOS_APPEND_ACK_DELAY_ENABLED` | `chaos_append_ack_delay_enabled` | `false` | `parse_env` |
| `ASTRAD_CHAOS_APPEND_ACK_DELAY_MAX_MS` | `chaos_append_ack_delay_max_ms` | `2000` | `parse_env` |
| `ASTRAD_CHAOS_APPEND_ACK_DELAY_MIN_MS` | `chaos_append_ack_delay_min_ms` | `500` | `parse_env` |
| `ASTRAD_CHAOS_APPEND_ACK_DELAY_NODE_ID` | `chaos_append_ack_delay_node_id` | `0` | `parse_env` |

## Cluster Identity and Networking

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_CLIENT_ADDR` | `client_addr` | `0.0.0.0:2379` | `env_var` |
| `ASTRAD_NODE_ID` | `node_id` | `1` | `parse_env` |
| `ASTRAD_PEERS` | `peers` | `(unset)` | `env_var` |
| `ASTRAD_RAFT_ADDR` | `raft_addr` | `0.0.0.0:2380` | `env_var` |
| `ASTRAD_RAFT_ADVERTISE_ADDR` | `raft_advertise_addr` | `(unset)` | `env_var` |

## General

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_DATA_DIR` | `data_dir` | `./data` | `env_var` |
| `ASTRAD_GATEWAY_READ_TICKET_ENABLED` | `gateway_read_ticket_enabled` | `false` | `parse_env` |
| `ASTRAD_GATEWAY_READ_TICKET_TTL_MS` | `gateway_read_ticket_ttl_ms` | `20` | `parse_env` |
| `ASTRAD_GATEWAY_SINGLEFLIGHT_ENABLED` | `gateway_singleflight_enabled` | `false` | `parse_env` |
| `ASTRAD_GATEWAY_SINGLEFLIGHT_MAX_WAITERS` | `gateway_singleflight_max_waiters` | `4096` | `parse_env` |
| `ASTRAD_HOT_REV_WINDOW` | `hot_revision_window` | `10000` | `parse_env` |
| `ASTRAD_MAX_MEMORY_MB` | `max_memory_mb` | `256` | `parse_env` |
| `ASTRAD_SST_TARGET_BYTES` | `sst_target_bytes` | `64 * 1024 * 1024` | `parse_env` |
| `ASTRAD_TIERING_INTERVAL_SECS` | `tiering_interval_secs` | `30` | `parse_env` |
| `ASTRAD_WATCH_BACKLOG_MODE` | `watch_backlog_mode` | `(unset)` | `env_var` |
| `ASTRAD_WATCH_BROADCAST_CAPACITY` | `watch_broadcast_capacity` | `1024` | `parse_env` |
| `ASTRAD_WATCH_RING_CAPACITY` | `watch_ring_capacity` | `2048` | `parse_env` |

## LSM Pressure Control

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_LSM_DELAY_BAND_L0_5_MS` | `lsm_delay_band_l0_5_ms` | `1` | `parse_env` |
| `ASTRAD_LSM_DELAY_BAND_L0_6_MS` | `lsm_delay_band_l0_6_ms` | `5` | `parse_env` |
| `ASTRAD_LSM_DELAY_BAND_L0_7_MS` | `lsm_delay_band_l0_7_ms` | `20` | `parse_env` |
| `ASTRAD_LSM_MAX_L0_FILES` | `lsm_max_l0_files` | `8` | `parse_env` |
| `ASTRAD_LSM_REJECT_AFTER_MS` | `lsm_reject_after_ms` | `800` | `parse_env` |
| `ASTRAD_LSM_REJECT_EXTRA_FILES` | `lsm_reject_extra_files` | `1` | `parse_env` |
| `ASTRAD_LSM_STALL_AT_FILES` | `lsm_stall_at_files` | `5` | `parse_env` |
| `ASTRAD_LSM_STALL_MAX_DELAY_MS` | `lsm_stall_max_delay_ms` | `200` | `parse_env` |
| `ASTRAD_LSM_SYNTH_FILE_BYTES` | `lsm_synth_file_bytes` | `8 * 1024 * 1024` | `parse_env` |

## Metrics

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_METRICS_ADDR` | `metrics_addr` | `0.0.0.0:9479` | `env_var` |
| `ASTRAD_METRICS_ENABLED` | `metrics_enabled` | `true` | `parse_env` |

## Profiles and Governor

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_PROFILE` | `profile` | `(unset)` | `env_var` |
| `ASTRAD_PROFILE_MIN_DWELL_SECS` | `profile_min_dwell_secs` | `10` | `parse_env` |
| `ASTRAD_PROFILE_SAMPLE_SECS` | `profile_sample_secs` | `5` | `parse_env` |

## Raft Timers and Replication

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_RAFT_ELECTION_TIMEOUT_MAX_MS` | `raft_election_timeout_max_ms` | `raft_election_timeout_min_ms.saturating_mul(2).max(500),` | `parse_env` |
| `ASTRAD_RAFT_ELECTION_TIMEOUT_MIN_MS` | `raft_election_timeout_min_ms` | `2500` | `parse_env` |
| `ASTRAD_RAFT_HEARTBEAT_INTERVAL_MS` | `raft_heartbeat_interval_ms` | `350` | `parse_env` |
| `ASTRAD_RAFT_MAX_PAYLOAD_ENTRIES` | `raft_max_payload_entries` | `5000` | `parse_env` |
| `ASTRAD_RAFT_REPLICATION_LAG_THRESHOLD` | `raft_replication_lag_threshold` | `2048` | `parse_env` |
| `ASTRAD_RAFT_TIMELINE_ENABLED` | `raft_timeline_enabled` | `true` | `parse_env` |
| `ASTRAD_RAFT_TIMELINE_SAMPLE_RATE` | `raft_timeline_sample_rate` | `64` | `parse_env` |

## Read and LIST Path

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_LIST_PREFETCH_CACHE_ENTRIES` | `list_prefetch_cache_entries` | `4096` | `parse_env` |
| `ASTRAD_LIST_PREFETCH_ENABLED` | `list_prefetch_enabled` | `true` | `parse_env` |
| `ASTRAD_LIST_PREFETCH_PAGES` | `list_prefetch_pages` | `2` | `parse_env` |
| `ASTRAD_LIST_PREFIX_FILTER_ENABLED` | `list_prefix_filter_enabled` | `true` | `parse_env` |
| `ASTRAD_LIST_REVISION_FILTER_ENABLED` | `list_revision_filter_enabled` | `true` | `parse_env` |
| `ASTRAD_READ_ISOLATION_ENABLED` | `read_isolation_enabled` | `true` | `parse_env` |

## S3 Tiering

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_S3_BUCKET` | `s3` | `us-east-1` | `env_var` |
| `ASTRAD_S3_ENDPOINT` | `s3` | `us-east-1` | `env_var` |
| `ASTRAD_S3_PREFIX` | `s3` | `us-east-1` | `env_var` |
| `ASTRAD_S3_REGION` | `s3` | `us-east-1` | `env_var` |

## WAL and Write Path

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_WAL_BATCH_MAX_BYTES` | `wal_max_batch_bytes` | `8 * 1024 * 1024` | `parse_env` |
| `ASTRAD_WAL_IO_ENGINE` | `wal_io_engine` | `(unset)` | `env_var` |
| `ASTRAD_WAL_LOW_CONCURRENCY_THRESHOLD` | `wal_low_concurrency_threshold` | `5` | `parse_env` |
| `ASTRAD_WAL_LOW_LINGER_US` | `wal_low_linger_us` | `0` | `parse_env` |
| `ASTRAD_WAL_MAX_BATCH_REQUESTS` | `wal_max_batch_requests` | `parse_env("ASTRAD_WAL_BATCH_MAX_ENTRIES", 1_000),` | `parse_env` |
| `ASTRAD_WAL_MAX_LINGER_US` | `wal_max_linger_us` | `2000` | `parse_env` |
| `ASTRAD_WAL_PENDING_LIMIT` | `wal_pending_limit` | `2000` | `parse_env` |
| `ASTRAD_WAL_SEGMENT_BYTES` | `wal_segment_bytes` | `64 * 1024 * 1024` | `parse_env` |

## gRPC Transport

| Variable | Field | Default | Source |
| --- | --- | --- | --- |
| `ASTRAD_GRPC_HTTP2_KEEPALIVE_INTERVAL_MS` | `grpc_http2_keepalive_interval_ms` | `15000` | `parse_env` |
| `ASTRAD_GRPC_HTTP2_KEEPALIVE_TIMEOUT_MS` | `grpc_http2_keepalive_timeout_ms` | `5000` | `parse_env` |
| `ASTRAD_GRPC_MAX_CONCURRENT_STREAMS` | `grpc_max_concurrent_streams` | `65535` | `parse_env` |
| `ASTRAD_GRPC_TCP_KEEPALIVE_MS` | `grpc_tcp_keepalive_ms` | `30000` | `parse_env` |

## Notes

- Optional values (`opt_env`) are represented as `(unset)` and become `None` when not provided.
- Some values are additionally clamped in code (`.max(...)`, `.min(...)`) after parsing.
- `ASTRAD_WAL_BATCH_MAX_ENTRIES` is retained as a fallback alias for `ASTRAD_WAL_MAX_BATCH_REQUESTS`.
