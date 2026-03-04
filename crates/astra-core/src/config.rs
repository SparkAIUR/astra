use std::env;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalIoEngine {
    Auto,
    IoUring,
    Posix,
}

impl WalIoEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            WalIoEngine::Auto => "auto",
            WalIoEngine::IoUring => "io_uring",
            WalIoEngine::Posix => "posix",
        }
    }
}

impl FromStr for WalIoEngine {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_ascii_lowercase();
        match s.as_str() {
            "auto" => Ok(Self::Auto),
            "io_uring" | "iouring" | "uring" => Ok(Self::IoUring),
            "posix" | "sync" => Ok(Self::Posix),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchBacklogMode {
    Relaxed,
    Strict,
}

impl WatchBacklogMode {
    pub fn as_str(self) -> &'static str {
        match self {
            WatchBacklogMode::Relaxed => "relaxed",
            WatchBacklogMode::Strict => "strict",
        }
    }
}

impl FromStr for WatchBacklogMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_ascii_lowercase();
        match s.as_str() {
            "relaxed" => Ok(Self::Relaxed),
            "strict" => Ok(Self::Strict),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutAdaptiveMode {
    Legacy,
    QueueBacklogDrain,
}

impl PutAdaptiveMode {
    pub fn as_str(self) -> &'static str {
        match self {
            PutAdaptiveMode::Legacy => "legacy",
            PutAdaptiveMode::QueueBacklogDrain => "queue_backlog_drain",
        }
    }
}

impl FromStr for PutAdaptiveMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_ascii_lowercase();
        match s.as_str() {
            "legacy" => Ok(Self::Legacy),
            "queue_backlog_drain" | "queue-backlog-drain" | "drain" => Ok(Self::QueueBacklogDrain),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AstraProfile {
    Kubernetes,
    Omni,
    Gateway,
    Auto,
}

impl AstraProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            AstraProfile::Kubernetes => "kubernetes",
            AstraProfile::Omni => "omni",
            AstraProfile::Gateway => "gateway",
            AstraProfile::Auto => "auto",
        }
    }
}

impl FromStr for AstraProfile {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_ascii_lowercase();
        match s.as_str() {
            "kubernetes" | "k8s" => Ok(Self::Kubernetes),
            "omni" => Ok(Self::Omni),
            "gateway" => Ok(Self::Gateway),
            "auto" => Ok(Self::Auto),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AstraConfig {
    pub node_id: u64,
    pub client_addr: String,
    pub raft_addr: String,
    pub raft_advertise_addr: String,
    pub peers: Vec<String>,
    pub data_dir: PathBuf,
    pub max_memory_mb: usize,
    pub hot_revision_window: i64,
    pub watch_ring_capacity: usize,
    pub watch_broadcast_capacity: usize,
    pub watch_backlog_mode: WatchBacklogMode,
    pub tiering_interval_secs: u64,
    pub sst_target_bytes: usize,
    pub wal_max_batch_requests: usize,
    pub wal_max_batch_bytes: usize,
    pub wal_max_linger_us: u64,
    pub wal_low_concurrency_threshold: usize,
    pub wal_low_linger_us: u64,
    pub wal_pending_limit: usize,
    pub wal_segment_bytes: u64,
    pub wal_io_engine: WalIoEngine,
    pub bg_io_throttle_enabled: bool,
    pub bg_io_tokens_per_sec: u64,
    pub bg_io_burst_tokens: u64,
    pub bg_io_sqe_throttle_enabled: bool,
    pub bg_io_sqe_tokens_per_sec: u64,
    pub bg_io_sqe_burst_tokens: u64,
    pub bg_io_min_chunk_bytes: usize,
    pub bg_io_max_chunk_bytes: usize,
    pub lsm_max_l0_files: usize,
    pub lsm_stall_at_files: usize,
    pub lsm_stall_max_delay_ms: u64,
    pub lsm_reject_after_ms: u64,
    pub lsm_reject_extra_files: usize,
    pub lsm_synth_file_bytes: usize,
    pub lsm_delay_band_l0_5_ms: u64,
    pub lsm_delay_band_l0_6_ms: u64,
    pub lsm_delay_band_l0_7_ms: u64,
    pub list_prefix_filter_enabled: bool,
    pub list_revision_filter_enabled: bool,
    pub list_prefetch_enabled: bool,
    pub list_prefetch_pages: usize,
    pub list_prefetch_cache_entries: usize,
    pub read_isolation_enabled: bool,
    pub gateway_read_ticket_enabled: bool,
    pub gateway_read_ticket_ttl_ms: u64,
    pub gateway_singleflight_enabled: bool,
    pub gateway_singleflight_max_waiters: usize,
    pub put_batch_max_requests: usize,
    pub put_batch_min_requests: usize,
    pub put_batch_max_linger_us: u64,
    pub put_batch_min_linger_us: u64,
    pub put_batch_max_bytes: usize,
    pub put_batch_pending_limit: usize,
    pub put_adaptive_enabled: bool,
    pub put_adaptive_mode: PutAdaptiveMode,
    pub put_adaptive_min_request_floor: usize,
    pub put_dispatch_concurrency: usize,
    pub put_target_queue_depth: usize,
    pub put_p99_budget_ms: u64,
    pub put_target_queue_wait_p99_ms: u64,
    pub put_target_quorum_ack_p99_ms: u64,
    pub put_token_lane_enabled: bool,
    pub put_token_dict_max_entries: usize,
    pub put_token_min_reuse: usize,
    pub profile: AstraProfile,
    pub profile_sample_secs: u64,
    pub profile_min_dwell_secs: u64,
    pub qos_tier0_prefixes: Vec<Vec<u8>>,
    pub qos_tier0_suffixes: Vec<Vec<u8>>,
    pub qos_tier0_max_batch_requests: usize,
    pub qos_tier0_max_linger_us: u64,
    pub raft_timeline_enabled: bool,
    pub raft_timeline_sample_rate: u64,
    pub raft_election_timeout_min_ms: u64,
    pub raft_election_timeout_max_ms: u64,
    pub raft_heartbeat_interval_ms: u64,
    pub raft_max_payload_entries: u64,
    pub raft_replication_lag_threshold: u64,
    pub grpc_max_concurrent_streams: u32,
    pub grpc_http2_keepalive_interval_ms: u64,
    pub grpc_http2_keepalive_timeout_ms: u64,
    pub grpc_tcp_keepalive_ms: u64,
    pub chaos_append_ack_delay_enabled: bool,
    pub chaos_append_ack_delay_min_ms: u64,
    pub chaos_append_ack_delay_max_ms: u64,
    pub chaos_append_ack_delay_node_id: u64,
    pub metrics_enabled: bool,
    pub metrics_addr: String,
    pub auth_enabled: bool,
    pub auth_issuer: Option<String>,
    pub auth_audience: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_jwt_hs256_secret: Option<String>,
    pub auth_tenant_claim: String,
    pub tenant_virtualization_enabled: bool,
    pub s3: Option<S3Config>,
}

impl AstraConfig {
    pub fn from_env() -> Self {
        let node_id = parse_env("ASTRAD_NODE_ID", 1_u64);
        let client_addr =
            env::var("ASTRAD_CLIENT_ADDR").unwrap_or_else(|_| "0.0.0.0:2379".to_string());
        let raft_addr = env::var("ASTRAD_RAFT_ADDR").unwrap_or_else(|_| "0.0.0.0:2380".to_string());
        let raft_advertise_addr =
            env::var("ASTRAD_RAFT_ADVERTISE_ADDR").unwrap_or_else(|_| raft_addr.clone());
        let peers = env::var("ASTRAD_PEERS")
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|x| !x.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let data_dir = env::var("ASTRAD_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));

        let max_memory_mb = parse_env("ASTRAD_MAX_MEMORY_MB", 256_usize);
        let hot_revision_window = parse_env("ASTRAD_HOT_REV_WINDOW", 10_000_i64);
        let watch_ring_capacity = parse_env("ASTRAD_WATCH_RING_CAPACITY", 2_048_usize).max(1);
        let watch_broadcast_capacity =
            parse_env("ASTRAD_WATCH_BROADCAST_CAPACITY", 1_024_usize).max(32);
        let watch_backlog_mode = env::var("ASTRAD_WATCH_BACKLOG_MODE")
            .ok()
            .and_then(|v| v.parse::<WatchBacklogMode>().ok())
            .unwrap_or(WatchBacklogMode::Relaxed);
        let tiering_interval_secs = parse_env("ASTRAD_TIERING_INTERVAL_SECS", 30_u64);
        let sst_target_bytes =
            parse_env("ASTRAD_SST_TARGET_BYTES", 64 * 1024 * 1024_usize).max(64 * 1024 * 1024);

        let wal_max_batch_requests = parse_env(
            "ASTRAD_WAL_MAX_BATCH_REQUESTS",
            parse_env("ASTRAD_WAL_BATCH_MAX_ENTRIES", 1_000_usize),
        )
        .max(1);
        let wal_max_batch_bytes =
            parse_env("ASTRAD_WAL_BATCH_MAX_BYTES", 8 * 1024 * 1024_usize).max(4 * 1024);
        let wal_max_linger_us = parse_env("ASTRAD_WAL_MAX_LINGER_US", 2_000_u64);
        let wal_low_concurrency_threshold =
            parse_env("ASTRAD_WAL_LOW_CONCURRENCY_THRESHOLD", 5_usize).max(1);
        let wal_low_linger_us = parse_env("ASTRAD_WAL_LOW_LINGER_US", 0_u64);
        let wal_pending_limit = parse_env("ASTRAD_WAL_PENDING_LIMIT", 2_000_usize).max(1);
        let wal_segment_bytes =
            parse_env("ASTRAD_WAL_SEGMENT_BYTES", 64 * 1024 * 1024_u64).max(4 * 1024);
        let wal_io_engine = env::var("ASTRAD_WAL_IO_ENGINE")
            .ok()
            .and_then(|v| v.parse::<WalIoEngine>().ok())
            .unwrap_or(WalIoEngine::Auto);
        let bg_io_throttle_enabled = parse_env("ASTRAD_BG_IO_THROTTLE_ENABLED", false);
        let bg_io_tokens_per_sec = parse_env("ASTRAD_BG_IO_TOKENS_PER_SEC", 8_192_u64).max(1);
        let bg_io_burst_tokens = parse_env("ASTRAD_BG_IO_BURST_TOKENS", 16_384_u64).max(1);
        let bg_io_sqe_throttle_enabled = parse_env("ASTRAD_BG_IO_SQE_THROTTLE_ENABLED", true);
        let bg_io_sqe_tokens_per_sec =
            parse_env("ASTRAD_BG_IO_SQE_TOKENS_PER_SEC", 1_024_u64).max(1);
        let bg_io_sqe_burst_tokens = parse_env("ASTRAD_BG_IO_SQE_BURST", 2_048_u64).max(1);
        let bg_io_min_chunk_bytes =
            parse_env("ASTRAD_BG_IO_MIN_CHUNK_BYTES", 256 * 1024_usize).max(4 * 1024);
        let bg_io_max_chunk_bytes =
            parse_env("ASTRAD_BG_IO_MAX_CHUNK_BYTES", 256 * 1024_usize).max(4 * 1024);
        let lsm_max_l0_files = parse_env("ASTRAD_LSM_MAX_L0_FILES", 8_usize).max(1);
        let lsm_stall_at_files = parse_env("ASTRAD_LSM_STALL_AT_FILES", 5_usize)
            .max(1)
            .min(lsm_max_l0_files);
        let lsm_stall_max_delay_ms = parse_env("ASTRAD_LSM_STALL_MAX_DELAY_MS", 200_u64).max(1);
        let lsm_reject_after_ms = parse_env("ASTRAD_LSM_REJECT_AFTER_MS", 800_u64).max(1);
        let lsm_reject_extra_files = parse_env("ASTRAD_LSM_REJECT_EXTRA_FILES", 1_usize);
        let lsm_synth_file_bytes =
            parse_env("ASTRAD_LSM_SYNTH_FILE_BYTES", 8 * 1024 * 1024_usize).max(4096);
        let lsm_delay_band_l0_5_ms = parse_env("ASTRAD_LSM_DELAY_BAND_L0_5_MS", 1_u64).max(1);
        let lsm_delay_band_l0_6_ms =
            parse_env("ASTRAD_LSM_DELAY_BAND_L0_6_MS", 5_u64).max(lsm_delay_band_l0_5_ms);
        let lsm_delay_band_l0_7_ms =
            parse_env("ASTRAD_LSM_DELAY_BAND_L0_7_MS", 20_u64).max(lsm_delay_band_l0_6_ms);
        let list_prefix_filter_enabled = parse_env("ASTRAD_LIST_PREFIX_FILTER_ENABLED", true);
        let list_revision_filter_enabled = parse_env("ASTRAD_LIST_REVISION_FILTER_ENABLED", true);
        let list_prefetch_enabled = parse_env("ASTRAD_LIST_PREFETCH_ENABLED", true);
        let list_prefetch_pages = parse_env("ASTRAD_LIST_PREFETCH_PAGES", 2_usize).max(1);
        let list_prefetch_cache_entries =
            parse_env("ASTRAD_LIST_PREFETCH_CACHE_ENTRIES", 4_096_usize).max(1);
        let read_isolation_enabled = parse_env("ASTRAD_READ_ISOLATION_ENABLED", true);
        let gateway_read_ticket_enabled = parse_env("ASTRAD_GATEWAY_READ_TICKET_ENABLED", false);
        let gateway_read_ticket_ttl_ms =
            parse_env("ASTRAD_GATEWAY_READ_TICKET_TTL_MS", 20_u64).max(1);
        let gateway_singleflight_enabled = parse_env("ASTRAD_GATEWAY_SINGLEFLIGHT_ENABLED", false);
        let gateway_singleflight_max_waiters =
            parse_env("ASTRAD_GATEWAY_SINGLEFLIGHT_MAX_WAITERS", 4_096_usize).max(1);
        let put_batch_max_requests = parse_env("ASTRAD_PUT_BATCH_MAX_REQUESTS", 256_usize).max(1);
        let put_batch_min_requests = parse_env("ASTRAD_PUT_BATCH_MIN_REQUESTS", 16_usize)
            .max(1)
            .min(put_batch_max_requests);
        let put_batch_max_linger_us = parse_env("ASTRAD_PUT_BATCH_MAX_LINGER_US", 2_000_u64);
        let put_batch_min_linger_us =
            parse_env("ASTRAD_PUT_BATCH_MIN_LINGER_US", 50_u64).min(put_batch_max_linger_us);
        let put_batch_max_bytes =
            parse_env("ASTRAD_PUT_BATCH_MAX_BYTES", 262_144_usize).max(4 * 1024);
        let put_batch_pending_limit =
            parse_env("ASTRAD_PUT_BATCH_PENDING_LIMIT", 10_000_usize).max(1);
        let put_adaptive_enabled = parse_env("ASTRAD_PUT_ADAPTIVE_ENABLED", true);
        let put_adaptive_mode = env::var("ASTRAD_PUT_ADAPTIVE_MODE")
            .ok()
            .and_then(|v| v.parse::<PutAdaptiveMode>().ok())
            .unwrap_or(PutAdaptiveMode::QueueBacklogDrain);
        let put_adaptive_min_request_floor =
            parse_env("ASTRAD_PUT_ADAPTIVE_MIN_REQUEST_FLOOR", 128_usize).max(1);
        let put_dispatch_concurrency = parse_env("ASTRAD_PUT_DISPATCH_CONCURRENCY", 1_usize).max(1);
        let put_target_queue_depth = parse_env("ASTRAD_PUT_TARGET_QUEUE_DEPTH", 512_usize).max(1);
        let put_p99_budget_ms = parse_env("ASTRAD_PUT_P99_BUDGET_MS", 550_u64).max(1);
        let put_target_queue_wait_p99_ms =
            parse_env("ASTRAD_PUT_TARGET_QUEUE_WAIT_P99_MS", 120_u64).max(1);
        let put_target_quorum_ack_p99_ms =
            parse_env("ASTRAD_PUT_TARGET_QUORUM_ACK_P99_MS", 300_u64).max(1);
        let put_token_lane_enabled = parse_env("ASTRAD_PUT_TOKEN_LANE_ENABLED", true);
        let put_token_dict_max_entries =
            parse_env("ASTRAD_PUT_TOKEN_DICT_MAX_ENTRIES", 4_096_usize).max(1);
        let put_token_min_reuse = parse_env("ASTRAD_PUT_TOKEN_MIN_REUSE", 2_usize).max(1);
        let profile = env::var("ASTRAD_PROFILE")
            .ok()
            .and_then(|v| v.parse::<AstraProfile>().ok())
            .unwrap_or(AstraProfile::Auto);
        let profile_sample_secs = parse_env("ASTRAD_PROFILE_SAMPLE_SECS", 5_u64).max(1);
        let profile_min_dwell_secs = parse_env("ASTRAD_PROFILE_MIN_DWELL_SECS", 10_u64).max(1);
        let qos_tier0_prefixes = parse_csv_bytes(
            "ASTRAD_QOS_TIER0_PREFIXES",
            &["/registry/leases/", "/omni/locks/"],
        );
        let qos_tier0_suffixes =
            parse_csv_bytes("ASTRAD_QOS_TIER0_SUFFIXES", &["/leader", "/lock"]);
        let qos_tier0_max_batch_requests =
            parse_env("ASTRAD_QOS_TIER0_MAX_BATCH_REQUESTS", 32_usize).max(1);
        let qos_tier0_max_linger_us = parse_env("ASTRAD_QOS_TIER0_MAX_LINGER_US", 0_u64);
        let raft_timeline_enabled = parse_env("ASTRAD_RAFT_TIMELINE_ENABLED", true);
        let raft_timeline_sample_rate =
            parse_env("ASTRAD_RAFT_TIMELINE_SAMPLE_RATE", 64_u64).max(1);
        let raft_election_timeout_min_ms =
            parse_env("ASTRAD_RAFT_ELECTION_TIMEOUT_MIN_MS", 2_500_u64).max(100);
        let raft_election_timeout_max_ms = parse_env(
            "ASTRAD_RAFT_ELECTION_TIMEOUT_MAX_MS",
            raft_election_timeout_min_ms.saturating_mul(2).max(500),
        )
        .max(raft_election_timeout_min_ms.saturating_add(1));
        let raft_heartbeat_interval_ms = parse_env("ASTRAD_RAFT_HEARTBEAT_INTERVAL_MS", 350_u64)
            .max(10)
            .min(raft_election_timeout_min_ms / 2);
        let raft_max_payload_entries =
            parse_env("ASTRAD_RAFT_MAX_PAYLOAD_ENTRIES", 5_000_u64).max(1);
        let raft_replication_lag_threshold =
            parse_env("ASTRAD_RAFT_REPLICATION_LAG_THRESHOLD", 2_048_u64).max(1);
        let grpc_max_concurrent_streams =
            parse_env("ASTRAD_GRPC_MAX_CONCURRENT_STREAMS", 65_535_u32).max(64);
        let grpc_http2_keepalive_interval_ms =
            parse_env("ASTRAD_GRPC_HTTP2_KEEPALIVE_INTERVAL_MS", 15_000_u64).max(1_000);
        let grpc_http2_keepalive_timeout_ms =
            parse_env("ASTRAD_GRPC_HTTP2_KEEPALIVE_TIMEOUT_MS", 5_000_u64).max(500);
        let grpc_tcp_keepalive_ms =
            parse_env("ASTRAD_GRPC_TCP_KEEPALIVE_MS", 30_000_u64).max(1_000);
        let chaos_append_ack_delay_enabled =
            parse_env("ASTRAD_CHAOS_APPEND_ACK_DELAY_ENABLED", false);
        let chaos_append_ack_delay_min_ms =
            parse_env("ASTRAD_CHAOS_APPEND_ACK_DELAY_MIN_MS", 500_u64).max(1);
        let chaos_append_ack_delay_max_ms =
            parse_env("ASTRAD_CHAOS_APPEND_ACK_DELAY_MAX_MS", 2_000_u64)
                .max(chaos_append_ack_delay_min_ms);
        let chaos_append_ack_delay_node_id =
            parse_env("ASTRAD_CHAOS_APPEND_ACK_DELAY_NODE_ID", 0_u64);
        let metrics_enabled = parse_env("ASTRAD_METRICS_ENABLED", true);
        let metrics_addr =
            env::var("ASTRAD_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9479".to_string());
        let auth_enabled = parse_env("ASTRAD_AUTH_ENABLED", false);
        let auth_issuer = opt_env("ASTRAD_AUTH_ISSUER");
        let auth_audience = opt_env("ASTRAD_AUTH_AUDIENCE");
        let auth_jwks_url = opt_env("ASTRAD_AUTH_JWKS_URL");
        let auth_jwt_hs256_secret = opt_env("ASTRAD_AUTH_JWT_HS256_SECRET");
        let auth_tenant_claim =
            env::var("ASTRAD_AUTH_TENANT_CLAIM").unwrap_or_else(|_| "tenant_id".to_string());
        let tenant_virtualization_enabled =
            parse_env("ASTRAD_TENANT_VIRTUALIZATION_ENABLED", auth_enabled);

        let s3 = match (
            env::var("ASTRAD_S3_ENDPOINT").ok(),
            env::var("ASTRAD_S3_BUCKET").ok(),
        ) {
            (Some(endpoint), Some(bucket)) => Some(S3Config {
                endpoint,
                bucket,
                region: env::var("ASTRAD_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
                key_prefix: env::var("ASTRAD_S3_PREFIX").unwrap_or_else(|_| "astra".to_string()),
            }),
            _ => None,
        };

        Self {
            node_id,
            client_addr,
            raft_addr,
            raft_advertise_addr,
            peers,
            data_dir,
            max_memory_mb,
            hot_revision_window,
            watch_ring_capacity,
            watch_broadcast_capacity,
            watch_backlog_mode,
            tiering_interval_secs,
            sst_target_bytes,
            wal_max_batch_requests,
            wal_max_batch_bytes,
            wal_max_linger_us,
            wal_low_concurrency_threshold,
            wal_low_linger_us,
            wal_pending_limit,
            wal_segment_bytes,
            wal_io_engine,
            bg_io_throttle_enabled,
            bg_io_tokens_per_sec,
            bg_io_burst_tokens,
            bg_io_sqe_throttle_enabled,
            bg_io_sqe_tokens_per_sec,
            bg_io_sqe_burst_tokens,
            bg_io_min_chunk_bytes,
            bg_io_max_chunk_bytes,
            lsm_max_l0_files,
            lsm_stall_at_files,
            lsm_stall_max_delay_ms,
            lsm_reject_after_ms,
            lsm_reject_extra_files,
            lsm_synth_file_bytes,
            lsm_delay_band_l0_5_ms,
            lsm_delay_band_l0_6_ms,
            lsm_delay_band_l0_7_ms,
            list_prefix_filter_enabled,
            list_revision_filter_enabled,
            list_prefetch_enabled,
            list_prefetch_pages,
            list_prefetch_cache_entries,
            read_isolation_enabled,
            gateway_read_ticket_enabled,
            gateway_read_ticket_ttl_ms,
            gateway_singleflight_enabled,
            gateway_singleflight_max_waiters,
            put_batch_max_requests,
            put_batch_min_requests,
            put_batch_max_linger_us,
            put_batch_min_linger_us,
            put_batch_max_bytes,
            put_batch_pending_limit,
            put_adaptive_enabled,
            put_adaptive_mode,
            put_adaptive_min_request_floor,
            put_dispatch_concurrency,
            put_target_queue_depth,
            put_p99_budget_ms,
            put_target_queue_wait_p99_ms,
            put_target_quorum_ack_p99_ms,
            put_token_lane_enabled,
            put_token_dict_max_entries,
            put_token_min_reuse,
            profile,
            profile_sample_secs,
            profile_min_dwell_secs,
            qos_tier0_prefixes,
            qos_tier0_suffixes,
            qos_tier0_max_batch_requests,
            qos_tier0_max_linger_us,
            raft_timeline_enabled,
            raft_timeline_sample_rate,
            raft_election_timeout_min_ms,
            raft_election_timeout_max_ms,
            raft_heartbeat_interval_ms,
            raft_max_payload_entries,
            raft_replication_lag_threshold,
            grpc_max_concurrent_streams,
            grpc_http2_keepalive_interval_ms,
            grpc_http2_keepalive_timeout_ms,
            grpc_tcp_keepalive_ms,
            chaos_append_ack_delay_enabled,
            chaos_append_ack_delay_min_ms,
            chaos_append_ack_delay_max_ms,
            chaos_append_ack_delay_node_id,
            metrics_enabled,
            metrics_addr,
            auth_enabled,
            auth_issuer,
            auth_audience,
            auth_jwks_url,
            auth_jwt_hs256_secret,
            auth_tenant_claim,
            tenant_virtualization_enabled,
            s3,
        }
    }

    pub fn max_memory_bytes(&self) -> usize {
        self.max_memory_mb.saturating_mul(1024 * 1024)
    }
}

fn parse_env<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(key)
        .ok()
        .and_then(|s| s.parse::<T>().ok())
        .unwrap_or(default)
}

fn opt_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_csv_bytes(key: &str, default: &[&str]) -> Vec<Vec<u8>> {
    if let Ok(raw) = env::var(key) {
        let parsed = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<_>>();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    default.iter().map(|s| s.as_bytes().to_vec()).collect()
}
