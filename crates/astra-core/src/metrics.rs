use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;

const QUEUE_WAIT_BUCKETS_MS: &[u64] = &[
    1, 2, 5, 10, 20, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 5_000,
];
const CLIENT_WRITE_BUCKETS_MS: &[u64] = &[
    1, 2, 5, 10, 20, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 5_000, 10_000,
];
const QUORUM_ACK_BUCKETS_MS: &[u64] = &[
    1, 2, 5, 10, 20, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 5_000, 10_000,
];
const BATCH_SIZE_BUCKETS: &[u64] = &[1, 2, 4, 8, 16, 32, 64, 96, 128, 192, 256, 384, 512, 1024];
const BG_IO_WAIT_BUCKETS_MS: &[u64] = &[
    1, 2, 5, 10, 20, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 5_000, 10_000,
];
const WRITE_STALL_BUCKETS_MS: &[u64] = &[
    1, 2, 5, 10, 20, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 5_000, 10_000,
];

#[derive(Clone, Copy, Debug)]
enum HistogramMode {
    SecondsFromMillis,
    Raw,
}

#[derive(Debug)]
struct Histogram {
    bounds: &'static [u64],
    mode: HistogramMode,
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum: AtomicU64,
}

impl Histogram {
    fn new(bounds: &'static [u64], mode: HistogramMode) -> Self {
        let buckets = (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            bounds,
            mode,
            buckets,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    fn observe(&self, value: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);

        let bucket_idx = self
            .bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(self.bounds.len());
        self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, metric_name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {metric_name} {help}");
        let _ = writeln!(out, "# TYPE {metric_name} histogram");

        let mut cumulative = 0_u64;
        for (idx, bound) in self.bounds.iter().enumerate() {
            cumulative = cumulative.saturating_add(self.buckets[idx].load(Ordering::Relaxed));
            let bound_label = match self.mode {
                HistogramMode::SecondsFromMillis => format!("{:.6}", (*bound as f64) / 1_000.0),
                HistogramMode::Raw => bound.to_string(),
            };
            let _ = writeln!(
                out,
                "{metric_name}_bucket{{le=\"{bound_label}\"}} {cumulative}"
            );
        }

        cumulative =
            cumulative.saturating_add(self.buckets[self.bounds.len()].load(Ordering::Relaxed));
        let _ = writeln!(out, "{metric_name}_bucket{{le=\"+Inf\"}} {cumulative}");

        let sum_value = self.sum.load(Ordering::Relaxed);
        let sum_label = match self.mode {
            HistogramMode::SecondsFromMillis => format!("{:.6}", (sum_value as f64) / 1_000.0),
            HistogramMode::Raw => sum_value.to_string(),
        };
        let _ = writeln!(out, "{metric_name}_sum {sum_label}");
        let _ = writeln!(
            out,
            "{metric_name}_count {}",
            self.count.load(Ordering::Relaxed)
        );
    }
}

#[derive(Debug)]
struct AstraMetrics {
    enabled: AtomicBool,
    put_queue_wait_ms: Histogram,
    put_queue_wait_tier0_ms: Histogram,
    put_queue_wait_normal_ms: Histogram,
    put_raft_write_ms: Histogram,
    put_quorum_ack_ms: Histogram,
    raft_append_rpc_client_ms: Histogram,
    raft_append_rpc_server_ms: Histogram,
    raft_install_snapshot_rpc_client_ms: Histogram,
    raft_install_snapshot_rpc_server_ms: Histogram,
    raft_snapshot_build_ms: Histogram,
    raft_snapshot_install_deserialize_ms: Histogram,
    raft_snapshot_install_load_ms: Histogram,
    put_batch_size: Histogram,
    bg_io_throttle_wait_ms: Histogram,
    put_batches_total: AtomicU64,
    put_batch_requests_total: AtomicU64,
    put_tier0_enqueued_total: AtomicU64,
    put_tier0_dispatch_total: AtomicU64,
    put_tier0_bypass_write_pressure_total: AtomicU64,
    quorum_ack_observations: AtomicU64,
    latest_quorum_ack_ms: AtomicU64,
    put_inflight_requests: AtomicU64,
    put_tier0_queue_depth: AtomicU64,
    put_normal_queue_depth: AtomicU64,
    wal_effective_linger_us: AtomicU64,
    wal_queue_depth: AtomicU64,
    wal_queue_bytes: AtomicU64,
    wal_logical_bytes: AtomicU64,
    wal_allocated_bytes: AtomicU64,
    wal_checkpoint_ms: Histogram,
    wal_checkpoint_total: AtomicU64,
    wal_checkpoint_reclaimed_bytes_total: AtomicU64,
    lsm_synth_l0_files: AtomicU64,
    write_stall_delay_ms: Histogram,
    write_stall_events_total: AtomicU64,
    write_stall_band_5_events_total: AtomicU64,
    write_stall_band_6_events_total: AtomicU64,
    write_stall_band_7_plus_events_total: AtomicU64,
    write_reject_events_total: AtomicU64,
    bg_io_tokens_available: AtomicU64,
    bg_io_sqe_tokens_available: AtomicU64,
    bg_io_throttle_events_total: AtomicU64,
    bg_io_sqe_wait_ms: Histogram,
    bg_io_sqe_throttle_events_total: AtomicU64,
    list_prefix_filter_skips_total: AtomicU64,
    list_prefix_filter_hits_total: AtomicU64,
    list_revision_filter_skips_total: AtomicU64,
    list_revision_filter_hits_total: AtomicU64,
    list_prefetch_hits_total: AtomicU64,
    list_prefetch_misses_total: AtomicU64,
    list_stream_chunks_total: AtomicU64,
    list_stream_bytes_total: AtomicU64,
    list_stream_hydrated_values_total: AtomicU64,
    list_stream_hydrated_bytes_total: AtomicU64,
    semantic_cache_hits_total: AtomicU64,
    semantic_cache_misses_total: AtomicU64,
    read_isolation_dispatch_total: AtomicU64,
    read_isolation_failures_total: AtomicU64,
    read_quorum_checks_total: AtomicU64,
    read_quorum_failures_total: AtomicU64,
    read_quorum_internal_total: AtomicU64,
    transport_closed_conn_unavailable_total: AtomicU64,
    forward_retry_attempts_total: AtomicU64,
    gateway_read_ticket_hits_total: AtomicU64,
    gateway_read_ticket_misses_total: AtomicU64,
    gateway_singleflight_leader_total: AtomicU64,
    gateway_singleflight_waiter_total: AtomicU64,
    gateway_singleflight_overflow_total: AtomicU64,
    gateway_singleflight_waiter_timeouts_total: AtomicU64,
    request_get_total: AtomicU64,
    request_list_total: AtomicU64,
    request_put_total: AtomicU64,
    request_delete_total: AtomicU64,
    request_txn_total: AtomicU64,
    request_lease_total: AtomicU64,
    request_watch_total: AtomicU64,
    request_tier0_total: AtomicU64,
    watch_delegate_reject_total: AtomicU64,
    watch_delegate_accept_total: AtomicU64,
    watch_active_streams: AtomicU64,
    watch_lagged_total: AtomicU64,
    watch_lagged_skipped_events_total: AtomicU64,
    watch_resync_total: AtomicU64,
    watch_resync_keys_total: AtomicU64,
    watch_emit_lag_ms: Histogram,
    watch_dispatch_queue_depth: AtomicU64,
    watch_slow_cancels_total: AtomicU64,
    watch_emit_batch_size: Histogram,
    raft_append_rpc_failures_total: AtomicU64,
    raft_install_snapshot_rpc_failures_total: AtomicU64,
    large_value_upload_ms: Histogram,
    large_value_upload_failures_total: AtomicU64,
    large_value_hydrate_ms: Histogram,
    large_value_hydrate_cache_hits_total: AtomicU64,
    large_value_hydrate_cache_misses_total: AtomicU64,
    profile_switch_total: AtomicU64,
    profile_active_kubernetes: AtomicU64,
    profile_active_omni: AtomicU64,
    profile_active_gateway: AtomicU64,
    profile_active_auto: AtomicU64,
    profile_applied_put_max_requests: AtomicU64,
    profile_applied_put_linger_us: AtomicU64,
    profile_applied_prefetch_entries: AtomicU64,
    profile_applied_bg_io_tokens_per_sec: AtomicU64,
}

impl AstraMetrics {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            put_queue_wait_ms: Histogram::new(
                QUEUE_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            put_queue_wait_tier0_ms: Histogram::new(
                QUEUE_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            put_queue_wait_normal_ms: Histogram::new(
                QUEUE_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            put_raft_write_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            put_quorum_ack_ms: Histogram::new(
                QUORUM_ACK_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_append_rpc_client_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_append_rpc_server_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_install_snapshot_rpc_client_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_install_snapshot_rpc_server_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_snapshot_build_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_snapshot_install_deserialize_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            raft_snapshot_install_load_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            put_batch_size: Histogram::new(BATCH_SIZE_BUCKETS, HistogramMode::Raw),
            bg_io_throttle_wait_ms: Histogram::new(
                BG_IO_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            put_batches_total: AtomicU64::new(0),
            put_batch_requests_total: AtomicU64::new(0),
            put_tier0_enqueued_total: AtomicU64::new(0),
            put_tier0_dispatch_total: AtomicU64::new(0),
            put_tier0_bypass_write_pressure_total: AtomicU64::new(0),
            quorum_ack_observations: AtomicU64::new(0),
            latest_quorum_ack_ms: AtomicU64::new(0),
            put_inflight_requests: AtomicU64::new(0),
            put_tier0_queue_depth: AtomicU64::new(0),
            put_normal_queue_depth: AtomicU64::new(0),
            wal_effective_linger_us: AtomicU64::new(0),
            wal_queue_depth: AtomicU64::new(0),
            wal_queue_bytes: AtomicU64::new(0),
            wal_logical_bytes: AtomicU64::new(0),
            wal_allocated_bytes: AtomicU64::new(0),
            wal_checkpoint_ms: Histogram::new(
                BG_IO_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            wal_checkpoint_total: AtomicU64::new(0),
            wal_checkpoint_reclaimed_bytes_total: AtomicU64::new(0),
            lsm_synth_l0_files: AtomicU64::new(0),
            write_stall_delay_ms: Histogram::new(
                WRITE_STALL_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            write_stall_events_total: AtomicU64::new(0),
            write_stall_band_5_events_total: AtomicU64::new(0),
            write_stall_band_6_events_total: AtomicU64::new(0),
            write_stall_band_7_plus_events_total: AtomicU64::new(0),
            write_reject_events_total: AtomicU64::new(0),
            bg_io_tokens_available: AtomicU64::new(0),
            bg_io_sqe_tokens_available: AtomicU64::new(0),
            bg_io_throttle_events_total: AtomicU64::new(0),
            bg_io_sqe_wait_ms: Histogram::new(
                BG_IO_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            bg_io_sqe_throttle_events_total: AtomicU64::new(0),
            list_prefix_filter_skips_total: AtomicU64::new(0),
            list_prefix_filter_hits_total: AtomicU64::new(0),
            list_revision_filter_skips_total: AtomicU64::new(0),
            list_revision_filter_hits_total: AtomicU64::new(0),
            list_prefetch_hits_total: AtomicU64::new(0),
            list_prefetch_misses_total: AtomicU64::new(0),
            list_stream_chunks_total: AtomicU64::new(0),
            list_stream_bytes_total: AtomicU64::new(0),
            list_stream_hydrated_values_total: AtomicU64::new(0),
            list_stream_hydrated_bytes_total: AtomicU64::new(0),
            semantic_cache_hits_total: AtomicU64::new(0),
            semantic_cache_misses_total: AtomicU64::new(0),
            read_isolation_dispatch_total: AtomicU64::new(0),
            read_isolation_failures_total: AtomicU64::new(0),
            read_quorum_checks_total: AtomicU64::new(0),
            read_quorum_failures_total: AtomicU64::new(0),
            read_quorum_internal_total: AtomicU64::new(0),
            transport_closed_conn_unavailable_total: AtomicU64::new(0),
            forward_retry_attempts_total: AtomicU64::new(0),
            gateway_read_ticket_hits_total: AtomicU64::new(0),
            gateway_read_ticket_misses_total: AtomicU64::new(0),
            gateway_singleflight_leader_total: AtomicU64::new(0),
            gateway_singleflight_waiter_total: AtomicU64::new(0),
            gateway_singleflight_overflow_total: AtomicU64::new(0),
            gateway_singleflight_waiter_timeouts_total: AtomicU64::new(0),
            request_get_total: AtomicU64::new(0),
            request_list_total: AtomicU64::new(0),
            request_put_total: AtomicU64::new(0),
            request_delete_total: AtomicU64::new(0),
            request_txn_total: AtomicU64::new(0),
            request_lease_total: AtomicU64::new(0),
            request_watch_total: AtomicU64::new(0),
            request_tier0_total: AtomicU64::new(0),
            watch_delegate_reject_total: AtomicU64::new(0),
            watch_delegate_accept_total: AtomicU64::new(0),
            watch_active_streams: AtomicU64::new(0),
            watch_lagged_total: AtomicU64::new(0),
            watch_lagged_skipped_events_total: AtomicU64::new(0),
            watch_resync_total: AtomicU64::new(0),
            watch_resync_keys_total: AtomicU64::new(0),
            watch_emit_lag_ms: Histogram::new(
                QUEUE_WAIT_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            watch_dispatch_queue_depth: AtomicU64::new(0),
            watch_slow_cancels_total: AtomicU64::new(0),
            watch_emit_batch_size: Histogram::new(BATCH_SIZE_BUCKETS, HistogramMode::Raw),
            raft_append_rpc_failures_total: AtomicU64::new(0),
            raft_install_snapshot_rpc_failures_total: AtomicU64::new(0),
            large_value_upload_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            large_value_upload_failures_total: AtomicU64::new(0),
            large_value_hydrate_ms: Histogram::new(
                CLIENT_WRITE_BUCKETS_MS,
                HistogramMode::SecondsFromMillis,
            ),
            large_value_hydrate_cache_hits_total: AtomicU64::new(0),
            large_value_hydrate_cache_misses_total: AtomicU64::new(0),
            profile_switch_total: AtomicU64::new(0),
            profile_active_kubernetes: AtomicU64::new(0),
            profile_active_omni: AtomicU64::new(0),
            profile_active_gateway: AtomicU64::new(0),
            profile_active_auto: AtomicU64::new(1),
            profile_applied_put_max_requests: AtomicU64::new(0),
            profile_applied_put_linger_us: AtomicU64::new(0),
            profile_applied_prefetch_entries: AtomicU64::new(0),
            profile_applied_bg_io_tokens_per_sec: AtomicU64::new(0),
        }
    }

    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
}

static METRICS: OnceLock<AstraMetrics> = OnceLock::new();

fn metrics() -> &'static AstraMetrics {
    METRICS.get_or_init(AstraMetrics::new)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RequestMixSnapshot {
    pub get: u64,
    pub list: u64,
    pub put: u64,
    pub delete: u64,
    pub txn: u64,
    pub lease: u64,
    pub watch: u64,
    pub tier0: u64,
}

impl RequestMixSnapshot {
    pub fn total(self) -> u64 {
        self.get
            .saturating_add(self.list)
            .saturating_add(self.put)
            .saturating_add(self.delete)
            .saturating_add(self.txn)
            .saturating_add(self.lease)
            .saturating_add(self.watch)
    }

    pub fn delta_since(self, previous: Self) -> Self {
        Self {
            get: self.get.saturating_sub(previous.get),
            list: self.list.saturating_sub(previous.list),
            put: self.put.saturating_sub(previous.put),
            delete: self.delete.saturating_sub(previous.delete),
            txn: self.txn.saturating_sub(previous.txn),
            lease: self.lease.saturating_sub(previous.lease),
            watch: self.watch.saturating_sub(previous.watch),
            tier0: self.tier0.saturating_sub(previous.tier0),
        }
    }
}

pub fn request_mix_snapshot() -> RequestMixSnapshot {
    let reg = metrics();
    if !reg.enabled() {
        return RequestMixSnapshot::default();
    }
    RequestMixSnapshot {
        get: reg.request_get_total.load(Ordering::Relaxed),
        list: reg.request_list_total.load(Ordering::Relaxed),
        put: reg.request_put_total.load(Ordering::Relaxed),
        delete: reg.request_delete_total.load(Ordering::Relaxed),
        txn: reg.request_txn_total.load(Ordering::Relaxed),
        lease: reg.request_lease_total.load(Ordering::Relaxed),
        watch: reg.request_watch_total.load(Ordering::Relaxed),
        tier0: reg.request_tier0_total.load(Ordering::Relaxed),
    }
}

pub fn set_enabled(enabled: bool) {
    metrics().enabled.store(enabled, Ordering::Relaxed);
}

pub fn observe_put_queue_wait_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_queue_wait_ms.observe(value_ms);
}

pub fn observe_put_queue_wait_ms_by_priority(value_ms: u64, tier0: bool) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    if tier0 {
        reg.put_queue_wait_tier0_ms.observe(value_ms);
    } else {
        reg.put_queue_wait_normal_ms.observe(value_ms);
    }
}

pub fn observe_put_raft_client_write_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_raft_write_ms.observe(value_ms);
}

pub fn observe_put_quorum_ack_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_quorum_ack_ms.observe(value_ms);
    reg.latest_quorum_ack_ms.store(value_ms, Ordering::Relaxed);
    reg.quorum_ack_observations.fetch_add(1, Ordering::Relaxed);
}

pub fn observe_raft_append_rpc_client_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_append_rpc_client_ms.observe(value_ms);
}

pub fn observe_raft_append_rpc_server_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_append_rpc_server_ms.observe(value_ms);
}

pub fn inc_raft_append_rpc_failures_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_append_rpc_failures_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn observe_raft_install_snapshot_rpc_client_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_install_snapshot_rpc_client_ms.observe(value_ms);
}

pub fn observe_raft_install_snapshot_rpc_server_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_install_snapshot_rpc_server_ms.observe(value_ms);
}

pub fn inc_raft_install_snapshot_rpc_failures_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_install_snapshot_rpc_failures_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn observe_raft_snapshot_build_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_snapshot_build_ms.observe(value_ms);
}

pub fn observe_raft_snapshot_install_deserialize_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_snapshot_install_deserialize_ms.observe(value_ms);
}

pub fn observe_raft_snapshot_install_load_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.raft_snapshot_install_load_ms.observe(value_ms);
}

pub fn observe_put_batch_size(size: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_batch_size.observe(size as u64);
}

pub fn inc_put_batches() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_batches_total.fetch_add(1, Ordering::Relaxed);
}

pub fn add_put_batch_requests(count: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_batch_requests_total
        .fetch_add(count as u64, Ordering::Relaxed);
}

pub fn inc_put_tier0_enqueued() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_tier0_enqueued_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_put_tier0_dispatch_events() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_tier0_dispatch_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_put_tier0_bypass_write_pressure() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_tier0_bypass_write_pressure_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn set_put_inflight_requests(count: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_inflight_requests
        .store(count as u64, Ordering::Relaxed);
}

pub fn set_put_tier0_queue_depth(count: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_tier0_queue_depth
        .store(count as u64, Ordering::Relaxed);
}

pub fn set_put_normal_queue_depth(count: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.put_normal_queue_depth
        .store(count as u64, Ordering::Relaxed);
}

pub fn current_put_inflight_requests() -> u64 {
    let reg = metrics();
    if !reg.enabled() {
        return 0;
    }
    reg.put_inflight_requests.load(Ordering::Relaxed)
}

pub fn set_wal_effective_linger_us(value: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.wal_effective_linger_us.store(value, Ordering::Relaxed);
}

pub fn set_wal_queue_depth(depth: usize) {
    let reg = metrics();
    reg.wal_queue_depth.store(depth as u64, Ordering::Relaxed);
}

pub fn set_wal_queue_bytes(bytes: usize) {
    let reg = metrics();
    reg.wal_queue_bytes.store(bytes as u64, Ordering::Relaxed);
}

pub fn set_wal_logical_bytes(bytes: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.wal_logical_bytes.store(bytes, Ordering::Relaxed);
}

pub fn set_wal_allocated_bytes(bytes: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.wal_allocated_bytes.store(bytes, Ordering::Relaxed);
}

pub fn observe_wal_checkpoint_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.wal_checkpoint_ms.observe(value_ms);
}

pub fn inc_wal_checkpoint_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.wal_checkpoint_total.fetch_add(1, Ordering::Relaxed);
}

pub fn add_wal_checkpoint_reclaimed_bytes(bytes: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.wal_checkpoint_reclaimed_bytes_total
        .fetch_add(bytes, Ordering::Relaxed);
}

pub fn current_wal_queue_depth() -> u64 {
    metrics().wal_queue_depth.load(Ordering::Relaxed)
}

pub fn current_wal_queue_bytes() -> u64 {
    metrics().wal_queue_bytes.load(Ordering::Relaxed)
}

pub fn set_lsm_synth_l0_files(value: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.lsm_synth_l0_files.store(value, Ordering::Relaxed);
}

pub fn observe_write_stall_delay_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.write_stall_delay_ms.observe(value_ms);
    reg.write_stall_events_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_stall_band_5_events() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.write_stall_band_5_events_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_stall_band_6_events() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.write_stall_band_6_events_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_stall_band_7_plus_events() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.write_stall_band_7_plus_events_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_write_reject_events() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.write_reject_events_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn set_bg_io_tokens_available(tokens: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.bg_io_tokens_available.store(tokens, Ordering::Relaxed);
}

pub fn set_bg_io_sqe_tokens_available(tokens: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.bg_io_sqe_tokens_available
        .store(tokens, Ordering::Relaxed);
}

pub fn observe_bg_io_throttle_wait_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.bg_io_throttle_wait_ms.observe(value_ms);
    reg.bg_io_throttle_events_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn observe_bg_io_sqe_wait_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.bg_io_sqe_wait_ms.observe(value_ms);
    reg.bg_io_sqe_throttle_events_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_list_prefix_filter_skips() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_prefix_filter_skips_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_list_prefix_filter_hits() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_prefix_filter_hits_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_list_revision_filter_skips() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_revision_filter_skips_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_list_revision_filter_hits() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_revision_filter_hits_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_list_prefetch_hits() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_prefetch_hits_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_list_prefetch_misses() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_prefetch_misses_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_read_isolation_dispatch() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.read_isolation_dispatch_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_read_isolation_failures() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.read_isolation_failures_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_read_quorum_checks() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.read_quorum_checks_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_read_quorum_failures() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.read_quorum_failures_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_read_quorum_internal() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.read_quorum_internal_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_transport_closed_conn_unavailable() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.transport_closed_conn_unavailable_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_forward_retry_attempts() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.forward_retry_attempts_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gateway_read_ticket_hits() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.gateway_read_ticket_hits_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gateway_read_ticket_misses() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.gateway_read_ticket_misses_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gateway_singleflight_leader() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.gateway_singleflight_leader_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gateway_singleflight_waiter() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.gateway_singleflight_waiter_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gateway_singleflight_overflow() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.gateway_singleflight_overflow_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_gateway_singleflight_waiter_timeout() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.gateway_singleflight_waiter_timeouts_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_get_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_get_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_list_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_list_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_put_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_put_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_delete_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_delete_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_txn_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_txn_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_lease_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_lease_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_watch_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_watch_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_request_tier0_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.request_tier0_total.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_watch_delegate_reject_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_delegate_reject_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_watch_delegate_accept_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_delegate_accept_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_watch_active_streams() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_active_streams.fetch_add(1, Ordering::Relaxed);
}

pub fn dec_watch_active_streams() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_active_streams.fetch_sub(1, Ordering::Relaxed);
}

pub fn inc_watch_lagged_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_lagged_total.fetch_add(1, Ordering::Relaxed);
}

pub fn add_watch_lagged_skipped_events(skipped: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_lagged_skipped_events_total
        .fetch_add(skipped, Ordering::Relaxed);
}

pub fn inc_watch_resync_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_resync_total.fetch_add(1, Ordering::Relaxed);
}

pub fn add_watch_resync_keys(count: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_resync_keys_total
        .fetch_add(count, Ordering::Relaxed);
}

pub fn observe_watch_emit_lag_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_emit_lag_ms.observe(value_ms);
}

pub fn set_watch_dispatch_queue_depth(count: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_dispatch_queue_depth
        .store(count as u64, Ordering::Relaxed);
}

pub fn inc_watch_slow_cancels_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_slow_cancels_total.fetch_add(1, Ordering::Relaxed);
}

pub fn observe_watch_emit_batch_size(size: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.watch_emit_batch_size.observe(size as u64);
}

pub fn observe_large_value_upload_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.large_value_upload_ms.observe(value_ms);
}

pub fn inc_large_value_upload_failures_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.large_value_upload_failures_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn observe_large_value_hydrate_ms(value_ms: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.large_value_hydrate_ms.observe(value_ms);
}

pub fn inc_large_value_hydrate_cache_hits_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.large_value_hydrate_cache_hits_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_large_value_hydrate_cache_misses_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.large_value_hydrate_cache_misses_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_semantic_cache_hits() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.semantic_cache_hits_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn inc_semantic_cache_misses() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.semantic_cache_misses_total
        .fetch_add(1, Ordering::Relaxed);
}

pub fn add_list_stream_chunk(bytes: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_stream_chunks_total.fetch_add(1, Ordering::Relaxed);
    reg.list_stream_bytes_total
        .fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn add_list_stream_hydrated(values: usize, bytes: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.list_stream_hydrated_values_total
        .fetch_add(values as u64, Ordering::Relaxed);
    reg.list_stream_hydrated_bytes_total
        .fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn inc_profile_switch_total() {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.profile_switch_total.fetch_add(1, Ordering::Relaxed);
}

pub fn set_profile_active(profile: &str) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    let (k8s, omni, gateway, auto) = match profile {
        "kubernetes" => (1, 0, 0, 0),
        "omni" => (0, 1, 0, 0),
        "gateway" => (0, 0, 1, 0),
        _ => (0, 0, 0, 1),
    };
    reg.profile_active_kubernetes.store(k8s, Ordering::Relaxed);
    reg.profile_active_omni.store(omni, Ordering::Relaxed);
    reg.profile_active_gateway.store(gateway, Ordering::Relaxed);
    reg.profile_active_auto.store(auto, Ordering::Relaxed);
}

pub fn set_profile_applied_put_max_requests(value: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.profile_applied_put_max_requests
        .store(value as u64, Ordering::Relaxed);
}

pub fn set_profile_applied_put_linger_us(value: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.profile_applied_put_linger_us
        .store(value, Ordering::Relaxed);
}

pub fn set_profile_applied_prefetch_entries(value: usize) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.profile_applied_prefetch_entries
        .store(value as u64, Ordering::Relaxed);
}

pub fn set_profile_applied_bg_io_tokens_per_sec(value: u64) {
    let reg = metrics();
    if !reg.enabled() {
        return;
    }
    reg.profile_applied_bg_io_tokens_per_sec
        .store(value, Ordering::Relaxed);
}

pub fn latest_put_quorum_ack_ms() -> Option<u64> {
    let reg = metrics();
    if !reg.enabled() {
        return None;
    }
    if reg.quorum_ack_observations.load(Ordering::Relaxed) == 0 {
        None
    } else {
        Some(reg.latest_quorum_ack_ms.load(Ordering::Relaxed))
    }
}

pub fn render_prometheus() -> String {
    let reg = metrics();
    let mut out = String::new();
    if !reg.enabled() {
        let _ = writeln!(&mut out, "# astra metrics disabled");
        return out;
    }

    reg.put_queue_wait_ms.render(
        &mut out,
        "astra_put_queue_wait_seconds",
        "Queue wait time between enqueue and batch dispatch for put requests.",
    );
    reg.put_queue_wait_tier0_ms.render(
        &mut out,
        "astra_put_queue_wait_tier0_seconds",
        "Queue wait time between enqueue and batch dispatch for tier0 put requests.",
    );
    reg.put_queue_wait_normal_ms.render(
        &mut out,
        "astra_put_queue_wait_normal_seconds",
        "Queue wait time between enqueue and batch dispatch for normal put requests.",
    );
    reg.put_raft_write_ms.render(
        &mut out,
        "astra_put_raft_client_write_seconds",
        "Duration of raft client_write for put batches.",
    );
    reg.put_quorum_ack_ms.render(
        &mut out,
        "astra_put_quorum_ack_seconds",
        "Time from batch submit to commit advancement signal.",
    );
    reg.raft_append_rpc_client_ms.render(
        &mut out,
        "astra_raft_append_rpc_client_seconds",
        "Leader-side append_entries RPC duration.",
    );
    reg.raft_append_rpc_server_ms.render(
        &mut out,
        "astra_raft_append_rpc_server_seconds",
        "Follower-side append_entries handler duration.",
    );
    reg.raft_install_snapshot_rpc_client_ms.render(
        &mut out,
        "astra_raft_install_snapshot_rpc_client_seconds",
        "Leader-side install_snapshot RPC duration.",
    );
    reg.raft_install_snapshot_rpc_server_ms.render(
        &mut out,
        "astra_raft_install_snapshot_rpc_server_seconds",
        "Follower-side install_snapshot handler duration.",
    );
    reg.raft_snapshot_build_ms.render(
        &mut out,
        "astra_raft_snapshot_build_seconds",
        "Duration of raft snapshot materialization.",
    );
    reg.raft_snapshot_install_deserialize_ms.render(
        &mut out,
        "astra_raft_snapshot_install_deserialize_seconds",
        "Duration of follower snapshot payload deserialization.",
    );
    reg.raft_snapshot_install_load_ms.render(
        &mut out,
        "astra_raft_snapshot_install_load_seconds",
        "Duration of follower snapshot state load into the KV store.",
    );
    reg.put_batch_size.render(
        &mut out,
        "astra_put_batch_size",
        "Number of put operations per dispatched batch.",
    );
    reg.bg_io_throttle_wait_ms.render(
        &mut out,
        "astra_bg_io_throttle_wait_seconds",
        "Time spent waiting for background IO token-bucket budget.",
    );
    reg.bg_io_sqe_wait_ms.render(
        &mut out,
        "astra_bg_io_sqe_wait_seconds",
        "Time spent waiting for background SQE operation budget.",
    );
    reg.write_stall_delay_ms.render(
        &mut out,
        "astra_write_stall_delay_seconds",
        "Write stall delay injected by L0 pressure controller.",
    );
    reg.watch_emit_lag_ms.render(
        &mut out,
        "astra_watch_emit_lag_seconds",
        "Delay between commit timestamp and watch event emission.",
    );
    reg.watch_emit_batch_size.render(
        &mut out,
        "astra_watch_emit_batch_size",
        "Number of watch events emitted per streamed watch response.",
    );
    reg.large_value_upload_ms.render(
        &mut out,
        "astra_large_value_upload_seconds",
        "Duration of large-value tier upload operations.",
    );
    reg.large_value_hydrate_ms.render(
        &mut out,
        "astra_large_value_hydrate_seconds",
        "Duration of large-value hydrate read operations from object tier.",
    );
    reg.wal_checkpoint_ms.render(
        &mut out,
        "astra_wal_checkpoint_seconds",
        "Duration of WAL checkpoint compaction rewrites.",
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_put_batches_total Total dispatched put batches."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_batches_total counter");
    let _ = writeln!(
        &mut out,
        "astra_put_batches_total {}",
        reg.put_batches_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_put_batch_requests_total Total put requests dispatched through the batcher."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_batch_requests_total counter");
    let _ = writeln!(
        &mut out,
        "astra_put_batch_requests_total {}",
        reg.put_batch_requests_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_put_tier0_enqueued_total Total tier0 put requests enqueued."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_tier0_enqueued_total counter");
    let _ = writeln!(
        &mut out,
        "astra_put_tier0_enqueued_total {}",
        reg.put_tier0_enqueued_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_put_tier0_dispatch_total Total tier0 batches dispatched."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_tier0_dispatch_total counter");
    let _ = writeln!(
        &mut out,
        "astra_put_tier0_dispatch_total {}",
        reg.put_tier0_dispatch_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_put_tier0_bypass_write_pressure_total Total tier0 requests bypassing write-pressure control."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_put_tier0_bypass_write_pressure_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_put_tier0_bypass_write_pressure_total {}",
        reg.put_tier0_bypass_write_pressure_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_put_inflight_requests Current estimated in-flight put requests (queued + dispatched)."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_inflight_requests gauge");
    let _ = writeln!(
        &mut out,
        "astra_put_inflight_requests {}",
        reg.put_inflight_requests.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_put_tier0_queue_depth Current estimated queued tier0 put requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_tier0_queue_depth gauge");
    let _ = writeln!(
        &mut out,
        "astra_put_tier0_queue_depth {}",
        reg.put_tier0_queue_depth.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_put_normal_queue_depth Current estimated queued normal put requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_put_normal_queue_depth gauge");
    let _ = writeln!(
        &mut out,
        "astra_put_normal_queue_depth {}",
        reg.put_normal_queue_depth.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_effective_linger_microseconds Effective WAL linger currently applied."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_wal_effective_linger_microseconds gauge"
    );
    let _ = writeln!(
        &mut out,
        "astra_wal_effective_linger_microseconds {}",
        reg.wal_effective_linger_us.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_queue_depth Current queued WAL frames waiting for flush."
    );
    let _ = writeln!(&mut out, "# TYPE astra_wal_queue_depth gauge");
    let _ = writeln!(
        &mut out,
        "astra_wal_queue_depth {}",
        reg.wal_queue_depth.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_queue_bytes Current queued WAL payload bytes waiting for flush."
    );
    let _ = writeln!(&mut out, "# TYPE astra_wal_queue_bytes gauge");
    let _ = writeln!(
        &mut out,
        "astra_wal_queue_bytes {}",
        reg.wal_queue_bytes.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_logical_bytes Current durable WAL bytes containing replayable records."
    );
    let _ = writeln!(&mut out, "# TYPE astra_wal_logical_bytes gauge");
    let _ = writeln!(
        &mut out,
        "astra_wal_logical_bytes {}",
        reg.wal_logical_bytes.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_allocated_bytes Current allocated WAL file bytes on disk."
    );
    let _ = writeln!(&mut out, "# TYPE astra_wal_allocated_bytes gauge");
    let _ = writeln!(
        &mut out,
        "astra_wal_allocated_bytes {}",
        reg.wal_allocated_bytes.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_checkpoint_total Total WAL checkpoint compaction rewrites."
    );
    let _ = writeln!(&mut out, "# TYPE astra_wal_checkpoint_total counter");
    let _ = writeln!(
        &mut out,
        "astra_wal_checkpoint_total {}",
        reg.wal_checkpoint_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_wal_checkpoint_reclaimed_bytes_total Total physical WAL bytes reclaimed by checkpoint compaction."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_wal_checkpoint_reclaimed_bytes_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_wal_checkpoint_reclaimed_bytes_total {}",
        reg.wal_checkpoint_reclaimed_bytes_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_lsm_synth_l0_files Synthetic L0 file pressure estimate derived from WAL queues."
    );
    let _ = writeln!(&mut out, "# TYPE astra_lsm_synth_l0_files gauge");
    let _ = writeln!(
        &mut out,
        "astra_lsm_synth_l0_files {}",
        reg.lsm_synth_l0_files.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_bg_io_tokens_available Current available tokens in the background IO token bucket."
    );
    let _ = writeln!(&mut out, "# TYPE astra_bg_io_tokens_available gauge");
    let _ = writeln!(
        &mut out,
        "astra_bg_io_tokens_available {}",
        reg.bg_io_tokens_available.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_bg_io_sqe_tokens_available Current available tokens in the background SQE token bucket."
    );
    let _ = writeln!(&mut out, "# TYPE astra_bg_io_sqe_tokens_available gauge");
    let _ = writeln!(
        &mut out,
        "astra_bg_io_sqe_tokens_available {}",
        reg.bg_io_sqe_tokens_available.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_bg_io_throttle_events_total Total background IO throttle wait events."
    );
    let _ = writeln!(&mut out, "# TYPE astra_bg_io_throttle_events_total counter");
    let _ = writeln!(
        &mut out,
        "astra_bg_io_throttle_events_total {}",
        reg.bg_io_throttle_events_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_bg_io_sqe_throttle_events_total Total background SQE throttle wait events."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_bg_io_sqe_throttle_events_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_bg_io_sqe_throttle_events_total {}",
        reg.bg_io_sqe_throttle_events_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_write_stall_events_total Total write requests delayed by L0 pressure control."
    );
    let _ = writeln!(&mut out, "# TYPE astra_write_stall_events_total counter");
    let _ = writeln!(
        &mut out,
        "astra_write_stall_events_total {}",
        reg.write_stall_events_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_write_reject_events_total Total write requests rejected by L0 pressure control."
    );
    let _ = writeln!(&mut out, "# TYPE astra_write_reject_events_total counter");
    let _ = writeln!(
        &mut out,
        "astra_write_reject_events_total {}",
        reg.write_reject_events_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_write_stall_band_5_events_total Total writes delayed by the L0==5 pressure band."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_write_stall_band_5_events_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_write_stall_band_5_events_total {}",
        reg.write_stall_band_5_events_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_write_stall_band_6_events_total Total writes delayed by the L0==6 pressure band."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_write_stall_band_6_events_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_write_stall_band_6_events_total {}",
        reg.write_stall_band_6_events_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_write_stall_band_7_plus_events_total Total writes delayed by the L0>=7 pressure band."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_write_stall_band_7_plus_events_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_write_stall_band_7_plus_events_total {}",
        reg.write_stall_band_7_plus_events_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_list_prefix_filter_skips_total LIST requests skipped by prefix negative filter."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_list_prefix_filter_skips_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_list_prefix_filter_skips_total {}",
        reg.list_prefix_filter_skips_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_list_prefix_filter_hits_total LIST requests processed after prefix filter positive hit."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_list_prefix_filter_hits_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_list_prefix_filter_hits_total {}",
        reg.list_prefix_filter_hits_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_list_revision_filter_skips_total LIST requests skipped by prefix+revision filter."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_list_revision_filter_skips_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_list_revision_filter_skips_total {}",
        reg.list_revision_filter_skips_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_list_revision_filter_hits_total LIST requests processed after prefix+revision filter hit."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_list_revision_filter_hits_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_list_revision_filter_hits_total {}",
        reg.list_revision_filter_hits_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_list_prefetch_hits_total LIST prefetch cache hits."
    );
    let _ = writeln!(&mut out, "# TYPE astra_list_prefetch_hits_total counter");
    let _ = writeln!(
        &mut out,
        "astra_list_prefetch_hits_total {}",
        reg.list_prefetch_hits_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_list_prefetch_misses_total LIST prefetch cache misses."
    );
    let _ = writeln!(&mut out, "# TYPE astra_list_prefetch_misses_total counter");
    let _ = writeln!(
        &mut out,
        "astra_list_prefetch_misses_total {}",
        reg.list_prefetch_misses_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_list_stream_chunks_total Total streamed LIST chunks emitted via admin StreamList."
    );
    let _ = writeln!(&mut out, "# TYPE astra_list_stream_chunks_total counter");
    let _ = writeln!(
        &mut out,
        "astra_list_stream_chunks_total {}",
        reg.list_stream_chunks_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_list_stream_bytes_total Total bytes emitted via admin StreamList chunks."
    );
    let _ = writeln!(&mut out, "# TYPE astra_list_stream_bytes_total counter");
    let _ = writeln!(
        &mut out,
        "astra_list_stream_bytes_total {}",
        reg.list_stream_bytes_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_list_stream_hydrated_values_total Total tiered values hydrated while emitting admin StreamList chunks."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_list_stream_hydrated_values_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_list_stream_hydrated_values_total {}",
        reg.list_stream_hydrated_values_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_list_stream_hydrated_bytes_total Total hydrated payload bytes emitted via admin StreamList."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_list_stream_hydrated_bytes_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_list_stream_hydrated_bytes_total {}",
        reg.list_stream_hydrated_bytes_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_semantic_cache_hits_total Total semantic hot-cache hits."
    );
    let _ = writeln!(&mut out, "# TYPE astra_semantic_cache_hits_total counter");
    let _ = writeln!(
        &mut out,
        "astra_semantic_cache_hits_total {}",
        reg.semantic_cache_hits_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_semantic_cache_misses_total Total semantic hot-cache misses."
    );
    let _ = writeln!(&mut out, "# TYPE astra_semantic_cache_misses_total counter");
    let _ = writeln!(
        &mut out,
        "astra_semantic_cache_misses_total {}",
        reg.semantic_cache_misses_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_read_isolation_dispatch_total Total read requests routed through isolated read lane."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_read_isolation_dispatch_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_read_isolation_dispatch_total {}",
        reg.read_isolation_dispatch_total.load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_read_isolation_failures_total Total isolated read dispatch failures that required inline fallback."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_read_isolation_failures_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_read_isolation_failures_total {}",
        reg.read_isolation_failures_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_read_quorum_checks_total Total linearizable read quorum checks attempted."
    );
    let _ = writeln!(&mut out, "# TYPE astra_read_quorum_checks_total counter");
    let _ = writeln!(
        &mut out,
        "astra_read_quorum_checks_total {}",
        reg.read_quorum_checks_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_read_quorum_failures_total Total linearizable read quorum check failures."
    );
    let _ = writeln!(&mut out, "# TYPE astra_read_quorum_failures_total counter");
    let _ = writeln!(
        &mut out,
        "astra_read_quorum_failures_total {}",
        reg.read_quorum_failures_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_read_quorum_internal_total Total quorum-related internal read errors."
    );
    let _ = writeln!(&mut out, "# TYPE astra_read_quorum_internal_total counter");
    let _ = writeln!(
        &mut out,
        "astra_read_quorum_internal_total {}",
        reg.read_quorum_internal_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_transport_closed_conn_unavailable_total Total unavailable responses attributed to closed transport connections."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_transport_closed_conn_unavailable_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_transport_closed_conn_unavailable_total {}",
        reg.transport_closed_conn_unavailable_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_forward_retry_attempts_total Total forwarded RPC retry attempts."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_forward_retry_attempts_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_forward_retry_attempts_total {}",
        reg.forward_retry_attempts_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_gateway_read_ticket_hits_total Total gateway read requests that reused an active read ticket."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_gateway_read_ticket_hits_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_gateway_read_ticket_hits_total {}",
        reg.gateway_read_ticket_hits_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_gateway_read_ticket_misses_total Total gateway read requests that required a fresh quorum check."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_gateway_read_ticket_misses_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_gateway_read_ticket_misses_total {}",
        reg.gateway_read_ticket_misses_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_gateway_singleflight_leader_total Total gateway read requests elected as singleflight leaders."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_gateway_singleflight_leader_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_gateway_singleflight_leader_total {}",
        reg.gateway_singleflight_leader_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_gateway_singleflight_waiter_total Total gateway read requests collapsed behind an in-flight leader request."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_gateway_singleflight_waiter_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_gateway_singleflight_waiter_total {}",
        reg.gateway_singleflight_waiter_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_gateway_singleflight_overflow_total Total gateway read requests that bypassed singleflight due to waiter cap."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_gateway_singleflight_overflow_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_gateway_singleflight_overflow_total {}",
        reg.gateway_singleflight_overflow_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_gateway_singleflight_waiter_timeouts_total Total gateway waiter requests that timed out waiting for singleflight leader completion."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_gateway_singleflight_waiter_timeouts_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_gateway_singleflight_waiter_timeouts_total {}",
        reg.gateway_singleflight_waiter_timeouts_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_request_get_total Total GET-like range requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_get_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_get_total {}",
        reg.request_get_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_list_total Total LIST/prefix range requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_list_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_list_total {}",
        reg.request_list_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_put_total Total put requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_put_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_put_total {}",
        reg.request_put_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_delete_total Total delete range requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_delete_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_delete_total {}",
        reg.request_delete_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_txn_total Total txn requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_txn_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_txn_total {}",
        reg.request_txn_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_lease_total Total lease API requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_lease_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_lease_total {}",
        reg.request_lease_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_watch_total Total watch stream requests."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_watch_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_watch_total {}",
        reg.request_watch_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_request_tier0_total Total requests classified as tier0 critical."
    );
    let _ = writeln!(&mut out, "# TYPE astra_request_tier0_total counter");
    let _ = writeln!(
        &mut out,
        "astra_request_tier0_total {}",
        reg.request_tier0_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_delegate_reject_total Total watch requests rejected by watch role policy."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_delegate_reject_total counter");
    let _ = writeln!(
        &mut out,
        "astra_watch_delegate_reject_total {}",
        reg.watch_delegate_reject_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_delegate_accept_total Total watch requests accepted on non-leader nodes by delegation policy."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_delegate_accept_total counter");
    let _ = writeln!(
        &mut out,
        "astra_watch_delegate_accept_total {}",
        reg.watch_delegate_accept_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_active_streams Current number of active watch streams."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_active_streams gauge");
    let _ = writeln!(
        &mut out,
        "astra_watch_active_streams {}",
        reg.watch_active_streams.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_lagged_total Total watch lag events observed from the broadcast ring."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_lagged_total counter");
    let _ = writeln!(
        &mut out,
        "astra_watch_lagged_total {}",
        reg.watch_lagged_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_lagged_skipped_events_total Total watch events skipped by lagged watch receivers."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_watch_lagged_skipped_events_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_watch_lagged_skipped_events_total {}",
        reg.watch_lagged_skipped_events_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_resync_total Total watch lag-compression resync attempts."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_resync_total counter");
    let _ = writeln!(
        &mut out,
        "astra_watch_resync_total {}",
        reg.watch_resync_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_resync_keys_total Total keys emitted via watch lag-compression resync."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_resync_keys_total counter");
    let _ = writeln!(
        &mut out,
        "astra_watch_resync_keys_total {}",
        reg.watch_resync_keys_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_dispatch_queue_depth Current watch stream response queue depth estimate."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_dispatch_queue_depth gauge");
    let _ = writeln!(
        &mut out,
        "astra_watch_dispatch_queue_depth {}",
        reg.watch_dispatch_queue_depth.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_watch_slow_cancels_total Total watch streams canceled due to slow consumer backpressure."
    );
    let _ = writeln!(&mut out, "# TYPE astra_watch_slow_cancels_total counter");
    let _ = writeln!(
        &mut out,
        "astra_watch_slow_cancels_total {}",
        reg.watch_slow_cancels_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_raft_append_rpc_failures_total Total failed append_entries RPC attempts."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_raft_append_rpc_failures_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_raft_append_rpc_failures_total {}",
        reg.raft_append_rpc_failures_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_raft_install_snapshot_rpc_failures_total Total failed install_snapshot RPC attempts."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_raft_install_snapshot_rpc_failures_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_raft_install_snapshot_rpc_failures_total {}",
        reg.raft_install_snapshot_rpc_failures_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_large_value_upload_failures_total Total failed large-value tier upload attempts."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_large_value_upload_failures_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_large_value_upload_failures_total {}",
        reg.large_value_upload_failures_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_large_value_hydrate_cache_hits_total Total hydrate cache hits for large-value tier references."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_large_value_hydrate_cache_hits_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_large_value_hydrate_cache_hits_total {}",
        reg.large_value_hydrate_cache_hits_total
            .load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_large_value_hydrate_cache_misses_total Total hydrate cache misses for large-value tier references."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_large_value_hydrate_cache_misses_total counter"
    );
    let _ = writeln!(
        &mut out,
        "astra_large_value_hydrate_cache_misses_total {}",
        reg.large_value_hydrate_cache_misses_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_switch_total Total profile transitions applied by the governor."
    );
    let _ = writeln!(&mut out, "# TYPE astra_profile_switch_total counter");
    let _ = writeln!(
        &mut out,
        "astra_profile_switch_total {}",
        reg.profile_switch_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_active_kubernetes Active profile gauge (1 when active)."
    );
    let _ = writeln!(&mut out, "# TYPE astra_profile_active_kubernetes gauge");
    let _ = writeln!(
        &mut out,
        "astra_profile_active_kubernetes {}",
        reg.profile_active_kubernetes.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_active_omni Active profile gauge (1 when active)."
    );
    let _ = writeln!(&mut out, "# TYPE astra_profile_active_omni gauge");
    let _ = writeln!(
        &mut out,
        "astra_profile_active_omni {}",
        reg.profile_active_omni.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_active_gateway Active profile gauge (1 when active)."
    );
    let _ = writeln!(&mut out, "# TYPE astra_profile_active_gateway gauge");
    let _ = writeln!(
        &mut out,
        "astra_profile_active_gateway {}",
        reg.profile_active_gateway.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_active_auto Active profile gauge (1 when active)."
    );
    let _ = writeln!(&mut out, "# TYPE astra_profile_active_auto gauge");
    let _ = writeln!(
        &mut out,
        "astra_profile_active_auto {}",
        reg.profile_active_auto.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_applied_put_max_requests Last profile-applied put max_requests."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_profile_applied_put_max_requests gauge"
    );
    let _ = writeln!(
        &mut out,
        "astra_profile_applied_put_max_requests {}",
        reg.profile_applied_put_max_requests.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_applied_put_linger_microseconds Last profile-applied put linger in microseconds."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_profile_applied_put_linger_microseconds gauge"
    );
    let _ = writeln!(
        &mut out,
        "astra_profile_applied_put_linger_microseconds {}",
        reg.profile_applied_put_linger_us.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_applied_prefetch_entries Last profile-applied list prefetch cache entry cap."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_profile_applied_prefetch_entries gauge"
    );
    let _ = writeln!(
        &mut out,
        "astra_profile_applied_prefetch_entries {}",
        reg.profile_applied_prefetch_entries.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        &mut out,
        "# HELP astra_profile_applied_bg_io_tokens_per_sec Last profile-applied background IO token rate."
    );
    let _ = writeln!(
        &mut out,
        "# TYPE astra_profile_applied_bg_io_tokens_per_sec gauge"
    );
    let _ = writeln!(
        &mut out,
        "astra_profile_applied_bg_io_tokens_per_sec {}",
        reg.profile_applied_bg_io_tokens_per_sec
            .load(Ordering::Relaxed)
    );

    out
}
