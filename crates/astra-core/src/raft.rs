use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::io::{Cursor, Read};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use astra_proto::astraraftpb::internal_raft_client::InternalRaftClient;
use astra_proto::astraraftpb::internal_raft_server::InternalRaft;
use astra_proto::astraraftpb::RaftBytes;
use openraft::entry::{Entry, EntryPayload};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, LogId, LogState, Raft, RaftLogReader, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify, RwLock};
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};
use tracing::{debug, info, warn};

use crate::config::{AstraConfig, WalIoEngine};
use crate::errors::StoreError;
use crate::metrics;
use crate::store::{
    DeleteOutput, KvStore, LeaseGrantOutput, LeaseRevokeOutput, PutOutput, RangeOutput,
    SnapshotState, ValueEntry,
};

openraft::declare_raft_types!(
    pub AstraTypeConfig:
        D = AstraWriteRequest,
        R = AstraWriteResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<AstraTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraBatchPutOp {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lease: i64,
    pub ignore_value: bool,
    pub ignore_lease: bool,
    #[serde(default)]
    pub prev_kv: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraBatchPutRefOp {
    pub key: Vec<u8>,
    pub value_idx: u32,
    pub lease: i64,
    pub ignore_value: bool,
    pub ignore_lease: bool,
    #[serde(default)]
    pub prev_kv: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraTokenValue {
    pub token_id: u32,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraBatchPutTokenOp {
    pub key: Vec<u8>,
    pub token_id: u32,
    pub lease: i64,
    pub ignore_value: bool,
    pub ignore_lease: bool,
    #[serde(default)]
    pub prev_kv: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraBatchPutResult {
    pub revision: i64,
    pub prev: Option<ValueEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraTxnCmpResult {
    Equal,
    Greater,
    Less,
    NotEqual,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraTxnCmpTarget {
    Version,
    CreateRevision,
    ModRevision,
    Value,
    Lease,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraTxnCmpValue {
    I64(i64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstraTxnCompare {
    pub result: AstraTxnCmpResult,
    pub target: AstraTxnCmpTarget,
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub target_value: AstraTxnCmpValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraTxnOp {
    Range {
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
        revision: i64,
        keys_only: bool,
        count_only: bool,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        ignore_value: bool,
        ignore_lease: bool,
        prev_kv: bool,
    },
    DeleteRange {
        key: Vec<u8>,
        range_end: Vec<u8>,
        prev_kv: bool,
    },
    Txn {
        compare: Vec<AstraTxnCompare>,
        success: Vec<AstraTxnOp>,
        failure: Vec<AstraTxnOp>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraTxnOpResponse {
    Range {
        revision: i64,
        count: i64,
        more: bool,
        kvs: Vec<(Vec<u8>, ValueEntry)>,
    },
    Put {
        revision: i64,
        prev: Option<ValueEntry>,
    },
    Delete {
        revision: i64,
        deleted: i64,
        prev_kvs: Vec<(Vec<u8>, ValueEntry)>,
    },
    Txn {
        revision: i64,
        succeeded: bool,
        responses: Vec<AstraTxnOpResponse>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraWriteRequest {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        ignore_value: bool,
        ignore_lease: bool,
        #[serde(default)]
        prev_kv: bool,
    },
    PutBatch {
        ops: Vec<AstraBatchPutOp>,
    },
    PutBatchRef {
        #[serde(default)]
        batch_id: u64,
        #[serde(default)]
        submit_ts_micros: u64,
        values: Vec<Vec<u8>>,
        ops: Vec<AstraBatchPutRefOp>,
    },
    PutBatchTokenized {
        #[serde(default)]
        batch_id: u64,
        #[serde(default)]
        submit_ts_micros: u64,
        #[serde(default)]
        dict_epoch: u64,
        #[serde(default)]
        dict_additions: Vec<AstraTokenValue>,
        ops: Vec<AstraBatchPutTokenOp>,
    },
    DeleteRange {
        key: Vec<u8>,
        range_end: Vec<u8>,
        prev_kv: bool,
    },
    Txn {
        compare: Vec<AstraTxnCompare>,
        success: Vec<AstraTxnOp>,
        failure: Vec<AstraTxnOp>,
    },
    Compact {
        revision: i64,
    },
    LeaseGrant {
        id: i64,
        ttl: i64,
    },
    LeaseRevoke {
        id: i64,
    },
    LeaseKeepAlive {
        id: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AstraWriteResponse {
    Put {
        revision: i64,
        prev: Option<ValueEntry>,
    },
    PutBatch {
        results: Vec<AstraBatchPutResult>,
    },
    Delete {
        revision: i64,
        deleted: i64,
        prev_kvs: Vec<(Vec<u8>, ValueEntry)>,
    },
    Txn {
        revision: i64,
        succeeded: bool,
        responses: Vec<AstraTxnOpResponse>,
    },
    Compact {
        revision: i64,
        compact_revision: i64,
    },
    LeaseGrant {
        revision: i64,
        id: i64,
        ttl: i64,
    },
    LeaseRevoke {
        revision: i64,
        deleted: i64,
    },
    LeaseKeepAlive {
        revision: i64,
        id: i64,
        ttl: i64,
    },
    LeaseTtl {
        revision: i64,
        id: i64,
        ttl: i64,
        granted_ttl: i64,
        keys: Vec<Vec<u8>>,
    },
    LeaseLeases {
        revision: i64,
        leases: Vec<i64>,
    },
    Empty,
}

#[derive(Debug, Clone)]
pub struct WalBatchConfig {
    pub max_batch_requests: usize,
    pub max_batch_bytes: usize,
    pub max_linger: Duration,
    pub low_concurrency_threshold: usize,
    pub low_linger: Duration,
    pub channel_capacity: usize,
    pub pending_limit: usize,
    pub segment_bytes: u64,
    pub io_engine: WalIoEngine,
}

impl Default for WalBatchConfig {
    fn default() -> Self {
        Self {
            max_batch_requests: 1_000,
            max_batch_bytes: 8 * 1024 * 1024,
            max_linger: Duration::from_millis(2),
            low_concurrency_threshold: 5,
            low_linger: Duration::ZERO,
            channel_capacity: 8_192,
            pending_limit: 2_000,
            segment_bytes: 64 * 1024 * 1024,
            io_engine: WalIoEngine::Auto,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftBootstrap {
    pub config: Arc<openraft::Config>,
}

impl RaftBootstrap {
    pub fn new(cfg: &AstraConfig) -> anyhow::Result<Self> {
        let cfg = openraft::Config {
            cluster_name: "astra".to_string(),
            election_timeout_min: cfg.raft_election_timeout_min_ms,
            election_timeout_max: cfg.raft_election_timeout_max_ms,
            heartbeat_interval: cfg.raft_heartbeat_interval_ms,
            snapshot_max_chunk_size: 2 * 1024 * 1024,
            max_payload_entries: cfg.raft_max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(512),
            replication_lag_threshold: cfg.raft_replication_lag_threshold,
            max_in_snapshot_log_to_keep: 64,
            purge_batch_size: 256,
            ..Default::default()
        };

        let cfg = cfg.validate()?;
        Ok(Self {
            config: Arc::new(cfg),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DurableRecord {
    Append {
        entries: Vec<Entry<AstraTypeConfig>>,
    },
    Truncate {
        since: LogId<u64>,
    },
    Purge {
        upto: LogId<u64>,
    },
    Vote {
        vote: Vote<u64>,
    },
    Committed {
        committed: Option<LogId<u64>>,
    },
}

#[derive(Debug)]
struct LogStoreState {
    logs: BTreeMap<u64, Entry<AstraTypeConfig>>,
    last_purged_log_id: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
}

impl Default for LogStoreState {
    fn default() -> Self {
        Self {
            logs: BTreeMap::new(),
            last_purged_log_id: None,
            vote: None,
            committed: None,
        }
    }
}

fn align_up(value: usize, align: usize) -> usize {
    if value % align == 0 {
        value
    } else {
        value + (align - (value % align))
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or_default()
}

fn encode_wal_frame(payload: &[u8], block_size: usize) -> Vec<u8> {
    let mut header = Vec::with_capacity(8);
    header.extend_from_slice(b"ASTR");
    header.extend_from_slice(&(payload.len() as u32).to_le_bytes());

    let total = header.len() + payload.len();
    let padded = align_up(total, block_size);
    let mut out = Vec::with_capacity(padded);
    out.extend_from_slice(&header);
    out.extend_from_slice(payload);
    out.resize(padded, 0_u8);
    out
}

#[derive(Debug)]
struct PosixBlockDevice {
    file: std::fs::File,
    offset: u64,
    allocated: u64,
    segment_bytes: u64,
    block_size: usize,
}

impl PosixBlockDevice {
    fn open(path: &Path, offset: u64, segment_bytes: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(path.parent().unwrap_or_else(|| Path::new(".")))?;

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(false)
            .write(true)
            .open(path)?;

        let mut this = Self {
            allocated: file.metadata()?.len(),
            file,
            offset,
            segment_bytes,
            block_size: 4096,
        };

        if this.allocated == 0 {
            this.preallocate(this.segment_bytes)?;
        }
        this.ensure_capacity(this.offset)?;
        Ok(this)
    }

    fn preallocate(&mut self, bytes: u64) -> std::io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let rc = unsafe {
                libc::fallocate(fd, 0, self.allocated as libc::off_t, bytes as libc::off_t)
            };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            self.allocated = self.allocated.saturating_add(bytes);
            info!(
                allocated_bytes = self.allocated,
                segment_bytes = self.segment_bytes,
                "wal fallocate preallocation complete"
            );
            return Ok(());
        }

        #[cfg(not(target_os = "linux"))]
        {
            let next = self.allocated.saturating_add(bytes);
            self.file.set_len(next)?;
            self.allocated = next;
            return Ok(());
        }
    }

    fn ensure_capacity(&mut self, required_end: u64) -> std::io::Result<()> {
        while required_end > self.allocated {
            self.preallocate(self.segment_bytes)?;
        }
        Ok(())
    }

    fn append_payload(&mut self, payload: &[u8]) -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;

        let frame = encode_wal_frame(payload, self.block_size);
        let required_end = self.offset.saturating_add(frame.len() as u64);
        self.ensure_capacity(required_end)?;

        let fd = self.file.as_raw_fd();
        let wrote = unsafe {
            libc::pwrite(
                fd,
                frame.as_ptr() as *const libc::c_void,
                frame.len(),
                self.offset as libc::off_t,
            )
        };
        if wrote < 0 || wrote as usize != frame.len() {
            return Err(std::io::Error::last_os_error());
        }
        self.offset = required_end;
        Ok(())
    }

    fn sync_data(&mut self) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let rc = unsafe { libc::fdatasync(fd) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            return Ok(());
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.file.sync_data()
        }
    }
}

#[derive(Debug)]
enum WalDevice {
    Posix(PosixBlockDevice),
}

impl WalDevice {
    fn open(
        path: &Path,
        offset: u64,
        cfg: &WalBatchConfig,
    ) -> std::io::Result<(Self, &'static str)> {
        if cfg.io_engine == WalIoEngine::IoUring {
            warn!(
                "io_uring requested but direct append path runs on tokio runtime; using posix wal engine"
            );
        }

        let d = PosixBlockDevice::open(path, offset, cfg.segment_bytes)?;
        info!("wal io engine selected: posix");
        Ok((WalDevice::Posix(d), "posix"))
    }

    fn append_payload(&mut self, payload: &[u8]) -> std::io::Result<()> {
        match self {
            WalDevice::Posix(d) => d.append_payload(payload),
        }
    }

    fn sync_data(&mut self) -> std::io::Result<()> {
        match self {
            WalDevice::Posix(d) => d.sync_data(),
        }
    }
}

enum WalWriteCompletion {
    Append(LogFlushed<AstraTypeConfig>),
    Waiter(oneshot::Sender<std::io::Result<()>>),
}

impl WalWriteCompletion {
    fn complete_ok(self) {
        match self {
            WalWriteCompletion::Append(callback) => callback.log_io_completed(Ok(())),
            WalWriteCompletion::Waiter(tx) => {
                let _ = tx.send(Ok(()));
            }
        }
    }

    fn complete_err(self, err_msg: &str) {
        match self {
            WalWriteCompletion::Append(callback) => {
                callback.log_io_completed(Err(std::io::Error::other(err_msg.to_string())))
            }
            WalWriteCompletion::Waiter(tx) => {
                let _ = tx.send(Err(std::io::Error::other(err_msg.to_string())));
            }
        }
    }
}

struct WalQueueItem {
    payload: Vec<u8>,
    append_entries: usize,
    timeline_batch_id: Option<u64>,
    timeline_submit_ts_micros: Option<u64>,
    completion: WalWriteCompletion,
}

#[derive(Debug, Clone, Copy)]
struct TimelineMarker {
    batch_id: u64,
    submit_ts_micros: u64,
}

#[derive(Default)]
struct WalQueueState {
    queue: VecDeque<WalQueueItem>,
    queued_bytes: usize,
    flushing: bool,
}

pub struct AstraLogStore {
    inner: Arc<RwLock<LogStoreState>>,
    wal_device: Arc<Mutex<WalDevice>>,
    wal_cfg: WalBatchConfig,
    wal_queue: Arc<AsyncMutex<WalQueueState>>,
    wal_queue_space: Arc<Notify>,
    last_flushed_committed: Option<LogId<u64>>,
    last_committed_flush_at: Instant,
    timeline_by_log_index: BTreeMap<u64, TimelineMarker>,
}

#[derive(Debug, Clone)]
pub struct AstraLogReader {
    inner: Arc<RwLock<LogStoreState>>,
}

impl AstraLogStore {
    const COMMITTED_FLUSH_INTERVAL: Duration = Duration::from_millis(25);
    const COMMITTED_FLUSH_STEP: u64 = 64;

    pub async fn open(data_dir: &Path, mut cfg: WalBatchConfig) -> Result<Self, StorageError<u64>> {
        cfg.max_batch_requests = cfg.max_batch_requests.max(1);
        cfg.max_batch_bytes = cfg.max_batch_bytes.max(4 * 1024);
        cfg.pending_limit = cfg.pending_limit.max(1);
        cfg.low_concurrency_threshold = cfg.low_concurrency_threshold.max(1);

        std::fs::create_dir_all(data_dir).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                e,
            )
        })?;

        let wal_path = data_dir.join("unified-raft.wal");
        let mut state = LogStoreState::default();
        let wal_offset = replay_wal(&wal_path, &mut state)?;
        let (device, engine) = WalDevice::open(&wal_path, wal_offset, &cfg).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                e,
            )
        })?;

        let last_flushed_committed = state.committed.clone();

        let inner = Arc::new(RwLock::new(state));

        info!(
            wal_path = %data_dir.join("unified-raft.wal").display(),
            max_batch_requests = cfg.max_batch_requests,
            max_batch_bytes = cfg.max_batch_bytes,
            max_linger_us = cfg.max_linger.as_micros(),
            low_concurrency_threshold = cfg.low_concurrency_threshold,
            low_linger_us = cfg.low_linger.as_micros(),
            pending_limit = cfg.pending_limit,
            segment_bytes = cfg.segment_bytes,
            io_engine = %cfg.io_engine.as_str(),
            selected_engine = engine,
            "wal direct append path initialized"
        );

        let mut timeline_by_log_index = BTreeMap::new();
        {
            let guard = inner.read().await;
            for (log_index, entry) in &guard.logs {
                if let EntryPayload::Normal(req) = &entry.payload {
                    match req {
                        AstraWriteRequest::PutBatchRef {
                            batch_id,
                            submit_ts_micros,
                            ..
                        }
                        | AstraWriteRequest::PutBatchTokenized {
                            batch_id,
                            submit_ts_micros,
                            ..
                        } if *batch_id > 0 && *submit_ts_micros > 0 => {
                            timeline_by_log_index.insert(
                                *log_index,
                                TimelineMarker {
                                    batch_id: *batch_id,
                                    submit_ts_micros: *submit_ts_micros,
                                },
                            );
                        }
                        _ => {}
                    }
                }
            }
        }

        let store = Self {
            inner,
            wal_device: Arc::new(Mutex::new(device)),
            wal_cfg: cfg,
            wal_queue: Arc::new(AsyncMutex::new(WalQueueState::default())),
            wal_queue_space: Arc::new(Notify::new()),
            last_flushed_committed,
            last_committed_flush_at: Instant::now(),
            timeline_by_log_index,
        };
        metrics::set_wal_queue_depth(0);
        metrics::set_wal_queue_bytes(0);
        Ok(store)
    }

    pub async fn wal_queue_snapshot(&self) -> (usize, usize) {
        let guard = self.wal_queue.lock().await;
        (guard.queue.len(), guard.queued_bytes)
    }

    fn encode_records_payload(records: Vec<DurableRecord>) -> Result<Vec<u8>, StorageError<u64>> {
        encode_durable_records(records).map_err(|e| {
            StorageError::from_io_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e)
        })
    }

    fn logical_write_ops(entry: &Entry<AstraTypeConfig>) -> usize {
        match &entry.payload {
            EntryPayload::Normal(AstraWriteRequest::PutBatch { ops }) => ops.len().max(1),
            EntryPayload::Normal(AstraWriteRequest::PutBatchRef { ops, .. }) => ops.len().max(1),
            EntryPayload::Normal(AstraWriteRequest::PutBatchTokenized { ops, .. }) => {
                ops.len().max(1)
            }
            EntryPayload::Normal(_) => 1,
            EntryPayload::Blank | EntryPayload::Membership(_) => 0,
        }
    }

    fn extract_timeline_meta(entries: &[Entry<AstraTypeConfig>]) -> (Option<u64>, Option<u64>) {
        for entry in entries {
            if let EntryPayload::Normal(req) = &entry.payload {
                match req {
                    AstraWriteRequest::PutBatchRef {
                        batch_id,
                        submit_ts_micros,
                        ..
                    }
                    | AstraWriteRequest::PutBatchTokenized {
                        batch_id,
                        submit_ts_micros,
                        ..
                    } => {
                        return (
                            if *batch_id > 0 { Some(*batch_id) } else { None },
                            if *submit_ts_micros > 0 {
                                Some(*submit_ts_micros)
                            } else {
                                None
                            },
                        );
                    }
                    _ => {}
                }
            }
        }
        (None, None)
    }

    fn extract_timeline_markers(entries: &[Entry<AstraTypeConfig>]) -> Vec<(u64, TimelineMarker)> {
        let mut out = Vec::new();
        for entry in entries {
            if let EntryPayload::Normal(req) = &entry.payload {
                match req {
                    AstraWriteRequest::PutBatchRef {
                        batch_id,
                        submit_ts_micros,
                        ..
                    }
                    | AstraWriteRequest::PutBatchTokenized {
                        batch_id,
                        submit_ts_micros,
                        ..
                    } if *batch_id > 0 && *submit_ts_micros > 0 => {
                        out.push((
                            entry.log_id.index,
                            TimelineMarker {
                                batch_id: *batch_id,
                                submit_ts_micros: *submit_ts_micros,
                            },
                        ));
                    }
                    _ => {}
                }
            }
        }
        out
    }

    async fn await_wal_flush(
        rx: oneshot::Receiver<std::io::Result<()>>,
    ) -> Result<(), StorageError<u64>> {
        match rx.await {
            Ok(result) => result.map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    e,
                )
            }),
            Err(e) => Err(StorageError::from_io_error(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Write,
                std::io::Error::other(format!("wal flush callback dropped: {e}")),
            )),
        }
    }

    fn spawn_wal_flush_worker(&self) {
        let wal_device = self.wal_device.clone();
        let wal_cfg = self.wal_cfg.clone();
        let wal_queue = self.wal_queue.clone();
        let wal_queue_space = self.wal_queue_space.clone();

        tokio::spawn(async move {
            loop {
                let put_inflight = metrics::current_put_inflight_requests();
                let mut effective_linger = wal_cfg.max_linger;
                if put_inflight < wal_cfg.low_concurrency_threshold as u64 {
                    effective_linger = wal_cfg.low_linger;
                }
                metrics::set_wal_effective_linger_us(effective_linger.as_micros() as u64);

                if effective_linger > Duration::ZERO {
                    let should_linger = {
                        let guard = wal_queue.lock().await;
                        !guard.queue.is_empty()
                            && guard.queue.len() < wal_cfg.max_batch_requests
                            && guard.queued_bytes < wal_cfg.max_batch_bytes
                    };
                    if should_linger {
                        tokio::time::sleep(effective_linger).await;
                    }
                }

                let batch = {
                    let mut guard = wal_queue.lock().await;
                    if guard.queue.is_empty() {
                        guard.flushing = false;
                        metrics::set_wal_queue_depth(0);
                        metrics::set_wal_queue_bytes(0);
                        wal_queue_space.notify_waiters();
                        break;
                    }

                    let mut drained = Vec::new();
                    let mut batch_bytes = 0_usize;
                    while let Some(front) = guard.queue.front() {
                        let front_bytes = front.payload.len();
                        let reached_request_limit = drained.len() >= wal_cfg.max_batch_requests;
                        let reached_byte_limit = !drained.is_empty()
                            && batch_bytes.saturating_add(front_bytes) > wal_cfg.max_batch_bytes;
                        if reached_request_limit || reached_byte_limit {
                            break;
                        }

                        let item = guard.queue.pop_front().expect("queue front exists");
                        guard.queued_bytes = guard.queued_bytes.saturating_sub(item.payload.len());
                        batch_bytes = batch_bytes.saturating_add(item.payload.len());
                        drained.push(item);
                    }

                    if drained.is_empty() {
                        if let Some(item) = guard.queue.pop_front() {
                            guard.queued_bytes =
                                guard.queued_bytes.saturating_sub(item.payload.len());
                            drained.push(item);
                        }
                    }
                    metrics::set_wal_queue_depth(guard.queue.len());
                    metrics::set_wal_queue_bytes(guard.queued_bytes);
                    wal_queue_space.notify_waiters();
                    drained
                };

                if batch.is_empty() {
                    continue;
                }

                let requests = batch.len();
                let append_entries = batch.iter().map(|item| item.append_entries).sum::<usize>();
                let payload_bytes = batch.iter().map(|item| item.payload.len()).sum::<usize>();
                let timeline_batch_id = batch.iter().find_map(|item| item.timeline_batch_id);
                let timeline_submit_ts_micros =
                    batch.iter().find_map(|item| item.timeline_submit_ts_micros);
                let mut payload = Vec::with_capacity(payload_bytes);
                for item in &batch {
                    payload.extend_from_slice(&item.payload);
                }

                let started = Instant::now();
                let io_result = tokio::task::spawn_blocking({
                    let wal_device = wal_device.clone();
                    move || -> std::io::Result<()> {
                        let mut wal_device = wal_device
                            .lock()
                            .map_err(|_| std::io::Error::other("wal device lock poisoned"))?;
                        wal_device.append_payload(&payload)?;
                        wal_device.sync_data()?;
                        Ok(())
                    }
                })
                .await;

                let wal_sync_duration_ms = started.elapsed().as_millis() as u64;

                let flush_outcome = match io_result {
                    Ok(result) => result,
                    Err(e) => Err(std::io::Error::other(format!(
                        "wal sync worker join error: {e}"
                    ))),
                };

                match flush_outcome {
                    Ok(()) => {
                        let since_submit_ms = timeline_submit_ts_micros
                            .and_then(|submit| now_micros().checked_sub(submit))
                            .map(|delta| delta / 1_000);
                        if append_entries >= 50 {
                            info!(
                                requests,
                                append_entries,
                                payload_bytes,
                                wal_sync_duration_ms,
                                batch_id = timeline_batch_id,
                                since_submit_ms,
                                fdatasync_calls = 1,
                                "wal vectorized append flush complete"
                            );
                        } else {
                            debug!(
                                requests,
                                append_entries,
                                payload_bytes,
                                wal_sync_duration_ms,
                                batch_id = timeline_batch_id,
                                since_submit_ms,
                                fdatasync_calls = 1,
                                "wal direct flush complete"
                            );
                        }
                        for item in batch {
                            item.completion.complete_ok();
                        }
                    }
                    Err(err) => {
                        let err_msg = err.to_string();
                        warn!(
                            requests,
                            append_entries,
                            payload_bytes,
                            wal_sync_duration_ms,
                            error = %err_msg,
                            "wal batch flush failed"
                        );
                        for item in batch {
                            item.completion.complete_err(&err_msg);
                        }
                    }
                }
            }
        });
    }

    async fn enqueue_payload(
        &self,
        payload: Vec<u8>,
        append_entries: usize,
        timeline_batch_id: Option<u64>,
        timeline_submit_ts_micros: Option<u64>,
        completion: WalWriteCompletion,
    ) -> Result<(), StorageError<u64>> {
        if payload.is_empty() {
            completion.complete_ok();
            return Ok(());
        }

        let payload_len = payload.len();
        let pending_limit = self.wal_cfg.pending_limit.max(1);
        let mut maybe_item = Some(WalQueueItem {
            payload,
            append_entries,
            timeline_batch_id,
            timeline_submit_ts_micros,
            completion,
        });

        loop {
            let mut start_worker = false;
            {
                let mut guard = self.wal_queue.lock().await;
                if guard.queue.len() < pending_limit {
                    let item = maybe_item.take().expect("wal queue item available");
                    guard.queued_bytes = guard.queued_bytes.saturating_add(payload_len);
                    guard.queue.push_back(item);
                    metrics::set_wal_queue_depth(guard.queue.len());
                    metrics::set_wal_queue_bytes(guard.queued_bytes);

                    if !guard.flushing {
                        guard.flushing = true;
                        start_worker = true;
                    }
                }
            }

            if maybe_item.is_none() {
                if start_worker {
                    self.spawn_wal_flush_worker();
                }
                return Ok(());
            }

            self.wal_queue_space.notified().await;
        }
    }

    async fn append_records(
        &self,
        records: Vec<DurableRecord>,
        append_entries: usize,
    ) -> Result<(), StorageError<u64>> {
        let payload = Self::encode_records_payload(records)?;
        let (tx, rx) = oneshot::channel::<std::io::Result<()>>();
        self.enqueue_payload(
            payload,
            append_entries,
            None,
            None,
            WalWriteCompletion::Waiter(tx),
        )
        .await?;
        Self::await_wal_flush(rx).await
    }

    async fn append_records_with_callback(
        &self,
        records: Vec<DurableRecord>,
        append_entries: usize,
        timeline_batch_id: Option<u64>,
        timeline_submit_ts_micros: Option<u64>,
        callback: LogFlushed<AstraTypeConfig>,
    ) -> Result<(), StorageError<u64>> {
        let payload = match Self::encode_records_payload(records) {
            Ok(payload) => payload,
            Err(err) => {
                callback.log_io_completed(Err(std::io::Error::other(err.to_string())));
                return Err(err);
            }
        };

        if let Err(err) = self
            .enqueue_payload(
                payload,
                append_entries,
                timeline_batch_id,
                timeline_submit_ts_micros,
                WalWriteCompletion::Append(callback),
            )
            .await
        {
            // Callback is delivered on flush completion; if enqueue itself fails, report here.
            // This path should be rare and indicates shutdown or internal queue failure.
            warn!(error = %err, "wal enqueue failed for append callback path");
            return Err(err);
        }
        Ok(())
    }
}

fn replay_wal(path: &Path, state: &mut LogStoreState) -> Result<u64, StorageError<u64>> {
    if !path.exists() {
        return Ok(0);
    }

    let bytes = std::fs::read(path).map_err(|e| {
        StorageError::from_io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e)
    })?;

    let mut offset = 0_usize;
    let mut durable_end = 0_usize;
    while offset + 8 <= bytes.len() {
        let header = &bytes[offset..offset + 8];
        if header[..4] == [0, 0, 0, 0] {
            offset = align_up(offset + 1, 4096);
            continue;
        }

        if &header[..4] != b"ASTR" {
            break;
        }

        let payload_len = u32::from_le_bytes(header[4..8].try_into().expect("slice len")) as usize;
        let start = offset + 8;
        let end = start + payload_len;
        if end > bytes.len() {
            break;
        }

        let payload = &bytes[start..end];
        let mut cursor = Cursor::new(payload);
        while (cursor.position() as usize) < payload.len() {
            let mut len_buf = [0_u8; 4];
            if cursor.read_exact(&mut len_buf).is_err() {
                break;
            }
            let rec_len = u32::from_le_bytes(len_buf) as usize;
            if rec_len == 0 {
                break;
            }

            let mut rec_buf = vec![0_u8; rec_len];
            if cursor.read_exact(&mut rec_buf).is_err() {
                break;
            }

            let rec: DurableRecord = match bincode::deserialize(&rec_buf) {
                Ok(rec) => rec,
                Err(_) => {
                    // Treat undecodable trailing data as EOF to keep startup resilient
                    // across WAL record-format changes.
                    break;
                }
            };

            apply_durable_record(state, rec);
        }

        offset = align_up(end, 4096);
        durable_end = offset;
    }

    Ok(durable_end as u64)
}

fn apply_durable_record(state: &mut LogStoreState, rec: DurableRecord) {
    match rec {
        DurableRecord::Append { entries } => {
            for entry in entries {
                state.logs.insert(entry.log_id.index, entry);
            }
        }
        DurableRecord::Truncate { since } => {
            let keys = state
                .logs
                .range(since.index..)
                .map(|(k, _)| *k)
                .collect::<Vec<_>>();
            for k in keys {
                state.logs.remove(&k);
            }
        }
        DurableRecord::Purge { upto } => {
            let keys = state
                .logs
                .range(..=upto.index)
                .map(|(k, _)| *k)
                .collect::<Vec<_>>();
            for k in keys {
                state.logs.remove(&k);
            }
            state.last_purged_log_id = Some(upto);
        }
        DurableRecord::Vote { vote } => {
            state.vote = Some(vote);
        }
        DurableRecord::Committed { committed } => {
            state.committed = committed;
        }
    }
}

fn encode_durable_records(records: Vec<DurableRecord>) -> std::io::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for rec in records {
        let rec_bytes = bincode::serialize(&rec)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        encoded.extend_from_slice(&(rec_bytes.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&rec_bytes);
    }
    Ok(encoded)
}

impl RaftLogReader<AstraTypeConfig> for AstraLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AstraTypeConfig>>, StorageError<u64>> {
        let (start, end) = range_to_bounds(range);
        let guard = self.inner.read().await;
        let out = guard
            .logs
            .range(start..end)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();
        Ok(out)
    }
}

impl RaftLogReader<AstraTypeConfig> for AstraLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AstraTypeConfig>>, StorageError<u64>> {
        let mut reader = AstraLogReader {
            inner: self.inner.clone(),
        };
        reader.try_get_log_entries(range).await
    }
}

fn range_to_bounds<RB: RangeBounds<u64>>(range: RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(v) => *v,
        Bound::Excluded(v) => v.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(v) => v.saturating_add(1),
        Bound::Excluded(v) => *v,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

impl RaftLogStorage<AstraTypeConfig> for AstraLogStore {
    type LogReader = AstraLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<AstraTypeConfig>, StorageError<u64>> {
        let guard = self.inner.read().await;
        let last_log_id = guard
            .logs
            .values()
            .last()
            .map(|e| e.log_id.clone())
            .or_else(|| guard.last_purged_log_id.clone());

        Ok(LogState {
            last_purged_log_id: guard.last_purged_log_id.clone(),
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        AstraLogReader {
            inner: self.inner.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut guard = self.inner.write().await;
            guard.vote = Some(vote.clone());
        }
        self.append_records(vec![DurableRecord::Vote { vote: vote.clone() }], 0)
            .await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let guard = self.inner.read().await;
        Ok(guard.vote.clone())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        {
            let mut guard = self.inner.write().await;
            guard.committed = committed.clone();
        }

        if let Some(committed_log_id) = committed.as_ref() {
            if let Some((tracked_log_index, marker)) = self
                .timeline_by_log_index
                .range(..=committed_log_id.index)
                .next_back()
                .map(|(idx, marker)| (*idx, *marker))
            {
                let since_submit_ms = now_micros()
                    .checked_sub(marker.submit_ts_micros)
                    .map(|delta| delta / 1_000)
                    .unwrap_or(0);
                info!(
                    stage = "quorum_ack_commit_advanced",
                    batch_id = marker.batch_id,
                    committed_index = committed_log_id.index,
                    tracked_log_index,
                    since_submit_ms,
                    "raft timeline"
                );
                metrics::observe_put_quorum_ack_ms(since_submit_ms);
            }

            let keep_from = committed_log_id.index.saturating_add(1);
            self.timeline_by_log_index = self.timeline_by_log_index.split_off(&keep_from);
        }

        let should_flush = match (&self.last_flushed_committed, &committed) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => true,
            (Some(prev), Some(next)) => {
                let step_reached =
                    next.index >= prev.index.saturating_add(Self::COMMITTED_FLUSH_STEP);
                step_reached
                    || self.last_committed_flush_at.elapsed() >= Self::COMMITTED_FLUSH_INTERVAL
            }
        };

        if should_flush {
            self.append_records(
                vec![DurableRecord::Committed {
                    committed: committed.clone(),
                }],
                0,
            )
            .await?;
            self.last_flushed_committed = committed;
            self.last_committed_flush_at = Instant::now();
        }

        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let guard = self.inner.read().await;
        Ok(guard.committed.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<AstraTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<AstraTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let entries_vec = entries.into_iter().collect::<Vec<_>>();
        if entries_vec.is_empty() {
            callback.log_io_completed(Ok(()));
            return Ok(());
        }

        let timeline_markers = Self::extract_timeline_markers(&entries_vec);

        {
            let mut guard = self.inner.write().await;
            for ent in &entries_vec {
                guard.logs.insert(ent.log_id.index, ent.clone());
            }
        }
        for (log_index, marker) in timeline_markers {
            self.timeline_by_log_index.insert(log_index, marker);
        }

        let append_entries = entries_vec
            .iter()
            .map(Self::logical_write_ops)
            .sum::<usize>()
            .max(1);
        let (timeline_batch_id, timeline_submit_ts_micros) =
            Self::extract_timeline_meta(&entries_vec);
        if let Some(batch_id) = timeline_batch_id {
            let since_submit_ms = timeline_submit_ts_micros
                .and_then(|submit| now_micros().checked_sub(submit))
                .map(|delta| delta / 1_000);
            debug!(
                stage = "raft_log_append_in_memory",
                batch_id,
                append_entries,
                log_entries = entries_vec.len(),
                since_submit_ms,
                "raft timeline"
            );
        }
        self.append_records_with_callback(
            vec![DurableRecord::Append {
                entries: entries_vec,
            }],
            append_entries,
            timeline_batch_id,
            timeline_submit_ts_micros,
            callback,
        )
        .await
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut guard = self.inner.write().await;
            let keys = guard
                .logs
                .range(log_id.index..)
                .map(|(k, _)| *k)
                .collect::<Vec<_>>();
            for k in keys {
                guard.logs.remove(&k);
            }
        }
        self.timeline_by_log_index = self.timeline_by_log_index.split_off(&log_id.index);

        self.append_records(vec![DurableRecord::Truncate { since: log_id }], 0)
            .await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut guard = self.inner.write().await;
            let keys = guard
                .logs
                .range(..=log_id.index)
                .map(|(k, _)| *k)
                .collect::<Vec<_>>();
            for k in keys {
                guard.logs.remove(&k);
            }
            guard.last_purged_log_id = Some(log_id.clone());
        }
        let keep_from = log_id.index.saturating_add(1);
        self.timeline_by_log_index = self.timeline_by_log_index.split_off(&keep_from);

        self.append_records(vec![DurableRecord::Purge { upto: log_id }], 0)
            .await
    }
}

#[derive(Debug, Clone)]
struct SnapshotBlob {
    meta: SnapshotMeta<u64, BasicNode>,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotPayloadV2 {
    store: SnapshotState,
    token_dict: Vec<(u32, Vec<u8>)>,
    token_dict_epoch: u64,
}

#[derive(Debug)]
struct AstraStateMachineShared {
    store: Arc<KvStore>,
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    current_snapshot: Option<SnapshotBlob>,
    token_dict: HashMap<u32, Vec<u8>>,
    token_dict_epoch: u64,
}

#[derive(Debug, Clone)]
pub struct AstraStateMachine {
    shared: Arc<RwLock<AstraStateMachineShared>>,
}

impl AstraStateMachine {
    pub fn new(store: Arc<KvStore>) -> Self {
        Self {
            shared: Arc::new(RwLock::new(AstraStateMachineShared {
                store,
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                current_snapshot: None,
                token_dict: HashMap::new(),
                token_dict_epoch: 0,
            })),
        }
    }
}

fn txn_compare_i64(lhs: i64, rhs: i64, result: &AstraTxnCmpResult) -> bool {
    match result {
        AstraTxnCmpResult::Equal => lhs == rhs,
        AstraTxnCmpResult::Greater => lhs > rhs,
        AstraTxnCmpResult::Less => lhs < rhs,
        AstraTxnCmpResult::NotEqual => lhs != rhs,
    }
}

fn txn_compare_bytes(lhs: &[u8], rhs: &[u8], result: &AstraTxnCmpResult) -> bool {
    match result {
        AstraTxnCmpResult::Equal => lhs == rhs,
        AstraTxnCmpResult::Greater => lhs > rhs,
        AstraTxnCmpResult::Less => lhs < rhs,
        AstraTxnCmpResult::NotEqual => lhs != rhs,
    }
}

fn txn_ops_have_write(ops: &[AstraTxnOp]) -> bool {
    for op in ops {
        match op {
            AstraTxnOp::Put { .. } | AstraTxnOp::DeleteRange { .. } => return true,
            AstraTxnOp::Txn {
                success, failure, ..
            } => {
                if txn_ops_have_write(success) || txn_ops_have_write(failure) {
                    return true;
                }
            }
            AstraTxnOp::Range { .. } => {}
        }
    }
    false
}

fn eval_txn_compare(store: &KvStore, cmp: &AstraTxnCompare) -> Result<bool, StoreError> {
    let out = store.range(&cmp.key, &cmp.range_end, 0, 0, false, false)?;

    if out.kvs.is_empty() {
        return match (&cmp.target, &cmp.target_value) {
            (AstraTxnCmpTarget::Value, AstraTxnCmpValue::Bytes(rhs)) => {
                Ok(txn_compare_bytes(&[], rhs, &cmp.result))
            }
            (
                AstraTxnCmpTarget::Version
                | AstraTxnCmpTarget::CreateRevision
                | AstraTxnCmpTarget::ModRevision
                | AstraTxnCmpTarget::Lease,
                AstraTxnCmpValue::I64(rhs),
            ) => Ok(txn_compare_i64(0, *rhs, &cmp.result)),
            _ => Err(StoreError::InvalidArgument(
                "txn compare target and value type mismatch".to_string(),
            )),
        };
    }

    for (_, kv) in out.kvs {
        let ok = match (&cmp.target, &cmp.target_value) {
            (AstraTxnCmpTarget::Version, AstraTxnCmpValue::I64(rhs)) => {
                txn_compare_i64(kv.version, *rhs, &cmp.result)
            }
            (AstraTxnCmpTarget::CreateRevision, AstraTxnCmpValue::I64(rhs)) => {
                txn_compare_i64(kv.create_revision, *rhs, &cmp.result)
            }
            (AstraTxnCmpTarget::ModRevision, AstraTxnCmpValue::I64(rhs)) => {
                txn_compare_i64(kv.mod_revision, *rhs, &cmp.result)
            }
            (AstraTxnCmpTarget::Lease, AstraTxnCmpValue::I64(rhs)) => {
                txn_compare_i64(kv.lease, *rhs, &cmp.result)
            }
            (AstraTxnCmpTarget::Value, AstraTxnCmpValue::Bytes(rhs)) => {
                txn_compare_bytes(&kv.value, rhs, &cmp.result)
            }
            _ => {
                return Err(StoreError::InvalidArgument(
                    "txn compare target and value type mismatch".to_string(),
                ));
            }
        };
        if !ok {
            return Ok(false);
        }
    }

    Ok(true)
}

fn eval_txn_compares(store: &KvStore, compares: &[AstraTxnCompare]) -> Result<bool, StoreError> {
    for cmp in compares {
        if !eval_txn_compare(store, cmp)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn apply_txn_ops(
    store: &KvStore,
    ops: &[AstraTxnOp],
    forced_revision: Option<i64>,
) -> Result<Vec<AstraTxnOpResponse>, StoreError> {
    let mut out = Vec::with_capacity(ops.len());

    for op in ops {
        match op {
            AstraTxnOp::Range {
                key,
                range_end,
                limit,
                revision,
                keys_only,
                count_only,
            } => {
                let RangeOutput {
                    revision,
                    count,
                    more,
                    kvs,
                } = store.range(key, range_end, *limit, *revision, *keys_only, *count_only)?;
                out.push(AstraTxnOpResponse::Range {
                    revision,
                    count,
                    more,
                    kvs,
                });
            }
            AstraTxnOp::Put {
                key,
                value,
                lease,
                ignore_value,
                ignore_lease,
                prev_kv,
            } => {
                let PutOutput {
                    revision,
                    prev,
                    current: _,
                } = if let Some(rev) = forced_revision {
                    store.apply_put_at_revision(
                        key.clone(),
                        value.clone(),
                        *lease,
                        *ignore_value,
                        *ignore_lease,
                        rev,
                    )?
                } else {
                    store.put(
                        key.clone(),
                        value.clone(),
                        *lease,
                        *ignore_value,
                        *ignore_lease,
                    )?
                };
                out.push(AstraTxnOpResponse::Put {
                    revision,
                    prev: if *prev_kv { prev } else { None },
                });
            }
            AstraTxnOp::DeleteRange {
                key,
                range_end,
                prev_kv,
            } => {
                let DeleteOutput {
                    revision,
                    deleted,
                    prev_kvs,
                } = if let Some(rev) = forced_revision {
                    store.apply_delete_at_revision(key, range_end, *prev_kv, rev)?
                } else {
                    store.delete_range(key, range_end, *prev_kv)?
                };
                out.push(AstraTxnOpResponse::Delete {
                    revision,
                    deleted,
                    prev_kvs,
                });
            }
            AstraTxnOp::Txn {
                compare,
                success,
                failure,
            } => {
                let succeeded = eval_txn_compares(store, compare)?;
                let branch = if succeeded { success } else { failure };
                let nested_forced = if txn_ops_have_write(branch) {
                    forced_revision
                } else {
                    None
                };
                let responses = apply_txn_ops(store, branch, nested_forced)?;
                out.push(AstraTxnOpResponse::Txn {
                    revision: forced_revision.unwrap_or_else(|| store.current_revision()),
                    succeeded,
                    responses,
                });
            }
        }
    }

    Ok(out)
}

#[derive(Debug, Clone)]
pub struct AstraSnapshotBuilder {
    shared: Arc<RwLock<AstraStateMachineShared>>,
}

impl RaftSnapshotBuilder<AstraTypeConfig> for AstraSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<AstraTypeConfig>, StorageError<u64>> {
        let (snapshot_state, token_dict, token_dict_epoch, last_log_id, last_membership) = {
            let guard = self.shared.read().await;
            (
                guard.store.snapshot_state(),
                guard.token_dict.clone(),
                guard.token_dict_epoch,
                guard.last_applied_log.clone(),
                guard.last_membership.clone(),
            )
        };

        let payload = SnapshotPayloadV2 {
            store: snapshot_state,
            token_dict: token_dict.into_iter().collect::<Vec<_>>(),
            token_dict_epoch,
        };

        let bytes = bincode::serialize(&payload).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            )
        })?;

        let snapshot_id = format!(
            "snapshot-{}-{}",
            last_log_id.as_ref().map(|v| v.index).unwrap_or_default(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or_default()
        );

        let meta = SnapshotMeta {
            last_log_id: last_log_id.clone(),
            last_membership: last_membership.clone(),
            snapshot_id,
        };

        {
            let mut guard = self.shared.write().await;
            guard.current_snapshot = Some(SnapshotBlob {
                meta: meta.clone(),
                bytes: bytes.clone(),
            });
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<AstraTypeConfig> for AstraStateMachine {
    type SnapshotBuilder = AstraSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let guard = self.shared.read().await;
        Ok((
            guard.last_applied_log.clone(),
            guard.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AstraWriteResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<AstraTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();

        let (store, mut token_dict, mut token_dict_epoch) = {
            let guard = self.shared.read().await;
            (
                guard.store.clone(),
                guard.token_dict.clone(),
                guard.token_dict_epoch,
            )
        };

        let mut responses = Vec::with_capacity(entries.len());

        let mut last_applied = None;
        let mut last_membership = None;

        for ent in entries {
            let log_id = ent.log_id.clone();

            match ent.payload {
                EntryPayload::Blank => {
                    responses.push(AstraWriteResponse::Empty);
                }
                EntryPayload::Membership(membership) => {
                    last_membership = Some(StoredMembership::new(Some(log_id.clone()), membership));
                    responses.push(AstraWriteResponse::Empty);
                }
                EntryPayload::Normal(req) => match req {
                    AstraWriteRequest::Put {
                        key,
                        value,
                        lease,
                        ignore_value,
                        ignore_lease,
                        prev_kv,
                    } => {
                        let PutOutput {
                            revision,
                            prev,
                            current: _,
                        } = store
                            .put(key, value, lease, ignore_value, ignore_lease)
                            .map_err(|e| {
                                StorageError::from_io_error(
                                    openraft::ErrorSubject::StateMachine,
                                    openraft::ErrorVerb::Write,
                                    std::io::Error::other(e.to_string()),
                                )
                            })?;

                        responses.push(AstraWriteResponse::Put {
                            revision,
                            prev: if prev_kv { prev } else { None },
                        });
                    }
                    AstraWriteRequest::PutBatch { ops } => {
                        let mut results = Vec::with_capacity(ops.len());
                        for op in ops {
                            let PutOutput {
                                revision,
                                prev,
                                current: _,
                            } = store
                                .put(op.key, op.value, op.lease, op.ignore_value, op.ignore_lease)
                                .map_err(|e| {
                                    StorageError::from_io_error(
                                        openraft::ErrorSubject::StateMachine,
                                        openraft::ErrorVerb::Write,
                                        std::io::Error::other(e.to_string()),
                                    )
                                })?;

                            results.push(AstraBatchPutResult {
                                revision,
                                prev: if op.prev_kv { prev } else { None },
                            });
                        }
                        responses.push(AstraWriteResponse::PutBatch { results });
                    }
                    AstraWriteRequest::PutBatchRef {
                        batch_id,
                        submit_ts_micros,
                        values,
                        ops,
                    } => {
                        let op_count = ops.len();
                        let unique_values = values.len();
                        let mut results = Vec::with_capacity(ops.len());
                        for op in ops {
                            let value =
                                values.get(op.value_idx as usize).cloned().ok_or_else(|| {
                                    StorageError::from_io_error(
                                        openraft::ErrorSubject::StateMachine,
                                        openraft::ErrorVerb::Write,
                                        std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            format!(
                                                "put batch reference index out of bounds: {}",
                                                op.value_idx
                                            ),
                                        ),
                                    )
                                })?;

                            let PutOutput {
                                revision,
                                prev,
                                current: _,
                            } = store
                                .put(op.key, value, op.lease, op.ignore_value, op.ignore_lease)
                                .map_err(|e| {
                                    StorageError::from_io_error(
                                        openraft::ErrorSubject::StateMachine,
                                        openraft::ErrorVerb::Write,
                                        std::io::Error::other(e.to_string()),
                                    )
                                })?;

                            results.push(AstraBatchPutResult {
                                revision,
                                prev: if op.prev_kv { prev } else { None },
                            });
                        }
                        if batch_id > 0 {
                            let since_submit_ms = now_micros()
                                .checked_sub(submit_ts_micros)
                                .map(|delta| delta / 1_000);
                            info!(
                                stage = "sm_apply_done",
                                batch_id,
                                op_count,
                                unique_values,
                                tokenized = false,
                                since_submit_ms,
                                "raft timeline"
                            );
                        }
                        responses.push(AstraWriteResponse::PutBatch { results });
                    }
                    AstraWriteRequest::PutBatchTokenized {
                        batch_id,
                        submit_ts_micros,
                        dict_epoch,
                        dict_additions,
                        ops,
                    } => {
                        if dict_epoch > token_dict_epoch {
                            token_dict_epoch = dict_epoch;
                        }
                        for add in &dict_additions {
                            token_dict.insert(add.token_id, add.value.clone());
                        }

                        let op_count = ops.len();
                        let dict_add_count = dict_additions.len();
                        let mut results = Vec::with_capacity(ops.len());
                        for op in ops {
                            let value = token_dict.get(&op.token_id).cloned().ok_or_else(|| {
                                StorageError::from_io_error(
                                    openraft::ErrorSubject::StateMachine,
                                    openraft::ErrorVerb::Write,
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!(
                                            "missing token in tokenized batch: {}",
                                            op.token_id
                                        ),
                                    ),
                                )
                            })?;

                            let PutOutput {
                                revision,
                                prev,
                                current: _,
                            } = store
                                .put(op.key, value, op.lease, op.ignore_value, op.ignore_lease)
                                .map_err(|e| {
                                    StorageError::from_io_error(
                                        openraft::ErrorSubject::StateMachine,
                                        openraft::ErrorVerb::Write,
                                        std::io::Error::other(e.to_string()),
                                    )
                                })?;

                            results.push(AstraBatchPutResult {
                                revision,
                                prev: if op.prev_kv { prev } else { None },
                            });
                        }

                        if batch_id > 0 {
                            let since_submit_ms = now_micros()
                                .checked_sub(submit_ts_micros)
                                .map(|delta| delta / 1_000);
                            info!(
                                stage = "sm_apply_done",
                                batch_id,
                                op_count,
                                dict_add_count,
                                dict_size = token_dict.len(),
                                tokenized = true,
                                since_submit_ms,
                                "raft timeline"
                            );
                        }
                        responses.push(AstraWriteResponse::PutBatch { results });
                    }
                    AstraWriteRequest::DeleteRange {
                        key,
                        range_end,
                        prev_kv,
                    } => {
                        let DeleteOutput {
                            revision,
                            deleted,
                            prev_kvs,
                        } = store.delete_range(&key, &range_end, prev_kv).map_err(|e| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::StateMachine,
                                openraft::ErrorVerb::Write,
                                std::io::Error::other(e.to_string()),
                            )
                        })?;

                        responses.push(AstraWriteResponse::Delete {
                            revision,
                            deleted,
                            prev_kvs,
                        });
                    }
                    AstraWriteRequest::Txn {
                        compare,
                        success,
                        failure,
                    } => {
                        let succeeded = eval_txn_compares(&store, &compare).map_err(|e| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::StateMachine,
                                openraft::ErrorVerb::Write,
                                std::io::Error::other(e.to_string()),
                            )
                        })?;
                        let branch = if succeeded { &success } else { &failure };
                        let forced_revision = if txn_ops_have_write(branch) {
                            Some(store.reserve_revision())
                        } else {
                            None
                        };
                        let op_responses =
                            apply_txn_ops(&store, branch, forced_revision).map_err(|e| {
                                StorageError::from_io_error(
                                    openraft::ErrorSubject::StateMachine,
                                    openraft::ErrorVerb::Write,
                                    std::io::Error::other(e.to_string()),
                                )
                            })?;
                        responses.push(AstraWriteResponse::Txn {
                            revision: forced_revision.unwrap_or_else(|| store.current_revision()),
                            succeeded,
                            responses: op_responses,
                        });
                    }
                    AstraWriteRequest::Compact { revision } => {
                        let compact_revision = store.compact_to(revision).map_err(|e| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::StateMachine,
                                openraft::ErrorVerb::Write,
                                std::io::Error::other(e.to_string()),
                            )
                        })?;
                        responses.push(AstraWriteResponse::Compact {
                            revision: store.current_revision(),
                            compact_revision,
                        });
                    }
                    AstraWriteRequest::LeaseGrant { id, ttl } => {
                        let LeaseGrantOutput { revision, id, ttl } =
                            store.lease_grant(id, ttl).map_err(|e| {
                                StorageError::from_io_error(
                                    openraft::ErrorSubject::StateMachine,
                                    openraft::ErrorVerb::Write,
                                    std::io::Error::other(e.to_string()),
                                )
                            })?;
                        responses.push(AstraWriteResponse::LeaseGrant { revision, id, ttl });
                    }
                    AstraWriteRequest::LeaseRevoke { id } => {
                        let LeaseRevokeOutput { revision, deleted } =
                            store.lease_revoke(id).map_err(|e| {
                                StorageError::from_io_error(
                                    openraft::ErrorSubject::StateMachine,
                                    openraft::ErrorVerb::Write,
                                    std::io::Error::other(e.to_string()),
                                )
                            })?;
                        responses.push(AstraWriteResponse::LeaseRevoke { revision, deleted });
                    }
                    AstraWriteRequest::LeaseKeepAlive { id } => {
                        let LeaseGrantOutput { revision, id, ttl } =
                            store.lease_keep_alive(id).map_err(|e| {
                                StorageError::from_io_error(
                                    openraft::ErrorSubject::StateMachine,
                                    openraft::ErrorVerb::Write,
                                    std::io::Error::other(e.to_string()),
                                )
                            })?;
                        responses.push(AstraWriteResponse::LeaseKeepAlive { revision, id, ttl });
                    }
                },
            }

            last_applied = Some(log_id);
        }

        let mut guard = self.shared.write().await;
        if let Some(last) = last_applied {
            guard.last_applied_log = Some(last);
        }
        if let Some(membership) = last_membership {
            guard.last_membership = membership;
        }
        guard.token_dict = token_dict;
        guard.token_dict_epoch = token_dict_epoch;

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AstraSnapshotBuilder {
            shared: self.shared.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let cursor = *snapshot;
        let bytes = cursor.into_inner();

        let (snapshot_state, token_dict, token_dict_epoch) =
            match bincode::deserialize::<SnapshotPayloadV2>(&bytes) {
                Ok(v2) => (
                    v2.store,
                    v2.token_dict.into_iter().collect::<HashMap<_, _>>(),
                    v2.token_dict_epoch,
                ),
                Err(_) => {
                    // Backward compatibility for v1 snapshots that only stored KvStore state.
                    let snapshot_state: SnapshotState =
                        bincode::deserialize(&bytes).map_err(|e| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                                openraft::ErrorVerb::Read,
                                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
                            )
                        })?;
                    (snapshot_state, HashMap::new(), 0)
                }
            };

        let store = {
            let guard = self.shared.read().await;
            guard.store.clone()
        };

        store.load_snapshot_state(snapshot_state).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                std::io::Error::other(e.to_string()),
            )
        })?;

        let mut guard = self.shared.write().await;
        guard.last_applied_log = meta.last_log_id.clone();
        guard.last_membership = meta.last_membership.clone();
        guard.token_dict = token_dict;
        guard.token_dict_epoch = token_dict_epoch;
        guard.current_snapshot = Some(SnapshotBlob {
            meta: meta.clone(),
            bytes,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<AstraTypeConfig>>, StorageError<u64>> {
        let guard = self.shared.read().await;
        Ok(guard.current_snapshot.as_ref().map(|blob| Snapshot {
            meta: blob.meta.clone(),
            snapshot: Box::new(Cursor::new(blob.bytes.clone())),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct AstraNetworkFactory;

#[derive(Debug, Clone)]
pub struct AstraNetwork {
    addr: String,
    client: Option<InternalRaftClient<Channel>>,
}

impl RaftNetworkFactory<AstraTypeConfig> for AstraNetworkFactory {
    type Network = AstraNetwork;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        Self::Network {
            addr: node.addr.clone(),
            client: None,
        }
    }
}

impl AstraNetwork {
    async fn ensure_client(
        &mut self,
    ) -> Result<&mut InternalRaftClient<Channel>, tonic::transport::Error> {
        let addr = if self.addr.starts_with("http://") || self.addr.starts_with("https://") {
            self.addr.clone()
        } else {
            format!("http://{}", self.addr)
        };
        if self.client.is_none() {
            self.client = Some(InternalRaftClient::connect(addr).await?);
        }
        Ok(self.client.as_mut().expect("client initialized"))
    }

    fn invalidate_client(&mut self) {
        self.client = None;
    }

    fn should_retry_status(status: &Status) -> bool {
        matches!(
            status.code(),
            Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::Unknown
        )
    }
}

impl RaftNetwork<AstraTypeConfig> for AstraNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<AstraTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let entry_count = rpc.entries.len();
        let first_log_index = rpc.entries.first().map(|e| e.log_id.index);
        let last_log_index = rpc.entries.last().map(|e| e.log_id.index);
        let payload = serde_json::to_vec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;
        let payload_bytes = payload.len();

        let timeout = option.hard_ttl();
        for attempt in 0..2 {
            let started = Instant::now();
            let mut req = Request::new(RaftBytes {
                payload: payload.clone(),
            });
            req.set_timeout(timeout);
            let resp = self
                .ensure_client()
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
                .append_entries(req)
                .await;

            match resp {
                Ok(resp) => {
                    let elapsed_ms = started.elapsed().as_millis() as u64;
                    debug!(
                        stage = "append_entries_rpc_send_done",
                        entry_count,
                        first_log_index,
                        last_log_index,
                        payload_bytes,
                        elapsed_ms,
                        attempt,
                        "raft timeline"
                    );
                    return serde_json::from_slice::<AppendEntriesResponse<u64>>(
                        &resp.into_inner().payload,
                    )
                    .map_err(|e| {
                        RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
                    });
                }
                Err(status) => {
                    let elapsed_ms = started.elapsed().as_millis() as u64;
                    debug!(
                        stage = "append_entries_rpc_send_done",
                        entry_count,
                        first_log_index,
                        last_log_index,
                        payload_bytes,
                        elapsed_ms,
                        attempt,
                        code = %status.code(),
                        success = false,
                        "raft timeline"
                    );
                    self.invalidate_client();
                    if attempt == 0 && Self::should_retry_status(&status) {
                        continue;
                    }
                    return Err(RPCError::Network(NetworkError::new(&status)));
                }
            }
        }

        Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other("append_entries exhausted retries"),
        )))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<AstraTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload = serde_json::to_vec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;

        let timeout = option.hard_ttl();
        for attempt in 0..2 {
            let mut req = Request::new(RaftBytes {
                payload: payload.clone(),
            });
            req.set_timeout(timeout);
            let resp = self
                .ensure_client()
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
                .install_snapshot(req)
                .await;

            match resp {
                Ok(resp) => {
                    return serde_json::from_slice::<InstallSnapshotResponse<u64>>(
                        &resp.into_inner().payload,
                    )
                    .map_err(|e| {
                        RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
                    });
                }
                Err(status) => {
                    self.invalidate_client();
                    if attempt == 0 && Self::should_retry_status(&status) {
                        continue;
                    }
                    return Err(RPCError::Network(NetworkError::new(&status)));
                }
            }
        }

        Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other("install_snapshot exhausted retries"),
        )))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = serde_json::to_vec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;

        let timeout = option.hard_ttl();
        for attempt in 0..2 {
            let mut req = Request::new(RaftBytes {
                payload: payload.clone(),
            });
            req.set_timeout(timeout);
            let resp = self
                .ensure_client()
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
                .vote(req)
                .await;

            match resp {
                Ok(resp) => {
                    return serde_json::from_slice::<VoteResponse<u64>>(&resp.into_inner().payload)
                        .map_err(|e| {
                            RPCError::Network(NetworkError::new(&std::io::Error::other(
                                e.to_string(),
                            )))
                        });
                }
                Err(status) => {
                    self.invalidate_client();
                    if attempt == 0 && Self::should_retry_status(&status) {
                        continue;
                    }
                    return Err(RPCError::Network(NetworkError::new(&status)));
                }
            }
        }

        Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other("vote exhausted retries"),
        )))
    }
}

#[derive(Clone)]
pub struct AstraRaftService {
    pub raft: Raft<AstraTypeConfig>,
    pub node_id: u64,
    pub chaos_append_ack_delay_enabled: bool,
    pub chaos_append_ack_delay_min: Duration,
    pub chaos_append_ack_delay_max: Duration,
    pub chaos_append_ack_delay_node_id: u64,
}

impl AstraRaftService {
    fn append_ack_chaos_delay(&self, entry_count: usize) -> Option<Duration> {
        if !self.chaos_append_ack_delay_enabled || entry_count == 0 {
            return None;
        }
        if self.chaos_append_ack_delay_node_id != 0
            && self.chaos_append_ack_delay_node_id != self.node_id
        {
            return None;
        }

        let min_ms = self.chaos_append_ack_delay_min.as_millis() as u64;
        let max_ms = self
            .chaos_append_ack_delay_max
            .as_millis()
            .max(min_ms as u128) as u64;
        let span = max_ms.saturating_sub(min_ms);
        let jitter = if span == 0 {
            0
        } else {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or_default();
            now_ns % (span + 1)
        };
        Some(Duration::from_millis(min_ms.saturating_add(jitter)))
    }
}

#[tonic::async_trait]
impl InternalRaft for AstraRaftService {
    async fn append_entries(
        &self,
        request: Request<RaftBytes>,
    ) -> Result<Response<RaftBytes>, Status> {
        let started = Instant::now();
        let rpc: AppendEntriesRequest<AstraTypeConfig> =
            serde_json::from_slice(&request.into_inner().payload)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let entry_count = rpc.entries.len();
        let first_log_index = rpc.entries.first().map(|e| e.log_id.index);
        let last_log_index = rpc.entries.last().map(|e| e.log_id.index);

        if let Some(delay) = self.append_ack_chaos_delay(entry_count) {
            tokio::time::sleep(delay).await;
            debug!(
                stage = "append_entries_chaos_delay",
                node_id = self.node_id,
                delay_ms = delay.as_millis() as u64,
                entry_count,
                first_log_index,
                last_log_index,
                "raft timeline"
            );
        }

        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        debug!(
            stage = "append_entries_rpc_recv_done",
            entry_count, first_log_index, last_log_index, elapsed_ms, "raft timeline"
        );

        let payload = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftBytes { payload }))
    }

    async fn vote(&self, request: Request<RaftBytes>) -> Result<Response<RaftBytes>, Status> {
        let rpc: VoteRequest<u64> = serde_json::from_slice(&request.into_inner().payload)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let payload = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftBytes { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftBytes>,
    ) -> Result<Response<RaftBytes>, Status> {
        let rpc: InstallSnapshotRequest<AstraTypeConfig> =
            serde_json::from_slice(&request.into_inner().payload)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let resp = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let payload = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftBytes { payload }))
    }
}

pub fn parse_raft_nodes(peers: &[String]) -> HashMap<u64, BasicNode> {
    peers
        .iter()
        .enumerate()
        .map(|(idx, p)| {
            let addr = p
                .strip_prefix("http://")
                .or_else(|| p.strip_prefix("https://"))
                .unwrap_or(p)
                .to_string();
            ((idx as u64) + 1, BasicNode { addr })
        })
        .collect::<HashMap<_, _>>()
}

pub async fn maybe_initialize(
    raft: &Raft<AstraTypeConfig>,
    nodes: HashMap<u64, BasicNode>,
) -> Result<(), RaftError<u64, openraft::error::InitializeError<u64, BasicNode>>> {
    if raft.is_initialized().await.unwrap_or(false) {
        return Ok(());
    }

    let members = nodes.into_iter().collect::<BTreeMap<_, _>>();
    raft.initialize(members).await
}
