use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use astra_core::config::{AstraConfig, AstraProfile, PutAdaptiveMode, S3Config};
use astra_core::errors::StoreError;
use astra_core::hal::HalProfile;
use astra_core::io_budget::IoTokenBucket;
use astra_core::memory::MemoryPressure;
use astra_core::metrics;
use astra_core::raft::{
    maybe_initialize, parse_raft_nodes, AstraBatchPutRefOp, AstraBatchPutTokenOp, AstraLogStore,
    AstraNetworkFactory, AstraRaftService, AstraStateMachine, AstraTokenValue, AstraTxnCmpResult,
    AstraTxnCmpTarget, AstraTxnCmpValue, AstraTxnCompare, AstraTxnOp, AstraTxnOpResponse,
    AstraTypeConfig, AstraWriteRequest, AstraWriteResponse, RaftBootstrap, WalBatchConfig,
};
use astra_core::store::{key_in_range, KvStore, RangeOutput, ValueEntry};
use astra_core::tiering::{decode_chunk_to_rows, TierManifest, TieringManager};
use astra_core::watch::{WatchEventKind, WatchFilter};
use astra_proto::astraadminpb::astra_admin_server::{AstraAdmin, AstraAdminServer};
use astra_proto::astraadminpb::{
    BulkLoadRequest, BulkLoadResponse, GetBulkLoadJobRequest, GetBulkLoadJobResponse,
};
use astra_proto::astraraftpb::internal_raft_server::InternalRaftServer;
use astra_proto::etcdserverpb::kv_client::KvClient;
use astra_proto::etcdserverpb::kv_server::{Kv, KvServer};
use astra_proto::etcdserverpb::lease_client::LeaseClient;
use astra_proto::etcdserverpb::lease_server::{Lease, LeaseServer};
use astra_proto::etcdserverpb::response_op::Response as ResponseOpResponse;
use astra_proto::etcdserverpb::watch_request::RequestUnion;
use astra_proto::etcdserverpb::watch_server::{Watch, WatchServer};
use astra_proto::etcdserverpb::{
    compare, request_op, CompactionRequest, CompactionResponse, Compare, CompareResult,
    CompareTarget, DeleteRangeRequest, DeleteRangeResponse, Event, EventType, LeaseGrantRequest,
    LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseLeasesRequest,
    LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse, LeaseStatus,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse, RequestOp, ResponseHeader, ResponseOp, TxnRequest, TxnResponse, WatchRequest,
    WatchResponse,
};
use astra_proto::mvccpb::KeyValue;
use clap::Parser;
use futures::Stream;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use openraft::error::RaftError;
use openraft::error::{CheckIsLeaderError, ClientWriteError};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "astrad")]
struct Args {
    #[arg(long)]
    node_id: Option<u64>,

    #[arg(long)]
    peers: Option<String>,

    #[arg(long)]
    client_addr: Option<String>,

    #[arg(long)]
    raft_addr: Option<String>,

    #[arg(long)]
    profile: Option<String>,
}

#[derive(Debug, Clone)]
struct TenantIdentity {
    tenant_id: String,
}

#[derive(Debug, Clone, Copy)]
struct Tenanting {
    enabled: bool,
}

impl Tenanting {
    const ROOT: &'static [u8] = b"/__tenant/";

    fn from_config(cfg: &AstraConfig) -> Self {
        Self {
            enabled: cfg.tenant_virtualization_enabled,
        }
    }

    fn tenant_from_request<T>(&self, request: &Request<T>) -> Result<Option<String>, Status> {
        if !self.enabled {
            return Ok(None);
        }
        let tenant = request
            .extensions()
            .get::<TenantIdentity>()
            .map(|id| id.tenant_id.clone())
            .ok_or_else(|| Status::unauthenticated("tenant identity missing from token"))?;
        Ok(Some(tenant))
    }

    fn prefix_bytes(&self, tenant: Option<&str>) -> Option<Vec<u8>> {
        if !self.enabled {
            return None;
        }
        let tenant = tenant?;
        let mut out = Vec::with_capacity(Self::ROOT.len() + tenant.len() + 1);
        out.extend_from_slice(Self::ROOT);
        out.extend_from_slice(tenant.as_bytes());
        out.push(b'/');
        Some(out)
    }

    fn encode_key(&self, tenant: Option<&str>, key: &[u8]) -> Vec<u8> {
        if let Some(prefix) = self.prefix_bytes(tenant) {
            let mut out = prefix;
            out.extend_from_slice(key);
            return out;
        }
        key.to_vec()
    }

    fn encode_range(
        &self,
        tenant: Option<&str>,
        key: &[u8],
        range_end: &[u8],
    ) -> (Vec<u8>, Vec<u8>) {
        let Some(prefix) = self.prefix_bytes(tenant) else {
            return (key.to_vec(), range_end.to_vec());
        };

        if range_end.is_empty() {
            let mut out_key = prefix;
            out_key.extend_from_slice(key);
            return (out_key, Vec::new());
        }

        if key.is_empty() && range_end == [0] {
            let range_end = prefix_end(&prefix);
            return (prefix, range_end);
        }

        let mut out_key = prefix.clone();
        out_key.extend_from_slice(key);
        let mut out_range_end = prefix;
        out_range_end.extend_from_slice(range_end);
        (out_key, out_range_end)
    }

    fn decode_key(&self, tenant: Option<&str>, key: &[u8]) -> Vec<u8> {
        let Some(prefix) = self.prefix_bytes(tenant) else {
            return key.to_vec();
        };
        key.strip_prefix(prefix.as_slice()).unwrap_or(key).to_vec()
    }
}

#[derive(Clone)]
enum JwtVerifier {
    Disabled,
    Hs256 {
        key: Arc<DecodingKey>,
        validation: Arc<Validation>,
    },
    Jwks {
        jwks: Arc<JwkSet>,
        validation: Arc<Validation>,
    },
}

#[derive(Clone)]
struct AuthRuntime {
    enabled: bool,
    tenant_claim: String,
    verifier: JwtVerifier,
}

impl AuthRuntime {
    async fn from_config(cfg: &AstraConfig) -> Result<Self> {
        if !cfg.auth_enabled {
            return Ok(Self {
                enabled: false,
                tenant_claim: cfg.auth_tenant_claim.clone(),
                verifier: JwtVerifier::Disabled,
            });
        }

        if let Some(secret) = cfg.auth_jwt_hs256_secret.as_ref() {
            let mut validation = Validation::new(Algorithm::HS256);
            if let Some(aud) = cfg.auth_audience.as_deref() {
                validation.set_audience(&[aud]);
            }
            if let Some(iss) = cfg.auth_issuer.as_deref() {
                validation.set_issuer(&[iss]);
            }
            return Ok(Self {
                enabled: true,
                tenant_claim: cfg.auth_tenant_claim.clone(),
                verifier: JwtVerifier::Hs256 {
                    key: Arc::new(DecodingKey::from_secret(secret.as_bytes())),
                    validation: Arc::new(validation),
                },
            });
        }

        let jwks_url = cfg.auth_jwks_url.as_ref().ok_or_else(|| {
            anyhow!("auth enabled but neither ASTRAD_AUTH_JWT_HS256_SECRET nor ASTRAD_AUTH_JWKS_URL is set")
        })?;
        let jwks = reqwest::Client::new()
            .get(jwks_url)
            .send()
            .await
            .with_context(|| format!("failed to fetch jwks from {jwks_url}"))?
            .error_for_status()
            .with_context(|| format!("jwks endpoint returned error status: {jwks_url}"))?
            .json::<JwkSet>()
            .await
            .with_context(|| format!("failed to decode jwks response: {jwks_url}"))?;
        if jwks.keys.is_empty() {
            bail!("jwks keyset is empty");
        }

        let mut validation = Validation::new(Algorithm::RS256);
        if let Some(aud) = cfg.auth_audience.as_deref() {
            validation.set_audience(&[aud]);
        }
        if let Some(iss) = cfg.auth_issuer.as_deref() {
            validation.set_issuer(&[iss]);
        }

        Ok(Self {
            enabled: true,
            tenant_claim: cfg.auth_tenant_claim.clone(),
            verifier: JwtVerifier::Jwks {
                jwks: Arc::new(jwks),
                validation: Arc::new(validation),
            },
        })
    }

    fn authorize(&self, request: &mut Request<()>) -> Result<(), Status> {
        if !self.enabled {
            return Ok(());
        }

        let token = bearer_token(request.metadata())
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;

        let claims = match &self.verifier {
            JwtVerifier::Disabled => JsonValue::Null,
            JwtVerifier::Hs256 { key, validation } => {
                decode::<JsonValue>(&token, key, validation)
                    .map_err(|e| Status::unauthenticated(format!("jwt decode failed: {e}")))?
                    .claims
            }
            JwtVerifier::Jwks { jwks, validation } => {
                let header = decode_header(&token).map_err(|e| {
                    Status::unauthenticated(format!("jwt header decode failed: {e}"))
                })?;
                let jwk = match header.kid.as_deref() {
                    Some(kid) => jwks
                        .find(kid)
                        .or_else(|| jwks.keys.first())
                        .ok_or_else(|| Status::unauthenticated("jwt key id not found"))?,
                    None => jwks
                        .keys
                        .first()
                        .ok_or_else(|| Status::unauthenticated("jwks has no keys"))?,
                };
                let key = DecodingKey::from_jwk(jwk)
                    .map_err(|e| Status::unauthenticated(format!("jwk decode key failed: {e}")))?;
                decode::<JsonValue>(&token, &key, validation)
                    .map_err(|e| Status::unauthenticated(format!("jwt decode failed: {e}")))?
                    .claims
            }
        };

        let tenant = claims
            .get(&self.tenant_claim)
            .and_then(JsonValue::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                Status::permission_denied(format!(
                    "missing required tenant claim `{}`",
                    self.tenant_claim
                ))
            })?;
        request.extensions_mut().insert(TenantIdentity {
            tenant_id: tenant.to_string(),
        });
        Ok(())
    }
}

#[derive(Clone)]
struct AuthzInterceptor {
    auth: Arc<AuthRuntime>,
}

impl Interceptor for AuthzInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        self.auth.authorize(&mut request)?;
        Ok(request)
    }
}

fn bearer_token(metadata: &MetadataMap) -> Option<String> {
    let raw = metadata.get("authorization")?.to_str().ok()?.trim();
    raw.strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

struct QueuedPutRequest {
    req: PutRequest,
    priority: WritePriority,
    response_tx: oneshot::Sender<Result<(i64, Option<ValueEntry>), Status>>,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WritePriority {
    Tier0,
    Normal,
}

impl WritePriority {
    fn as_str(self) -> &'static str {
        match self {
            WritePriority::Tier0 => "tier0",
            WritePriority::Normal => "normal",
        }
    }
}

#[derive(Clone, Debug)]
struct SemanticQos {
    tier0_prefixes: Arc<Vec<Vec<u8>>>,
    tier0_suffixes: Arc<Vec<Vec<u8>>>,
}

impl SemanticQos {
    const TENANT_ROOT: &'static [u8] = b"/__tenant/";

    fn from_config(cfg: &AstraConfig) -> Self {
        Self {
            tier0_prefixes: Arc::new(cfg.qos_tier0_prefixes.clone()),
            tier0_suffixes: Arc::new(cfg.qos_tier0_suffixes.clone()),
        }
    }

    fn strip_tenant_prefix<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        if let Some(rest) = key.strip_prefix(Self::TENANT_ROOT) {
            if let Some(pos) = rest.iter().position(|b| *b == b'/') {
                return &rest[pos + 1..];
            }
        }
        key
    }

    fn key_priority(&self, key: &[u8]) -> WritePriority {
        let key = self.strip_tenant_prefix(key);
        if self
            .tier0_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix.as_slice()))
            || self
                .tier0_suffixes
                .iter()
                .any(|suffix| key.ends_with(suffix.as_slice()))
        {
            WritePriority::Tier0
        } else {
            WritePriority::Normal
        }
    }
}

#[derive(Debug, Clone)]
struct PutBatcherConfig {
    initial_max_requests: usize,
    min_max_requests: usize,
    max_max_requests: usize,
    initial_linger_us: u64,
    min_linger_us: u64,
    max_linger_us: u64,
    max_batch_bytes: usize,
    pending_limit: usize,
    adaptive_enabled: bool,
    adaptive_mode: PutAdaptiveMode,
    adaptive_min_request_floor: usize,
    dispatch_concurrency: usize,
    target_queue_depth: usize,
    p99_budget_ms: u64,
    target_queue_wait_p99_ms: u64,
    target_quorum_ack_p99_ms: u64,
    token_lane_enabled: bool,
    token_dict_max_entries: usize,
    token_min_reuse: usize,
    tier0_max_requests: usize,
    tier0_linger_us: u64,
    timeline_enabled: bool,
    timeline_sample_rate: u64,
}

impl PutBatcherConfig {
    fn normalized(mut self) -> Self {
        self.initial_max_requests = self.initial_max_requests.max(1);
        self.min_max_requests = self.min_max_requests.max(1);
        self.max_max_requests = self.max_max_requests.max(self.min_max_requests);
        self.initial_max_requests = self
            .initial_max_requests
            .clamp(self.min_max_requests, self.max_max_requests);

        self.max_linger_us = self.max_linger_us.max(self.min_linger_us);
        self.initial_linger_us = self
            .initial_linger_us
            .clamp(self.min_linger_us, self.max_linger_us);

        self.max_batch_bytes = self.max_batch_bytes.max(4 * 1024);
        self.pending_limit = self.pending_limit.max(1);
        self.tier0_max_requests = self.tier0_max_requests.max(1);
        self.dispatch_concurrency = self.dispatch_concurrency.clamp(1, 8);
        self.target_queue_depth = self.target_queue_depth.max(1);
        self.p99_budget_ms = self.p99_budget_ms.max(1);
        self.target_queue_wait_p99_ms = self.target_queue_wait_p99_ms.max(1);
        self.target_quorum_ack_p99_ms = self.target_quorum_ack_p99_ms.max(1);
        self.adaptive_min_request_floor = self
            .adaptive_min_request_floor
            .clamp(self.min_max_requests, self.max_max_requests);
        self.token_dict_max_entries = self.token_dict_max_entries.max(1);
        self.token_min_reuse = self.token_min_reuse.max(1);
        self.timeline_sample_rate = self.timeline_sample_rate.max(1);
        self
    }
}

#[derive(Debug)]
struct AdaptiveController {
    enabled: bool,
    mode: PutAdaptiveMode,
    min_request_floor: usize,
    target_queue_depth: usize,
    p99_budget_ms: u64,
    target_queue_wait_p99_ms: u64,
    target_quorum_ack_p99_ms: u64,
    min_max_requests: usize,
    max_max_requests: usize,
    min_linger_us: u64,
    max_linger_us: u64,
    current_max_requests: usize,
    current_linger_us: u64,
    last_tune: Instant,
    write_latency_ms_window: VecDeque<u64>,
    queue_wait_ms_window: VecDeque<u64>,
    quorum_ack_ms_window: VecDeque<u64>,
}

impl AdaptiveController {
    const TUNE_INTERVAL: Duration = Duration::from_millis(250);
    const MAX_WINDOW: usize = 256;

    fn new(cfg: &PutBatcherConfig) -> Self {
        Self {
            enabled: cfg.adaptive_enabled,
            mode: cfg.adaptive_mode,
            min_request_floor: cfg.adaptive_min_request_floor,
            target_queue_depth: cfg.target_queue_depth,
            p99_budget_ms: cfg.p99_budget_ms,
            target_queue_wait_p99_ms: cfg.target_queue_wait_p99_ms,
            target_quorum_ack_p99_ms: cfg.target_quorum_ack_p99_ms,
            min_max_requests: cfg.min_max_requests,
            max_max_requests: cfg.max_max_requests,
            min_linger_us: cfg.min_linger_us,
            max_linger_us: cfg.max_linger_us,
            current_max_requests: cfg.initial_max_requests,
            current_linger_us: cfg.initial_linger_us,
            last_tune: Instant::now(),
            write_latency_ms_window: VecDeque::with_capacity(Self::MAX_WINDOW),
            queue_wait_ms_window: VecDeque::with_capacity(Self::MAX_WINDOW),
            quorum_ack_ms_window: VecDeque::with_capacity(Self::MAX_WINDOW),
        }
    }

    fn current(&self) -> (usize, u64) {
        (self.current_max_requests, self.current_linger_us)
    }

    fn observe(
        &mut self,
        write_duration_ms: u64,
        queue_depth: usize,
        queue_wait_p99_ms: u64,
        quorum_ack_ms: Option<u64>,
        had_error: bool,
    ) -> Option<(usize, u64, u64, u64, u64, &'static str)> {
        self.write_latency_ms_window.push_back(write_duration_ms);
        if self.write_latency_ms_window.len() > Self::MAX_WINDOW {
            self.write_latency_ms_window.pop_front();
        }

        self.queue_wait_ms_window.push_back(queue_wait_p99_ms);
        if self.queue_wait_ms_window.len() > Self::MAX_WINDOW {
            self.queue_wait_ms_window.pop_front();
        }

        if let Some(v) = quorum_ack_ms {
            self.quorum_ack_ms_window.push_back(v);
            if self.quorum_ack_ms_window.len() > Self::MAX_WINDOW {
                self.quorum_ack_ms_window.pop_front();
            }
        }

        if !self.enabled || self.last_tune.elapsed() < Self::TUNE_INTERVAL {
            return None;
        }
        self.last_tune = Instant::now();

        let p99_ms = percentile_ms(&self.write_latency_ms_window, 99);
        let queue_wait_window_p99_ms = percentile_ms(&self.queue_wait_ms_window, 99);
        let quorum_ack_window_p99_ms = if self.quorum_ack_ms_window.is_empty() {
            0
        } else {
            percentile_ms(&self.quorum_ack_ms_window, 99)
        };
        let req_floor = self.min_request_floor.max(self.min_max_requests);
        let service_latency_over_budget = p99_ms > self.p99_budget_ms
            || (!self.quorum_ack_ms_window.is_empty()
                && quorum_ack_window_p99_ms > self.target_quorum_ack_p99_ms);
        let backlog_pressure = queue_depth > self.target_queue_depth
            || queue_wait_window_p99_ms > self.target_queue_wait_p99_ms;

        let mut reason = "hold";
        match self.mode {
            PutAdaptiveMode::Legacy => {
                if had_error || service_latency_over_budget || backlog_pressure {
                    let dec_req = self
                        .current_max_requests
                        .saturating_sub((self.current_max_requests / 3).max(1));
                    let dec_linger = self
                        .current_linger_us
                        .saturating_sub((self.current_linger_us / 3).max(1));
                    self.current_max_requests = dec_req.max(req_floor);
                    self.current_linger_us = dec_linger.max(self.min_linger_us);
                    reason = "decrease";
                } else if queue_depth > self.target_queue_depth && p99_ms <= self.p99_budget_ms {
                    let inc_req = self
                        .current_max_requests
                        .saturating_add((self.current_max_requests / 8).max(1));
                    let inc_linger = self
                        .current_linger_us
                        .saturating_add((self.current_linger_us / 8).max(1));
                    self.current_max_requests = inc_req.min(self.max_max_requests);
                    self.current_linger_us = inc_linger.min(self.max_linger_us);
                    reason = "increase";
                } else if queue_depth <= (self.target_queue_depth / 4).max(1)
                    && p99_ms < (self.p99_budget_ms / 2).max(1)
                {
                    self.current_linger_us = self
                        .current_linger_us
                        .saturating_sub((self.current_linger_us / 6).max(1))
                        .max(self.min_linger_us);
                    reason = "trim_linger";
                }
            }
            PutAdaptiveMode::QueueBacklogDrain => {
                if had_error || service_latency_over_budget {
                    let dec_req = self
                        .current_max_requests
                        .saturating_sub((self.current_max_requests / 4).max(1));
                    let inc_linger = self
                        .current_linger_us
                        .saturating_add((self.current_linger_us / 5).max(1));
                    self.current_max_requests = dec_req.max(req_floor);
                    self.current_linger_us = inc_linger.min(self.max_linger_us);
                    reason = "decrease_service_guard";
                } else if backlog_pressure {
                    let inc_req = self
                        .current_max_requests
                        .saturating_add((self.current_max_requests / 5).max(1));
                    let dec_linger = self
                        .current_linger_us
                        .saturating_sub((self.current_linger_us / 5).max(1));
                    self.current_max_requests = inc_req.min(self.max_max_requests);
                    self.current_linger_us = dec_linger.max(self.min_linger_us);
                    reason = "drain_backlog";
                } else if queue_depth <= (self.target_queue_depth / 4).max(1)
                    && queue_wait_window_p99_ms < (self.target_queue_wait_p99_ms / 2).max(1)
                    && p99_ms < (self.p99_budget_ms / 2).max(1)
                {
                    self.current_linger_us = self
                        .current_linger_us
                        .saturating_sub((self.current_linger_us / 6).max(1))
                        .max(self.min_linger_us);
                    reason = "trim_linger";
                }
            }
        }

        Some((
            self.current_max_requests,
            self.current_linger_us,
            p99_ms,
            queue_wait_window_p99_ms,
            quorum_ack_window_p99_ms,
            reason,
        ))
    }
}

#[derive(Debug)]
struct TokenLane {
    enabled: bool,
    dict: HashMap<Vec<u8>, u32>,
    observed: HashMap<Vec<u8>, usize>,
    next_token: u32,
    dict_max_entries: usize,
    min_reuse: usize,
}

impl TokenLane {
    fn new(enabled: bool, dict_max_entries: usize, min_reuse: usize) -> Self {
        Self {
            enabled,
            dict: HashMap::new(),
            observed: HashMap::new(),
            next_token: 1,
            dict_max_entries: dict_max_entries.max(1),
            min_reuse: min_reuse.max(1),
        }
    }

    fn next_token_id(next_token: &mut u32) -> u32 {
        let token = (*next_token).max(1);
        *next_token = next_token.wrapping_add(1).max(1);
        token
    }

    fn build_ref_request(
        batch_id: u64,
        submit_ts_micros: u64,
        batch: &[QueuedPutRequest],
    ) -> AstraWriteRequest {
        let mut values = Vec::<Vec<u8>>::new();
        let mut value_indexes = HashMap::<Vec<u8>, u32>::new();
        let mut ops = Vec::with_capacity(batch.len());
        for item in batch {
            let value_idx = if let Some(idx) = value_indexes.get(&item.req.value) {
                *idx
            } else {
                let idx = values.len() as u32;
                let value = item.req.value.clone();
                values.push(value.clone());
                value_indexes.insert(value, idx);
                idx
            };

            ops.push(AstraBatchPutRefOp {
                key: item.req.key.clone(),
                value_idx,
                lease: item.req.lease,
                ignore_value: item.req.ignore_value,
                ignore_lease: item.req.ignore_lease,
                prev_kv: item.req.prev_kv,
            });
        }

        AstraWriteRequest::PutBatchRef {
            batch_id,
            submit_ts_micros,
            values,
            ops,
        }
    }

    fn build_request(
        &mut self,
        batch_id: u64,
        submit_ts_micros: u64,
        batch: &[QueuedPutRequest],
    ) -> AstraWriteRequest {
        if !self.enabled {
            return Self::build_ref_request(batch_id, submit_ts_micros, batch);
        }

        let mut observed_deltas = HashMap::<Vec<u8>, usize>::new();
        let mut pending_tokens = HashMap::<Vec<u8>, u32>::new();
        let mut staged_next_token = self.next_token;

        let mut dict_additions = Vec::<AstraTokenValue>::new();
        let mut ops = Vec::<AstraBatchPutTokenOp>::with_capacity(batch.len());
        for item in batch {
            let value = &item.req.value;
            let token_id = if let Some(id) = self.dict.get(value) {
                *id
            } else if let Some(id) = pending_tokens.get(value) {
                *id
            } else {
                let seen_base = self.observed.get(value).copied().unwrap_or(0);
                let delta_seen = observed_deltas.get(value).copied().unwrap_or(0);
                let seen = seen_base.saturating_add(delta_seen).saturating_add(1);
                observed_deltas.insert(value.clone(), delta_seen.saturating_add(1));

                if seen < self.min_reuse {
                    return Self::build_ref_request(batch_id, submit_ts_micros, batch);
                }
                if self.dict.len().saturating_add(pending_tokens.len()) >= self.dict_max_entries {
                    return Self::build_ref_request(batch_id, submit_ts_micros, batch);
                }

                let next = Self::next_token_id(&mut staged_next_token);
                pending_tokens.insert(value.clone(), next);
                dict_additions.push(AstraTokenValue {
                    token_id: next,
                    value: value.clone(),
                });
                next
            };

            ops.push(AstraBatchPutTokenOp {
                key: item.req.key.clone(),
                token_id,
                lease: item.req.lease,
                ignore_value: item.req.ignore_value,
                ignore_lease: item.req.ignore_lease,
                prev_kv: item.req.prev_kv,
            });
        }

        for (value, delta) in observed_deltas {
            let seen = self.observed.entry(value).or_insert(0);
            *seen = seen.saturating_add(delta);
        }
        for (value, token_id) in pending_tokens {
            self.dict.insert(value, token_id);
        }
        self.next_token = staged_next_token;

        if self.observed.len() > self.dict_max_entries.saturating_mul(4) {
            self.observed.clear();
        }

        AstraWriteRequest::PutBatchTokenized {
            batch_id,
            submit_ts_micros,
            dict_epoch: 0,
            dict_additions,
            ops,
        }
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or_default()
}

fn now_millis() -> u64 {
    now_micros() / 1_000
}

#[derive(Debug, Clone)]
struct CloneableStatus {
    code: Code,
    message: String,
}

impl CloneableStatus {
    fn from_status(status: &Status) -> Self {
        Self {
            code: status.code(),
            message: status.message().to_string(),
        }
    }

    fn into_status(self) -> Status {
        Status::new(self.code, self.message)
    }
}

#[derive(Debug, Clone)]
struct GatewayReadTicket {
    enabled: bool,
    ttl_micros: u64,
    last_grant_micros: Arc<AtomicU64>,
    refresh_lock: Arc<AsyncMutex<()>>,
}

impl GatewayReadTicket {
    fn from_config(cfg: &AstraConfig) -> Self {
        Self {
            enabled: cfg.gateway_read_ticket_enabled,
            ttl_micros: cfg.gateway_read_ticket_ttl_ms.max(1).saturating_mul(1_000),
            last_grant_micros: Arc::new(AtomicU64::new(0)),
            refresh_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    fn is_fresh(&self) -> bool {
        if !self.enabled {
            return false;
        }
        let granted_at = self.last_grant_micros.load(Ordering::Relaxed);
        if granted_at == 0 {
            return false;
        }
        now_micros().saturating_sub(granted_at) <= self.ttl_micros
    }

    fn mark_fresh(&self) {
        if !self.enabled {
            return;
        }
        self.last_grant_micros
            .store(now_micros(), Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct GatewayGetSingleflightKey {
    key: Vec<u8>,
    revision: i64,
    keys_only: bool,
    count_only: bool,
}

#[derive(Debug, Default)]
struct GatewayGetSingleflightWaiters {
    waiters: Vec<oneshot::Sender<Result<RangeOutput, CloneableStatus>>>,
}

#[derive(Debug)]
enum GatewayGetJoinRole {
    LeaderTracked,
    Waiter(oneshot::Receiver<Result<RangeOutput, CloneableStatus>>),
    Bypass,
}

#[derive(Clone)]
struct GatewayGetSingleflight {
    enabled: bool,
    max_waiters: usize,
    waiter_timeout: Duration,
    state: Arc<AsyncMutex<HashMap<GatewayGetSingleflightKey, GatewayGetSingleflightWaiters>>>,
}

impl GatewayGetSingleflight {
    fn from_config(cfg: &AstraConfig) -> Self {
        Self {
            enabled: cfg.gateway_singleflight_enabled,
            max_waiters: cfg.gateway_singleflight_max_waiters.max(1),
            waiter_timeout: Duration::from_millis(2_000),
            state: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    async fn join_or_lead(&self, key: GatewayGetSingleflightKey) -> GatewayGetJoinRole {
        if !self.enabled {
            return GatewayGetJoinRole::Bypass;
        }
        let mut guard = self.state.lock().await;
        if let Some(waiters) = guard.get_mut(&key) {
            if waiters.waiters.len() >= self.max_waiters {
                metrics::inc_gateway_singleflight_overflow();
                return GatewayGetJoinRole::Bypass;
            }
            let (tx, rx) = oneshot::channel();
            waiters.waiters.push(tx);
            metrics::inc_gateway_singleflight_waiter();
            return GatewayGetJoinRole::Waiter(rx);
        }
        guard.insert(key, GatewayGetSingleflightWaiters::default());
        metrics::inc_gateway_singleflight_leader();
        GatewayGetJoinRole::LeaderTracked
    }

    async fn await_waiter(
        &self,
        waiter: oneshot::Receiver<Result<RangeOutput, CloneableStatus>>,
    ) -> Option<Result<RangeOutput, Status>> {
        if !self.enabled {
            return None;
        }
        match tokio::time::timeout(self.waiter_timeout, waiter).await {
            Ok(Ok(Ok(out))) => Some(Ok(out)),
            Ok(Ok(Err(err))) => Some(Err(err.into_status())),
            Ok(Err(_)) => None,
            Err(_) => {
                metrics::inc_gateway_singleflight_waiter_timeout();
                None
            }
        }
    }

    async fn complete(
        &self,
        key: &GatewayGetSingleflightKey,
        result: &Result<RangeOutput, Status>,
    ) {
        if !self.enabled {
            return;
        }
        let waiters = {
            let mut guard = self.state.lock().await;
            guard.remove(key).map(|v| v.waiters).unwrap_or_default()
        };
        if waiters.is_empty() {
            return;
        }
        let payload = match result {
            Ok(out) => Ok(out.clone()),
            Err(status) => Err(CloneableStatus::from_status(status)),
        };
        for waiter in waiters {
            let _ = waiter.send(payload.clone());
        }
    }
}

#[derive(Debug, Clone)]
struct WritePressureConfig {
    max_l0_files: usize,
    stall_at_files: usize,
    stall_max_delay_ms: u64,
    reject_after_ms: u64,
    reject_extra_files: usize,
    synth_file_bytes: usize,
    delay_band_l0_5_ms: u64,
    delay_band_l0_6_ms: u64,
    delay_band_l0_7_ms: u64,
    persist_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LsmPressurePersistedState {
    updated_at_unix_ms: u64,
    wal_queue_depth: u64,
    wal_queue_bytes: u64,
    put_queue_depth: u64,
    synth_l0_files: u64,
}

#[derive(Debug)]
struct WritePressureState {
    reject_since: Option<Instant>,
    last_persist_at: Instant,
    last_persist_l0_files: u64,
}

#[derive(Clone)]
struct WritePressureGate {
    cfg: Arc<WritePressureConfig>,
    state: Arc<tokio::sync::Mutex<WritePressureState>>,
}

impl WritePressureGate {
    fn new(cfg: &AstraConfig) -> Self {
        let persist_path = cfg.data_dir.join("lsm-pressure.json");
        let mut last_persist_l0_files = 0_u64;
        if let Ok(raw) = std::fs::read_to_string(&persist_path) {
            if let Ok(saved) = serde_json::from_str::<LsmPressurePersistedState>(&raw) {
                last_persist_l0_files = saved.synth_l0_files;
            }
        }
        Self {
            cfg: Arc::new(WritePressureConfig {
                max_l0_files: cfg.lsm_max_l0_files.max(1),
                stall_at_files: cfg
                    .lsm_stall_at_files
                    .max(1)
                    .min(cfg.lsm_max_l0_files.max(1)),
                stall_max_delay_ms: cfg.lsm_stall_max_delay_ms.max(1),
                reject_after_ms: cfg.lsm_reject_after_ms.max(1),
                reject_extra_files: cfg.lsm_reject_extra_files,
                synth_file_bytes: cfg.lsm_synth_file_bytes.max(4096),
                delay_band_l0_5_ms: cfg.lsm_delay_band_l0_5_ms.max(1),
                delay_band_l0_6_ms: cfg.lsm_delay_band_l0_6_ms.max(1),
                delay_band_l0_7_ms: cfg.lsm_delay_band_l0_7_ms.max(1),
                persist_path,
            }),
            state: Arc::new(tokio::sync::Mutex::new(WritePressureState {
                reject_since: None,
                last_persist_at: Instant::now(),
                last_persist_l0_files,
            })),
        }
    }

    fn synth_l0_files(
        &self,
        wal_queue_depth: u64,
        wal_queue_bytes: u64,
        put_queue_depth: u64,
    ) -> u64 {
        let depth = wal_queue_depth.max(put_queue_depth);
        let from_depth = (depth.saturating_add(127)) / 128;
        let synth_file_bytes = self.cfg.synth_file_bytes as u64;
        let from_bytes = (wal_queue_bytes.saturating_add(synth_file_bytes.saturating_sub(1)))
            / synth_file_bytes.max(1);
        from_depth.max(from_bytes)
    }

    async fn enforce(&self, put_queue_depth: usize, priority: WritePriority) -> Result<(), Status> {
        if priority == WritePriority::Tier0 {
            metrics::inc_put_tier0_bypass_write_pressure();
            return Ok(());
        }
        let wal_queue_depth = metrics::current_wal_queue_depth();
        let wal_queue_bytes = metrics::current_wal_queue_bytes();
        let put_queue_depth = put_queue_depth as u64;
        let synth_l0_files = self.synth_l0_files(wal_queue_depth, wal_queue_bytes, put_queue_depth);
        metrics::set_lsm_synth_l0_files(synth_l0_files);

        let max_l0 = self.cfg.max_l0_files as u64;
        let stall_at = self.cfg.stall_at_files as u64;
        let gradient_start = stall_at.min(5);
        let reject_at = max_l0.saturating_add(self.cfg.reject_extra_files as u64);
        let mut delay_ms = 0_u64;
        let mut reject = false;
        let mut persist_payload: Option<LsmPressurePersistedState> = None;

        {
            let now = Instant::now();
            let mut state = self.state.lock().await;

            if synth_l0_files > reject_at {
                let since = state.reject_since.get_or_insert(now);
                let reject_after = Duration::from_millis(self.cfg.reject_after_ms);
                if now.saturating_duration_since(*since) >= reject_after {
                    reject = true;
                } else {
                    delay_ms = self.cfg.stall_max_delay_ms;
                }
            } else {
                state.reject_since = None;
                if synth_l0_files >= gradient_start {
                    delay_ms = match synth_l0_files {
                        0..=4 => 0,
                        5 => self.cfg.delay_band_l0_5_ms,
                        6 => self.cfg.delay_band_l0_6_ms,
                        _ => {
                            let extra_pressure = synth_l0_files.saturating_sub(7);
                            self.cfg
                                .delay_band_l0_7_ms
                                .saturating_add(extra_pressure.saturating_mul(5))
                        }
                    }
                    .min(self.cfg.stall_max_delay_ms);
                }
            }

            let should_persist = now.saturating_duration_since(state.last_persist_at)
                >= Duration::from_secs(1)
                && (synth_l0_files != state.last_persist_l0_files || synth_l0_files >= stall_at);
            if should_persist {
                state.last_persist_at = now;
                state.last_persist_l0_files = synth_l0_files;
                persist_payload = Some(LsmPressurePersistedState {
                    updated_at_unix_ms: now_millis(),
                    wal_queue_depth,
                    wal_queue_bytes,
                    put_queue_depth,
                    synth_l0_files,
                });
            }
        }

        if let Some(payload) = persist_payload {
            if let Ok(json) = serde_json::to_vec_pretty(&payload) {
                if let Some(parent) = self.cfg.persist_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&self.cfg.persist_path, json);
            }
        }

        if reject {
            metrics::inc_write_reject_events();
            return Err(Status::resource_exhausted(format!(
                "write stalled by l0 pressure: synth_l0_files={} reject_at={} wal_queue_depth={} wal_queue_bytes={}",
                synth_l0_files, reject_at, wal_queue_depth, wal_queue_bytes
            )));
        }

        if delay_ms > 0 {
            metrics::observe_write_stall_delay_ms(delay_ms);
            match synth_l0_files {
                5 => metrics::inc_write_stall_band_5_events(),
                6 => metrics::inc_write_stall_band_6_events(),
                7.. => metrics::inc_write_stall_band_7_plus_events(),
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ListPrefetchCacheKey {
    key: Vec<u8>,
    range_end: Vec<u8>,
    limit: i64,
    revision: i64,
    keys_only: bool,
    count_only: bool,
}

#[derive(Debug, Default)]
struct ListPrefetchCacheState {
    entries: HashMap<ListPrefetchCacheKey, RangeOutput>,
    order: VecDeque<ListPrefetchCacheKey>,
}

#[derive(Clone)]
struct ListPrefetchCache {
    enabled: bool,
    prefetch_pages: Arc<AtomicUsize>,
    max_entries: Arc<AtomicUsize>,
    state: Arc<tokio::sync::Mutex<ListPrefetchCacheState>>,
}

impl ListPrefetchCache {
    fn new(enabled: bool, prefetch_pages: usize, max_entries: usize) -> Self {
        Self {
            enabled,
            prefetch_pages: Arc::new(AtomicUsize::new(prefetch_pages.max(1))),
            max_entries: Arc::new(AtomicUsize::new(max_entries.max(1))),
            state: Arc::new(tokio::sync::Mutex::new(ListPrefetchCacheState::default())),
        }
    }

    fn set_limits(&self, prefetch_pages: usize, max_entries: usize) {
        self.prefetch_pages
            .store(prefetch_pages.max(1), Ordering::Relaxed);
        self.max_entries
            .store(max_entries.max(1), Ordering::Relaxed);
    }

    fn prefetch_pages(&self) -> usize {
        self.prefetch_pages.load(Ordering::Relaxed).max(1)
    }

    fn max_entries(&self) -> usize {
        self.max_entries.load(Ordering::Relaxed).max(1)
    }

    async fn get(&self, key: &ListPrefetchCacheKey) -> Option<RangeOutput> {
        if !self.enabled {
            return None;
        }
        let guard = self.state.lock().await;
        guard.entries.get(key).cloned()
    }

    async fn put(&self, key: ListPrefetchCacheKey, value: RangeOutput) {
        if !self.enabled {
            return;
        }
        let mut guard = self.state.lock().await;
        if guard.entries.contains_key(&key) {
            guard.entries.insert(key, value);
            return;
        }
        guard.order.push_back(key.clone());
        guard.entries.insert(key, value);
        while guard.entries.len() > self.max_entries() {
            if let Some(oldest) = guard.order.pop_front() {
                guard.entries.remove(&oldest);
            } else {
                break;
            }
        }
    }

    async fn invalidate_all(&self) {
        if !self.enabled {
            return;
        }
        let mut guard = self.state.lock().await;
        guard.entries.clear();
        guard.order.clear();
    }

    fn should_prefetch(&self, req: &RangeRequest) -> bool {
        self.enabled && self.prefetch_pages() > 0 && req.limit > 0 && !req.count_only
    }
}

fn next_page_key_from_result(result: &RangeOutput) -> Option<Vec<u8>> {
    let (last_key, _) = result.kvs.last()?;
    let mut next = last_key.clone();
    next.push(0);
    Some(next)
}

async fn run_isolated_range(
    store: Arc<KvStore>,
    read_isolation_enabled: bool,
    key: Vec<u8>,
    range_end: Vec<u8>,
    limit: i64,
    revision: i64,
    keys_only: bool,
    count_only: bool,
) -> Result<RangeOutput, StoreError> {
    if !read_isolation_enabled {
        return store.range(&key, &range_end, limit, revision, keys_only, count_only);
    }

    metrics::inc_read_isolation_dispatch();
    let key_inline = key.clone();
    let range_end_inline = range_end.clone();
    let store_for_blocking = store.clone();
    match tokio::task::spawn_blocking(move || {
        store_for_blocking.range(&key, &range_end, limit, revision, keys_only, count_only)
    })
    .await
    {
        Ok(out) => out,
        Err(err) => {
            metrics::inc_read_isolation_failures();
            warn!(error = %err, "read isolation dispatch failed; falling back inline");
            store.range(
                &key_inline,
                &range_end_inline,
                limit,
                revision,
                keys_only,
                count_only,
            )
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MsStats {
    avg_ms: u64,
    p50_ms: u64,
    p90_ms: u64,
    p99_ms: u64,
    max_ms: u64,
}

fn percentile_from_slice(values: &[u64], pct: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = (sorted.len().saturating_sub(1) * pct) / 100;
    sorted[idx]
}

fn percentile_ms(window: &VecDeque<u64>, pct: usize) -> u64 {
    if window.is_empty() {
        return 0;
    }
    let values = window.iter().copied().collect::<Vec<_>>();
    percentile_from_slice(&values, pct)
}

fn summarize_ms(values: &[u64]) -> MsStats {
    if values.is_empty() {
        return MsStats {
            avg_ms: 0,
            p50_ms: 0,
            p90_ms: 0,
            p99_ms: 0,
            max_ms: 0,
        };
    }
    let total = values.iter().copied().fold(0_u64, u64::saturating_add);
    MsStats {
        avg_ms: total / (values.len() as u64),
        p50_ms: percentile_from_slice(values, 50),
        p90_ms: percentile_from_slice(values, 90),
        p99_ms: percentile_from_slice(values, 99),
        max_ms: values.iter().copied().max().unwrap_or(0),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfileTuning {
    profile: AstraProfile,
    put_max_requests: usize,
    put_linger_us: u64,
    prefetch_pages: usize,
    prefetch_entries: usize,
    bg_io_tokens_per_sec: u64,
    bg_io_burst_tokens: u64,
    bg_io_sqe_tokens_per_sec: u64,
    bg_io_sqe_burst_tokens: u64,
}

fn prefetch_entries_for_fraction(
    max_memory_bytes: usize,
    fraction_pct: usize,
    floor: usize,
) -> usize {
    let fraction_pct = fraction_pct.min(95);
    let bytes_budget = max_memory_bytes.saturating_mul(fraction_pct) / 100;
    // Use a coarse per-entry memory estimate to translate memory budget to cache entries.
    let estimated_entry_bytes = 4 * 1024_usize;
    floor.max(bytes_budget / estimated_entry_bytes).max(1)
}

fn profile_tuning(cfg: &AstraConfig, profile: AstraProfile) -> ProfileTuning {
    let put_max = cfg.put_batch_max_requests.max(1);
    let put_min = cfg.put_batch_min_requests.max(1).min(put_max);
    let prefetch_pages = cfg.list_prefetch_pages.max(1);
    let prefetch_entries = cfg.list_prefetch_cache_entries.max(1);
    let bg_tokens = cfg.bg_io_tokens_per_sec.max(1);
    let bg_burst = cfg.bg_io_burst_tokens.max(1);
    let bg_sqe_tokens = cfg.bg_io_sqe_tokens_per_sec.max(1);
    let bg_sqe_burst = cfg.bg_io_sqe_burst_tokens.max(1);

    match profile {
        AstraProfile::Kubernetes => ProfileTuning {
            profile,
            put_max_requests: put_min.max(32).min(put_max),
            put_linger_us: 0,
            prefetch_pages: prefetch_pages.max(4),
            prefetch_entries: prefetch_entries_for_fraction(
                cfg.max_memory_bytes(),
                70,
                prefetch_entries,
            ),
            bg_io_tokens_per_sec: (bg_tokens.saturating_mul(3) / 4).max(1),
            bg_io_burst_tokens: (bg_burst.saturating_mul(3) / 4).max(1),
            bg_io_sqe_tokens_per_sec: (bg_sqe_tokens.saturating_mul(3) / 4).max(1),
            bg_io_sqe_burst_tokens: (bg_sqe_burst.saturating_mul(3) / 4).max(1),
        },
        AstraProfile::Omni => ProfileTuning {
            profile,
            put_max_requests: put_max,
            put_linger_us: 250,
            prefetch_pages: prefetch_pages.clamp(1, 2),
            prefetch_entries: prefetch_entries.max(2_048),
            bg_io_tokens_per_sec: bg_tokens.saturating_mul(2).max(bg_tokens),
            bg_io_burst_tokens: bg_burst.saturating_mul(2).max(bg_burst),
            bg_io_sqe_tokens_per_sec: bg_sqe_tokens.saturating_mul(2).max(bg_sqe_tokens),
            bg_io_sqe_burst_tokens: bg_sqe_burst.saturating_mul(2).max(bg_sqe_burst),
        },
        AstraProfile::Gateway => ProfileTuning {
            profile,
            put_max_requests: put_min.max(16).min(put_max).min(128),
            put_linger_us: cfg.put_batch_min_linger_us.min(50),
            prefetch_pages: 1,
            prefetch_entries: prefetch_entries.saturating_div(2).max(512),
            bg_io_tokens_per_sec: bg_tokens,
            bg_io_burst_tokens: bg_burst,
            bg_io_sqe_tokens_per_sec: bg_sqe_tokens,
            bg_io_sqe_burst_tokens: bg_sqe_burst,
        },
        AstraProfile::Auto => profile_tuning(cfg, AstraProfile::Kubernetes),
    }
}

fn choose_auto_profile(
    mix_delta: metrics::RequestMixSnapshot,
    current: AstraProfile,
) -> AstraProfile {
    let total = mix_delta.total();
    if total == 0 {
        return current;
    }

    let watch = mix_delta.watch as f64 / total as f64;
    let read = (mix_delta.get.saturating_add(mix_delta.list)) as f64 / total as f64;
    let write = (mix_delta
        .put
        .saturating_add(mix_delta.delete)
        .saturating_add(mix_delta.txn)
        .saturating_add(mix_delta.lease)) as f64
        / total as f64;
    let tier0 = mix_delta.tier0 as f64 / total as f64;

    if watch >= 0.35 {
        AstraProfile::Gateway
    } else if write >= 0.45 {
        AstraProfile::Omni
    } else if read >= 0.50 || tier0 >= 0.10 {
        AstraProfile::Kubernetes
    } else {
        current
    }
}

async fn apply_profile_tuning(
    tuning: ProfileTuning,
    put_batcher: Option<&PutBatcher>,
    list_prefetch_cache: &ListPrefetchCache,
    bg_io_limiter: &IoTokenBucket,
    bg_sqe_limiter: &IoTokenBucket,
) {
    if let Some(batcher) = put_batcher {
        batcher.set_normal_profile_overrides(tuning.put_max_requests, tuning.put_linger_us);
    }
    list_prefetch_cache.set_limits(tuning.prefetch_pages, tuning.prefetch_entries);
    bg_io_limiter
        .configure(tuning.bg_io_tokens_per_sec, tuning.bg_io_burst_tokens)
        .await;
    bg_sqe_limiter
        .configure(
            tuning.bg_io_sqe_tokens_per_sec,
            tuning.bg_io_sqe_burst_tokens,
        )
        .await;

    metrics::set_profile_active(tuning.profile.as_str());
    metrics::set_profile_applied_put_max_requests(tuning.put_max_requests);
    metrics::set_profile_applied_put_linger_us(tuning.put_linger_us);
    metrics::set_profile_applied_prefetch_entries(tuning.prefetch_entries);
    metrics::set_profile_applied_bg_io_tokens_per_sec(tuning.bg_io_tokens_per_sec);
}

fn apply_startup_profile_defaults(cfg: &mut AstraConfig) {
    if cfg.profile == AstraProfile::Gateway {
        // Gateway profile benefits from larger watch buffers to absorb burst fan-out.
        cfg.watch_ring_capacity = cfg.watch_ring_capacity.max(65_536);
        cfg.watch_broadcast_capacity = cfg.watch_broadcast_capacity.max(8_192);
    }
}

fn start_profile_governor(
    cfg: AstraConfig,
    put_batcher: Option<PutBatcher>,
    list_prefetch_cache: ListPrefetchCache,
    bg_io_limiter: IoTokenBucket,
    bg_sqe_limiter: IoTokenBucket,
) {
    tokio::spawn(async move {
        let mut active_profile = if cfg.profile == AstraProfile::Auto {
            AstraProfile::Kubernetes
        } else {
            cfg.profile
        };

        let initial_tuning = profile_tuning(&cfg, active_profile);
        apply_profile_tuning(
            initial_tuning,
            put_batcher.as_ref(),
            &list_prefetch_cache,
            &bg_io_limiter,
            &bg_sqe_limiter,
        )
        .await;
        let mut last_applied = Some(initial_tuning);
        info!(
            profile = %active_profile.as_str(),
            put_max_requests = initial_tuning.put_max_requests,
            put_linger_us = initial_tuning.put_linger_us,
            prefetch_pages = initial_tuning.prefetch_pages,
            prefetch_entries = initial_tuning.prefetch_entries,
            bg_io_tokens_per_sec = initial_tuning.bg_io_tokens_per_sec,
            "profile tuning applied"
        );

        if cfg.profile != AstraProfile::Auto {
            return;
        }

        let mut last_snapshot = metrics::request_mix_snapshot();
        let dwell = Duration::from_secs(cfg.profile_min_dwell_secs.max(1));
        let sample = Duration::from_secs(cfg.profile_sample_secs.max(1));
        let mut last_switch = Instant::now();

        loop {
            tokio::time::sleep(sample).await;
            let current_snapshot = metrics::request_mix_snapshot();
            let mix_delta = current_snapshot.delta_since(last_snapshot);
            last_snapshot = current_snapshot;

            let suggested = choose_auto_profile(mix_delta, active_profile);
            if suggested != active_profile && last_switch.elapsed() >= dwell {
                active_profile = suggested;
                last_switch = Instant::now();
                metrics::inc_profile_switch_total();
            }

            let next_tuning = profile_tuning(&cfg, active_profile);
            if last_applied != Some(next_tuning) {
                apply_profile_tuning(
                    next_tuning,
                    put_batcher.as_ref(),
                    &list_prefetch_cache,
                    &bg_io_limiter,
                    &bg_sqe_limiter,
                )
                .await;
                info!(
                    profile = %active_profile.as_str(),
                    put_max_requests = next_tuning.put_max_requests,
                    put_linger_us = next_tuning.put_linger_us,
                    prefetch_pages = next_tuning.prefetch_pages,
                    prefetch_entries = next_tuning.prefetch_entries,
                    bg_io_tokens_per_sec = next_tuning.bg_io_tokens_per_sec,
                    sampled_total = mix_delta.total(),
                    sampled_get = mix_delta.get,
                    sampled_list = mix_delta.list,
                    sampled_put = mix_delta.put,
                    sampled_delete = mix_delta.delete,
                    sampled_txn = mix_delta.txn,
                    sampled_lease = mix_delta.lease,
                    sampled_watch = mix_delta.watch,
                    "auto profile governor update"
                );
                last_applied = Some(next_tuning);
            }
        }
    });
}

fn estimate_ref_op_bytes(req: &PutRequest, value_is_new: bool) -> usize {
    let mut bytes = 48 + req.key.len();
    if value_is_new {
        bytes = bytes.saturating_add(req.value.len().saturating_add(8));
    } else {
        bytes = bytes.saturating_add(4);
    }
    bytes
}

#[derive(Clone)]
struct PutBatcher {
    tx_normal: mpsc::Sender<QueuedPutRequest>,
    tx_tier0: mpsc::Sender<QueuedPutRequest>,
    pending_limit_normal: usize,
    pending_limit_tier0: usize,
    normal_override_max_requests: Arc<AtomicUsize>,
    normal_override_linger_us: Arc<AtomicU64>,
}

struct BatchWriteOutcome {
    batch_id: u64,
    priority: WritePriority,
    timeline_sampled: bool,
    request_count: usize,
    write_duration_ms: u64,
    queue_wait_p99_ms: u64,
    had_error: bool,
}

impl PutBatcher {
    fn spawn(raft: Raft<AstraTypeConfig>, cfg: PutBatcherConfig) -> Self {
        let cfg = cfg.normalized();
        let tier0_pending_limit = if cfg.pending_limit < 64 {
            cfg.pending_limit
        } else {
            (cfg.pending_limit / 4).max(64)
        };
        let normal_pending_limit = cfg.pending_limit;
        let (tx_tier0, mut rx_tier0) = mpsc::channel::<QueuedPutRequest>(tier0_pending_limit);
        let (tx_normal, mut rx_normal) = mpsc::channel::<QueuedPutRequest>(normal_pending_limit);
        let normal_override_max_requests = Arc::new(AtomicUsize::new(0));
        let normal_override_linger_us = Arc::new(AtomicU64::new(u64::MAX));
        let normal_override_max_requests_task = normal_override_max_requests.clone();
        let normal_override_linger_us_task = normal_override_linger_us.clone();
        let next_batch_id = Arc::new(AtomicU64::new(1));

        tokio::spawn(async move {
            let mut carry_tier0: Option<QueuedPutRequest> = None;
            let mut carry_normal: Option<QueuedPutRequest> = None;
            let mut tier0_closed = false;
            let mut normal_closed = false;
            let mut in_flight = JoinSet::<BatchWriteOutcome>::new();
            let mut adaptive = AdaptiveController::new(&cfg);
            let mut token_lane = TokenLane::new(
                cfg.token_lane_enabled,
                cfg.token_dict_max_entries,
                cfg.token_min_reuse,
            );
            let mut in_flight_request_count = 0_usize;
            metrics::set_put_inflight_requests(0);
            metrics::set_put_tier0_queue_depth(0);
            metrics::set_put_normal_queue_depth(0);

            loop {
                while in_flight.len() < cfg.dispatch_concurrency {
                    let first = if let Some(item) = carry_tier0.take() {
                        Some((item, WritePriority::Tier0))
                    } else if let Some(item) = carry_normal.take() {
                        Some((item, WritePriority::Normal))
                    } else if in_flight.is_empty() {
                        loop {
                            if tier0_closed && normal_closed {
                                break None;
                            }
                            tokio::select! {
                                biased;
                                maybe = rx_tier0.recv(), if !tier0_closed => {
                                    match maybe {
                                        Some(item) => break Some((item, WritePriority::Tier0)),
                                        None => tier0_closed = true,
                                    }
                                }
                                maybe = rx_normal.recv(), if !normal_closed => {
                                    match maybe {
                                        Some(item) => break Some((item, WritePriority::Normal)),
                                        None => normal_closed = true,
                                    }
                                }
                            }
                        }
                    } else {
                        match rx_tier0.try_recv() {
                            Ok(item) => Some((item, WritePriority::Tier0)),
                            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                                match rx_normal.try_recv() {
                                    Ok(item) => Some((item, WritePriority::Normal)),
                                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
                                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                                        normal_closed = true;
                                        None
                                    }
                                }
                            }
                            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                                tier0_closed = true;
                                match rx_normal.try_recv() {
                                    Ok(item) => Some((item, WritePriority::Normal)),
                                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
                                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                                        normal_closed = true;
                                        None
                                    }
                                }
                            }
                        }
                    };

                    let Some((first, batch_priority)) = first else {
                        break;
                    };

                    let queue_depth_at_start = rx_tier0.len().saturating_add(rx_normal.len());
                    let (current_max_requests, current_linger_us) = if batch_priority
                        == WritePriority::Tier0
                    {
                        (cfg.tier0_max_requests, cfg.tier0_linger_us)
                    } else {
                        let (mut req_cap, mut linger_us) = adaptive.current();
                        let override_req =
                            normal_override_max_requests_task.load(Ordering::Relaxed);
                        if override_req > 0 {
                            req_cap =
                                override_req.clamp(cfg.min_max_requests, cfg.max_max_requests);
                        }
                        let override_linger =
                            normal_override_linger_us_task.load(Ordering::Relaxed);
                        if override_linger != u64::MAX {
                            linger_us = override_linger.clamp(cfg.min_linger_us, cfg.max_linger_us);
                        }
                        (req_cap, linger_us)
                    };

                    let mut batch = Vec::with_capacity(current_max_requests);
                    let mut value_seen = HashSet::<Vec<u8>>::new();
                    let mut est_bytes = 64_usize;
                    debug_assert_eq!(first.priority, batch_priority);
                    let first_value_is_new = !value_seen.contains(first.req.value.as_slice());
                    if first_value_is_new {
                        value_seen.insert(first.req.value.clone());
                    }
                    est_bytes = est_bytes
                        .saturating_add(estimate_ref_op_bytes(&first.req, first_value_is_new));
                    batch.push(first);

                    let deadline =
                        tokio::time::Instant::now() + Duration::from_micros(current_linger_us);
                    while batch.len() < current_max_requests {
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            break;
                        }

                        let timeout = deadline.saturating_duration_since(now);
                        let next = if batch_priority == WritePriority::Tier0 {
                            match tokio::time::timeout(timeout, rx_tier0.recv()).await {
                                Ok(Some(next)) => Some(next),
                                Ok(None) => {
                                    tier0_closed = true;
                                    None
                                }
                                Err(_) => None,
                            }
                        } else {
                            match tokio::time::timeout(timeout, rx_normal.recv()).await {
                                Ok(Some(next)) => Some(next),
                                Ok(None) => {
                                    normal_closed = true;
                                    None
                                }
                                Err(_) => None,
                            }
                        };

                        match next {
                            Some(next) => {
                                debug_assert_eq!(next.priority, batch_priority);
                                let value_is_new = !value_seen.contains(next.req.value.as_slice());
                                let next_est = estimate_ref_op_bytes(&next.req, value_is_new);
                                if !batch.is_empty()
                                    && est_bytes.saturating_add(next_est) > cfg.max_batch_bytes
                                {
                                    if batch_priority == WritePriority::Tier0 {
                                        carry_tier0 = Some(next);
                                    } else {
                                        carry_normal = Some(next);
                                    }
                                    break;
                                }
                                if value_is_new {
                                    value_seen.insert(next.req.value.clone());
                                }
                                est_bytes = est_bytes.saturating_add(next_est);
                                batch.push(next);
                            }
                            None => break,
                        }
                    }

                    let batch_id = next_batch_id.fetch_add(1, Ordering::Relaxed);
                    let submit_ts_micros = now_micros();
                    let timeline_sampled =
                        cfg.timeline_enabled && (batch_id % cfg.timeline_sample_rate == 0);
                    let queue_wait_samples_ms = batch
                        .iter()
                        .map(|item| item.enqueued_at.elapsed().as_millis() as u64)
                        .collect::<Vec<_>>();
                    let queue_wait_stats = summarize_ms(&queue_wait_samples_ms);
                    for queue_wait_ms in &queue_wait_samples_ms {
                        metrics::observe_put_queue_wait_ms(*queue_wait_ms);
                        metrics::observe_put_queue_wait_ms_by_priority(
                            *queue_wait_ms,
                            batch_priority == WritePriority::Tier0,
                        );
                    }
                    metrics::observe_put_batch_size(batch.len());
                    metrics::inc_put_batches();
                    metrics::add_put_batch_requests(batch.len());
                    if batch_priority == WritePriority::Tier0 {
                        metrics::inc_put_tier0_dispatch_events();
                    }

                    if timeline_sampled {
                        info!(
                            stage = "put_batch_dispatch",
                            batch_id,
                            priority = %batch_priority.as_str(),
                            request_count = batch.len(),
                            queue_depth = queue_depth_at_start,
                            est_bytes,
                            current_max_requests,
                            current_linger_us,
                            queue_wait_avg_ms = queue_wait_stats.avg_ms,
                            queue_wait_p50_ms = queue_wait_stats.p50_ms,
                            queue_wait_p90_ms = queue_wait_stats.p90_ms,
                            queue_wait_p99_ms = queue_wait_stats.p99_ms,
                            max_queue_wait_ms = queue_wait_stats.max_ms,
                            token_lane_enabled = cfg.token_lane_enabled,
                            in_flight_writes = in_flight.len(),
                            "raft timeline"
                        );
                    }

                    let write_req = token_lane.build_request(batch_id, submit_ts_micros, &batch);
                    in_flight_request_count = in_flight_request_count.saturating_add(batch.len());
                    metrics::set_put_inflight_requests(
                        in_flight_request_count
                            .saturating_add(rx_tier0.len())
                            .saturating_add(rx_normal.len())
                            .saturating_add(usize::from(carry_tier0.is_some()))
                            .saturating_add(usize::from(carry_normal.is_some())),
                    );
                    metrics::set_put_tier0_queue_depth(
                        rx_tier0
                            .len()
                            .saturating_add(usize::from(carry_tier0.is_some())),
                    );
                    metrics::set_put_normal_queue_depth(
                        rx_normal
                            .len()
                            .saturating_add(usize::from(carry_normal.is_some())),
                    );
                    let raft_for_write = raft.clone();
                    in_flight.spawn(async move {
                        let write_started = Instant::now();
                        let write_res = raft_for_write.client_write(write_req).await;
                        let write_duration_ms = write_started.elapsed().as_millis() as u64;
                        metrics::observe_put_raft_client_write_ms(write_duration_ms);
                        let mut had_error = false;
                        let request_count = batch.len();

                        match write_res {
                            Ok(resp) => match resp.data {
                                AstraWriteResponse::PutBatch { results } => {
                                    if results.len() != batch.len() {
                                        let msg = format!(
                                            "unexpected batch response size: got {} results for {} requests",
                                            results.len(),
                                            batch.len()
                                        );
                                        had_error = true;
                                        for item in batch {
                                            let _ = item
                                                .response_tx
                                                .send(Err(Status::internal(msg.clone())));
                                        }
                                    } else {
                                        for (item, result) in
                                            batch.into_iter().zip(results.into_iter())
                                        {
                                            let _ = item
                                                .response_tx
                                                .send(Ok((result.revision, result.prev)));
                                        }
                                    }
                                }
                                AstraWriteResponse::Put { revision, prev } if batch.len() == 1 => {
                                    if let Some(item) = batch.into_iter().next() {
                                        let _ = item.response_tx.send(Ok((revision, prev)));
                                    }
                                }
                                _ => {
                                    had_error = true;
                                    for item in batch {
                                        let _ = item.response_tx.send(Err(Status::internal(
                                            "unexpected state machine response for put batch",
                                        )));
                                    }
                                }
                            },
                            Err(err) => {
                                had_error = true;
                                let status = map_raft_write_err(err);
                                let code = status.code();
                                let msg = status.message().to_string();
                                for item in batch {
                                    let _ = item.response_tx.send(Err(Status::new(code, msg.clone())));
                                }
                            }
                        }

                        BatchWriteOutcome {
                            batch_id,
                            priority: batch_priority,
                            timeline_sampled,
                            request_count,
                            write_duration_ms,
                            queue_wait_p99_ms: queue_wait_stats.p99_ms,
                            had_error,
                        }
                    });
                }

                if in_flight.is_empty() {
                    if tier0_closed
                        && normal_closed
                        && carry_tier0.is_none()
                        && carry_normal.is_none()
                    {
                        break;
                    }
                    if carry_tier0.is_none() && carry_normal.is_none() {
                        loop {
                            if tier0_closed && normal_closed {
                                break;
                            }
                            tokio::select! {
                                biased;
                                maybe = rx_tier0.recv(), if !tier0_closed => {
                                    match maybe {
                                        Some(item) => {
                                            carry_tier0 = Some(item);
                                            break;
                                        }
                                        None => tier0_closed = true,
                                    }
                                }
                                maybe = rx_normal.recv(), if !normal_closed => {
                                    match maybe {
                                        Some(item) => {
                                            carry_normal = Some(item);
                                            break;
                                        }
                                        None => normal_closed = true,
                                    }
                                }
                            }
                        }
                        if carry_tier0.is_some() || carry_normal.is_some() {
                            continue;
                        }
                        if tier0_closed && normal_closed {
                            break;
                        }
                        continue;
                    }
                    continue;
                }

                match in_flight.join_next().await {
                    Some(Ok(outcome)) => {
                        in_flight_request_count =
                            in_flight_request_count.saturating_sub(outcome.request_count);
                        let queue_depth_after = rx_tier0.len().saturating_add(rx_normal.len());
                        metrics::set_put_inflight_requests(
                            in_flight_request_count
                                .saturating_add(queue_depth_after)
                                .saturating_add(usize::from(carry_tier0.is_some()))
                                .saturating_add(usize::from(carry_normal.is_some())),
                        );
                        metrics::set_put_tier0_queue_depth(
                            rx_tier0
                                .len()
                                .saturating_add(usize::from(carry_tier0.is_some())),
                        );
                        metrics::set_put_normal_queue_depth(
                            rx_normal
                                .len()
                                .saturating_add(usize::from(carry_normal.is_some())),
                        );
                        if outcome.timeline_sampled {
                            info!(
                                stage = "raft_client_write_done",
                                batch_id = outcome.batch_id,
                                priority = %outcome.priority.as_str(),
                                write_duration_ms = outcome.write_duration_ms,
                                queue_depth = queue_depth_after,
                                had_error = outcome.had_error,
                                in_flight_writes = in_flight.len(),
                                "raft timeline"
                            );
                        }

                        if outcome.priority == WritePriority::Normal {
                            if let Some((
                                next_req_cap,
                                next_linger_us,
                                p99_write_ms,
                                p99_queue_wait_ms,
                                p99_quorum_ack_ms,
                                reason,
                            )) = adaptive.observe(
                                outcome.write_duration_ms,
                                queue_depth_after,
                                outcome.queue_wait_p99_ms,
                                metrics::latest_put_quorum_ack_ms(),
                                outcome.had_error,
                            ) {
                                info!(
                                    current_max_requests = next_req_cap,
                                    current_linger_us = next_linger_us,
                                    p99_write_ms,
                                    p99_queue_wait_ms,
                                    p99_quorum_ack_ms,
                                    queue_depth = queue_depth_after,
                                    in_flight_writes = in_flight.len(),
                                    adaptive_mode = %adaptive.mode.as_str(),
                                    reason,
                                    "put adaptive controller tuned"
                                );
                            }
                        }
                    }
                    Some(Err(err)) => {
                        warn!(error = %err, "put batch write task join failure");
                        metrics::set_put_inflight_requests(
                            in_flight_request_count
                                .saturating_add(rx_tier0.len())
                                .saturating_add(rx_normal.len())
                                .saturating_add(usize::from(carry_tier0.is_some()))
                                .saturating_add(usize::from(carry_normal.is_some())),
                        );
                        metrics::set_put_tier0_queue_depth(
                            rx_tier0
                                .len()
                                .saturating_add(usize::from(carry_tier0.is_some())),
                        );
                        metrics::set_put_normal_queue_depth(
                            rx_normal
                                .len()
                                .saturating_add(usize::from(carry_normal.is_some())),
                        );
                    }
                    None => {
                        if tier0_closed
                            && normal_closed
                            && carry_tier0.is_none()
                            && carry_normal.is_none()
                        {
                            metrics::set_put_inflight_requests(0);
                            metrics::set_put_tier0_queue_depth(0);
                            metrics::set_put_normal_queue_depth(0);
                            break;
                        }
                    }
                }
            }
        });

        Self {
            tx_normal,
            tx_tier0,
            pending_limit_normal: normal_pending_limit,
            pending_limit_tier0: tier0_pending_limit,
            normal_override_max_requests,
            normal_override_linger_us,
        }
    }

    async fn submit(
        &self,
        req: PutRequest,
        priority: WritePriority,
    ) -> Result<(i64, Option<ValueEntry>), Status> {
        let (tx, rx) = oneshot::channel();
        let sender = if priority == WritePriority::Tier0 {
            &self.tx_tier0
        } else {
            &self.tx_normal
        };
        sender
            .send(QueuedPutRequest {
                req,
                priority,
                response_tx: tx,
                enqueued_at: Instant::now(),
            })
            .await
            .map_err(|_| Status::unavailable("put batcher unavailable"))?;
        if priority == WritePriority::Tier0 {
            metrics::inc_put_tier0_enqueued();
        }

        rx.await
            .map_err(|_| Status::unavailable("put batcher response dropped"))?
    }

    fn queue_depth_estimate(&self, priority: WritePriority) -> usize {
        let tier0 = self
            .pending_limit_tier0
            .saturating_sub(self.tx_tier0.capacity());
        let normal = self
            .pending_limit_normal
            .saturating_sub(self.tx_normal.capacity());
        match priority {
            WritePriority::Tier0 => tier0,
            WritePriority::Normal => normal,
        }
    }

    fn queue_depth_estimate_total(&self) -> usize {
        self.queue_depth_estimate(WritePriority::Tier0)
            .saturating_add(self.queue_depth_estimate(WritePriority::Normal))
    }

    fn set_normal_profile_overrides(&self, max_requests: usize, linger_us: u64) {
        self.normal_override_max_requests
            .store(max_requests.max(1), Ordering::Relaxed);
        self.normal_override_linger_us
            .store(linger_us, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
struct BulkLoadJob {
    job_id: String,
    status: String,
    message: String,
    records_total: u64,
    records_applied: u64,
    started_at_unix_ms: u64,
    finished_at_unix_ms: u64,
}

impl BulkLoadJob {
    fn new(job_id: String) -> Self {
        Self {
            job_id,
            status: "queued".to_string(),
            message: String::new(),
            records_total: 0,
            records_applied: 0,
            started_at_unix_ms: now_millis(),
            finished_at_unix_ms: 0,
        }
    }
}

enum BulkLoadManifestSource {
    Local {
        manifest_parent: PathBuf,
    },
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
        manifest_dir: String,
    },
}

fn parse_s3_uri(uri: &str) -> std::result::Result<(String, String), String> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| "manifest source must use s3://".to_string())?;
    let mut parts = rest.splitn(2, '/');
    let bucket = parts
        .next()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "s3 uri is missing bucket".to_string())?;
    let key = parts
        .next()
        .map(|v| v.trim_matches('/'))
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "s3 uri is missing key".to_string())?;
    Ok((bucket.to_string(), key.to_string()))
}

fn parse_manifest_checksum(raw: &str) -> Option<u32> {
    let v = raw.trim();
    if v.is_empty() {
        return None;
    }
    if let Some(hex) = v.strip_prefix("0x").or_else(|| v.strip_prefix("0X")) {
        return u32::from_str_radix(hex, 16).ok();
    }
    if v.len() <= 8 && v.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(parsed) = u32::from_str_radix(v, 16) {
            return Some(parsed);
        }
    }
    v.parse::<u32>().ok()
}

fn join_s3_key(base: &str, child: &str) -> String {
    let base = base.trim_matches('/');
    let child = child.trim_matches('/');
    if base.is_empty() {
        return child.to_string();
    }
    if child.is_empty() {
        return base.to_string();
    }
    format!("{base}/{child}")
}

async fn build_s3_client(cfg: &S3Config) -> aws_sdk_s3::Client {
    let shared_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(cfg.endpoint.clone())
        .region(aws_config::Region::new(cfg.region.clone()))
        .load()
        .await;
    let s3_conf = aws_sdk_s3::config::Builder::from(&shared_cfg)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(s3_conf)
}

#[derive(Clone)]
struct AstraAdminService {
    raft: Raft<AstraTypeConfig>,
    member_id: u64,
    tenanting: Tenanting,
    s3_cfg: Option<S3Config>,
    jobs: Arc<tokio::sync::Mutex<HashMap<String, BulkLoadJob>>>,
    next_job_id: Arc<AtomicU64>,
}

impl AstraAdminService {
    fn new(
        raft: Raft<AstraTypeConfig>,
        member_id: u64,
        tenanting: Tenanting,
        s3_cfg: Option<S3Config>,
    ) -> Self {
        Self {
            raft,
            member_id,
            tenanting,
            s3_cfg,
            jobs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            next_job_id: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn update_job<F>(&self, job_id: &str, f: F)
    where
        F: FnOnce(&mut BulkLoadJob),
    {
        let mut guard = self.jobs.lock().await;
        if let Some(job) = guard.get_mut(job_id) {
            f(job);
        }
    }

    async fn run_bulkload_job(
        &self,
        job_id: String,
        tenant_id: Option<String>,
        req: BulkLoadRequest,
    ) -> std::result::Result<(), String> {
        self.update_job(&job_id, |job| {
            job.status = "running".to_string();
            job.message = "validating manifest".to_string();
            job.started_at_unix_ms = now_millis();
        })
        .await;

        let manifest_source = req.manifest_source.clone();
        let (manifest_bytes, manifest, source): (Vec<u8>, TierManifest, BulkLoadManifestSource) =
            if manifest_source.starts_with("s3://") {
                let s3_cfg = self.s3_cfg.as_ref().ok_or_else(|| {
                    "s3 manifest source requires ASTRAD_S3_* configuration".to_string()
                })?;
                let (bucket, key) = parse_s3_uri(&manifest_source)?;
                let manifest_dir = key
                    .rsplit_once('/')
                    .map(|(dir, _)| dir.to_string())
                    .unwrap_or_default();
                let client = build_s3_client(s3_cfg).await;
                let manifest_obj = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| {
                        format!("failed to fetch manifest from s3://{bucket}/{key}: {e}")
                    })?;
                let bytes = manifest_obj
                    .body
                    .collect()
                    .await
                    .map_err(|e| {
                        format!("failed to read manifest body from s3://{bucket}/{key}: {e}")
                    })?
                    .into_bytes()
                    .to_vec();
                let manifest: TierManifest = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("invalid manifest json: {e}"))?;
                (
                    bytes,
                    manifest,
                    BulkLoadManifestSource::S3 {
                        client,
                        bucket,
                        manifest_dir,
                    },
                )
            } else {
                let manifest_path = PathBuf::from(&manifest_source);
                let bytes = tokio::fs::read(&manifest_path)
                    .await
                    .map_err(|e| format!("failed to read manifest: {e}"))?;
                let manifest: TierManifest = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("invalid manifest json: {e}"))?;
                let manifest_parent = manifest_path
                    .parent()
                    .ok_or_else(|| "manifest has no parent directory".to_string())?
                    .to_path_buf();
                (
                    bytes,
                    manifest,
                    BulkLoadManifestSource::Local { manifest_parent },
                )
            };

        if !req.manifest_checksum.trim().is_empty() {
            let expected = parse_manifest_checksum(&req.manifest_checksum).ok_or_else(|| {
                format!(
                    "invalid manifest_checksum `{}` (expected hex like a1b2c3d4 or decimal)",
                    req.manifest_checksum
                )
            })?;
            let actual = crc32c::crc32c(&manifest_bytes);
            if expected != actual {
                return Err(format!(
                    "manifest checksum mismatch: expected {:08x}, got {:08x}",
                    expected, actual
                ));
            }
        }

        let total_records = manifest
            .chunks
            .iter()
            .map(|c| c.records as u64)
            .sum::<u64>();
        self.update_job(&job_id, |job| {
            job.records_total = total_records;
            job.message = "ingesting records".to_string();
        })
        .await;

        if req.dry_run {
            self.update_job(&job_id, |job| {
                job.status = "succeeded".to_string();
                job.message = "dry-run successful".to_string();
                job.records_applied = total_records;
                job.finished_at_unix_ms = now_millis();
            })
            .await;
            return Ok(());
        }

        let mut applied = 0_u64;
        let mut ops: Vec<astra_core::raft::AstraBatchPutOp> = Vec::new();
        let flush_ops = |ops: &mut Vec<astra_core::raft::AstraBatchPutOp>| {
            if ops.is_empty() {
                return None;
            }
            Some(std::mem::take(ops))
        };

        for chunk in &manifest.chunks {
            let bytes = match &source {
                BulkLoadManifestSource::Local { manifest_parent } => {
                    let chunk_path = manifest_parent.join(&chunk.key);
                    tokio::fs::read(&chunk_path).await.map_err(|e| {
                        format!("failed to read chunk {}: {e}", chunk_path.display())
                    })?
                }
                BulkLoadManifestSource::S3 {
                    client,
                    bucket,
                    manifest_dir,
                } => {
                    let key = join_s3_key(manifest_dir, &chunk.key);
                    let out = client
                        .get_object()
                        .bucket(bucket)
                        .key(&key)
                        .send()
                        .await
                        .map_err(|e| format!("failed to fetch chunk s3://{bucket}/{key}: {e}"))?;
                    out.body
                        .collect()
                        .await
                        .map_err(|e| format!("failed to read chunk body s3://{bucket}/{key}: {e}"))?
                        .into_bytes()
                        .to_vec()
                }
            };
            let checksum = crc32c::crc32c(&bytes);
            if checksum != chunk.crc32c {
                return Err(format!(
                    "checksum mismatch for chunk {}: expected {:08x}, got {:08x}",
                    chunk.key, chunk.crc32c, checksum
                ));
            }
            let rows = decode_chunk_to_rows(&bytes)
                .map_err(|e| format!("failed to decode chunk {}: {e}", chunk.key))?;

            for (key, value) in rows {
                let key = self.tenanting.encode_key(tenant_id.as_deref(), &key);
                ops.push(astra_core::raft::AstraBatchPutOp {
                    key,
                    value: value.value,
                    lease: value.lease,
                    ignore_value: false,
                    ignore_lease: false,
                    prev_kv: false,
                });
                applied += 1;

                if ops.len() >= 500 {
                    if let Some(batch) = flush_ops(&mut ops) {
                        let write_req = AstraWriteRequest::PutBatch { ops: batch };
                        self.raft.client_write(write_req).await.map_err(|e| {
                            format!("bulkload write failed: {}", map_raft_write_err(e))
                        })?;
                    }
                    self.update_job(&job_id, |job| {
                        job.records_applied = applied;
                    })
                    .await;
                }
            }
        }

        if let Some(batch) = flush_ops(&mut ops) {
            let write_req = AstraWriteRequest::PutBatch { ops: batch };
            self.raft
                .client_write(write_req)
                .await
                .map_err(|e| format!("bulkload write failed: {}", map_raft_write_err(e)))?;
        }

        self.update_job(&job_id, |job| {
            job.status = "succeeded".to_string();
            job.message = "bulkload completed".to_string();
            job.records_applied = applied;
            job.finished_at_unix_ms = now_millis();
        })
        .await;

        Ok(())
    }
}

#[tonic::async_trait]
impl AstraAdmin for AstraAdminService {
    async fn bulk_load(
        &self,
        request: Request<BulkLoadRequest>,
    ) -> Result<Response<BulkLoadResponse>, Status> {
        let tenant_from_token = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        if req.manifest_source.trim().is_empty() {
            return Err(Status::invalid_argument("manifest_source is required"));
        }
        let tenant_id = if req.tenant_id.trim().is_empty() {
            tenant_from_token
        } else {
            Some(req.tenant_id.clone())
        };
        if self.tenanting.enabled && tenant_id.is_none() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let leader = self.raft.current_leader().await;
        if leader.is_some() && leader != Some(self.member_id) {
            return Err(Status::failed_precondition(
                "bulkload must be submitted to current leader",
            ));
        }

        let seq = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let job_id = format!("bulkload-{seq:08}");
        {
            let mut guard = self.jobs.lock().await;
            guard.insert(job_id.clone(), BulkLoadJob::new(job_id.clone()));
        }

        let svc = self.clone();
        let job_id_for_task = job_id.clone();
        tokio::spawn(async move {
            if let Err(err) = svc
                .run_bulkload_job(job_id_for_task.clone(), tenant_id, req)
                .await
            {
                svc.update_job(&job_id_for_task, |job| {
                    job.status = "failed".to_string();
                    job.message = err;
                    job.finished_at_unix_ms = now_millis();
                })
                .await;
            }
        });

        Ok(Response::new(BulkLoadResponse {
            job_id,
            accepted: true,
            message: "bulkload accepted".to_string(),
        }))
    }

    async fn get_bulk_load_job(
        &self,
        request: Request<GetBulkLoadJobRequest>,
    ) -> Result<Response<GetBulkLoadJobResponse>, Status> {
        let req = request.into_inner();
        if req.job_id.trim().is_empty() {
            return Err(Status::invalid_argument("job_id is required"));
        }

        let guard = self.jobs.lock().await;
        let Some(job) = guard.get(&req.job_id) else {
            return Err(Status::not_found("bulkload job not found"));
        };

        Ok(Response::new(GetBulkLoadJobResponse {
            job_id: job.job_id.clone(),
            status: job.status.clone(),
            message: job.message.clone(),
            records_total: job.records_total,
            records_applied: job.records_applied,
            started_at_unix_ms: job.started_at_unix_ms,
            finished_at_unix_ms: job.finished_at_unix_ms,
        }))
    }
}

#[derive(Clone)]
struct EtcdKvService {
    store: Arc<KvStore>,
    raft: Raft<AstraTypeConfig>,
    cluster_id: u64,
    member_id: u64,
    tenanting: Tenanting,
    leader_client_by_id: Arc<HashMap<u64, String>>,
    put_batcher: Option<PutBatcher>,
    write_pressure: WritePressureGate,
    semantic_qos: SemanticQos,
    list_prefetch_cache: ListPrefetchCache,
    read_isolation_enabled: bool,
    gateway_read_ticket: GatewayReadTicket,
    gateway_singleflight: GatewayGetSingleflight,
    timeline_enabled: bool,
    timeline_sample_rate: u64,
    timeline_seq: Arc<AtomicU64>,
}

impl EtcdKvService {
    fn header(&self, revision: i64) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 1,
        }
    }

    fn forward_target(
        &self,
        leader_node: Option<&BasicNode>,
        leader_id: Option<u64>,
    ) -> Option<String> {
        if let Some(node) = leader_node {
            return Some(raft_addr_to_client_addr(&node.addr));
        }

        leader_id.and_then(|id| self.leader_client_by_id.get(&id).cloned())
    }

    fn forward_target_for_write(
        &self,
        err: &RaftError<u64, ClientWriteError<u64, BasicNode>>,
    ) -> Option<String> {
        let fwd = err.forward_to_leader::<BasicNode>()?;
        self.forward_target(fwd.leader_node.as_ref(), fwd.leader_id)
    }

    fn forward_target_for_read(
        &self,
        err: &RaftError<u64, CheckIsLeaderError<u64, BasicNode>>,
    ) -> Option<String> {
        let fwd = err.forward_to_leader::<BasicNode>()?;
        self.forward_target(fwd.leader_node.as_ref(), fwd.leader_id)
    }

    async fn kv_client(&self, target: &str) -> Result<KvClient<tonic::transport::Channel>, Status> {
        let endpoint = if target.starts_with("http://") || target.starts_with("https://") {
            target.to_string()
        } else {
            format!("http://{target}")
        };

        KvClient::connect(endpoint)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect forwarded leader: {e}")))
    }

    fn attach_auth_metadata<T>(
        mut req: Request<T>,
        auth_header: Option<&str>,
    ) -> Result<Request<T>, Status> {
        if let Some(value) = auth_header {
            req.metadata_mut().insert(
                "authorization",
                value
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid authorization metadata"))?,
            );
        }
        Ok(req)
    }

    async fn forward_range(
        &self,
        req: RangeRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<RangeResponse>, Status> {
        let mut client = self.kv_client(target).await?;
        client
            .range(Self::attach_auth_metadata(Request::new(req), auth_header)?)
            .await
    }

    async fn forward_range_any(
        &self,
        req: &RangeRequest,
        preferred: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<RangeResponse>, Status> {
        let mut last_err: Option<Status> = None;
        for target in self.forward_candidates(preferred) {
            match self.forward_range(req.clone(), &target, auth_header).await {
                Ok(resp) => return Ok(resp),
                Err(status) if Self::should_retry_forward(&status) => {
                    metrics::inc_forward_retry_attempts();
                    if status.code() == Code::Unavailable
                        && status
                            .message()
                            .to_ascii_lowercase()
                            .contains("closed network connection")
                    {
                        metrics::inc_transport_closed_conn_unavailable();
                    }
                    last_err = Some(status);
                }
                Err(status) => return Err(status),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no reachable leader for read")))
    }

    async fn forward_put(
        &self,
        req: PutRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<PutResponse>, Status> {
        let mut client = self.kv_client(target).await?;
        client
            .put(Self::attach_auth_metadata(Request::new(req), auth_header)?)
            .await
    }

    async fn forward_delete_range(
        &self,
        req: DeleteRangeRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let mut client = self.kv_client(target).await?;
        client
            .delete_range(Self::attach_auth_metadata(Request::new(req), auth_header)?)
            .await
    }

    async fn forward_txn(
        &self,
        req: TxnRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<TxnResponse>, Status> {
        let mut client = self.kv_client(target).await?;
        client
            .txn(Self::attach_auth_metadata(Request::new(req), auth_header)?)
            .await
    }

    async fn forward_compact(
        &self,
        req: CompactionRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let mut client = self.kv_client(target).await?;
        client
            .compact(Self::attach_auth_metadata(Request::new(req), auth_header)?)
            .await
    }

    async fn execute_range(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
        revision: i64,
        keys_only: bool,
        count_only: bool,
    ) -> Result<RangeOutput, Status> {
        run_isolated_range(
            self.store.clone(),
            self.read_isolation_enabled,
            key,
            range_end,
            limit,
            revision,
            keys_only,
            count_only,
        )
        .await
        .map_err(map_store_err)
    }

    async fn execute_range_with_prefetch(
        &self,
        req: &RangeRequest,
        key: Vec<u8>,
        range_end: Vec<u8>,
        cache_key: ListPrefetchCacheKey,
    ) -> Result<RangeOutput, Status> {
        if self.list_prefetch_cache.should_prefetch(req) {
            if let Some(cached) = self.list_prefetch_cache.get(&cache_key).await {
                metrics::inc_list_prefetch_hits();
                return Ok(cached);
            }

            metrics::inc_list_prefetch_misses();
            let fetched = self
                .execute_range(
                    key.clone(),
                    range_end.clone(),
                    req.limit,
                    req.revision,
                    req.keys_only,
                    req.count_only,
                )
                .await?;
            let resolved_key = ListPrefetchCacheKey {
                revision: fetched.revision,
                ..cache_key
            };
            self.list_prefetch_cache
                .put(resolved_key.clone(), fetched.clone())
                .await;

            if fetched.more && !fetched.kvs.is_empty() {
                let cache = self.list_prefetch_cache.clone();
                let store = self.store.clone();
                let read_isolation_enabled = self.read_isolation_enabled;
                let next_range_end = range_end;
                let limit = req.limit;
                let keys_only = req.keys_only;
                let count_only = req.count_only;
                let revision = fetched.revision;
                let pages = cache.prefetch_pages();
                let mut cursor = fetched.clone();
                tokio::spawn(async move {
                    for _ in 0..pages {
                        if !cursor.more {
                            break;
                        }
                        let Some(next_key) = next_page_key_from_result(&cursor) else {
                            break;
                        };
                        let next_cache_key = ListPrefetchCacheKey {
                            key: next_key.clone(),
                            range_end: next_range_end.clone(),
                            limit,
                            revision,
                            keys_only,
                            count_only,
                        };
                        if cache.get(&next_cache_key).await.is_some() {
                            break;
                        }
                        let Ok(next_result) = run_isolated_range(
                            store.clone(),
                            read_isolation_enabled,
                            next_key.clone(),
                            next_range_end.clone(),
                            limit,
                            revision,
                            keys_only,
                            count_only,
                        )
                        .await
                        else {
                            break;
                        };
                        cache.put(next_cache_key, next_result.clone()).await;
                        cursor = next_result;
                    }
                });
            }
            return Ok(fetched);
        }

        self.execute_range(
            key,
            range_end,
            req.limit,
            req.revision,
            req.keys_only,
            req.count_only,
        )
        .await
    }

    fn range_output_to_response(
        &self,
        tenant: Option<&str>,
        req: &RangeRequest,
        result: RangeOutput,
    ) -> Response<RangeResponse> {
        let kvs = if req.count_only {
            Vec::new()
        } else {
            result
                .kvs
                .into_iter()
                .map(|(k, v)| to_pb_kv(self.tenanting.decode_key(tenant, &k), v))
                .collect::<Vec<_>>()
        };
        Response::new(RangeResponse {
            header: Some(self.header(result.revision)),
            kvs,
            more: result.more,
            count: result.count,
        })
    }

    fn forward_candidates(&self, preferred: Option<&str>) -> Vec<String> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut out = Vec::new();

        if let Some(target) = preferred {
            let target = target.to_string();
            if seen.insert(target.clone()) {
                out.push(target);
            }
        }

        for (node_id, target) in self.leader_client_by_id.iter() {
            if *node_id == self.member_id {
                continue;
            }
            if seen.insert(target.clone()) {
                out.push(target.clone());
            }
        }
        out
    }

    fn should_retry_forward(status: &Status) -> bool {
        matches!(
            status.code(),
            Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
        )
    }

    async fn maybe_enforce_write_pressure(
        &self,
        put_queue_depth: usize,
        priority: WritePriority,
    ) -> Result<(), Status> {
        if self.raft.current_leader().await == Some(self.member_id) {
            self.write_pressure
                .enforce(put_queue_depth, priority)
                .await?;
        }
        Ok(())
    }

    fn txn_ops_include_tier0(&self, tenant: Option<&str>, ops: &[RequestOp]) -> bool {
        for op in ops {
            let Some(request) = op.request.as_ref() else {
                continue;
            };
            match request {
                request_op::Request::RequestPut(r) => {
                    let key = self.tenanting.encode_key(tenant, &r.key);
                    if self.semantic_qos.key_priority(&key) == WritePriority::Tier0 {
                        return true;
                    }
                }
                request_op::Request::RequestDeleteRange(r) => {
                    let (key, _) = self.tenanting.encode_range(tenant, &r.key, &r.range_end);
                    if self.semantic_qos.key_priority(&key) == WritePriority::Tier0 {
                        return true;
                    }
                }
                request_op::Request::RequestTxn(r) => {
                    if self.txn_ops_include_tier0(tenant, &r.success)
                        || self.txn_ops_include_tier0(tenant, &r.failure)
                    {
                        return true;
                    }
                }
                request_op::Request::RequestRange(_) => {}
            }
        }
        false
    }

    fn txn_priority(&self, tenant: Option<&str>, req: &TxnRequest) -> WritePriority {
        if self.txn_ops_include_tier0(tenant, &req.success)
            || self.txn_ops_include_tier0(tenant, &req.failure)
        {
            WritePriority::Tier0
        } else {
            WritePriority::Normal
        }
    }

    async fn forward_put_any(
        &self,
        req: &PutRequest,
        preferred: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<PutResponse>, Status> {
        let mut last_err: Option<Status> = None;
        for target in self.forward_candidates(preferred) {
            match self.forward_put(req.clone(), &target, auth_header).await {
                Ok(resp) => return Ok(resp),
                Err(status) if Self::should_retry_forward(&status) => {
                    metrics::inc_forward_retry_attempts();
                    if status.code() == Code::Unavailable
                        && status
                            .message()
                            .to_ascii_lowercase()
                            .contains("closed network connection")
                    {
                        metrics::inc_transport_closed_conn_unavailable();
                    }
                    last_err = Some(status);
                }
                Err(status) => return Err(status),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no reachable leader for write")))
    }

    async fn forward_delete_any(
        &self,
        req: &DeleteRangeRequest,
        preferred: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let mut last_err: Option<Status> = None;
        for target in self.forward_candidates(preferred) {
            match self
                .forward_delete_range(req.clone(), &target, auth_header)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(status) if Self::should_retry_forward(&status) => {
                    metrics::inc_forward_retry_attempts();
                    if status.code() == Code::Unavailable
                        && status
                            .message()
                            .to_ascii_lowercase()
                            .contains("closed network connection")
                    {
                        metrics::inc_transport_closed_conn_unavailable();
                    }
                    last_err = Some(status);
                }
                Err(status) => return Err(status),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no reachable leader for delete")))
    }

    async fn forward_txn_any(
        &self,
        req: &TxnRequest,
        preferred: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<TxnResponse>, Status> {
        let mut last_err: Option<Status> = None;
        for target in self.forward_candidates(preferred) {
            match self.forward_txn(req.clone(), &target, auth_header).await {
                Ok(resp) => return Ok(resp),
                Err(status) if Self::should_retry_forward(&status) => {
                    metrics::inc_forward_retry_attempts();
                    if status.code() == Code::Unavailable
                        && status
                            .message()
                            .to_ascii_lowercase()
                            .contains("closed network connection")
                    {
                        metrics::inc_transport_closed_conn_unavailable();
                    }
                    last_err = Some(status);
                }
                Err(status) => return Err(status),
            }
        }
        Err(last_err.unwrap_or_else(|| Status::unavailable("no reachable leader for txn")))
    }

    async fn forward_compact_any(
        &self,
        req: &CompactionRequest,
        preferred: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let mut last_err: Option<Status> = None;
        for target in self.forward_candidates(preferred) {
            match self
                .forward_compact(req.clone(), &target, auth_header)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(status) if Self::should_retry_forward(&status) => {
                    metrics::inc_forward_retry_attempts();
                    if status.code() == Code::Unavailable
                        && status
                            .message()
                            .to_ascii_lowercase()
                            .contains("closed network connection")
                    {
                        metrics::inc_transport_closed_conn_unavailable();
                    }
                    last_err = Some(status);
                }
                Err(status) => return Err(status),
            }
        }
        Err(last_err
            .unwrap_or_else(|| Status::unavailable("no reachable leader for compaction request")))
    }

    async fn put_direct(
        &self,
        req: PutRequest,
        tenant: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<PutResponse>, Status> {
        let physical_key = self.tenanting.encode_key(tenant, &req.key);
        let key = req.key.clone();
        let write_req = AstraWriteRequest::Put {
            key: physical_key,
            value: req.value.clone(),
            lease: req.lease,
            ignore_value: req.ignore_value,
            ignore_lease: req.ignore_lease,
            prev_kv: req.prev_kv,
        };

        let resp = match self.raft.client_write(write_req).await {
            Ok(resp) => resp,
            Err(err) => {
                let target = self.forward_target_for_write(&err);
                if target.is_some() || err.forward_to_leader::<BasicNode>().is_some() {
                    return self
                        .forward_put_any(&req, target.as_deref(), auth_header)
                        .await;
                }
                return Err(map_raft_write_err(err));
            }
        };

        let (revision, prev) = match resp.data {
            AstraWriteResponse::Put { revision, prev } => (revision, prev),
            _ => {
                return Err(Status::internal(
                    "unexpected state machine response for put request",
                ))
            }
        };
        self.list_prefetch_cache.invalidate_all().await;

        Ok(Response::new(PutResponse {
            header: Some(self.header(revision)),
            prev_kv: prev.map(|v| to_pb_kv(key, v)),
        }))
    }

    async fn compact_direct(
        &self,
        req: CompactionRequest,
        auth_header: Option<&str>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let write_req = AstraWriteRequest::Compact {
            revision: req.revision,
        };
        let resp = match self.raft.client_write(write_req).await {
            Ok(resp) => resp,
            Err(err) => {
                let target = self.forward_target_for_write(&err);
                if target.is_some() || err.forward_to_leader::<BasicNode>().is_some() {
                    return self
                        .forward_compact_any(&req, target.as_deref(), auth_header)
                        .await;
                }
                return Err(map_raft_write_err(err));
            }
        };

        let revision = match resp.data {
            AstraWriteResponse::Compact { revision, .. } => revision,
            _ => {
                return Err(Status::internal(
                    "unexpected state machine response for compact request",
                ))
            }
        };
        self.list_prefetch_cache.invalidate_all().await;

        Ok(Response::new(CompactionResponse {
            header: Some(self.header(revision)),
        }))
    }

    fn map_pb_compare(
        &self,
        tenant: Option<&str>,
        cmp: Compare,
    ) -> Result<AstraTxnCompare, Status> {
        let result = match CompareResult::try_from(cmp.result)
            .map_err(|_| Status::invalid_argument("invalid txn compare result"))?
        {
            CompareResult::Equal => AstraTxnCmpResult::Equal,
            CompareResult::Greater => AstraTxnCmpResult::Greater,
            CompareResult::Less => AstraTxnCmpResult::Less,
            CompareResult::NotEqual => AstraTxnCmpResult::NotEqual,
        };

        let target = match CompareTarget::try_from(cmp.target)
            .map_err(|_| Status::invalid_argument("invalid txn compare target"))?
        {
            CompareTarget::Version => AstraTxnCmpTarget::Version,
            CompareTarget::Create => AstraTxnCmpTarget::CreateRevision,
            CompareTarget::Mod => AstraTxnCmpTarget::ModRevision,
            CompareTarget::Value => AstraTxnCmpTarget::Value,
            CompareTarget::Lease => AstraTxnCmpTarget::Lease,
        };

        let target_value = match target {
            AstraTxnCmpTarget::Version => {
                let value = match cmp.target_union {
                    Some(compare::TargetUnion::Version(v)) => v,
                    None => 0,
                    _ => {
                        return Err(Status::invalid_argument(
                            "txn compare target/value mismatch (version)",
                        ))
                    }
                };
                AstraTxnCmpValue::I64(value)
            }
            AstraTxnCmpTarget::CreateRevision => {
                let value = match cmp.target_union {
                    Some(compare::TargetUnion::CreateRevision(v)) => v,
                    None => 0,
                    _ => {
                        return Err(Status::invalid_argument(
                            "txn compare target/value mismatch (create_revision)",
                        ))
                    }
                };
                AstraTxnCmpValue::I64(value)
            }
            AstraTxnCmpTarget::ModRevision => {
                let value = match cmp.target_union {
                    Some(compare::TargetUnion::ModRevision(v)) => v,
                    None => 0,
                    _ => {
                        return Err(Status::invalid_argument(
                            "txn compare target/value mismatch (mod_revision)",
                        ))
                    }
                };
                AstraTxnCmpValue::I64(value)
            }
            AstraTxnCmpTarget::Lease => {
                let value = match cmp.target_union {
                    Some(compare::TargetUnion::Lease(v)) => v,
                    None => 0,
                    _ => {
                        return Err(Status::invalid_argument(
                            "txn compare target/value mismatch (lease)",
                        ))
                    }
                };
                AstraTxnCmpValue::I64(value)
            }
            AstraTxnCmpTarget::Value => {
                let value = match cmp.target_union {
                    Some(compare::TargetUnion::Value(v)) => v,
                    None => Vec::new(),
                    _ => {
                        return Err(Status::invalid_argument(
                            "txn compare target/value mismatch (value)",
                        ))
                    }
                };
                AstraTxnCmpValue::Bytes(value)
            }
        };

        let (key, range_end) = self
            .tenanting
            .encode_range(tenant, &cmp.key, &cmp.range_end);
        Ok(AstraTxnCompare {
            result,
            target,
            key,
            range_end,
            target_value,
        })
    }

    fn map_pb_request_ops(
        &self,
        tenant: Option<&str>,
        ops: Vec<RequestOp>,
    ) -> Result<Vec<AstraTxnOp>, Status> {
        ops.into_iter()
            .map(|op| self.map_pb_request_op(tenant, op))
            .collect()
    }

    fn map_pb_request_op(&self, tenant: Option<&str>, op: RequestOp) -> Result<AstraTxnOp, Status> {
        let request = op
            .request
            .ok_or_else(|| Status::invalid_argument("txn request op missing request payload"))?;
        match request {
            request_op::Request::RequestRange(r) => {
                let (key, range_end) = self.tenanting.encode_range(tenant, &r.key, &r.range_end);
                Ok(AstraTxnOp::Range {
                    key,
                    range_end,
                    limit: r.limit,
                    revision: r.revision,
                    keys_only: r.keys_only,
                    count_only: r.count_only,
                })
            }
            request_op::Request::RequestPut(r) => Ok(AstraTxnOp::Put {
                key: self.tenanting.encode_key(tenant, &r.key),
                value: r.value,
                lease: r.lease,
                ignore_value: r.ignore_value,
                ignore_lease: r.ignore_lease,
                prev_kv: r.prev_kv,
            }),
            request_op::Request::RequestDeleteRange(r) => {
                let (key, range_end) = self.tenanting.encode_range(tenant, &r.key, &r.range_end);
                Ok(AstraTxnOp::DeleteRange {
                    key,
                    range_end,
                    prev_kv: r.prev_kv,
                })
            }
            request_op::Request::RequestTxn(r) => {
                let compare = r
                    .compare
                    .into_iter()
                    .map(|cmp| self.map_pb_compare(tenant, cmp))
                    .collect::<Result<Vec<_>, _>>()?;
                let success = self.map_pb_request_ops(tenant, r.success)?;
                let failure = self.map_pb_request_ops(tenant, r.failure)?;
                Ok(AstraTxnOp::Txn {
                    compare,
                    success,
                    failure,
                })
            }
        }
    }

    fn map_txn_op_responses(
        &self,
        tenant: Option<&str>,
        ops: &[AstraTxnOp],
        responses: &[AstraTxnOpResponse],
    ) -> Result<Vec<ResponseOp>, Status> {
        if ops.len() != responses.len() {
            return Err(Status::internal("txn op/response length mismatch"));
        }

        let mut out = Vec::with_capacity(responses.len());
        for (op, resp) in ops.iter().zip(responses.iter()) {
            match (op, resp) {
                (
                    AstraTxnOp::Range { .. },
                    AstraTxnOpResponse::Range {
                        revision,
                        count,
                        more,
                        kvs,
                    },
                ) => {
                    let kvs = kvs
                        .iter()
                        .map(|(k, v)| to_pb_kv(self.tenanting.decode_key(tenant, k), v.clone()))
                        .collect::<Vec<_>>();
                    out.push(ResponseOp {
                        response: Some(ResponseOpResponse::ResponseRange(RangeResponse {
                            header: Some(self.header(*revision)),
                            kvs,
                            more: *more,
                            count: *count,
                        })),
                    });
                }
                (AstraTxnOp::Put { key, .. }, AstraTxnOpResponse::Put { revision, prev }) => {
                    let logical_key = self.tenanting.decode_key(tenant, key);
                    out.push(ResponseOp {
                        response: Some(ResponseOpResponse::ResponsePut(PutResponse {
                            header: Some(self.header(*revision)),
                            prev_kv: prev.clone().map(|v| to_pb_kv(logical_key.clone(), v)),
                        })),
                    });
                }
                (
                    AstraTxnOp::DeleteRange { .. },
                    AstraTxnOpResponse::Delete {
                        revision,
                        deleted,
                        prev_kvs,
                    },
                ) => {
                    let prev_kvs = prev_kvs
                        .iter()
                        .map(|(k, v)| to_pb_kv(self.tenanting.decode_key(tenant, k), v.clone()))
                        .collect::<Vec<_>>();
                    out.push(ResponseOp {
                        response: Some(ResponseOpResponse::ResponseDeleteRange(
                            DeleteRangeResponse {
                                header: Some(self.header(*revision)),
                                deleted: *deleted,
                                prev_kvs,
                            },
                        )),
                    });
                }
                (
                    AstraTxnOp::Txn {
                        success, failure, ..
                    },
                    AstraTxnOpResponse::Txn {
                        revision,
                        succeeded,
                        responses,
                    },
                ) => {
                    let branch = if *succeeded { success } else { failure };
                    let responses = self.map_txn_op_responses(tenant, branch, responses)?;
                    out.push(ResponseOp {
                        response: Some(ResponseOpResponse::ResponseTxn(TxnResponse {
                            header: Some(self.header(*revision)),
                            succeeded: *succeeded,
                            responses,
                        })),
                    });
                }
                _ => {
                    return Err(Status::internal(
                        "txn response type did not match request op type",
                    ))
                }
            }
        }
        Ok(out)
    }

    async fn txn_direct(
        &self,
        req: TxnRequest,
        tenant: Option<&str>,
        auth_header: Option<&str>,
    ) -> Result<Response<TxnResponse>, Status> {
        let forward_req = req.clone();
        let compare = req
            .compare
            .into_iter()
            .map(|cmp| self.map_pb_compare(tenant, cmp))
            .collect::<Result<Vec<_>, _>>()?;
        let success = self.map_pb_request_ops(tenant, req.success)?;
        let failure = self.map_pb_request_ops(tenant, req.failure)?;
        let write_req = AstraWriteRequest::Txn {
            compare,
            success: success.clone(),
            failure: failure.clone(),
        };

        let resp = match self.raft.client_write(write_req).await {
            Ok(resp) => resp,
            Err(err) => {
                let target = self.forward_target_for_write(&err);
                if target.is_some() || err.forward_to_leader::<BasicNode>().is_some() {
                    return self
                        .forward_txn_any(&forward_req, target.as_deref(), auth_header)
                        .await;
                }
                return Err(map_raft_write_err(err));
            }
        };

        let (revision, succeeded, responses) = match resp.data {
            AstraWriteResponse::Txn {
                revision,
                succeeded,
                responses,
            } => (revision, succeeded, responses),
            _ => {
                return Err(Status::internal(
                    "unexpected state machine response for txn request",
                ))
            }
        };
        self.list_prefetch_cache.invalidate_all().await;

        let branch = if succeeded { &success } else { &failure };
        let responses = self.map_txn_op_responses(tenant, branch, &responses)?;

        Ok(Response::new(TxnResponse {
            header: Some(self.header(revision)),
            succeeded,
            responses,
        }))
    }
}

#[tonic::async_trait]
impl Kv for EtcdKvService {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        let is_get = req.range_end.is_empty() && req.limit <= 1 && !req.count_only;
        if is_get {
            metrics::inc_request_get_total();
        } else {
            metrics::inc_request_list_total();
        }

        let mut needs_quorum_check = !self.gateway_read_ticket.is_fresh();
        if !needs_quorum_check {
            needs_quorum_check = self.raft.current_leader().await != Some(self.member_id);
        }
        if needs_quorum_check {
            let _refresh_guard = self.gateway_read_ticket.refresh_lock.lock().await;
            let mut still_needs_check = !self.gateway_read_ticket.is_fresh();
            if !still_needs_check {
                still_needs_check = self.raft.current_leader().await != Some(self.member_id);
            }
            if still_needs_check {
                metrics::inc_read_quorum_checks();
                metrics::inc_gateway_read_ticket_misses();
                if let Err(err) = self.raft.ensure_linearizable().await {
                    metrics::inc_read_quorum_failures();
                    if let Some(target) = self.forward_target_for_read(&err) {
                        return self
                            .forward_range_any(&req, Some(&target), auth_header.as_deref())
                            .await;
                    }
                    let status = map_raft_read_err(err);
                    if status.code() == Code::Internal {
                        metrics::inc_read_quorum_internal();
                    }
                    return Err(status);
                }
                self.gateway_read_ticket.mark_fresh();
            } else {
                metrics::inc_gateway_read_ticket_hits();
            }
        } else {
            metrics::inc_gateway_read_ticket_hits();
        }

        let (key, range_end) =
            self.tenanting
                .encode_range(tenant.as_deref(), &req.key, &req.range_end);
        let revision_hint = if req.revision > 0 {
            req.revision
        } else {
            self.store.current_revision()
        };
        let cache_key = ListPrefetchCacheKey {
            key: key.clone(),
            range_end: range_end.clone(),
            limit: req.limit,
            revision: revision_hint,
            keys_only: req.keys_only,
            count_only: req.count_only,
        };

        let execute_local_read = || async {
            self.execute_range_with_prefetch(
                &req,
                key.clone(),
                range_end.clone(),
                cache_key.clone(),
            )
            .await
        };

        if is_get {
            let gate_key = GatewayGetSingleflightKey {
                key: key.clone(),
                revision: req.revision,
                keys_only: req.keys_only,
                count_only: req.count_only,
            };
            match self
                .gateway_singleflight
                .join_or_lead(gate_key.clone())
                .await
            {
                GatewayGetJoinRole::Waiter(waiter) => {
                    if let Some(waited) = self.gateway_singleflight.await_waiter(waiter).await {
                        let result = waited?;
                        return Ok(self.range_output_to_response(tenant.as_deref(), &req, result));
                    }
                }
                GatewayGetJoinRole::LeaderTracked => {
                    let result = execute_local_read().await;
                    self.gateway_singleflight.complete(&gate_key, &result).await;
                    let result = result?;
                    return Ok(self.range_output_to_response(tenant.as_deref(), &req, result));
                }
                GatewayGetJoinRole::Bypass => {}
            }
        }

        let result = execute_local_read().await?;
        Ok(self.range_output_to_response(tenant.as_deref(), &req, result))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        if self.store.memory_pressure() == MemoryPressure::Critical {
            return Err(Status::resource_exhausted(
                "memory pressure critical; write shedding enabled",
            ));
        }

        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        metrics::inc_request_put_total();
        let physical_key = self.tenanting.encode_key(tenant.as_deref(), &req.key);
        let write_priority = self.semantic_qos.key_priority(&physical_key);
        if write_priority == WritePriority::Tier0 {
            metrics::inc_request_tier0_total();
        }
        let put_queue_depth = self
            .put_batcher
            .as_ref()
            .map(|b| {
                if write_priority == WritePriority::Tier0 {
                    b.queue_depth_estimate(WritePriority::Tier0)
                } else {
                    b.queue_depth_estimate_total()
                }
            })
            .unwrap_or(0);
        self.maybe_enforce_write_pressure(put_queue_depth, write_priority)
            .await?;

        if let Some(put_batcher) = &self.put_batcher {
            if self.raft.current_leader().await == Some(self.member_id) {
                if self.timeline_enabled {
                    let seq = self.timeline_seq.fetch_add(1, Ordering::Relaxed);
                    if seq % self.timeline_sample_rate.max(1) == 0 {
                        info!(
                            stage = "put_enqueue",
                            key_len = req.key.len(),
                            value_len = req.value.len(),
                            priority = %write_priority.as_str(),
                            "raft timeline"
                        );
                    }
                }
                let mut local_req = req.clone();
                local_req.key = physical_key.clone();
                let key = req.key.clone();
                match put_batcher.submit(local_req, write_priority).await {
                    Ok((revision, prev)) => {
                        self.list_prefetch_cache.invalidate_all().await;
                        return Ok(Response::new(PutResponse {
                            header: Some(self.header(revision)),
                            prev_kv: prev.map(|v| to_pb_kv(key, v)),
                        }));
                    }
                    Err(status) if Self::should_retry_forward(&status) => {
                        return self
                            .forward_put_any(&req, None, auth_header.as_deref())
                            .await;
                    }
                    Err(status) => return Err(status),
                }
            }
        }

        self.put_direct(req, tenant.as_deref(), auth_header.as_deref())
            .await
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        let (key, range_end) =
            self.tenanting
                .encode_range(tenant.as_deref(), &req.key, &req.range_end);
        let priority = self.semantic_qos.key_priority(&key);
        self.maybe_enforce_write_pressure(0, priority).await?;
        metrics::inc_request_delete_total();
        if priority == WritePriority::Tier0 {
            metrics::inc_request_tier0_total();
        }

        let write_req = AstraWriteRequest::DeleteRange {
            key,
            range_end,
            prev_kv: req.prev_kv,
        };

        let resp = match self.raft.client_write(write_req).await {
            Ok(resp) => resp,
            Err(err) => {
                let target = self.forward_target_for_write(&err);
                if target.is_some() || err.forward_to_leader::<BasicNode>().is_some() {
                    return self
                        .forward_delete_any(&req, target.as_deref(), auth_header.as_deref())
                        .await;
                }
                return Err(map_raft_write_err(err));
            }
        };

        let (revision, deleted, prev_kvs) = match resp.data {
            AstraWriteResponse::Delete {
                revision,
                deleted,
                prev_kvs,
            } => (revision, deleted, prev_kvs),
            _ => {
                return Err(Status::internal(
                    "unexpected state machine response for delete_range",
                ))
            }
        };
        self.list_prefetch_cache.invalidate_all().await;

        let prev = prev_kvs
            .into_iter()
            .map(|(k, v)| to_pb_kv(self.tenanting.decode_key(tenant.as_deref(), &k), v))
            .collect::<Vec<_>>();

        Ok(Response::new(DeleteRangeResponse {
            header: Some(self.header(revision)),
            deleted,
            prev_kvs: prev,
        }))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        if self.store.memory_pressure() == MemoryPressure::Critical {
            return Err(Status::resource_exhausted(
                "memory pressure critical; write shedding enabled",
            ));
        }

        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        let priority = self.txn_priority(tenant.as_deref(), &req);
        self.maybe_enforce_write_pressure(0, priority).await?;
        metrics::inc_request_txn_total();
        if priority == WritePriority::Tier0 {
            metrics::inc_request_tier0_total();
        }

        self.txn_direct(req, tenant.as_deref(), auth_header.as_deref())
            .await
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        self.maybe_enforce_write_pressure(0, WritePriority::Normal)
            .await?;
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let req = request.into_inner();

        self.compact_direct(req, auth_header.as_deref()).await
    }
}

#[derive(Clone)]
struct EtcdLeaseService {
    store: Arc<KvStore>,
    raft: Raft<AstraTypeConfig>,
    cluster_id: u64,
    member_id: u64,
    tenanting: Tenanting,
    leader_client_by_id: Arc<HashMap<u64, String>>,
    write_pressure: WritePressureGate,
    list_prefetch_cache: ListPrefetchCache,
}

impl EtcdLeaseService {
    fn header(&self, revision: i64) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 1,
        }
    }

    fn forward_target(
        &self,
        leader_node: Option<&BasicNode>,
        leader_id: Option<u64>,
    ) -> Option<String> {
        if let Some(node) = leader_node {
            return Some(raft_addr_to_client_addr(&node.addr));
        }
        leader_id.and_then(|id| self.leader_client_by_id.get(&id).cloned())
    }

    fn forward_target_for_write(
        &self,
        err: &RaftError<u64, ClientWriteError<u64, BasicNode>>,
    ) -> Option<String> {
        let fwd = err.forward_to_leader::<BasicNode>()?;
        self.forward_target(fwd.leader_node.as_ref(), fwd.leader_id)
    }

    fn forward_target_for_read(
        &self,
        err: &RaftError<u64, CheckIsLeaderError<u64, BasicNode>>,
    ) -> Option<String> {
        let fwd = err.forward_to_leader::<BasicNode>()?;
        self.forward_target(fwd.leader_node.as_ref(), fwd.leader_id)
    }

    async fn lease_client(
        &self,
        target: &str,
    ) -> Result<LeaseClient<tonic::transport::Channel>, Status> {
        let endpoint = if target.starts_with("http://") || target.starts_with("https://") {
            target.to_string()
        } else {
            format!("http://{target}")
        };
        LeaseClient::connect(endpoint)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect forwarded leader: {e}")))
    }

    async fn forward_lease_grant(
        &self,
        req: LeaseGrantRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let mut client = self.lease_client(target).await?;
        client
            .lease_grant(EtcdKvService::attach_auth_metadata(
                Request::new(req),
                auth_header,
            )?)
            .await
    }

    async fn forward_lease_revoke(
        &self,
        req: LeaseRevokeRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let mut client = self.lease_client(target).await?;
        client
            .lease_revoke(EtcdKvService::attach_auth_metadata(
                Request::new(req),
                auth_header,
            )?)
            .await
    }

    async fn forward_lease_time_to_live(
        &self,
        req: LeaseTimeToLiveRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let mut client = self.lease_client(target).await?;
        client
            .lease_time_to_live(EtcdKvService::attach_auth_metadata(
                Request::new(req),
                auth_header,
            )?)
            .await
    }

    async fn forward_lease_leases(
        &self,
        req: LeaseLeasesRequest,
        target: &str,
        auth_header: Option<&str>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let mut client = self.lease_client(target).await?;
        client
            .lease_leases(EtcdKvService::attach_auth_metadata(
                Request::new(req),
                auth_header,
            )?)
            .await
    }

    async fn maybe_enforce_write_pressure(&self) -> Result<(), Status> {
        if self.raft.current_leader().await == Some(self.member_id) {
            self.write_pressure.enforce(0, WritePriority::Tier0).await?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Lease for EtcdLeaseService {
    type LeaseKeepAliveStream =
        Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, Status>> + Send + 'static>>;

    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        self.maybe_enforce_write_pressure().await?;
        metrics::inc_request_lease_total();
        metrics::inc_request_tier0_total();
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let _tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        let write_req = AstraWriteRequest::LeaseGrant {
            id: req.id,
            ttl: req.ttl,
        };
        let resp = match self.raft.client_write(write_req).await {
            Ok(resp) => resp,
            Err(err) => {
                if let Some(target) = self.forward_target_for_write(&err) {
                    return self
                        .forward_lease_grant(req, &target, auth_header.as_deref())
                        .await;
                }
                return Err(map_raft_write_err(err));
            }
        };

        let (revision, id, ttl) = match resp.data {
            AstraWriteResponse::LeaseGrant { revision, id, ttl } => (revision, id, ttl),
            _ => {
                return Err(Status::internal(
                    "unexpected state machine response for lease_grant",
                ))
            }
        };
        self.list_prefetch_cache.invalidate_all().await;

        Ok(Response::new(LeaseGrantResponse {
            header: Some(self.header(revision)),
            id,
            ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        self.maybe_enforce_write_pressure().await?;
        metrics::inc_request_lease_total();
        metrics::inc_request_tier0_total();
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let _tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();
        let write_req = AstraWriteRequest::LeaseRevoke { id: req.id };
        let resp = match self.raft.client_write(write_req).await {
            Ok(resp) => resp,
            Err(err) => {
                if let Some(target) = self.forward_target_for_write(&err) {
                    return self
                        .forward_lease_revoke(req, &target, auth_header.as_deref())
                        .await;
                }
                return Err(map_raft_write_err(err));
            }
        };

        let revision = match resp.data {
            AstraWriteResponse::LeaseRevoke { revision, .. } => revision,
            _ => {
                return Err(Status::internal(
                    "unexpected state machine response for lease_revoke",
                ))
            }
        };
        self.list_prefetch_cache.invalidate_all().await;

        Ok(Response::new(LeaseRevokeResponse {
            header: Some(self.header(revision)),
        }))
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let _tenant = self.tenanting.tenant_from_request(&request)?;
        let mut inbound = request.into_inner();
        let svc = self.clone();
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                let next = match inbound.message().await {
                    Ok(next) => next,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                };

                let Some(req) = next else {
                    break;
                };

                if let Err(status) = svc.maybe_enforce_write_pressure().await {
                    let _ = tx.send(Err(status)).await;
                    break;
                }
                metrics::inc_request_lease_total();
                metrics::inc_request_tier0_total();

                let resp = match svc
                    .raft
                    .client_write(AstraWriteRequest::LeaseKeepAlive { id: req.id })
                    .await
                {
                    Ok(resp) => resp,
                    Err(err) => {
                        let _ = tx.send(Err(map_raft_write_err(err))).await;
                        break;
                    }
                };

                let (revision, id, ttl) = match resp.data {
                    AstraWriteResponse::LeaseKeepAlive { revision, id, ttl } => (revision, id, ttl),
                    _ => {
                        let _ = tx
                            .send(Err(Status::internal(
                                "unexpected state machine response for lease_keep_alive",
                            )))
                            .await;
                        break;
                    }
                };
                svc.list_prefetch_cache.invalidate_all().await;

                if tx
                    .send(Ok(LeaseKeepAliveResponse {
                        header: Some(svc.header(revision)),
                        id,
                        ttl,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        metrics::inc_request_lease_total();
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();

        if let Err(err) = self.raft.ensure_linearizable().await {
            if let Some(target) = self.forward_target_for_read(&err) {
                return self
                    .forward_lease_time_to_live(req, &target, auth_header.as_deref())
                    .await;
            }
            return Err(map_raft_read_err(err));
        }

        let output = self
            .store
            .lease_time_to_live(req.id, req.keys)
            .map_err(map_store_err)?;
        let keys = output
            .keys
            .into_iter()
            .map(|k| self.tenanting.decode_key(tenant.as_deref(), &k))
            .collect::<Vec<_>>();

        Ok(Response::new(LeaseTimeToLiveResponse {
            header: Some(self.header(output.revision)),
            id: output.id,
            ttl: output.ttl,
            granted_ttl: output.granted_ttl,
            keys,
        }))
    }

    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        metrics::inc_request_lease_total();
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        let _tenant = self.tenanting.tenant_from_request(&request)?;
        let req = request.into_inner();

        if let Err(err) = self.raft.ensure_linearizable().await {
            if let Some(target) = self.forward_target_for_read(&err) {
                return self
                    .forward_lease_leases(req, &target, auth_header.as_deref())
                    .await;
            }
            return Err(map_raft_read_err(err));
        }

        let revision = self.store.current_revision();
        let leases = self
            .store
            .lease_list()
            .into_iter()
            .map(|id| LeaseStatus { id })
            .collect::<Vec<_>>();

        Ok(Response::new(LeaseLeasesResponse {
            header: Some(self.header(revision)),
            leases,
        }))
    }
}

#[derive(Clone)]
struct EtcdWatchService {
    store: Arc<KvStore>,
    cluster_id: u64,
    member_id: u64,
    tenanting: Tenanting,
    next_watch_id: Arc<AtomicI64>,
}

impl EtcdWatchService {
    fn header(&self, revision: i64) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 1,
        }
    }
}

#[tonic::async_trait]
impl Watch for EtcdWatchService {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + 'static>>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        metrics::inc_request_watch_total();
        let tenant = self.tenanting.tenant_from_request(&request)?;
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("expected create request"))?;

        let create = match first.request_union {
            Some(RequestUnion::CreateRequest(c)) => c,
            _ => {
                return Err(Status::invalid_argument(
                    "first watch message must be create_request",
                ))
            }
        };

        let watch_id = self.next_watch_id.fetch_add(1, Ordering::SeqCst);

        let compact_revision = self.store.compact_revision();
        if create.start_revision > 0 && create.start_revision <= compact_revision {
            let (tx, rx) = mpsc::channel(4);
            let _ = tx
                .send(Ok(WatchResponse {
                    header: Some(self.header(self.store.current_revision())),
                    watch_id,
                    created: false,
                    canceled: true,
                    compact_revision,
                    cancel_reason: "required revision has been compacted".to_string(),
                    events: Vec::new(),
                }))
                .await;
            return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
        }

        let start_revision = if create.start_revision <= 0 {
            self.store.current_revision() + 1
        } else {
            create.start_revision
        };

        let (physical_key, physical_range_end) =
            self.tenanting
                .encode_range(tenant.as_deref(), &create.key, &create.range_end);
        let filter = WatchFilter {
            key: physical_key,
            range_end: physical_range_end,
            start_revision,
        };

        let mut sub = self.store.subscribe_watch(filter.clone());
        let (tx, rx) = mpsc::channel(1024);

        tx.send(Ok(WatchResponse {
            header: Some(self.header(self.store.current_revision())),
            watch_id,
            created: true,
            canceled: false,
            compact_revision,
            cancel_reason: String::new(),
            events: Vec::new(),
        }))
        .await
        .map_err(|_| Status::internal("watch channel closed"))?;

        for ev in sub.backlog.drain(..) {
            if !filter.matches(ev.key.as_slice()) {
                continue;
            }
            let logical_key = self.tenanting.decode_key(tenant.as_deref(), &ev.key);
            tx.send(Ok(WatchResponse {
                header: Some(self.header(ev.mod_revision)),
                watch_id,
                created: false,
                canceled: false,
                compact_revision: 0,
                cancel_reason: String::new(),
                events: vec![to_pb_event(&ev, logical_key)],
            }))
            .await
            .map_err(|_| Status::internal("watch channel closed"))?;
        }

        let cluster_id = self.cluster_id;
        let member_id = self.member_id;
        let tenanting = self.tenanting;
        let tenant = tenant.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    inbound_msg = inbound.message() => {
                        match inbound_msg {
                            Ok(Some(msg)) => {
                                match msg.request_union {
                                    Some(RequestUnion::CancelRequest(c)) if c.watch_id == watch_id => {
                                        let _ = tx.send(Ok(WatchResponse {
                                            header: None,
                                            watch_id,
                                            created: false,
                                            canceled: true,
                                            compact_revision: 0,
                                            cancel_reason: "watch canceled".to_string(),
                                            events: Vec::new(),
                                        })).await;
                                        break;
                                    }
                                    Some(RequestUnion::ProgressRequest(_)) => {
                                        let _ = tx.send(Ok(WatchResponse {
                                            header: None,
                                            watch_id,
                                            created: false,
                                            canceled: false,
                                            compact_revision: 0,
                                            cancel_reason: String::new(),
                                            events: Vec::new(),
                                        })).await;
                                    }
                                    _ => {}
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                let _ = tx.send(Err(Status::internal(format!("watch stream error: {err}")))).await;
                                break;
                            }
                        }
                    }
                    recv = sub.receiver.recv() => {
                        match recv {
                            Ok(ev) => {
                                if ev.mod_revision < start_revision {
                                    continue;
                                }
                                if !key_in_range(ev.key.as_slice(), &filter.key, &filter.range_end) {
                                    continue;
                                }
                                let logical_key = tenanting.decode_key(tenant.as_deref(), &ev.key);

                                if tx.send(Ok(WatchResponse {
                                    header: Some(ResponseHeader {
                                        cluster_id,
                                        member_id,
                                        revision: ev.mod_revision,
                                        raft_term: 1,
                                    }),
                                    watch_id,
                                    created: false,
                                    canceled: false,
                                    compact_revision: 0,
                                    cancel_reason: String::new(),
                                    events: vec![to_pb_event(&ev, logical_key)],
                                })).await.is_err() {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped, watch_id, "watch receiver lagged behind ring events");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn to_pb_event(ev: &astra_core::watch::WatchEvent, key: Vec<u8>) -> Event {
    let (event_type, value) = match ev.kind {
        WatchEventKind::Put => (EventType::Put as i32, ev.value.as_ref().to_vec()),
        WatchEventKind::Delete => (EventType::Delete as i32, Vec::new()),
    };

    Event {
        r#type: event_type,
        kv: Some(KeyValue {
            key,
            create_revision: ev.create_revision,
            mod_revision: ev.mod_revision,
            version: ev.version,
            value,
            lease: ev.lease,
        }),
        prev_kv: Some(KeyValue {
            key: Vec::new(),
            create_revision: ev.create_revision,
            mod_revision: ev.mod_revision,
            version: ev.version,
            value: ev.prev_value.as_ref().to_vec(),
            lease: ev.lease,
        }),
    }
}

fn to_pb_kv(key: Vec<u8>, v: ValueEntry) -> KeyValue {
    KeyValue {
        key,
        create_revision: v.create_revision,
        mod_revision: v.mod_revision,
        version: v.version,
        value: v.value,
        lease: v.lease,
    }
}

fn map_store_err(err: StoreError) -> Status {
    match err {
        StoreError::Compacted(_) => {
            Status::out_of_range("etcdserver: mvcc: required revision has been compacted")
        }
        StoreError::ResourceExhausted(msg) => Status::resource_exhausted(msg),
        StoreError::InvalidArgument(msg) => Status::invalid_argument(msg),
        StoreError::KeyNotFound => Status::not_found("key not found"),
        other => Status::internal(other.to_string()),
    }
}

fn map_raft_write_err(err: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> Status {
    let raw = err.to_string();
    if raw.contains("disk queue saturated") {
        return Status::resource_exhausted("Disk queue saturated");
    }
    if let Some(fwd) = err.forward_to_leader::<BasicNode>() {
        let leader = fwd
            .leader_node
            .as_ref()
            .map(|n| n.addr.clone())
            .or_else(|| fwd.leader_id.map(|id| format!("node-{id}")))
            .unwrap_or_else(|| "unknown".to_string());
        return Status::unavailable(format!("not leader; forward to {leader}"));
    }
    Status::internal(err.to_string())
}

fn map_raft_read_err(err: RaftError<u64, CheckIsLeaderError<u64, BasicNode>>) -> Status {
    if let Some(fwd) = err.forward_to_leader::<BasicNode>() {
        let leader = fwd
            .leader_node
            .as_ref()
            .map(|n| n.addr.clone())
            .or_else(|| fwd.leader_id.map(|id| format!("node-{id}")))
            .unwrap_or_else(|| "unknown".to_string());
        return Status::unavailable(format!("not leader; forward to {leader}"));
    }
    Status::internal(err.to_string())
}

fn normalize_addr(addr: &str) -> String {
    addr.strip_prefix("http://")
        .or_else(|| addr.strip_prefix("https://"))
        .unwrap_or(addr)
        .trim()
        .to_string()
}

fn raft_addr_to_client_addr(addr: &str) -> String {
    let normalized = normalize_addr(addr);
    if let Some((host, _port)) = normalized.rsplit_once(':') {
        format!("{host}:2379")
    } else {
        format!("{normalized}:2379")
    }
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    vec![0]
}

fn build_leader_client_map(cfg: &AstraConfig) -> HashMap<u64, String> {
    let mut map = cfg
        .peers
        .iter()
        .enumerate()
        .map(|(idx, addr)| ((idx as u64) + 1, raft_addr_to_client_addr(addr)))
        .collect::<HashMap<_, _>>();

    map.insert(
        cfg.node_id,
        raft_addr_to_client_addr(&cfg.raft_advertise_addr),
    );
    map
}

async fn serve_metrics_endpoint(addr: std::net::SocketAddr) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind metrics listener on {addr}"))?;

    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .context("metrics listener accept failed")?;
        tokio::spawn(async move {
            let mut req_buf = [0_u8; 1024];
            let _ =
                tokio::time::timeout(Duration::from_millis(250), stream.read(&mut req_buf)).await;
            let body = metrics::render_prometheus();
            let header = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(body.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,astrad=debug,astra_core=debug".into()),
        )
        .init();

    let args = Args::parse();
    let mut cfg = AstraConfig::from_env();
    if let Some(node_id) = args.node_id {
        cfg.node_id = node_id;
    }
    if let Some(peers) = args.peers {
        cfg.peers = peers
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect();
    }
    if let Some(addr) = args.client_addr {
        cfg.client_addr = addr;
    }
    if let Some(addr) = args.raft_addr {
        cfg.raft_addr = addr;
    }
    if let Some(profile) = args.profile {
        cfg.profile = profile.parse::<AstraProfile>().map_err(|_| {
            anyhow!("invalid --profile value `{profile}` (expected kubernetes|omni|gateway|auto)")
        })?;
    }
    apply_startup_profile_defaults(&mut cfg);

    metrics::set_enabled(cfg.metrics_enabled);
    if cfg.metrics_enabled {
        let metrics_addr: std::net::SocketAddr = cfg
            .metrics_addr
            .parse()
            .with_context(|| format!("invalid metrics_addr socket format: {}", cfg.metrics_addr))?;
        info!(%metrics_addr, "astrad metrics endpoint enabled");
        tokio::spawn(async move {
            if let Err(err) = serve_metrics_endpoint(metrics_addr).await {
                error!(error = %err, %metrics_addr, "metrics endpoint failed");
            }
        });
    } else {
        info!("astrad metrics endpoint disabled");
    }

    let hal = HalProfile::detect();
    info!("{}", hal.startup_line());

    let raft_boot = RaftBootstrap::new(&cfg).context("openraft bootstrap failed")?;
    info!(
        cluster = %raft_boot.config.cluster_name,
        min_election = raft_boot.config.election_timeout_min,
        max_election = raft_boot.config.election_timeout_max,
        heartbeat_interval = raft_boot.config.heartbeat_interval,
        max_payload_entries = raft_boot.config.max_payload_entries,
        replication_lag_threshold = raft_boot.config.replication_lag_threshold,
        "openraft config initialized"
    );

    let store = Arc::new(KvStore::open(
        &cfg.data_dir,
        cfg.max_memory_bytes(),
        cfg.hot_revision_window,
        cfg.list_prefix_filter_enabled,
        cfg.list_revision_filter_enabled,
        cfg.watch_ring_capacity,
        cfg.watch_broadcast_capacity,
        cfg.watch_backlog_mode,
    )?);

    match TieringManager::restore_if_empty(cfg.node_id, cfg.s3.as_ref(), store.clone()).await {
        Ok(true) => info!("restored state from object tier"),
        Ok(false) => info!("object-tier restore skipped"),
        Err(err) => warn!(error = %err, "object-tier restore failed; continuing with local state"),
    }

    let wal_cfg = WalBatchConfig {
        max_batch_requests: cfg.wal_max_batch_requests,
        max_batch_bytes: cfg.wal_max_batch_bytes,
        max_linger: Duration::from_micros(cfg.wal_max_linger_us),
        low_concurrency_threshold: cfg.wal_low_concurrency_threshold,
        low_linger: Duration::from_micros(cfg.wal_low_linger_us),
        pending_limit: cfg.wal_pending_limit,
        segment_bytes: cfg.wal_segment_bytes,
        io_engine: cfg.wal_io_engine,
        ..Default::default()
    };

    let log_store = AstraLogStore::open(&cfg.data_dir, wal_cfg)
        .await
        .map_err(|e| anyhow::anyhow!("failed to open raft log store: {e}"))?;
    let state_machine = AstraStateMachine::new(store.clone());

    let raft = Raft::new(
        cfg.node_id,
        raft_boot.config.clone(),
        AstraNetworkFactory,
        log_store,
        state_machine,
    )
    .await
    .context("failed to start raft")?;

    let mut nodes = parse_raft_nodes(&cfg.peers);
    if nodes.is_empty() {
        nodes.insert(
            cfg.node_id,
            BasicNode {
                addr: cfg.raft_advertise_addr.clone(),
            },
        );
    } else {
        nodes.insert(
            cfg.node_id,
            BasicNode {
                addr: cfg.raft_advertise_addr.clone(),
            },
        );
    }

    let raft_addr = cfg
        .raft_addr
        .parse()
        .context("invalid raft_addr socket format")?;

    let grpc_max_concurrent_streams = cfg.grpc_max_concurrent_streams.max(1);
    let grpc_http2_keepalive_interval =
        Duration::from_millis(cfg.grpc_http2_keepalive_interval_ms.max(1));
    let grpc_http2_keepalive_timeout =
        Duration::from_millis(cfg.grpc_http2_keepalive_timeout_ms.max(1));
    let grpc_tcp_keepalive = Duration::from_millis(cfg.grpc_tcp_keepalive_ms.max(1));
    info!(
        grpc_max_concurrent_streams,
        grpc_http2_keepalive_interval_ms = cfg.grpc_http2_keepalive_interval_ms,
        grpc_http2_keepalive_timeout_ms = cfg.grpc_http2_keepalive_timeout_ms,
        grpc_tcp_keepalive_ms = cfg.grpc_tcp_keepalive_ms,
        "gRPC transport settings"
    );

    let raft_service = AstraRaftService {
        raft: raft.clone(),
        node_id: cfg.node_id,
        chaos_append_ack_delay_enabled: cfg.chaos_append_ack_delay_enabled,
        chaos_append_ack_delay_min: Duration::from_millis(cfg.chaos_append_ack_delay_min_ms.max(1)),
        chaos_append_ack_delay_max: Duration::from_millis(cfg.chaos_append_ack_delay_max_ms.max(1)),
        chaos_append_ack_delay_node_id: cfg.chaos_append_ack_delay_node_id,
    };
    tokio::spawn(async move {
        if let Err(err) = tonic::transport::Server::builder()
            .max_concurrent_streams(grpc_max_concurrent_streams)
            .http2_keepalive_interval(Some(grpc_http2_keepalive_interval))
            .http2_keepalive_timeout(Some(grpc_http2_keepalive_timeout))
            .tcp_keepalive(Some(grpc_tcp_keepalive))
            .add_service(InternalRaftServer::new(raft_service))
            .serve(raft_addr)
            .await
        {
            error!(error = %err, "internal raft server failed");
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    if let Err(err) = maybe_initialize(&raft, nodes).await {
        warn!(error = %err, "raft initialize returned error");
    }

    let bg_io_limiter = IoTokenBucket::new(
        cfg.bg_io_throttle_enabled,
        cfg.bg_io_tokens_per_sec,
        cfg.bg_io_burst_tokens,
    );
    let bg_sqe_limiter = IoTokenBucket::new_for_lane(
        astra_core::io_budget::IoBudgetLane::Sqe,
        cfg.bg_io_sqe_throttle_enabled,
        cfg.bg_io_sqe_tokens_per_sec,
        cfg.bg_io_sqe_burst_tokens,
    );
    if cfg.bg_io_throttle_enabled {
        info!(
            tokens_per_sec = cfg.bg_io_tokens_per_sec,
            burst_tokens = cfg.bg_io_burst_tokens,
            "background IO token bucket enabled"
        );
    } else {
        info!("background IO token bucket disabled");
    }
    if cfg.bg_io_sqe_throttle_enabled {
        info!(
            tokens_per_sec = cfg.bg_io_sqe_tokens_per_sec,
            burst_tokens = cfg.bg_io_sqe_burst_tokens,
            "background SQE token bucket enabled"
        );
    } else {
        info!("background SQE token bucket disabled");
    }
    info!(
        lsm_max_l0_files = cfg.lsm_max_l0_files,
        lsm_stall_at_files = cfg.lsm_stall_at_files,
        lsm_stall_max_delay_ms = cfg.lsm_stall_max_delay_ms,
        lsm_reject_after_ms = cfg.lsm_reject_after_ms,
        lsm_reject_extra_files = cfg.lsm_reject_extra_files,
        lsm_delay_band_l0_5_ms = cfg.lsm_delay_band_l0_5_ms,
        lsm_delay_band_l0_6_ms = cfg.lsm_delay_band_l0_6_ms,
        lsm_delay_band_l0_7_ms = cfg.lsm_delay_band_l0_7_ms,
        lsm_synth_file_bytes = cfg.lsm_synth_file_bytes,
        "l0 write pressure control enabled"
    );
    info!(
        list_prefix_filter_enabled = cfg.list_prefix_filter_enabled,
        list_revision_filter_enabled = cfg.list_revision_filter_enabled,
        list_prefetch_enabled = cfg.list_prefetch_enabled,
        list_prefetch_pages = cfg.list_prefetch_pages,
        list_prefetch_cache_entries = cfg.list_prefetch_cache_entries,
        read_isolation_enabled = cfg.read_isolation_enabled,
        gateway_read_ticket_enabled = cfg.gateway_read_ticket_enabled,
        gateway_read_ticket_ttl_ms = cfg.gateway_read_ticket_ttl_ms,
        gateway_singleflight_enabled = cfg.gateway_singleflight_enabled,
        gateway_singleflight_max_waiters = cfg.gateway_singleflight_max_waiters,
        "list acceleration settings"
    );
    info!(
        profile = %cfg.profile.as_str(),
        profile_sample_secs = cfg.profile_sample_secs,
        profile_min_dwell_secs = cfg.profile_min_dwell_secs,
        qos_tier0_prefixes = cfg.qos_tier0_prefixes.len(),
        qos_tier0_suffixes = cfg.qos_tier0_suffixes.len(),
        qos_tier0_max_batch_requests = cfg.qos_tier0_max_batch_requests,
        qos_tier0_max_linger_us = cfg.qos_tier0_max_linger_us,
        "profile + semantic qos settings"
    );

    let tier_chunk_target = cfg
        .sst_target_bytes
        .max(cfg.bg_io_min_chunk_bytes)
        .min(cfg.bg_io_max_chunk_bytes);
    info!(
        bg_io_min_chunk_bytes = cfg.bg_io_min_chunk_bytes,
        bg_io_max_chunk_bytes = cfg.bg_io_max_chunk_bytes,
        tier_chunk_target_bytes = tier_chunk_target,
        "tiering chunk target configuration"
    );
    let _tiering = TieringManager::start(
        cfg.node_id,
        cfg.s3.clone(),
        Duration::from_secs(cfg.tiering_interval_secs),
        tier_chunk_target,
        store.clone(),
        bg_io_limiter.clone(),
        bg_sqe_limiter.clone(),
    );
    let auth_runtime = Arc::new(
        AuthRuntime::from_config(&cfg)
            .await
            .context("failed to initialize auth runtime")?,
    );
    if auth_runtime.enabled {
        info!(
            tenant_claim = %cfg.auth_tenant_claim,
            tenant_virtualization = cfg.tenant_virtualization_enabled,
            "jwt auth enabled"
        );
    } else {
        info!(
            tenant_virtualization = cfg.tenant_virtualization_enabled,
            "jwt auth disabled"
        );
    }

    let put_batcher = if cfg.put_batch_max_requests > 1 {
        info!(
            max_requests = cfg.put_batch_max_requests,
            min_requests = cfg.put_batch_min_requests,
            max_linger_us = cfg.put_batch_max_linger_us,
            min_linger_us = cfg.put_batch_min_linger_us,
            max_batch_bytes = cfg.put_batch_max_bytes,
            pending_limit = cfg.put_batch_pending_limit,
            adaptive_enabled = cfg.put_adaptive_enabled,
            adaptive_mode = %cfg.put_adaptive_mode.as_str(),
            adaptive_min_request_floor = cfg.put_adaptive_min_request_floor,
            dispatch_concurrency = cfg.put_dispatch_concurrency,
            target_queue_depth = cfg.put_target_queue_depth,
            p99_budget_ms = cfg.put_p99_budget_ms,
            target_queue_wait_p99_ms = cfg.put_target_queue_wait_p99_ms,
            target_quorum_ack_p99_ms = cfg.put_target_quorum_ack_p99_ms,
            token_lane_enabled = cfg.put_token_lane_enabled,
            token_dict_max_entries = cfg.put_token_dict_max_entries,
            token_min_reuse = cfg.put_token_min_reuse,
            tier0_max_requests = cfg.qos_tier0_max_batch_requests,
            tier0_linger_us = cfg.qos_tier0_max_linger_us,
            timeline_enabled = cfg.raft_timeline_enabled,
            timeline_sample_rate = cfg.raft_timeline_sample_rate,
            "leader put micro-batcher enabled"
        );
        Some(PutBatcher::spawn(
            raft.clone(),
            PutBatcherConfig {
                initial_max_requests: cfg.put_batch_max_requests,
                min_max_requests: cfg.put_batch_min_requests,
                max_max_requests: cfg.put_batch_max_requests,
                initial_linger_us: cfg.put_batch_max_linger_us,
                min_linger_us: cfg.put_batch_min_linger_us,
                max_linger_us: cfg.put_batch_max_linger_us,
                max_batch_bytes: cfg.put_batch_max_bytes,
                pending_limit: cfg.put_batch_pending_limit,
                adaptive_enabled: cfg.put_adaptive_enabled,
                adaptive_mode: cfg.put_adaptive_mode,
                adaptive_min_request_floor: cfg.put_adaptive_min_request_floor,
                dispatch_concurrency: cfg.put_dispatch_concurrency,
                target_queue_depth: cfg.put_target_queue_depth,
                p99_budget_ms: cfg.put_p99_budget_ms,
                target_queue_wait_p99_ms: cfg.put_target_queue_wait_p99_ms,
                target_quorum_ack_p99_ms: cfg.put_target_quorum_ack_p99_ms,
                token_lane_enabled: cfg.put_token_lane_enabled,
                token_dict_max_entries: cfg.put_token_dict_max_entries,
                token_min_reuse: cfg.put_token_min_reuse,
                tier0_max_requests: cfg.qos_tier0_max_batch_requests,
                tier0_linger_us: cfg.qos_tier0_max_linger_us,
                timeline_enabled: cfg.raft_timeline_enabled,
                timeline_sample_rate: cfg.raft_timeline_sample_rate,
            },
        ))
    } else {
        info!("leader put micro-batcher disabled (max_requests <= 1)");
        None
    };
    let tenanting = Tenanting::from_config(&cfg);
    let write_pressure = WritePressureGate::new(&cfg);
    let semantic_qos = SemanticQos::from_config(&cfg);
    let list_prefetch_cache = ListPrefetchCache::new(
        cfg.list_prefetch_enabled,
        cfg.list_prefetch_pages,
        cfg.list_prefetch_cache_entries,
    );
    let gateway_read_ticket = GatewayReadTicket::from_config(&cfg);
    let gateway_singleflight = GatewayGetSingleflight::from_config(&cfg);
    start_profile_governor(
        cfg.clone(),
        put_batcher.clone(),
        list_prefetch_cache.clone(),
        bg_io_limiter,
        bg_sqe_limiter,
    );

    let kv = EtcdKvService {
        store: store.clone(),
        raft: raft.clone(),
        cluster_id: 1,
        member_id: cfg.node_id,
        tenanting,
        leader_client_by_id: Arc::new(build_leader_client_map(&cfg)),
        put_batcher,
        write_pressure: write_pressure.clone(),
        semantic_qos: semantic_qos.clone(),
        list_prefetch_cache: list_prefetch_cache.clone(),
        read_isolation_enabled: cfg.read_isolation_enabled,
        gateway_read_ticket,
        gateway_singleflight,
        timeline_enabled: cfg.raft_timeline_enabled,
        timeline_sample_rate: cfg.raft_timeline_sample_rate.max(1),
        timeline_seq: Arc::new(AtomicU64::new(1)),
    };
    let lease = EtcdLeaseService {
        store: store.clone(),
        raft: raft.clone(),
        cluster_id: 1,
        member_id: cfg.node_id,
        tenanting,
        leader_client_by_id: Arc::new(build_leader_client_map(&cfg)),
        write_pressure,
        list_prefetch_cache,
    };

    let watch = EtcdWatchService {
        store,
        cluster_id: 1,
        member_id: cfg.node_id,
        tenanting,
        next_watch_id: Arc::new(AtomicI64::new(1)),
    };
    let admin = AstraAdminService::new(raft.clone(), cfg.node_id, tenanting, cfg.s3.clone());

    let addr = cfg
        .client_addr
        .parse()
        .context("invalid client_addr socket format")?;

    info!(%addr, raft_addr = %cfg.raft_addr, "astrad listening");
    let authz = AuthzInterceptor {
        auth: auth_runtime.clone(),
    };

    tonic::transport::Server::builder()
        .max_concurrent_streams(grpc_max_concurrent_streams)
        .http2_keepalive_interval(Some(grpc_http2_keepalive_interval))
        .http2_keepalive_timeout(Some(grpc_http2_keepalive_timeout))
        .tcp_keepalive(Some(grpc_tcp_keepalive))
        .add_service(KvServer::with_interceptor(kv, authz.clone()))
        .add_service(LeaseServer::with_interceptor(lease, authz.clone()))
        .add_service(WatchServer::with_interceptor(watch, authz.clone()))
        .add_service(AstraAdminServer::with_interceptor(admin, authz))
        .serve(addr)
        .await
        .map_err(|e| {
            error!(error = %e, "server failed");
            anyhow::anyhow!(e)
        })?;

    Ok(())
}
