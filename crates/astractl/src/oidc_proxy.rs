use anyhow::{anyhow, Context, Result};
use astra_proto::etcdserverpb::kv_client::KvClient;
use astra_proto::etcdserverpb::kv_server::{Kv, KvServer};
use astra_proto::etcdserverpb::lease_client::LeaseClient;
use astra_proto::etcdserverpb::lease_server::{Lease, LeaseServer};
use astra_proto::etcdserverpb::watch_client::WatchClient;
use astra_proto::etcdserverpb::watch_server::{Watch, WatchServer};
use astra_proto::etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse, TxnRequest, TxnResponse, WatchRequest, WatchResponse,
};
use clap::Args;
use futures::Stream;
use reqwest::Client;
use serde::Deserialize;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Args)]
#[command(name = "oidc-proxy")]
pub struct OidcProxyArgs {
    #[arg(long, env = "ASTRA_PROXY_LISTEN_ADDR", default_value = "0.0.0.0:23790")]
    pub listen_addr: String,

    #[arg(long = "upstream", env = "ASTRA_PROXY_UPSTREAMS", value_delimiter = ',', required = true)]
    pub upstreams: Vec<String>,

    #[arg(long, env = "ASTRA_PROXY_ISSUER")]
    pub issuer: Option<String>,

    #[arg(long, env = "ASTRA_PROXY_TOKEN_URL")]
    pub token_url: Option<String>,

    #[arg(long, env = "ASTRA_PROXY_CLIENT_ID")]
    pub client_id: String,

    #[arg(long, env = "ASTRA_PROXY_CLIENT_SECRET")]
    pub client_secret: String,

    #[arg(long, env = "ASTRA_PROXY_SCOPE")]
    pub scope: Option<String>,

    #[arg(long, env = "ASTRA_PROXY_REFRESH_SKEW_SECS", default_value_t = 60)]
    pub refresh_skew_secs: u64,

    #[arg(long, env = "ASTRA_PROXY_REFRESH_BACKOFF_SECS", default_value_t = 5)]
    pub refresh_backoff_secs: u64,

    #[arg(long, env = "ASTRA_PROXY_MAX_MESSAGE_BYTES", default_value_t = 64 * 1024 * 1024)]
    pub max_message_bytes: usize,
}

#[derive(Debug, Clone)]
struct ProxyConfig {
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    refresh_skew: Duration,
    refresh_backoff: Duration,
}

#[derive(Debug, Clone)]
struct Upstream {
    endpoint: String,
    channel: Channel,
    max_message_bytes: usize,
}

impl Upstream {
    fn kv_client(&self) -> KvClient<Channel> {
        KvClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_message_bytes)
            .max_encoding_message_size(self.max_message_bytes)
    }

    fn watch_client(&self) -> WatchClient<Channel> {
        WatchClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_message_bytes)
            .max_encoding_message_size(self.max_message_bytes)
    }

    fn lease_client(&self) -> LeaseClient<Channel> {
        LeaseClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_message_bytes)
            .max_encoding_message_size(self.max_message_bytes)
    }
}

#[derive(Debug, Clone)]
struct TokenState {
    access_token: String,
    expires_at: Instant,
}

#[derive(Debug)]
struct ProxyState {
    upstreams: Vec<Upstream>,
    next_upstream: AtomicUsize,
    token: RwLock<TokenState>,
}

impl ProxyState {
    fn next_upstream(&self) -> Upstream {
        let idx = self.next_upstream.fetch_add(1, Ordering::Relaxed);
        self.upstreams[idx % self.upstreams.len()].clone()
    }

    async fn access_token(&self) -> Result<String, Status> {
        let token = self.token.read().await;
        if Instant::now() >= token.expires_at {
            return Err(Status::unavailable(
                "oidc proxy token expired before refresh completed",
            ));
        }
        Ok(token.access_token.clone())
    }

    async fn add_auth<T>(&self, req: &mut Request<T>) -> Result<(), Status> {
        let token = self.access_token().await?;
        let header = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|err| Status::internal(format!("invalid authorization header: {err}")))?;
        req.metadata_mut().insert("authorization", header);
        Ok(())
    }

    async fn replace_token(&self, next: TokenState) {
        let mut slot = self.token.write().await;
        *slot = next;
    }
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    token_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

#[derive(Debug)]
struct TokenFetch {
    access_token: String,
    expires_at: Instant,
    refresh_in: Duration,
}

#[derive(Clone)]
struct EtcdOidcProxy {
    state: Arc<ProxyState>,
}

type ProxyStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

impl EtcdOidcProxy {
    fn new(state: Arc<ProxyState>) -> Self {
        Self { state }
    }
}

pub async fn serve(args: OidcProxyArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .try_init();

    let listen_addr: SocketAddr = args
        .listen_addr
        .parse()
        .with_context(|| format!("invalid listen addr {}", args.listen_addr))?;

    let token_url = resolve_token_url(args.token_url.clone(), args.issuer.clone()).await?;
    let config = ProxyConfig {
        token_url,
        client_id: args.client_id,
        client_secret: args.client_secret,
        scope: args.scope,
        refresh_skew: Duration::from_secs(args.refresh_skew_secs.max(1)),
        refresh_backoff: Duration::from_secs(args.refresh_backoff_secs.max(1)),
    };
    let http = Client::builder()
        .build()
        .context("failed to create http client")?;

    let upstreams = build_upstreams(&args.upstreams, args.max_message_bytes)?;
    let initial = fetch_access_token(&http, &config).await?;
    let state = Arc::new(ProxyState {
        upstreams,
        next_upstream: AtomicUsize::new(0),
        token: RwLock::new(TokenState {
            access_token: initial.access_token.clone(),
            expires_at: initial.expires_at,
        }),
    });

    tokio::spawn(refresh_loop(state.clone(), http.clone(), config.clone()));

    info!(listen_addr = %listen_addr, upstreams = args.upstreams.join(","), "starting Astra OIDC etcd proxy");

    let proxy = EtcdOidcProxy::new(state);
    Server::builder()
        .add_service(KvServer::new(proxy.clone()))
        .add_service(LeaseServer::new(proxy.clone()))
        .add_service(WatchServer::new(proxy))
        .serve(listen_addr)
        .await
        .context("oidc proxy server failed")
}

fn build_upstreams(raw: &[String], max_message_bytes: usize) -> Result<Vec<Upstream>> {
    raw.iter()
        .map(|endpoint| {
            let endpoint = endpoint.trim();
            if endpoint.is_empty() {
                return Err(anyhow!("upstream endpoint cannot be empty"));
            }
            let channel = Endpoint::from_shared(endpoint.to_string())
                .with_context(|| format!("invalid upstream endpoint {endpoint}"))?
                .connect_lazy();
            Ok(Upstream {
                endpoint: endpoint.to_string(),
                channel,
                max_message_bytes,
            })
        })
        .collect()
}

async fn resolve_token_url(token_url: Option<String>, issuer: Option<String>) -> Result<String> {
    if let Some(token_url) = token_url {
        return Ok(token_url);
    }

    let issuer = issuer.ok_or_else(|| anyhow!("--issuer or --token-url is required"))?;
    let issuer = issuer.trim_end_matches('/');
    let discovery_url = format!("{issuer}/.well-known/openid-configuration");
    let discovery: DiscoveryDocument = reqwest::get(&discovery_url)
        .await
        .with_context(|| format!("failed to fetch discovery document {discovery_url}"))?
        .error_for_status()
        .with_context(|| format!("discovery document returned error for {discovery_url}"))?
        .json()
        .await
        .context("failed to decode discovery document")?;
    Ok(discovery.token_endpoint)
}

async fn fetch_access_token(client: &Client, config: &ProxyConfig) -> Result<TokenFetch> {
    let mut form = vec![
        ("grant_type".to_string(), "client_credentials".to_string()),
        ("client_id".to_string(), config.client_id.clone()),
        ("client_secret".to_string(), config.client_secret.clone()),
    ];
    if let Some(scope) = &config.scope {
        form.push(("scope".to_string(), scope.clone()));
    }

    let token: TokenResponse = client
        .post(&config.token_url)
        .form(&form)
        .send()
        .await
        .with_context(|| format!("failed to reach token endpoint {}", config.token_url))?
        .error_for_status()
        .with_context(|| format!("token endpoint {} returned error", config.token_url))?
        .json()
        .await
        .context("failed to decode token response")?;

    let expires_in = token.expires_in.unwrap_or(300).max(30);
    let ttl = Duration::from_secs(expires_in);
    let refresh_in = if expires_in > config.refresh_skew.as_secs().saturating_mul(2) {
        ttl.saturating_sub(config.refresh_skew)
    } else {
        Duration::from_secs(((expires_in as f64) * 0.8).round() as u64).max(Duration::from_secs(1))
    };

    Ok(TokenFetch {
        access_token: token.access_token,
        expires_at: Instant::now() + ttl,
        refresh_in,
    })
}

async fn refresh_loop(state: Arc<ProxyState>, client: Client, config: ProxyConfig) {
    let mut sleep_for = Duration::from_secs(1);
    loop {
        tokio::time::sleep(sleep_for).await;
        match fetch_access_token(&client, &config).await {
            Ok(token) => {
                let expires_in_secs = token
                    .expires_at
                    .saturating_duration_since(Instant::now())
                    .as_secs();
                state
                    .replace_token(TokenState {
                        access_token: token.access_token,
                        expires_at: token.expires_at,
                    })
                    .await;
                sleep_for = token.refresh_in;
                info!(expires_in_secs, refresh_in_secs = sleep_for.as_secs(), "refreshed OIDC access token for Astra proxy");
            }
            Err(err) => {
                warn!(error = %err, retry_in_secs = config.refresh_backoff.as_secs(), "failed to refresh OIDC access token for Astra proxy");
                sleep_for = config.refresh_backoff;
            }
        }
    }
}

fn upstream_status(err: impl std::fmt::Display, endpoint: &str) -> Status {
    Status::unavailable(format!("upstream {endpoint} request failed: {err}"))
}

fn client_stream<T>(inbound: Streaming<T>) -> impl Stream<Item = T> + Send + 'static
where
    T: Send + 'static,
{
    futures::stream::unfold(inbound, |mut inbound| async move {
        match inbound.message().await {
            Ok(Some(message)) => Some((message, inbound)),
            Ok(None) => None,
            Err(status) => {
                warn!(error = %status, "closing client stream after inbound stream error");
                None
            }
        }
    })
}

#[tonic::async_trait]
impl Kv for EtcdOidcProxy {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.kv_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .range(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.kv_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .put(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.kv_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .delete_range(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.kv_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .txn(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.kv_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .compact(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }
}

#[tonic::async_trait]
impl Lease for EtcdOidcProxy {
    type LeaseKeepAliveStream = ProxyStream<LeaseKeepAliveResponse>;

    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.lease_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .lease_grant(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.lease_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .lease_revoke(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.lease_client();
        let (metadata, extensions, inbound) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, client_stream(inbound));
        self.state.add_auth(&mut outbound).await?;
        let response = client
            .lease_keep_alive(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))?;
        Ok(Response::new(Box::pin(response.into_inner())))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.lease_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .lease_time_to_live(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }

    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.lease_client();
        let (metadata, extensions, message) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, message);
        self.state.add_auth(&mut outbound).await?;
        client
            .lease_leases(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))
    }
}

#[tonic::async_trait]
impl Watch for EtcdOidcProxy {
    type WatchStream = ProxyStream<WatchResponse>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let upstream = self.state.next_upstream();
        let mut client = upstream.watch_client();
        let (metadata, extensions, inbound) = request.into_parts();
        let mut outbound = Request::from_parts(metadata, extensions, client_stream(inbound));
        self.state.add_auth(&mut outbound).await?;
        let response = client
            .watch(outbound)
            .await
            .map_err(|err| upstream_status(err, &upstream.endpoint))?;
        Ok(Response::new(Box::pin(response.into_inner())))
    }
}
