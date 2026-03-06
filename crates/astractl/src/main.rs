mod oidc_proxy;

use anyhow::Result;
use astra_proto::astraadminpb::astra_admin_client::AstraAdminClient;
use astra_proto::astraadminpb::{BulkLoadRequest, GetBulkLoadJobRequest, StreamListRequest};
use astra_proto::etcdserverpb::watch_request::RequestUnion;
use astra_proto::etcdserverpb::{
    kv_client::KvClient, watch_client::WatchClient, PutRequest, RangeRequest, WatchCreateRequest,
    WatchRequest,
};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Endpoint;
use tonic::Request;

#[derive(Debug, Parser)]
#[command(name = "astractl")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoint: String,

    #[arg(long)]
    bearer_token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
        #[arg(long)]
        prefix: bool,
    },
    Watch {
        key: String,
        #[arg(long)]
        prefix: bool,
        #[arg(long, default_value_t = 0)]
        from_revision: i64,
    },
    Blast {
        #[arg(long, default_value = "/blast")]
        prefix: String,
        #[arg(long, default_value_t = 10000)]
        count: u32,
        #[arg(long, default_value_t = 32768)]
        payload_size: usize,
        #[arg(long, default_value_t = 30)]
        put_timeout_secs: u64,
        #[arg(long, default_value_t = 1)]
        concurrency: u32,
    },
    PutStorm {
        #[arg(long, default_value = "/storm")]
        prefix: String,
        #[arg(long, default_value_t = 1000)]
        keys: u32,
        #[arg(long, default_value_t = 1024)]
        payload_size: usize,
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        #[arg(long, default_value_t = 128)]
        concurrency: u32,
        #[arg(long, default_value_t = 0)]
        target_rps: u64,
        #[arg(long)]
        output: Option<String>,
    },
    BulkLoad {
        manifest: String,
        #[arg(long, default_value = "default")]
        tenant_id: String,
        #[arg(long)]
        manifest_checksum: Option<String>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_overwrite: bool,
    },
    BulkLoadJob {
        job_id: String,
    },
    StreamList {
        key: String,
        #[arg(long)]
        prefix: bool,
        #[arg(long, default_value_t = 0)]
        limit: i64,
        #[arg(long, default_value_t = 0)]
        revision: i64,
        #[arg(long, default_value_t = 2 * 1024 * 1024)]
        page_size_bytes: u64,
        #[arg(long, default_value_t = false)]
        keys_only: bool,
        #[arg(long, default_value_t = false)]
        count_only: bool,
    },
    WatchCrucible {
        key: String,
        #[arg(long)]
        prefix: bool,
        #[arg(long, default_value_t = 10_000)]
        watchers: u32,
        #[arg(long, default_value_t = 64)]
        streams: u32,
        #[arg(long, default_value_t = 32)]
        updates: u32,
        #[arg(long, default_value_t = 1024)]
        payload_size: usize,
        #[arg(long, default_value_t = 2_000)]
        settle_ms: u64,
        #[arg(long, default_value_t = 120)]
        create_timeout_secs: u64,
        #[arg(long)]
        watch_endpoints: Option<String>,
        #[arg(long)]
        output: Option<String>,
    },
    OidcProxy(oidc_proxy::OidcProxyArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Put { key, value } => {
            put(&cli.endpoint, cli.bearer_token.as_deref(), key, value).await?
        }
        Commands::Get { key, prefix } => {
            get(&cli.endpoint, cli.bearer_token.as_deref(), key, prefix).await?
        }
        Commands::Watch {
            key,
            prefix,
            from_revision,
        } => {
            watch(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                key,
                prefix,
                from_revision,
            )
            .await?
        }
        Commands::Blast {
            prefix,
            count,
            payload_size,
            put_timeout_secs,
            concurrency,
        } => {
            blast(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                prefix,
                count,
                payload_size,
                put_timeout_secs,
                concurrency,
            )
            .await?
        }
        Commands::PutStorm {
            prefix,
            keys,
            payload_size,
            duration_secs,
            concurrency,
            target_rps,
            output,
        } => {
            put_storm(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                prefix,
                keys,
                payload_size,
                duration_secs,
                concurrency,
                target_rps,
                output,
            )
            .await?
        }
        Commands::BulkLoad {
            manifest,
            tenant_id,
            manifest_checksum,
            dry_run,
            allow_overwrite,
        } => {
            bulk_load(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                manifest,
                tenant_id,
                manifest_checksum,
                dry_run,
                allow_overwrite,
            )
            .await?
        }
        Commands::BulkLoadJob { job_id } => {
            bulk_load_job(&cli.endpoint, cli.bearer_token.as_deref(), job_id).await?
        }
        Commands::StreamList {
            key,
            prefix,
            limit,
            revision,
            page_size_bytes,
            keys_only,
            count_only,
        } => {
            stream_list(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                key,
                prefix,
                limit,
                revision,
                page_size_bytes,
                keys_only,
                count_only,
            )
            .await?
        }
        Commands::WatchCrucible {
            key,
            prefix,
            watchers,
            streams,
            updates,
            payload_size,
            settle_ms,
            create_timeout_secs,
            watch_endpoints,
            output,
        } => {
            watch_crucible(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                key,
                prefix,
                watchers,
                streams,
                updates,
                payload_size,
                settle_ms,
                create_timeout_secs,
                watch_endpoints,
                output,
            )
            .await?
        }
        Commands::OidcProxy(args) => oidc_proxy::serve(args).await?,
    }

    Ok(())
}

fn add_auth<T>(req: &mut Request<T>, bearer_token: Option<&str>) -> Result<()> {
    if let Some(token) = bearer_token {
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }
    Ok(())
}

async fn put(endpoint: &str, bearer_token: Option<&str>, key: String, value: String) -> Result<()> {
    let mut client = KvClient::connect(endpoint.to_string()).await?;
    let mut req = Request::new(PutRequest {
        key: key.into_bytes(),
        value: value.into_bytes(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    });
    add_auth(&mut req, bearer_token)?;

    let resp = client.put(req).await?.into_inner();
    println!(
        "ok revision={}",
        resp.header.map(|h| h.revision).unwrap_or_default()
    );
    Ok(())
}

async fn get(endpoint: &str, bearer_token: Option<&str>, key: String, prefix: bool) -> Result<()> {
    let mut client = KvClient::connect(endpoint.to_string()).await?;
    let key_bytes = key.into_bytes();
    let range_end = if prefix {
        prefix_end(&key_bytes)
    } else {
        Vec::new()
    };

    let mut req = Request::new(RangeRequest {
        key: key_bytes,
        range_end,
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    });
    add_auth(&mut req, bearer_token)?;

    let resp = client.range(req).await?.into_inner();
    for kv in resp.kvs {
        println!(
            "{}={}",
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value)
        );
    }
    println!(
        "count={} revision={}",
        resp.count,
        resp.header.map(|h| h.revision).unwrap_or_default()
    );

    Ok(())
}

async fn watch(
    endpoint: &str,
    bearer_token: Option<&str>,
    key: String,
    prefix: bool,
    from_revision: i64,
) -> Result<()> {
    let mut client = WatchClient::connect(endpoint.to_string()).await?;

    let key_bytes = key.into_bytes();
    let range_end = if prefix {
        prefix_end(&key_bytes)
    } else {
        Vec::new()
    };

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(WatchRequest {
        request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
            key: key_bytes,
            range_end,
            start_revision: from_revision,
            progress_notify: false,
            prev_kv: false,
        })),
    })
    .await?;

    drop(tx);

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut req = Request::new(stream);
    add_auth(&mut req, bearer_token)?;
    let mut resp = client.watch(req).await?.into_inner();

    while let Some(msg) = resp.next().await {
        let msg = msg?;
        if msg.created {
            println!("watch created id={}", msg.watch_id);
            continue;
        }
        if msg.canceled {
            println!(
                "watch canceled id={} compact_rev={} reason={}",
                msg.watch_id, msg.compact_revision, msg.cancel_reason
            );
            break;
        }

        for ev in msg.events {
            if let Some(kv) = ev.kv {
                println!(
                    "event type={} key={} value={} rev={}",
                    ev.r#type,
                    String::from_utf8_lossy(&kv.key),
                    String::from_utf8_lossy(&kv.value),
                    kv.mod_revision
                );
            }
        }
    }

    Ok(())
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

async fn blast(
    endpoint: &str,
    bearer_token: Option<&str>,
    prefix: String,
    count: u32,
    payload_size: usize,
    put_timeout_secs: u64,
    concurrency: u32,
) -> Result<()> {
    let concurrency = concurrency.max(1);
    let payload = "x".repeat(payload_size);
    let next_idx = Arc::new(AtomicU64::new(0));
    let success = Arc::new(AtomicU64::new(0));
    let exhausted = Arc::new(AtomicU64::new(0));
    let stopped = Arc::new(AtomicBool::new(false));
    let last_error = Arc::new(tokio::sync::Mutex::new(String::new()));
    let timeout_secs = put_timeout_secs.max(1);
    let mut set = tokio::task::JoinSet::new();

    for _ in 0..concurrency {
        let endpoint = endpoint.to_string();
        let token = bearer_token.map(ToOwned::to_owned);
        let prefix = prefix.clone();
        let payload = payload.clone();
        let next_idx = next_idx.clone();
        let success = success.clone();
        let exhausted = exhausted.clone();
        let stopped = stopped.clone();
        let last_error = last_error.clone();

        set.spawn(async move {
            let mut client = KvClient::connect(endpoint).await?;
            loop {
                if stopped.load(Ordering::Relaxed) {
                    break;
                }
                let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= count as u64 {
                    break;
                }

                let key = format!("{prefix}/{idx}");
                let req = PutRequest {
                    key: key.into_bytes(),
                    value: payload.as_bytes().to_vec(),
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                };

                let mut req = Request::new(req);
                add_auth(&mut req, token.as_deref())?;
                let put =
                    tokio::time::timeout(Duration::from_secs(timeout_secs), client.put(req)).await;
                match put {
                    Ok(Ok(_)) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(status)) if status.code() == tonic::Code::ResourceExhausted => {
                        exhausted.fetch_add(1, Ordering::Relaxed);
                        if !stopped.swap(true, Ordering::Relaxed) {
                            let mut guard = last_error.lock().await;
                            *guard = status.message().to_string();
                        }
                        break;
                    }
                    Ok(Err(status)) => {
                        if !stopped.swap(true, Ordering::Relaxed) {
                            let mut guard = last_error.lock().await;
                            *guard = format!("{:?}: {}", status.code(), status.message());
                        }
                        break;
                    }
                    Err(_) => {
                        if !stopped.swap(true, Ordering::Relaxed) {
                            let mut guard = last_error.lock().await;
                            *guard = format!("timeout waiting for put response ({timeout_secs}s)");
                        }
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    while let Some(result) = set.join_next().await {
        result??;
    }

    let success = success.load(Ordering::Relaxed);
    let exhausted = exhausted.load(Ordering::Relaxed);
    let last_error = last_error.lock().await.clone();
    println!("blast success={success} exhausted={exhausted} last_error={last_error}");
    Ok(())
}

fn latency_bucket_bounds_ms() -> &'static [u64] {
    &[
        1, 2, 5, 10, 20, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 5000,
    ]
}

fn latency_bucket_index(bounds: &[u64], value_ms: u64) -> usize {
    bounds
        .iter()
        .position(|b| value_ms <= *b)
        .unwrap_or(bounds.len())
}

fn approx_quantile_ms(
    buckets: &[AtomicU64],
    bounds: &[u64],
    total: u64,
    q_num: u64,
    q_den: u64,
) -> u64 {
    if total == 0 || q_den == 0 {
        return 0;
    }
    let target = ((total.saturating_mul(q_num)) + (q_den - 1)) / q_den;
    let mut seen = 0_u64;
    for (idx, bucket) in buckets.iter().enumerate() {
        seen = seen.saturating_add(bucket.load(Ordering::Relaxed));
        if seen >= target.max(1) {
            return bounds
                .get(idx)
                .copied()
                .unwrap_or_else(|| bounds.last().copied().unwrap_or(5_000) * 2);
        }
    }
    bounds.last().copied().unwrap_or(5_000) * 2
}

async fn put_storm(
    endpoint: &str,
    bearer_token: Option<&str>,
    prefix: String,
    keys: u32,
    payload_size: usize,
    duration_secs: u64,
    concurrency: u32,
    target_rps: u64,
    output: Option<String>,
) -> Result<()> {
    if keys == 0 {
        return Err(anyhow::anyhow!("keys must be > 0"));
    }
    if payload_size == 0 {
        return Err(anyhow::anyhow!("payload-size must be > 0"));
    }
    if duration_secs == 0 {
        return Err(anyhow::anyhow!("duration-secs must be > 0"));
    }
    if concurrency == 0 {
        return Err(anyhow::anyhow!("concurrency must be > 0"));
    }

    let started = Instant::now();
    let stop_at = started + Duration::from_secs(duration_secs);
    let counters = Arc::new(AtomicU64::new(0));
    let success = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let sum_lat_ms = Arc::new(AtomicU64::new(0));
    let max_lat_ms = Arc::new(AtomicU64::new(0));
    let bounds = latency_bucket_bounds_ms();
    let mut bucket_vec = Vec::with_capacity(bounds.len() + 1);
    for _ in 0..(bounds.len() + 1) {
        bucket_vec.push(AtomicU64::new(0));
    }
    let buckets = Arc::new(bucket_vec);
    let mut set = tokio::task::JoinSet::new();

    for worker_idx in 0..concurrency {
        let endpoint = endpoint.to_string();
        let token = bearer_token.map(ToOwned::to_owned);
        let prefix = prefix.clone();
        let counters = counters.clone();
        let success = success.clone();
        let failures = failures.clone();
        let sum_lat_ms = sum_lat_ms.clone();
        let max_lat_ms = max_lat_ms.clone();
        let buckets = buckets.clone();

        let per_worker_interval = if target_rps > 0 {
            let per_worker_rps = ((target_rps as f64) / (concurrency as f64)).max(1.0);
            Some(Duration::from_secs_f64(1.0 / per_worker_rps))
        } else {
            None
        };

        set.spawn(async move {
            let mut client = KvClient::connect(endpoint).await?;
            let mut next_tick = Instant::now();
            loop {
                let now = Instant::now();
                if now >= stop_at {
                    break;
                }
                if let Some(interval) = per_worker_interval {
                    if next_tick > now {
                        tokio::time::sleep_until(next_tick.into()).await;
                    }
                    next_tick += interval;
                }

                let op = counters.fetch_add(1, Ordering::Relaxed);
                let key_idx = op % (keys as u64);
                let key = format!("{prefix}/{key_idx}");
                let mut value = format!(
                    "{worker_idx:04}:{op:016}:{micros:016}:",
                    micros = started.elapsed().as_micros()
                )
                .into_bytes();
                if value.len() < payload_size {
                    value.resize(payload_size, b'x');
                } else if value.len() > payload_size {
                    value.truncate(payload_size);
                }

                let mut req = Request::new(PutRequest {
                    key: key.into_bytes(),
                    value,
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                });
                add_auth(&mut req, token.as_deref())?;

                let op_started = Instant::now();
                let put = tokio::time::timeout(Duration::from_secs(30), client.put(req)).await;
                match put {
                    Ok(Ok(_)) => {
                        let lat_ms = op_started.elapsed().as_millis() as u64;
                        success.fetch_add(1, Ordering::Relaxed);
                        sum_lat_ms.fetch_add(lat_ms, Ordering::Relaxed);
                        loop {
                            let prev = max_lat_ms.load(Ordering::Relaxed);
                            if lat_ms <= prev {
                                break;
                            }
                            if max_lat_ms
                                .compare_exchange(
                                    prev,
                                    lat_ms,
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                            {
                                break;
                            }
                        }
                        let idx = latency_bucket_index(bounds, lat_ms);
                        buckets[idx].fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(_)) | Err(_) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(inner) => inner?,
            Err(e) => return Err(anyhow::anyhow!("worker join failed: {e}")),
        }
    }

    let elapsed = started.elapsed();
    let ok = success.load(Ordering::Relaxed);
    let err = failures.load(Ordering::Relaxed);
    let total = ok.saturating_add(err);
    let elapsed_secs = elapsed.as_secs_f64().max(1e-9);
    let rps = (ok as f64) / elapsed_secs;
    let avg_ms = if ok == 0 {
        0.0
    } else {
        (sum_lat_ms.load(Ordering::Relaxed) as f64) / (ok as f64)
    };
    let p50_ms = approx_quantile_ms(&buckets, bounds, ok, 50, 100);
    let p99_ms = approx_quantile_ms(&buckets, bounds, ok, 99, 100);

    let summary = json!({
        "prefix": prefix,
        "keys": keys,
        "payload_size": payload_size,
        "duration_secs": duration_secs,
        "concurrency": concurrency,
        "target_rps": target_rps,
        "ok": ok,
        "errors": err,
        "total": total,
        "rps": rps,
        "latency_ms": {
            "avg": avg_ms,
            "p50_approx": p50_ms,
            "p99_approx": p99_ms,
            "max": max_lat_ms.load(Ordering::Relaxed),
        },
    });

    println!("{}", serde_json::to_string_pretty(&summary)?);
    if let Some(path) = output {
        std::fs::write(path, serde_json::to_vec_pretty(&summary)?)?;
    }
    Ok(())
}

async fn bulk_load(
    endpoint: &str,
    bearer_token: Option<&str>,
    manifest: String,
    tenant_id: String,
    manifest_checksum: Option<String>,
    dry_run: bool,
    allow_overwrite: bool,
) -> Result<()> {
    let mut client = AstraAdminClient::connect(endpoint.to_string()).await?;
    let mut req = Request::new(BulkLoadRequest {
        tenant_id,
        manifest_source: manifest,
        manifest_checksum: manifest_checksum.unwrap_or_default(),
        dry_run,
        allow_overwrite,
    });
    add_auth(&mut req, bearer_token)?;
    let resp = client.bulk_load(req).await?.into_inner();
    println!(
        "accepted={} job_id={} message={}",
        resp.accepted, resp.job_id, resp.message
    );
    Ok(())
}

async fn bulk_load_job(endpoint: &str, bearer_token: Option<&str>, job_id: String) -> Result<()> {
    let mut client = AstraAdminClient::connect(endpoint.to_string()).await?;
    let mut req = Request::new(GetBulkLoadJobRequest { job_id });
    add_auth(&mut req, bearer_token)?;
    let resp = client.get_bulk_load_job(req).await?.into_inner();
    println!(
        "job_id={} status={} records={}/{} started_ms={} finished_ms={} message={}",
        resp.job_id,
        resp.status,
        resp.records_applied,
        resp.records_total,
        resp.started_at_unix_ms,
        resp.finished_at_unix_ms,
        resp.message
    );
    Ok(())
}

async fn stream_list(
    endpoint: &str,
    bearer_token: Option<&str>,
    key: String,
    prefix: bool,
    limit: i64,
    revision: i64,
    page_size_bytes: u64,
    keys_only: bool,
    count_only: bool,
) -> Result<()> {
    let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
        .connect()
        .await?;
    let mut client = AstraAdminClient::new(channel)
        .max_decoding_message_size(512 * 1024 * 1024)
        .max_encoding_message_size(512 * 1024 * 1024);
    let key_bytes = key.into_bytes();
    let range_end = if prefix {
        prefix_end(&key_bytes)
    } else {
        Vec::new()
    };

    let mut req = Request::new(StreamListRequest {
        key: key_bytes,
        range_end,
        limit,
        revision,
        keys_only,
        count_only,
        page_size_bytes,
    });
    add_auth(&mut req, bearer_token)?;

    let mut stream = client.stream_list(req).await?.into_inner();
    let mut pages = 0u64;
    let mut total = 0u64;
    while let Some(chunk) = stream.message().await? {
        pages = pages.saturating_add(1);
        total = total.saturating_add(chunk.kvs.len() as u64);
        println!(
            "page={} revision={} count={} kvs={} more={} next_key_len={}",
            pages,
            chunk.revision,
            chunk.count,
            chunk.kvs.len(),
            chunk.more,
            chunk.next_key.len()
        );
    }
    println!("stream_list pages={} kvs_total={}", pages, total);
    Ok(())
}

async fn watch_crucible(
    endpoint: &str,
    bearer_token: Option<&str>,
    key: String,
    prefix: bool,
    watchers: u32,
    streams: u32,
    updates: u32,
    payload_size: usize,
    settle_ms: u64,
    create_timeout_secs: u64,
    watch_endpoints: Option<String>,
    output: Option<String>,
) -> Result<()> {
    if watchers == 0 {
        return Err(anyhow::anyhow!("watchers must be > 0"));
    }
    let key_bytes = key.into_bytes();
    let range_end = if prefix {
        prefix_end(&key_bytes)
    } else {
        Vec::new()
    };
    let setup_parallelism = streams.max(1) as usize;

    let created = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let events = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stream_errors = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let setup_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(setup_parallelism));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut join_set = tokio::task::JoinSet::new();

    let mut watch_targets = vec![endpoint.to_string()];
    if let Some(raw) = watch_endpoints {
        for item in raw.split(',') {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                continue;
            }
            let normalized = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
                trimmed.to_string()
            } else {
                format!("http://{trimmed}")
            };
            if !watch_targets.iter().any(|v| v == &normalized) {
                watch_targets.push(normalized);
            }
        }
    }
    let target_count = watch_targets.len();
    let watch_channels = {
        let mut channels = Vec::with_capacity(setup_parallelism.max(1));
        for idx in 0..setup_parallelism.max(1) {
            let target = watch_targets[idx % target_count].clone();
            let channel = Endpoint::from_shared(target)?.connect().await?;
            channels.push(channel);
        }
        channels
    };

    let started_at = std::time::Instant::now();
    for idx in 0..watchers {
        let channel = watch_channels[idx as usize % watch_channels.len()].clone();
        let token = bearer_token.map(ToOwned::to_owned);
        let key = key_bytes.clone();
        let range_end = range_end.clone();
        let created = created.clone();
        let events = events.clone();
        let stream_errors = stream_errors.clone();
        let setup_semaphore = setup_semaphore.clone();
        let mut stop_rx = stop_rx.clone();

        join_set.spawn(async move {
            let mut setup_permit = match setup_semaphore.acquire_owned().await {
                Ok(p) => Some(p),
                Err(_) => None,
            };
            let mut client = WatchClient::new(channel);

            let (tx, rx) = tokio::sync::mpsc::channel(4);
            if tx
                .send(WatchRequest {
                    request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                        key: key.clone(),
                        range_end: range_end.clone(),
                        start_revision: 0,
                        progress_notify: false,
                        prev_kv: false,
                    })),
                })
                .await
                .is_err()
            {
                stream_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            let mut req = Request::new(stream);
            if let Some(token) = &token {
                let Ok(value) = format!("Bearer {token}").parse() else {
                    stream_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                };
                req.metadata_mut().insert("authorization", value);
            }

            let mut resp = match client.watch(req).await {
                Ok(r) => r.into_inner(),
                Err(_) => {
                    stream_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };

            let mut created_seen = false;
            let mut error_reported = false;

            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    msg = resp.next() => {
                        match msg {
                            Some(Ok(msg)) => {
                                if msg.created && !created_seen {
                                    created_seen = true;
                                    created.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let _ = setup_permit.take();
                                }
                                if !msg.events.is_empty() {
                                    events.fetch_add(msg.events.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                }
                                if msg.canceled {
                                    break;
                                }
                            }
                            Some(Err(_)) => {
                                stream_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                error_reported = true;
                                break;
                            }
                            None => break,
                        }
                    }
                }
            }

            if !created_seen && !error_reported {
                stream_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let _ = setup_permit.take();
            drop(tx);
        });
    }

    let create_deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(create_timeout_secs.max(1));
    while std::time::Instant::now() < create_deadline {
        let ready = created.load(std::sync::atomic::Ordering::Relaxed);
        if ready >= watchers as u64 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let created_count = created.load(std::sync::atomic::Ordering::Relaxed);

    let mut put_error = String::new();
    let mut put_lat_ms = Vec::with_capacity(updates as usize);
    let mut put_updates_ok = 0_u32;
    let mut kv_client = None;
    for target in &watch_targets {
        match KvClient::connect(target.clone()).await {
            Ok(client) => {
                kv_client = Some(client);
                break;
            }
            Err(err) => {
                if put_error.is_empty() {
                    put_error = format!("kv connect failed for {target}: {err}");
                }
            }
        }
    }

    if let Some(mut kv) = kv_client {
        for i in 0..updates {
            let payload = format!("{:08}:{}", i, "x".repeat(payload_size));
            let mut req = Request::new(PutRequest {
                key: key_bytes.clone(),
                value: payload.into_bytes(),
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            });
            add_auth(&mut req, bearer_token)?;
            let start = std::time::Instant::now();
            let put_res =
                tokio::time::timeout(std::time::Duration::from_secs(60), kv.put(req)).await;
            match put_res {
                Ok(Ok(_)) => {
                    put_updates_ok = put_updates_ok.saturating_add(1);
                }
                Ok(Err(e)) => {
                    put_error = format!("watch crucible put failed: {e}");
                    break;
                }
                Err(_) => {
                    put_error = "watch crucible put timed out".to_string();
                    break;
                }
            }
            put_lat_ms.push(start.elapsed().as_millis() as u64);
        }
    } else if put_error.is_empty() {
        put_error = "no reachable endpoint for watch crucible put updates".to_string();
    }

    tokio::time::sleep(std::time::Duration::from_millis(settle_ms.max(100))).await;
    let _ = stop_tx.send(true);
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}

    let elapsed_ms = started_at.elapsed().as_millis() as u64;
    let mut sorted = put_lat_ms.clone();
    sorted.sort_unstable();
    let p50 = sorted
        .get((sorted.len().saturating_sub(1)) * 50 / 100)
        .copied()
        .unwrap_or(0);
    let p99 = sorted
        .get((sorted.len().saturating_sub(1)) * 99 / 100)
        .copied()
        .unwrap_or(0);

    let summary = json!({
        "watchers_target": watchers,
        "streams_target": streams,
        "watch_targets": target_count,
        "channel_pool_size": watch_channels.len(),
        "setup_parallelism": setup_parallelism,
        "watchers_created": created_count,
        "updates": updates,
        "updates_ok": put_updates_ok,
        "put_error": put_error,
        "events_received": events.load(std::sync::atomic::Ordering::Relaxed),
        "stream_errors": stream_errors.load(std::sync::atomic::Ordering::Relaxed),
        "duration_ms": elapsed_ms,
        "put_latency_ms": {
            "p50": p50,
            "p99": p99,
        }
    });

    println!("{}", serde_json::to_string_pretty(&summary)?);
    if let Some(path) = output {
        std::fs::write(path, serde_json::to_vec_pretty(&summary)?)?;
    }
    Ok(())
}
