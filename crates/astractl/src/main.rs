use anyhow::Result;
use astra_proto::astraadminpb::astra_admin_client::AstraAdminClient;
use astra_proto::astraadminpb::{BulkLoadRequest, GetBulkLoadJobRequest};
use astra_proto::etcdserverpb::watch_request::RequestUnion;
use astra_proto::etcdserverpb::{
    kv_client::KvClient, watch_client::WatchClient, PutRequest, RangeRequest, WatchCreateRequest,
    WatchRequest,
};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use serde_json::json;
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
        } => {
            blast(
                &cli.endpoint,
                cli.bearer_token.as_deref(),
                prefix,
                count,
                payload_size,
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
) -> Result<()> {
    let mut client = KvClient::connect(endpoint.to_string()).await?;
    let payload = "x".repeat(payload_size);

    let mut success = 0_u32;
    let mut exhausted = 0_u32;
    let mut last_error = String::new();

    for i in 0..count {
        let key = format!("{prefix}/{i}");
        let req = PutRequest {
            key: key.into_bytes(),
            value: payload.as_bytes().to_vec(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        };

        let mut req = Request::new(req);
        add_auth(&mut req, bearer_token)?;
        match client.put(req).await {
            Ok(_) => success += 1,
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                exhausted += 1;
                last_error = status.message().to_string();
                break;
            }
            Err(status) => {
                last_error = format!("{:?}: {}", status.code(), status.message());
                break;
            }
        }
    }

    println!("blast success={success} exhausted={exhausted} last_error={last_error}");
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

    let started_at = std::time::Instant::now();
    for idx in 0..watchers {
        let endpoint = watch_targets[idx as usize % target_count].clone();
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
            let mut client = match WatchClient::connect(endpoint).await {
                Ok(c) => c,
                Err(_) => {
                    stream_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };

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

    let mut kv = KvClient::connect(endpoint.to_string()).await?;
    let mut put_lat_ms = Vec::with_capacity(updates as usize);
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
        let put_res = tokio::time::timeout(std::time::Duration::from_secs(60), kv.put(req)).await;
        match put_res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(anyhow::anyhow!("watch crucible put failed: {e}"));
            }
            Err(_) => {
                return Err(anyhow::anyhow!("watch crucible put timed out"));
            }
        }
        put_lat_ms.push(start.elapsed().as_millis() as u64);
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
        "setup_parallelism": setup_parallelism,
        "watchers_created": created_count,
        "updates": updates,
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
