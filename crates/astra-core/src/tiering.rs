use std::collections::HashSet;
use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::config::S3Config;
use crate::errors::{StoreError, TieringError};
use crate::io_budget::IoTokenBucket;
use crate::store::{KvStore, SnapshotState, ValueEntry};

#[derive(Debug)]
pub struct TieringManager {
    handle: JoinHandle<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierManifest {
    pub version: u32,
    pub revision: i64,
    pub compact_revision: i64,
    pub chunks: Vec<TierChunkMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierChunkMeta {
    pub key: String,
    pub size: usize,
    pub crc32c: u32,
    pub records: usize,
}

pub const CHUNK_MAGIC: &[u8; 4] = b"SST1";

impl TieringManager {
    pub fn start(
        node_id: u64,
        cfg: Option<S3Config>,
        interval: Duration,
        sst_target_bytes: usize,
        store: Arc<KvStore>,
        io_limiter: IoTokenBucket,
        sqe_limiter: IoTokenBucket,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let Some(cfg) = cfg else {
                info!("tiering disabled: no S3 configuration provided");
                return;
            };

            if node_id != 1 {
                info!(node_id, "tiering uploader disabled on non-primary node");
                return;
            }

            let effective_target_bytes = sst_target_bytes.max(4 * 1024);

            let shared_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .endpoint_url(cfg.endpoint.clone())
                .region(aws_config::Region::new(cfg.region.clone()))
                .load()
                .await;

            let s3_conf = aws_sdk_s3::config::Builder::from(&shared_cfg)
                .force_path_style(true)
                .build();
            let client = aws_sdk_s3::Client::from_conf(s3_conf);

            loop {
                tokio::time::sleep(interval).await;

                let snapshot = store.snapshot_state();
                if snapshot.kv.is_empty() {
                    continue;
                }

                let (metas, blobs) = build_sst_chunks(&snapshot.kv, effective_target_bytes);
                if metas.is_empty() {
                    continue;
                }

                let base = format!("{}/cluster-1", cfg.key_prefix);

                for (meta, body) in metas.iter().zip(blobs.iter()) {
                    sqe_limiter.acquire(1).await;
                    io_limiter.acquire(io_tokens_for_bytes(body.len())).await;
                    let object_key = format!("{base}/{}", meta.key);
                    let put = client
                        .put_object()
                        .bucket(&cfg.bucket)
                        .key(&object_key)
                        .metadata("crc32c", meta.crc32c.to_string())
                        .body(ByteStream::from(body.clone()))
                        .send()
                        .await;

                    if let Err(err) = put {
                        warn!(error = %err, key = %object_key, "chunk tier upload failed");
                    }
                }

                let manifest = TierManifest {
                    version: 2,
                    revision: snapshot.revision,
                    compact_revision: snapshot.compact_revision,
                    chunks: metas,
                };

                let manifest_key = format!("{base}/manifest.json");
                let manifest_bytes = match serde_json::to_vec_pretty(&manifest) {
                    Ok(v) => v,
                    Err(err) => {
                        error!(error = %err, "failed to serialize manifest");
                        continue;
                    }
                };

                io_limiter
                    .acquire(io_tokens_for_bytes(manifest_bytes.len()))
                    .await;
                sqe_limiter.acquire(1).await;
                let out = client
                    .put_object()
                    .bucket(&cfg.bucket)
                    .key(&manifest_key)
                    .body(ByteStream::from(manifest_bytes))
                    .send()
                    .await;

                match out {
                    Ok(_) => {
                        debug!(bucket = %cfg.bucket, key = %manifest_key, "uploaded manifest and chunked sstables");
                        if let Err(err) = gc_stale_chunks(
                            &client,
                            &cfg.bucket,
                            &base,
                            &manifest,
                            &io_limiter,
                            &sqe_limiter,
                        )
                        .await
                        {
                            warn!(error = %err, "failed to delete stale chunk objects");
                        }
                    }
                    Err(err) => warn!(error = %err, "manifest upload failed"),
                }
            }
        });

        Self { handle }
    }

    pub async fn join(self) {
        let _ = self.handle.await;
    }

    pub async fn restore_if_empty(
        _node_id: u64,
        cfg: Option<&S3Config>,
        store: Arc<KvStore>,
    ) -> Result<bool, TieringError> {
        let Some(cfg) = cfg else {
            return Ok(false);
        };

        if !store.is_empty() {
            return Ok(false);
        }

        let shared_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(cfg.endpoint.clone())
            .region(aws_config::Region::new(cfg.region.clone()))
            .load()
            .await;

        let s3_conf = aws_sdk_s3::config::Builder::from(&shared_cfg)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(s3_conf);

        let base = format!("{}/cluster-1", cfg.key_prefix);
        let manifest_key = format!("{base}/manifest.json");

        let manifest_obj = client
            .get_object()
            .bucket(&cfg.bucket)
            .key(&manifest_key)
            .send()
            .await
            .map_err(|e| TieringError::AwsSdk(e.to_string()))?;

        let manifest_bytes = manifest_obj
            .body
            .collect()
            .await
            .map_err(|e| TieringError::AwsSdk(e.to_string()))?
            .into_bytes()
            .to_vec();

        let manifest: TierManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| TieringError::Store(StoreError::Internal(e.to_string())))?;

        let mut kv = Vec::new();
        for chunk in &manifest.chunks {
            let key = format!("{base}/{}", chunk.key);
            let obj = client
                .get_object()
                .bucket(&cfg.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| TieringError::AwsSdk(e.to_string()))?;

            let bytes = obj
                .body
                .collect()
                .await
                .map_err(|e| TieringError::AwsSdk(e.to_string()))?
                .into_bytes()
                .to_vec();

            let checksum = crc32c::crc32c(&bytes);
            if checksum != chunk.crc32c {
                return Err(TieringError::ChecksumMismatch);
            }

            decode_chunk(&bytes, &mut kv)
                .map_err(|e| TieringError::Store(StoreError::Internal(e.to_string())))?;
        }

        store
            .load_snapshot_state(SnapshotState {
                kv,
                leases: Vec::new(),
                next_lease_id: 1,
                revision: manifest.revision,
                compact_revision: manifest.compact_revision,
            })
            .map_err(StoreError::from)
            .map_err(TieringError::from)?;

        info!(bucket = %cfg.bucket, key = %manifest_key, "restored state from chunked object storage");
        Ok(true)
    }
}

async fn gc_stale_chunks(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    base: &str,
    manifest: &TierManifest,
    io_limiter: &IoTokenBucket,
    sqe_limiter: &IoTokenBucket,
) -> Result<(), TieringError> {
    let keep = manifest
        .chunks
        .iter()
        .map(|c| format!("{base}/{}", c.key))
        .collect::<HashSet<_>>();

    let prefix = format!("{base}/chunks/");
    let mut continuation: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(&prefix);
        if let Some(token) = continuation.clone() {
            req = req.continuation_token(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| TieringError::AwsSdk(e.to_string()))?;

        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                if !keep.contains(key) {
                    sqe_limiter.acquire(1).await;
                    io_limiter.acquire(1).await;
                    client
                        .delete_object()
                        .bucket(bucket)
                        .key(key)
                        .send()
                        .await
                        .map_err(|e| TieringError::AwsSdk(e.to_string()))?;
                }
            }
        }

        if !resp.is_truncated().unwrap_or(false) {
            break;
        }
        continuation = resp.next_continuation_token().map(|s| s.to_string());
    }

    Ok(())
}

fn io_tokens_for_bytes(bytes: usize) -> u64 {
    ((bytes as u64).saturating_add(4095) / 4096).max(1)
}

fn encode_record(buf: &mut Vec<u8>, key: &[u8], v: &ValueEntry) {
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(v.value.len() as u32).to_le_bytes());
    buf.extend_from_slice(&v.value);
    buf.extend_from_slice(&v.create_revision.to_le_bytes());
    buf.extend_from_slice(&v.mod_revision.to_le_bytes());
    buf.extend_from_slice(&v.version.to_le_bytes());
    buf.extend_from_slice(&v.lease.to_le_bytes());
}

pub fn build_sst_chunks(
    rows: &[(Vec<u8>, ValueEntry)],
    target_bytes: usize,
) -> (Vec<TierChunkMeta>, Vec<Vec<u8>>) {
    let mut metas = Vec::new();
    let mut blobs = Vec::new();

    let mut chunk_idx = 1_u64;
    let mut current = Vec::new();
    current.extend_from_slice(CHUNK_MAGIC);
    let mut records = 0usize;

    for (key, val) in rows {
        let mut record = Vec::new();
        encode_record(&mut record, key, val);

        if current.len() + record.len() > target_bytes && records > 0 {
            let crc = crc32c::crc32c(&current);
            let name = format!("chunks/{chunk_idx:05}-{crc:08x}.sst");
            metas.push(TierChunkMeta {
                key: name,
                size: current.len(),
                crc32c: crc,
                records,
            });
            blobs.push(current);

            chunk_idx += 1;
            current = Vec::new();
            current.extend_from_slice(CHUNK_MAGIC);
            records = 0;
        }

        current.extend_from_slice(&record);
        records += 1;
    }

    if records > 0 {
        let crc = crc32c::crc32c(&current);
        let name = format!("chunks/{chunk_idx:05}-{crc:08x}.sst");
        metas.push(TierChunkMeta {
            key: name,
            size: current.len(),
            crc32c: crc,
            records,
        });
        blobs.push(current);
    }

    (metas, blobs)
}

pub fn decode_chunk(
    bytes: &[u8],
    out: &mut Vec<(Vec<u8>, ValueEntry)>,
) -> Result<(), std::io::Error> {
    if bytes.len() < CHUNK_MAGIC.len() || &bytes[..CHUNK_MAGIC.len()] != CHUNK_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid chunk magic",
        ));
    }

    let mut cur = Cursor::new(&bytes[CHUNK_MAGIC.len()..]);

    while (cur.position() as usize) < (bytes.len() - CHUNK_MAGIC.len()) {
        let mut u32_buf = [0_u8; 4];
        if cur.read_exact(&mut u32_buf).is_err() {
            break;
        }
        let key_len = u32::from_le_bytes(u32_buf) as usize;

        let mut key = vec![0_u8; key_len];
        cur.read_exact(&mut key)?;

        cur.read_exact(&mut u32_buf)?;
        let val_len = u32::from_le_bytes(u32_buf) as usize;

        let mut value = vec![0_u8; val_len];
        cur.read_exact(&mut value)?;

        let mut i64_buf = [0_u8; 8];
        cur.read_exact(&mut i64_buf)?;
        let create_revision = i64::from_le_bytes(i64_buf);
        cur.read_exact(&mut i64_buf)?;
        let mod_revision = i64::from_le_bytes(i64_buf);
        cur.read_exact(&mut i64_buf)?;
        let version = i64::from_le_bytes(i64_buf);
        cur.read_exact(&mut i64_buf)?;
        let lease = i64::from_le_bytes(i64_buf);

        out.push((
            key,
            ValueEntry {
                value,
                create_revision,
                mod_revision,
                version,
                lease,
            },
        ));
    }

    Ok(())
}

pub fn decode_chunk_to_rows(bytes: &[u8]) -> Result<Vec<(Vec<u8>, ValueEntry)>, std::io::Error> {
    let mut out = Vec::new();
    decode_chunk(bytes, &mut out)?;
    Ok(out)
}

pub fn build_chunk_bundle(
    rows: &[(Vec<u8>, ValueEntry)],
    target_bytes: usize,
    revision: i64,
    compact_revision: i64,
) -> (TierManifest, Vec<(TierChunkMeta, Vec<u8>)>) {
    let (metas, blobs) = build_sst_chunks(rows, target_bytes);
    let bundle = metas.iter().cloned().zip(blobs).collect::<Vec<_>>();
    (
        TierManifest {
            version: 2,
            revision,
            compact_revision,
            chunks: metas,
        },
        bundle,
    )
}
