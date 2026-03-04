use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use astra_core::store::ValueEntry;
use astra_core::tiering::build_chunk_bundle;
use astra_proto::astraadminpb::astra_admin_client::AstraAdminClient;
use astra_proto::astraadminpb::{BulkLoadRequest, GetBulkLoadJobRequest};
use astra_proto::etcdserverpb::kv_client::KvClient;
use astra_proto::etcdserverpb::RangeRequest;
use aws_sdk_s3::primitives::ByteStream;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::time::sleep;
use tonic::Request;
use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc};

#[derive(Debug, Parser)]
#[command(name = "astra-forge")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Compile {
        #[arg(long)]
        input: String,
        #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
        input_format: InputFormat,
        #[arg(long, default_value = "./forge-out")]
        out_dir: String,
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        chunk_target_bytes: usize,
        #[arg(long)]
        wasm: Option<String>,
        #[arg(long)]
        key_prefix: Option<String>,
        #[arg(long, default_value = "etcd")]
        etcd_bin: String,
        #[arg(long, default_value = "etcdutl")]
        etcdutl_bin: String,
    },
    BulkLoad {
        #[arg(long, default_value = "http://127.0.0.1:2379")]
        endpoint: String,
        #[arg(long)]
        manifest: String,
        #[arg(long, default_value = "default")]
        tenant_id: String,
        #[arg(long)]
        manifest_checksum: Option<String>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_overwrite: bool,
        #[arg(long)]
        bearer_token: Option<String>,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 500)]
        poll_interval_ms: u64,
    },
    Converge {
        #[arg(long = "source", required = true)]
        sources: Vec<String>,
        #[arg(long = "tenant-id", required = true)]
        tenant_ids: Vec<String>,
        #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
        input_format: InputFormat,
        #[arg(long)]
        apply_wasm: Option<String>,
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        chunk_target_bytes: usize,
        #[arg(long)]
        dest: String,
        #[arg(long)]
        endpoint: String,
        #[arg(long, default_value = "us-east-1")]
        region: String,
        #[arg(long, default_value = "http://127.0.0.1:2379")]
        astra_endpoint: String,
        #[arg(long)]
        bearer_token: Option<String>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_overwrite: bool,
        #[arg(long, default_value = "etcd")]
        etcd_bin: String,
        #[arg(long, default_value = "etcdutl")]
        etcdutl_bin: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum InputFormat {
    Auto,
    Jsonl,
    Endpoint,
    DbSnapshot,
}

#[derive(Debug, Clone)]
struct ForgeRow {
    key: Vec<u8>,
    entry: ValueEntry,
}

#[derive(Debug, Deserialize)]
struct JsonLineRecord {
    key: Option<String>,
    value: Option<String>,
    key_b64: Option<String>,
    value_b64: Option<String>,
    lease: Option<i64>,
    create_revision: Option<i64>,
    mod_revision: Option<i64>,
    version: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WasmRecordInput {
    key_b64: String,
    value_b64: String,
    lease: i64,
    create_revision: i64,
    mod_revision: i64,
    version: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WasmRecordOutput {
    #[serde(default)]
    drop: bool,
    key_b64: String,
    value_b64: String,
    #[serde(default)]
    lease: Option<i64>,
    #[serde(default)]
    create_revision: Option<i64>,
    #[serde(default)]
    mod_revision: Option<i64>,
    #[serde(default)]
    version: Option<i64>,
}

struct WasmTransformer {
    store: Store<()>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    dealloc: Option<TypedFunc<(i32, i32), ()>>,
    transform: TypedFunc<(i32, i32), i64>,
}

impl WasmTransformer {
    fn load(path: &Path) -> Result<Self> {
        let engine = Engine::default();
        let module = Module::from_file(&engine, path)
            .with_context(|| format!("failed to load wasm module from {}", path.display()))?;
        let mut store = Store::new(&engine, ());
        let instance =
            Instance::new(&mut store, &module, &[]).context("failed to instantiate wasm module")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow!("wasm export `memory` is required"))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .context("wasm export `alloc(i32)->i32` is required")?;
        let transform = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "transform")
            .context("wasm export `transform(i32,i32)->i64` is required")?;
        let dealloc = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
            .ok();

        Ok(Self {
            store,
            memory,
            alloc,
            dealloc,
            transform,
        })
    }

    fn transform_record(&mut self, row: ForgeRow) -> Result<Option<ForgeRow>> {
        let input = WasmRecordInput {
            key_b64: B64.encode(&row.key),
            value_b64: B64.encode(&row.entry.value),
            lease: row.entry.lease,
            create_revision: row.entry.create_revision,
            mod_revision: row.entry.mod_revision,
            version: row.entry.version,
        };
        let input_bytes = serde_json::to_vec(&input).context("failed to serialize wasm input")?;
        let in_len = i32::try_from(input_bytes.len()).context("wasm input too large")?;
        let in_ptr = self
            .alloc
            .call(&mut self.store, in_len)
            .context("wasm alloc() failed")?;
        self.memory
            .write(&mut self.store, in_ptr as usize, &input_bytes)
            .context("failed to write wasm input memory")?;

        let packed = self
            .transform
            .call(&mut self.store, (in_ptr, in_len))
            .context("wasm transform() failed")?;
        if let Some(dealloc) = &self.dealloc {
            let _ = dealloc.call(&mut self.store, (in_ptr, in_len));
        }

        let out_ptr = ((packed >> 32) & 0xffff_ffff) as usize;
        let out_len = (packed as u64 & 0xffff_ffff) as usize;
        if out_len == 0 {
            return Ok(None);
        }
        let mut out = vec![0_u8; out_len];
        self.memory
            .read(&self.store, out_ptr, &mut out)
            .context("failed to read wasm output memory")?;
        if let Some(dealloc) = &self.dealloc {
            let _ = dealloc.call(&mut self.store, (out_ptr as i32, out_len as i32));
        }

        let output: WasmRecordOutput =
            serde_json::from_slice(&out).context("failed to decode wasm output JSON")?;
        if output.drop {
            return Ok(None);
        }

        let key = B64
            .decode(output.key_b64)
            .context("failed to decode key_b64 from wasm output")?;
        let value = B64
            .decode(output.value_b64)
            .context("failed to decode value_b64 from wasm output")?;
        let entry = ValueEntry {
            value,
            lease: output.lease.unwrap_or(row.entry.lease),
            create_revision: output.create_revision.unwrap_or(row.entry.create_revision),
            mod_revision: output.mod_revision.unwrap_or(row.entry.mod_revision),
            version: output.version.unwrap_or(row.entry.version),
        };
        Ok(Some(ForgeRow { key, entry }))
    }
}

struct EtcdProc {
    child: Child,
    _tmpdir: TempDir,
}

impl Drop for EtcdProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug, Serialize)]
struct ConvergeSourceSummary {
    source: String,
    tenant_id: String,
    rows: usize,
    chunks: usize,
    s3_manifest: String,
    manifest_checksum: String,
}

#[derive(Debug, Serialize)]
struct ConvergeSummary {
    source_count: usize,
    tenant_count: usize,
    dest: String,
    admin_endpoint: String,
    dry_run: bool,
    applied: Vec<ConvergeSourceSummary>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Compile {
            input,
            input_format,
            out_dir,
            chunk_target_bytes,
            wasm,
            key_prefix,
            etcd_bin,
            etcdutl_bin,
        } => {
            run_compile(
                input,
                input_format,
                out_dir,
                chunk_target_bytes,
                wasm,
                key_prefix,
                etcd_bin,
                etcdutl_bin,
            )
            .await?
        }
        Commands::BulkLoad {
            endpoint,
            manifest,
            tenant_id,
            manifest_checksum,
            dry_run,
            allow_overwrite,
            bearer_token,
            wait,
            poll_interval_ms,
        } => {
            run_bulk_load(
                endpoint,
                manifest,
                tenant_id,
                manifest_checksum,
                dry_run,
                allow_overwrite,
                bearer_token,
                wait,
                poll_interval_ms,
            )
            .await?
        }
        Commands::Converge {
            sources,
            tenant_ids,
            input_format,
            apply_wasm,
            chunk_target_bytes,
            dest,
            endpoint,
            region,
            astra_endpoint,
            bearer_token,
            dry_run,
            allow_overwrite,
            etcd_bin,
            etcdutl_bin,
        } => {
            run_converge(
                sources,
                tenant_ids,
                input_format,
                apply_wasm,
                chunk_target_bytes,
                dest,
                endpoint,
                region,
                astra_endpoint,
                bearer_token,
                dry_run,
                allow_overwrite,
                etcd_bin,
                etcdutl_bin,
            )
            .await?
        }
    }
    Ok(())
}

async fn run_compile(
    input: String,
    input_format: InputFormat,
    out_dir: String,
    chunk_target_bytes: usize,
    wasm: Option<String>,
    key_prefix: Option<String>,
    etcd_bin: String,
    etcdutl_bin: String,
) -> Result<()> {
    let (rows, manifest, chunks) = compile_rows_to_bundle(
        &input,
        input_format,
        chunk_target_bytes,
        wasm.as_deref(),
        key_prefix.as_deref(),
        &etcd_bin,
        &etcdutl_bin,
    )
    .await?;
    write_chunk_bundle(Path::new(&out_dir), &manifest, chunks)?;
    println!(
        "compiled rows={} chunks={} revision={} manifest={}",
        rows,
        manifest.chunks.len(),
        manifest.revision,
        Path::new(&out_dir).join("manifest.json").display()
    );
    Ok(())
}

async fn run_converge(
    sources: Vec<String>,
    tenant_ids: Vec<String>,
    input_format: InputFormat,
    apply_wasm: Option<String>,
    chunk_target_bytes: usize,
    dest: String,
    endpoint: String,
    region: String,
    astra_endpoint: String,
    bearer_token: Option<String>,
    dry_run: bool,
    allow_overwrite: bool,
    etcd_bin: String,
    etcdutl_bin: String,
) -> Result<()> {
    if sources.is_empty() {
        bail!("at least one --source is required");
    }
    if sources.len() != tenant_ids.len() {
        bail!(
            "--source count ({}) must match --tenant-id count ({})",
            sources.len(),
            tenant_ids.len()
        );
    }

    let (bucket, base_prefix) = parse_s3_uri(&dest)?;
    let client = s3_client(&endpoint, &region).await;
    let tmp = tempfile::tempdir().context("failed to create converge workspace")?;
    let mut applied = Vec::with_capacity(sources.len());

    for (idx, (source, tenant_id)) in sources.iter().zip(tenant_ids.iter()).enumerate() {
        eprintln!(
            "[converge {}/{}] source={} tenant={}",
            idx + 1,
            sources.len(),
            source,
            tenant_id
        );
        let tenant_prefix = format!("/__tenant/{tenant_id}/");
        let out_dir = tmp
            .path()
            .join(format!("bundle-{}", sanitize_component(tenant_id)));
        let (rows, manifest, chunks) = compile_rows_to_bundle(
            source,
            input_format,
            chunk_target_bytes,
            apply_wasm.as_deref(),
            Some(&tenant_prefix),
            &etcd_bin,
            &etcdutl_bin,
        )
        .await?;
        write_chunk_bundle(&out_dir, &manifest, chunks)?;

        let prefix = join_s3_key_prefix(
            &base_prefix,
            &format!("converge/{}/{}", idx + 1, sanitize_component(tenant_id)),
        );
        let upload = upload_bundle_to_s3(&client, &bucket, &prefix, &out_dir).await?;
        let manifest_uri = format!("s3://{}/{}", bucket, upload.manifest_key);

        run_bulk_load(
            astra_endpoint.clone(),
            manifest_uri.clone(),
            String::new(),
            Some(upload.manifest_checksum.clone()),
            dry_run,
            allow_overwrite,
            bearer_token.clone(),
            true,
            500,
        )
        .await?;

        applied.push(ConvergeSourceSummary {
            source: source.clone(),
            tenant_id: tenant_id.clone(),
            rows,
            chunks: manifest.chunks.len(),
            s3_manifest: manifest_uri,
            manifest_checksum: upload.manifest_checksum,
        });
    }

    let summary = ConvergeSummary {
        source_count: sources.len(),
        tenant_count: tenant_ids.len(),
        dest,
        admin_endpoint: astra_endpoint,
        dry_run,
        applied,
    };
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn run_bulk_load(
    endpoint: String,
    manifest: String,
    tenant_id: String,
    manifest_checksum: Option<String>,
    dry_run: bool,
    allow_overwrite: bool,
    bearer_token: Option<String>,
    wait: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    let mut client = AstraAdminClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("failed to connect to admin endpoint: {endpoint}"))?;

    let mut req = Request::new(BulkLoadRequest {
        tenant_id,
        manifest_source: manifest,
        manifest_checksum: manifest_checksum.unwrap_or_default(),
        dry_run,
        allow_overwrite,
    });
    if let Some(token) = &bearer_token {
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }

    let submit = client.bulk_load(req).await?.into_inner();
    println!(
        "accepted={} job_id={} message={}",
        submit.accepted, submit.job_id, submit.message
    );

    if !wait {
        return Ok(());
    }

    loop {
        let mut req = Request::new(GetBulkLoadJobRequest {
            job_id: submit.job_id.clone(),
        });
        if let Some(token) = &bearer_token {
            req.metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse()?);
        }
        let job = client.get_bulk_load_job(req).await?.into_inner();
        println!(
            "job_id={} status={} records={}/{} message={}",
            job.job_id, job.status, job.records_applied, job.records_total, job.message
        );
        match job.status.as_str() {
            "succeeded" => return Ok(()),
            "failed" => bail!("bulkload job failed: {}", job.message),
            _ => sleep(Duration::from_millis(poll_interval_ms.max(100))).await,
        }
    }
}

fn detect_input_format(format: InputFormat, input: &str) -> InputFormat {
    match format {
        InputFormat::Auto => {
            if input.starts_with("http://") || input.starts_with("https://") {
                InputFormat::Endpoint
            } else if input.ends_with(".db")
                || input.ends_with(".snap")
                || input.ends_with(".snapshot")
            {
                InputFormat::DbSnapshot
            } else {
                InputFormat::Jsonl
            }
        }
        other => other,
    }
}

async fn compile_rows_to_bundle(
    input: &str,
    input_format: InputFormat,
    chunk_target_bytes: usize,
    wasm: Option<&str>,
    key_prefix: Option<&str>,
    etcd_bin: &str,
    etcdutl_bin: &str,
) -> Result<(
    usize,
    astra_core::tiering::TierManifest,
    Vec<(astra_core::tiering::TierChunkMeta, Vec<u8>)>,
)> {
    let mut rows = load_rows_from_input(input, input_format, etcd_bin, etcdutl_bin).await?;

    if let Some(prefix) = key_prefix {
        apply_key_prefix(&mut rows, prefix.as_bytes());
    }
    if let Some(path) = wasm {
        rows = apply_wasm_transform(rows, path)?;
    }

    let before_filter = rows.len();
    rows.retain(|row| !row.key.is_empty());
    let filtered = before_filter.saturating_sub(rows.len());
    if filtered > 0 {
        eprintln!("filtered {} rows with empty keys", filtered);
    }

    if rows.is_empty() {
        bail!("no rows found after extraction/transform");
    }

    rows.sort_by(|a, b| a.key.cmp(&b.key));
    let revision = rows
        .iter()
        .map(|r| r.entry.mod_revision)
        .max()
        .unwrap_or(rows.len() as i64);
    let compact_revision = revision.saturating_sub(1);
    let row_count = rows.len();
    let kv = rows
        .into_iter()
        .map(|row| (row.key, row.entry))
        .collect::<Vec<_>>();

    let (manifest, chunks) = build_chunk_bundle(
        &kv,
        chunk_target_bytes.max(4 * 1024),
        revision,
        compact_revision,
    );
    Ok((row_count, manifest, chunks))
}

async fn load_rows_from_input(
    input: &str,
    input_format: InputFormat,
    etcd_bin: &str,
    etcdutl_bin: &str,
) -> Result<Vec<ForgeRow>> {
    let format = detect_input_format(input_format, input);
    match format {
        InputFormat::Jsonl => load_rows_from_jsonl(Path::new(input)),
        InputFormat::Endpoint => load_rows_from_endpoint(input).await,
        InputFormat::DbSnapshot => {
            load_rows_from_db_snapshot(Path::new(input), etcd_bin, etcdutl_bin).await
        }
        InputFormat::Auto => unreachable!("auto is resolved by detect_input_format"),
    }
}

fn apply_key_prefix(rows: &mut [ForgeRow], prefix: &[u8]) {
    for row in rows {
        let mut key = Vec::with_capacity(prefix.len() + row.key.len());
        key.extend_from_slice(prefix);
        key.extend_from_slice(&row.key);
        row.key = key;
    }
}

fn apply_wasm_transform(rows: Vec<ForgeRow>, wasm: &str) -> Result<Vec<ForgeRow>> {
    let mut transformer = WasmTransformer::load(Path::new(wasm))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(next) = transformer.transform_record(row)? {
            out.push(next);
        }
    }
    Ok(out)
}

fn load_rows_from_jsonl(path: &Path) -> Result<Vec<ForgeRow>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut rows = Vec::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {}", idx + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let rec: JsonLineRecord = serde_json::from_str(trimmed)
            .with_context(|| format!("invalid json at line {}", idx + 1))?;
        let key = decode_bytes_field(rec.key, rec.key_b64)
            .with_context(|| format!("invalid key at line {}", idx + 1))?;
        let value = decode_bytes_field(rec.value, rec.value_b64)
            .with_context(|| format!("invalid value at line {}", idx + 1))?;
        let mod_revision = rec.mod_revision.unwrap_or((idx + 1) as i64).max(1);
        let create_revision = rec.create_revision.unwrap_or(mod_revision).max(1);
        rows.push(ForgeRow {
            key,
            entry: ValueEntry {
                value,
                lease: rec.lease.unwrap_or(0),
                create_revision,
                mod_revision,
                version: rec.version.unwrap_or(1).max(1),
            },
        });
    }
    Ok(rows)
}

fn decode_bytes_field(plain: Option<String>, b64: Option<String>) -> Result<Vec<u8>> {
    if let Some(v) = b64 {
        return B64.decode(v).context("invalid base64 field");
    }
    plain
        .map(|s| s.into_bytes())
        .ok_or_else(|| anyhow!("field is required"))
}

async fn load_rows_from_endpoint(endpoint: &str) -> Result<Vec<ForgeRow>> {
    let mut client = KvClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("failed to connect kv endpoint: {endpoint}"))?;
    let mut reqs = vec![
        RangeRequest {
            key: Vec::new(),
            range_end: vec![0],
            limit: 0,
            revision: 0,
            keys_only: false,
            count_only: false,
        },
        RangeRequest {
            key: vec![0],
            range_end: vec![0],
            limit: 0,
            revision: 0,
            keys_only: false,
            count_only: false,
        },
        RangeRequest {
            key: vec![0],
            range_end: Vec::new(),
            limit: 0,
            revision: 0,
            keys_only: false,
            count_only: false,
        },
    ]
    .into_iter();

    let first = reqs
        .next()
        .ok_or_else(|| anyhow!("internal error: missing range request"))?;
    let resp = match client.range(first).await {
        Ok(resp) => resp.into_inner(),
        Err(err) => {
            let mut last_err = err.to_string();
            let mut success = None;
            for req in reqs {
                match client.range(req).await {
                    Ok(resp) => {
                        success = Some(resp.into_inner());
                        break;
                    }
                    Err(next_err) => {
                        last_err = next_err.to_string();
                    }
                }
            }
            success.ok_or_else(|| anyhow!("failed to list rows from {endpoint}: {last_err}"))?
        }
    };

    let mut rows = Vec::with_capacity(resp.kvs.len());
    for kv in resp.kvs {
        rows.push(ForgeRow {
            key: kv.key,
            entry: ValueEntry {
                value: kv.value,
                create_revision: kv.create_revision,
                mod_revision: kv.mod_revision,
                version: kv.version,
                lease: kv.lease,
            },
        });
    }
    Ok(rows)
}

async fn load_rows_from_db_snapshot(
    snapshot: &Path,
    etcd_bin: &str,
    etcdutl_bin: &str,
) -> Result<Vec<ForgeRow>> {
    let tmp = tempfile::tempdir().context("failed to create temp dir for snapshot restore")?;
    let data_dir = tmp.path().join("restore");
    std::fs::create_dir_all(&data_dir)?;

    let peer_port = reserve_port()?;
    let client_port = reserve_port()?;
    let peer_url = format!("http://127.0.0.1:{peer_port}");
    let client_url = format!("http://127.0.0.1:{client_port}");
    let initial_cluster = format!("forge={peer_url}");

    let restore = Command::new(etcdutl_bin)
        .arg("snapshot")
        .arg("restore")
        .arg(snapshot.as_os_str())
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--name")
        .arg("forge")
        .arg("--initial-cluster")
        .arg(&initial_cluster)
        .arg("--initial-advertise-peer-urls")
        .arg(&peer_url)
        .arg("--skip-hash-check")
        .status()
        .with_context(|| format!("failed to execute `{etcdutl_bin} snapshot restore`"))?;
    if !restore.success() {
        bail!("`{etcdutl_bin} snapshot restore` failed");
    }

    let child = Command::new(etcd_bin)
        .arg("--name")
        .arg("forge")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--listen-peer-urls")
        .arg(&peer_url)
        .arg("--initial-advertise-peer-urls")
        .arg(&peer_url)
        .arg("--listen-client-urls")
        .arg(&client_url)
        .arg("--advertise-client-urls")
        .arg(&client_url)
        .arg("--initial-cluster")
        .arg(&initial_cluster)
        .arg("--initial-cluster-state")
        .arg("new")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to spawn `{etcd_bin}`"))?;

    let _proc = EtcdProc {
        child,
        _tmpdir: tmp,
    };

    let endpoint = format!("http://127.0.0.1:{client_port}");
    wait_for_endpoint(&endpoint).await?;
    load_rows_from_endpoint(&endpoint).await
}

async fn wait_for_endpoint(endpoint: &str) -> Result<()> {
    let mut last_err = String::new();
    for _ in 0..120 {
        match KvClient::connect(endpoint.to_string()).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_err = err.to_string();
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
    bail!("timed out waiting for endpoint {endpoint}: {last_err}");
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to reserve local port")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn write_chunk_bundle(
    out_dir: &Path,
    manifest: &astra_core::tiering::TierManifest,
    chunks: Vec<(astra_core::tiering::TierChunkMeta, Vec<u8>)>,
) -> Result<()> {
    std::fs::create_dir_all(out_dir)?;
    for (meta, blob) in chunks {
        let chunk_path = out_dir.join(PathBuf::from(&meta.key));
        if let Some(parent) = chunk_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = File::create(&chunk_path)
            .with_context(|| format!("failed to create {}", chunk_path.display()))?;
        file.write_all(&blob)
            .with_context(|| format!("failed to write {}", chunk_path.display()))?;
    }
    let manifest_path = out_dir.join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(manifest)?)
        .with_context(|| format!("failed to write {}", manifest_path.display()))?;
    Ok(())
}

#[derive(Debug)]
struct UploadedBundle {
    manifest_key: String,
    manifest_checksum: String,
}

async fn s3_client(endpoint: &str, region: &str) -> aws_sdk_s3::Client {
    let shared_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(endpoint.to_string())
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;
    let s3_conf = aws_sdk_s3::config::Builder::from(&shared_cfg)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(s3_conf)
}

fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("destination must be an s3:// URI"))?;
    let mut parts = rest.splitn(2, '/');
    let bucket = parts
        .next()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("s3 URI is missing bucket name"))?;
    let key = parts
        .next()
        .map(|s| s.trim_matches('/'))
        .unwrap_or("")
        .trim_matches('/');
    Ok((bucket.to_string(), key.to_string()))
}

fn join_s3_key_prefix(base: &str, suffix: &str) -> String {
    let base = base.trim_matches('/');
    let suffix = suffix.trim_matches('/');
    match (base.is_empty(), suffix.is_empty()) {
        (true, true) => String::new(),
        (true, false) => suffix.to_string(),
        (false, true) => base.to_string(),
        (false, false) => format!("{base}/{suffix}"),
    }
}

fn sanitize_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "tenant".to_string()
    } else {
        out
    }
}

fn collect_bundle_files(dir: &Path, root: &Path, out: &mut Vec<(PathBuf, String)>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_bundle_files(&path, root, out)?;
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .with_context(|| format!("failed to build relative path for {}", path.display()))?;
        let rel_key = rel.to_string_lossy().replace('\\', "/");
        out.push((path, rel_key));
    }
    Ok(())
}

async fn upload_bundle_to_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key_prefix: &str,
    bundle_dir: &Path,
) -> Result<UploadedBundle> {
    let mut files = Vec::new();
    collect_bundle_files(bundle_dir, bundle_dir, &mut files)?;
    if files.is_empty() {
        bail!("bundle directory is empty: {}", bundle_dir.display());
    }

    let mut manifest_key = None;
    let mut manifest_checksum = None;
    for (path, rel_key) in files {
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let object_key = join_s3_key_prefix(key_prefix, &rel_key);
        client
            .put_object()
            .bucket(bucket)
            .key(&object_key)
            .body(ByteStream::from(bytes.clone()))
            .send()
            .await
            .with_context(|| format!("failed to upload s3://{bucket}/{object_key}"))?;
        if rel_key == "manifest.json" {
            manifest_key = Some(object_key);
            manifest_checksum = Some(format!("{:08x}", crc32c::crc32c(&bytes)));
        }
    }

    let manifest_key = manifest_key.ok_or_else(|| anyhow!("bundle missing manifest.json"))?;
    let manifest_checksum =
        manifest_checksum.ok_or_else(|| anyhow!("manifest checksum missing"))?;
    Ok(UploadedBundle {
        manifest_key,
        manifest_checksum,
    })
}
