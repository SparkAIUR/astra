# Astra

Astra is an etcd-compatible control-plane datastore for Kubernetes, K3s, Omni, and other high-churn metadata systems.

It keeps the client contract that control planes expect, then adds workload-aware batching, multi-tenant virtualization, migration tooling, and deployment patterns that are practical for real operators.

## What ships in this repo

- `astrad`: the Astra server daemon
- `astractl`: operator and developer CLI
- `astra-forge`: snapshot, converge, and bulk-load tooling
- `refs/scripts/`: deploy, validation, docs, public-report, and release automation
- `refs/sandbox/omni/`: public Omni-on-Astra sandbox assets

## Quickstart

```bash
./quickstart.sh
```

Validate a basic etcd-compatible write and read:

```bash
docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --endpoints=http://127.0.0.1:2379 put /quickstart/hello astra

docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --endpoints=http://127.0.0.1:2379 get /quickstart/hello
```

## Common paths

- Replace etcd: [`docs/etcd-replacement.mdx`](docs/etcd-replacement.mdx)
- Run K3s on Astra: [`docs/guides/deploy-k3s-single-node.mdx`](docs/guides/deploy-k3s-single-node.mdx)
- Run Omni on Astra: [`docs/guides/deploy-omni-with-astra.mdx`](docs/guides/deploy-omni-with-astra.mdx)
- Benchmark results: [`BENCHMARKS.md`](BENCHMARKS.md)

## Docs

Public docs are Sparkify/Mintlify-style docs sourced from `docs/`.

Build them locally with:

```bash
make docs-check
```

## Images

Current public release candidate line:

- `docker.io/halceon/astra:v0.1.1-rc1`
- `docker.io/halceon/astra-forge:v0.1.1-rc1`
- `docker.io/nudevco/astra:v0.1.1-rc1`

## Crates

Public crate line for this release pass:

- `astra-proto = 0.1.1-rc1`
- `astra-core = 0.1.1-rc1`
- `astractl = 0.1.1-rc1`
- `astrad = 0.1.1-rc1`
- `astra-forge = 0.1.1-rc1`

## Development

```bash
cargo check --workspace
cargo test --workspace
make docs-check
```

## License

Licensed under Apache-2.0. See [`LICENSE`](LICENSE).
