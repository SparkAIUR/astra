# Astra

Astra is a disaggregated control-plane datastore that provides etcd-compatible APIs with strict CP behavior, multi-tenant virtualization, and performance-tuning primitives for Kubernetes/edge control planes.

## Components

- `astrad`: server runtime.
- `astractl`: operator/developer CLI.
- `astra-forge`: migration and bulk-load tooling.

## Quickstart

```bash
./quickstart.sh
```

Then verify:

```bash
docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --endpoints=http://127.0.0.1:2379 put /quickstart/hello astra

docker run --rm --network host quay.io/coreos/etcd:v3.6.8 \
  etcdctl --endpoints=http://127.0.0.1:2379 get /quickstart/hello
```

## Documentation

- Site source: `docs/`
- VitePress config: `docs/.vitepress/config.mjs`

Build docs:

```bash
npm --prefix docs ci
npm --prefix docs run docs:build
```

## Image Publishing

Stable release tags use semantic versioning (`vX.Y.Z`).

Public image repositories:

- `docker.io/halceon/astra`
- `docker.io/nudevco/astra`

## Development

```bash
cargo check --workspace
cargo test --workspace
```

## License

Licensed under Apache-2.0. See `LICENSE`.
