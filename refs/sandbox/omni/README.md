# Omni on Astra Sandbox

This public sandbox shows how to run Omni on top of Astra with Keycloak-backed OIDC.

It is intentionally generic:

- no internal domains,
- no live credentials,
- no private tenant names,
- no environment-specific secrets.

Internal operators can keep environment-specific overlays under `refs/sandbox/private/omni`, which is intentionally untracked in the public mirror.

## Layout

- `cluster/`: shared Astra StatefulSet for the `sidero` namespace.
- `helm/`: Omni values and Secret examples.
- `migration/`: raw Omni snapshot import job and auth Secret examples.
- `keycloak/`: realm/client setup instructions.
- `scripts/fetch-keycloak-token.sh`: inspect a tenant service-account token.

## Deployment Model

- One Omni release maps to one Astra tenant.
- Human users authenticate to Omni with OIDC.
- Astra receives only per-tenant service-account tokens via the local `astractl oidc-proxy` sidecar.
- Human users can belong to multiple Keycloak groups such as `tenant:tenant-a`, `tenant:tenant-b`.
- Astra service clients must emit exactly one `tenant_id=<tenant>` claim.

## Generic Host Pattern

Choose a base domain and keep a stable tenant slug. The examples below assume:

- UI/API: `https://{tenant}.omni.example.net`
- Kubernetes proxy: `https://{tenant}.kube.omni.example.net`
- SideroLink API: `https://{tenant}.siderolink.omni.example.net`
- gRPC alias: `https://{tenant}.grpc.omni.example.net`
- workload-proxy parent: `proxy.omni.example.net`

Omni's OIDC redirect URI is:

- `https://{tenant}.omni.example.net/oidc/consume`

## Keycloak Setup

Read `refs/sandbox/omni/keycloak/README.md` and start from `refs/sandbox/omni/keycloak/realm-config.example.yaml`.

You can validate your realm state with:

```bash
uv run --project refs/scripts python refs/scripts/keycloak/provision_omni_realm.py \
  --config refs/sandbox/omni/keycloak/realm-config.example.yaml \
  --mode validate
```

Once your admin account has the required Keycloak admin roles, apply it with:

```bash
uv run --project refs/scripts python refs/scripts/keycloak/provision_omni_realm.py \
  --config refs/sandbox/omni/keycloak/realm-config.example.yaml \
  --mode apply
```

## Deploy Astra

```bash
kubectl apply -f refs/sandbox/omni/cluster/astra-oidc-secret.example.yaml
kubectl apply -k refs/sandbox/omni/cluster
```

## Prepare Omni Inputs

```bash
kubectl apply -f refs/sandbox/omni/helm/omni-astra-proxy-oidc.example.yaml
kubectl apply -f refs/sandbox/omni/helm/omni-etcd-key.example.yaml
```

Render before install:

```bash
helm template tenant-a /path/to/omni/chart \
  -n sidero \
  -f refs/sandbox/omni/helm/omni-values.base.yaml \
  -f refs/sandbox/omni/helm/omni-values.instance.example.yaml
```

## Migrate Existing Omni Snapshots

```bash
kubectl apply -f refs/sandbox/omni/migration/sources.secret.example.yaml
kubectl apply -f refs/sandbox/omni/migration/auth.secret.example.yaml
kubectl apply -k refs/sandbox/omni/migration
```

The migration job expects raw etcd snapshots and uses `docker.io/halceon/astra-forge:{tag}` for compile and bulk-load steps.
