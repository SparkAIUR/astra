# Keycloak Setup for Omni on Astra

This guide shows the Keycloak objects required for Omni + Astra integration.

## Assumptions

- Keycloak realm: `omni`
- Astra audience: `astra`
- Astra tenant claim: `tenant_id`
- Kubernetes namespace: `sidero`
- One Omni release per Astra tenant

## Token Model

Use two token shapes:

1. Human Omni login tokens
   - issued to Omni UI clients,
   - may include multiple Keycloak groups such as `tenant:tenant-a`, `tenant:tenant-b`,
   - should expose those memberships in a `groups` claim.

2. Astra service tokens
   - issued to the per-tenant Astra service client,
   - used only by the local `astractl oidc-proxy`,
   - must contain exactly one `tenant_id=<tenant>` claim,
   - must contain `aud=astra`.

Do not use human login tokens directly against Astra.

## Required Realm Objects

Create or reconcile the following objects:

- Audience/resource client: `astra`
- Optional client scope: `groups`
- Per-tenant UI client: `omni-<tenant>-ui`
- Per-tenant Astra service client: `omni-<tenant>-astra`
- Migration/admin service client: `astra-migrator`
- One group per tenant: `tenant:<tenant>`

## UI Client Settings

For `omni-<tenant>-ui`:

- protocol: `openid-connect`
- confidential client
- `standardFlowEnabled=true`
- `directAccessGrantsEnabled=false`
- `serviceAccountsEnabled=false`
- redirect URI: `https://{tenant}.omni.example.net/oidc/consume`
- web origin: `https://{tenant}.omni.example.net`
- optional client scope: `groups`

## Astra Service Client Settings

For `omni-<tenant>-astra`:

- protocol: `openid-connect`
- confidential client
- `standardFlowEnabled=false`
- `directAccessGrantsEnabled=false`
- `serviceAccountsEnabled=true`
- protocol mapper: hardcoded audience `astra`
- protocol mapper: hardcoded claim `tenant_id=<tenant>`

## Migration Client Settings

For `astra-migrator`:

- same service-account shape as `omni-<tenant>-astra`
- hardcoded audience `astra`
- hardcoded claim `tenant_id=ops`

## Generic Config File

Start from `refs/sandbox/omni/keycloak/realm-config.example.yaml`. Replace:

- `KEYCLOAK_BASE_URL`
- `KEYCLOAK_ADMIN_USERNAME`
- `KEYCLOAK_ADMIN_PASSWORD`
- tenant hostnames
- client secrets
- account UUIDs

## Validate Tokens

Inspect a tenant Astra service token:

```bash
refs/sandbox/omni/scripts/fetch-keycloak-token.sh \
  --issuer 'https://keycloak.example.net/realms/omni' \
  --client-id 'omni-tenant-a-astra' \
  --client-secret '<client-secret>' \
  --claim tenant_id
```

The token should contain:

- `aud=astra`
- `tenant_id=tenant-a`
