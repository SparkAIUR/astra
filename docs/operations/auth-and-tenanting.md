---
title: Auth and Tenanting
summary: Configure JWT authorization and tenant claim mapping in Astra.
audience: operators
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astrad/src/main.rs
  - crates/astra-core/src/config.rs
related_artifacts:
  - refs/scripts/validation/phase5-scenario-auth-tenant.sh
---

# Auth and Tenanting

## Auth Modes

- Disabled (development only).
- HS256 shared secret.
- JWKS-backed verification.

## Tenant Mapping

- Tenant identity is extracted from configured claim (`ASTRAD_AUTH_TENANT_CLAIM`).
- With tenant virtualization enabled, keys are mapped into tenant-prefixed namespaces.

## Recommended Validation

- Token missing claim should fail authorization.
- Tenant A token cannot read/write tenant B keys.
