---
title: Tenant Virtualization
summary: How Astra isolates overlapping keyspaces through tenant-aware key prefixing and auth claims.
audience: both
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - crates/astrad/src/main.rs
  - crates/astra-forge/src/main.rs
related_artifacts:
  - refs/scripts/validation/phase11-scenario-fleet-cutover.sh
---

# Tenant Virtualization

Astra supports virtual tenant keyspaces by prefixing keys with:

```text
/__tenant/<tenant_id>/...
```

## Where It Is Applied

- Online API path: request tenant identity from auth claim.
- Migration path (`astra-forge converge`): explicit tenant IDs per source snapshot.

## Why It Matters

- Enables multi-tenant disaggregation with overlapping raw key names.
- Prevents cross-tenant key collision during convergence and steady-state operation.

## Guardrails

- Auth and tenant claim mapping must be configured consistently.
- Migration validation must include per-tenant key parity and marker checks.
