# AGENTS.md

## Purpose
This file is the persistent working-knowledge index for the public Astra repository. It describes how to resume work quickly, where public and private context lives, and how to keep the maintainer workspace durable across sessions.

## Project Identity
- Project: `astra`
- Server binary: `astrad`
- CLI binary: `astractl`
- Migration binary: `astra-forge`
- API identity: `astra` / `kv.astra`
- Public images:
  - `docker.io/halceon/astra:{tag}`
  - `docker.io/halceon/astra-forge:{tag}`
  - `docker.io/nudevco/astra:{tag}`

## Canonical Public Inputs
- `README.md`
- `docs/`
- `BENCHMARKS.md`
- `refs/scripts/`
- `refs/sandbox/omni/`

## Private Maintainer Workspace
These paths are intentionally gitignored and may contain internal notes, plans, benchmark notes, or unpublished reports:

- `refs/tasks/specs/`
- `refs/tasks/reports/`
- `refs/docs/`
- `refs/sandbox/private/`

Use them freely for maintainer context. Do not reference them from tracked public docs or code.

## Startup Checklist
1. Read `refs/docs/STATE.yaml` if it exists locally.
2. Read `refs/docs/KB.md` if it exists locally.
3. Run `uv run --project refs/scripts ctxctl mem compact-context --token-budget 1200` when the local maintainer workspace is available.
4. Regenerate docs and public reports before release work.

## Operational Workflow
1. Keep public docs, examples, and release surfaces aligned with the current code.
2. Use `refs/tasks/specs` and `refs/tasks/reports` for longer planning or execution logs.
3. Keep commit boundaries atomic.
4. Keep private-only context inside ignored `refs/` paths.

## Quick Command Index
- Docs sync: `uv run --project refs/scripts python refs/scripts/docs/sync_docs.py`
- Docs validate: `uv run --project refs/scripts python refs/scripts/docs/validate_docs.py`
- Public hygiene: `uv run --project refs/scripts python refs/scripts/public/validate_public_hygiene.py --repo .`
- Public reports: `uv run --project refs/scripts python refs/scripts/public/generate_public_reports.py`
- Omni Keycloak provisioning: `uv run --project refs/scripts python refs/scripts/keycloak/provision_omni_realm.py --help`
- Release images: `refs/scripts/release/push-final-candidate.sh <tag>` and `refs/scripts/release/push-forge-image.sh <tag>`
- Publish crates: `refs/scripts/release/publish-crates.sh --version <version>`

## Release Conventions
- Default public prerelease line: `v0.1.1-rc1`
- Prefer explicit image tags in docs and manifests.
- Keep `BENCHMARKS.md` and `docs/research/benchmark-results.mdx` regenerated before public release work.
- Track GitHub workflows with `gh run list`, `gh run watch`, and `gh run view --log-failed`.
