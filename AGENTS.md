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
- Implementation Specs: `refs/tasks/specs`
- Reports: `refs/tasks/reports`
- Learned knowledge: `refs/docs/KB.md`
- Current execution state: `refs/docs/STATE.yaml`

## Private Maintainer Workspace
These paths are intentionally gitignored and may contain internal notes, plans, benchmark notes, or unpublished reports:

- `refs/tasks/specs/`
- `refs/tasks/reports/`
- `refs/docs/`
- `refs/sandbox/private/`

Use them freely for maintainer context. Do not reference them from tracked public docs or code.

## Startup Checklist (Every New Session)
1. Read the current state snapshot and knowledge-base notes from the local ignored `refs/docs/` workspace if it exists.
2. Read any active maintainer task specs or reports from the local ignored `refs/tasks/` workspace if they exist.
3. Run `uv run --project refs/scripts ctxctl mem compact-context --token-budget 1200` when the local maintainer workspace is available.
4. Regenerate docs and public reports before release work.

## Operational Workflow
1. Keep public docs, examples, and release surfaces aligned with the current code.
2. Use `refs/tasks/specs` and `refs/tasks/reports` for longer planning or execution logs.
3. Add durable findings to `KB.md` (not ephemeral notes).
4. Commit in atomic, task-scoped increments.
5. Keep interfaces stable and document deviations immediately.
6. Keep private-only context inside ignored `refs/` paths.

## Commit Protocol
- Prefer one commit per atomic task.
- Commit message style:
  - `docs: ...`
  - `scripts: ...`
  - `bootstrap: ...`
  - `core: ...`
  - `api: ...`
  - `storage: ...`
  - `ops: ...`
  - `test: ...`
  - `release: ...`
- Never batch unrelated changes into one commit.

## Quick Command Index
- Docs sync: `uv run --project refs/scripts python refs/scripts/docs/sync_docs.py`
- Docs validate: `uv run --project refs/scripts python refs/scripts/docs/validate_docs.py`
- Public hygiene: `uv run --project refs/scripts python refs/scripts/public/validate_public_hygiene.py --repo .`
- Public reports: `uv run --project refs/scripts python refs/scripts/public/generate_public_reports.py`
- Omni Keycloak provisioning: `uv run --project refs/scripts python refs/scripts/keycloak/provision_omni_realm.py --help`
- Release images: `refs/scripts/release/push-final-candidate.sh <tag>` and `refs/scripts/release/push-forge-image.sh <tag>`
- Publish crates: `refs/scripts/release/publish-crates.sh --version <version>`
- Context CLI help: `uv run --project refs/scripts ctxctl --help`
- Show current state: `uv run --project refs/scripts ctxctl state show`
- Append state update: `uv run --project refs/scripts ctxctl state update --summary "..." --set phase=... --set status=...`
- Add KB entry:
  - `uv run --project refs/scripts ctxctl kb add --type finding --title "..." --content "..." --evidence "..." --impact "..." --follow-up "..."`
- Add memory record:  `uv run --project refs/scripts ctxctl mem add --kind decision --title "..." --content "..."`
- Compact context packet: `uv run --project refs/scripts ctxctl mem compact-context --token-budget 1200`
- Build full context packet (state + KB + memories): `uv run --project refs/scripts ctxctl context pack --query "<topic>" --token-budget 1800`

## Remote Host Conventions
- For Ubuntu 24.04 amd64 remote nodes with root SSH access, bootstrap first:
  - `refs/scripts/validation/bootstrap-ubuntu24-remote.sh root@host-ip`
- For remote command execution, prefer Astra wrappers when present:
  - `ssh root@host-ip "command -v astra-run >/dev/null 2>&1 && astra-run 'cd \"\$ASTRA_REPO_DIR\" && <command>' || (source ~/.bashrc && <command>)"`
- For remote environment inspection, prefer `astra-env` when present:
  - `ssh root@host-ip "command -v astra-env >/dev/null 2>&1 && astra-env || (source ~/.bashrc && env | grep '^ASTRA_')"`
- Default fallback behavior (when wrappers are unavailable): run via shell init:
  - `ssh root@host-ip "source ~/.bashrc && <command>"`

## Release Conventions
- Default public prerelease line: `v0.1.1-rc1`
- Prefer explicit image tags in docs and manifests.
- Keep `BENCHMARKS.md` and `docs/research/benchmark-results.mdx` regenerated before public release work.
- Track GitHub workflows with `gh run list`, `gh run watch`, and `gh run view --log-failed`.

