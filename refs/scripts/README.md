# ctxctl

Repository context/state CLI for Astra.

## Commands
- `ctxctl init`
- `ctxctl state show`
- `ctxctl state update --summary "..." --set key=value --update "..."`
- `ctxctl kb add --type decision --title "..." --content "..."`
- `ctxctl kb search --query "..."`
- `ctxctl mem add --kind finding --title "..." --content "..."`
- `ctxctl mem query --query "..."`
- `ctxctl mem compact-context --token-budget 1200`
- `ctxctl docs sync --docs-dir docs --replace`
- `ctxctl context pack --query "phase10" --token-budget 1800`
- `ctxctl sync markdown-to-db`

## Development
```bash
uv sync --project refs/scripts --extra dev
uv run --project refs/scripts ctxctl --help
uv run --project refs/scripts pytest -q
```

## Validation Helpers
```bash
# Make target (recommended):
REPORT=phase12 make report-msg

# 1) Compact noisy logs (drops DEBUG by default, strips timestamps, compacts repeats)
uv run --project refs/scripts python refs/scripts/validation/compact_log.py \
  <validation-log.txt>

# 2) Build report message markdown with inlined referenced files
uv run --project refs/scripts python refs/scripts/validation/build_validation_report_msg.py \
  refs/tasks/reports/<report>.md \
  --repo-root . \
  --clean-referenced-logs
```

## Docs Automation
```bash
# Regenerate generated docs artifacts:
uv run --project refs/scripts python refs/scripts/docs/sync_docs.py

# Validate docs metadata, source mapping, and markdown links:
uv run --project refs/scripts python refs/scripts/docs/validate_docs.py

# Check source map drift only:
uv run --project refs/scripts python refs/scripts/docs/sync_source_map.py --check
```

## Remote Host Bootstrap
```bash
# Bootstrap a fresh Ubuntu 24.04 amd64 host (root SSH)
refs/scripts/validation/bootstrap-ubuntu24-remote.sh root@host-ip

# Optional env overrides for workspace/tool versions:
ASTRA_WORKSPACE=/root/astra-phase4 GHZ_VERSION=v0.121.0 \
  refs/scripts/validation/bootstrap-ubuntu24-remote.sh root@host-ip

# Run commands with Astra env/path preloaded:
ssh root@host-ip "astra-env"
ssh root@host-ip "astra-run 'cd \"\$ASTRA_REPO_DIR\" && docker compose version && ghz --version'"
```

## Deployment Automation (Single Node K3s + Astra)
```bash
# Full remote orchestration from local machine:
refs/scripts/deploy/deploy-k3s-single-node-remote.sh \
  --host root@host-ip \
  -- \
  --disk-device auto \
  --validation smoke

# Run directly on host:
refs/scripts/deploy/deploy-k3s-single-node.sh \
  --disk-device auto \
  --validation smoke

# Optional full readiness validation gate:
refs/scripts/deploy/deploy-k3s-single-node.sh \
  --disk-device auto \
  --validation full
```
