# Contributing

## Prerequisites

- Rust toolchain (stable)
- Docker + Docker Compose
- Python 3.11+ with `uv` for script tooling
- Node.js 20+ for docs build

## Development Workflow

1. Create focused branches from `main`.
2. Keep commits atomic and test-backed.
3. Run local checks before opening PRs:

```bash
cargo check --workspace
cargo test --workspace
uv run --project refs/scripts pytest -q
make docs-check
```

## Pull Requests

- Keep scope tight and include rationale in PR description.
- Update docs when behavior/config/api changes.
- Ensure CI passes.

## Reporting Issues

Use GitHub Issues with reproduction steps, expected behavior, and actual behavior.
