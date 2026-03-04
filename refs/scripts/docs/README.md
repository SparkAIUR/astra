# Docs Automation Scripts

This directory contains deterministic generators and validators for the Astra docs site.

## Scripts
- `sync_env_vars_doc.py`: Generates `docs/reference/config/env-vars.md` from `AstraConfig::from_env`.
- `sync_metric_catalog.py`: Generates `docs/reference/metrics/metric-catalog.md` from metrics registry source.
- `sync_source_map.py`: Generates `docs/.meta/source-map.yaml` from docs frontmatter.
- `validate_docs.py`: Validates frontmatter completeness, source-of-truth references, and markdown links.
- `check_docs_impact.py`: Warns/fails when code changes land without docs updates.
- `sync_docs.py`: Orchestrates all generators.

## Usage
```bash
uv run --project refs/scripts python refs/scripts/docs/sync_docs.py
uv run --project refs/scripts python refs/scripts/docs/validate_docs.py
```
