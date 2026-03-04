# Public Mirror Tooling

This directory contains tooling for exporting the private Astra repository into a clean public mirror repository.

## Files

- `public-export-manifest.yaml`: include/exclude boundaries, rewrite rules, and commit buckets.
- `export_public_repo.py`: deterministic exporter from private repo to public repo.
- `validate_public_hygiene.py`: checks for local absolute paths and private-reference leakage in public repo files.

## Usage

### 1) Full export (sanitized docs + rewrites)

```bash
uv run --project refs/scripts python refs/scripts/public/export_public_repo.py \
  --source /Volumes/S0/github/_sparkai/astra \
  --dest /Volumes/S0/github/_halceon/astra \
  --mode full \
  --rewrite \
  --sanitize-docs
```

### 2) Bucket export (for chronological replay commits)

```bash
uv run --project refs/scripts python refs/scripts/public/export_public_repo.py \
  --source /Volumes/S0/github/_sparkai/astra \
  --dest /Volumes/S0/github/_halceon/astra \
  --mode incremental \
  --bucket workspace \
  --rewrite \
  --sanitize-docs
```

### 3) Boundary/hygiene checks

```bash
uv run --project refs/scripts python refs/scripts/public/export_public_repo.py \
  --source /Volumes/S0/github/_sparkai/astra \
  --dest /Volumes/S0/github/_halceon/astra \
  --check

uv run --project refs/scripts python refs/scripts/public/validate_public_hygiene.py \
  --repo /Volumes/S0/github/_halceon/astra
```
