---
title: Docs Sources of Truth
summary: Mapping between documentation pages and authoritative code/artifact sources.
audience: contributors
status: canonical
last_verified: 2026-03-03
source_of_truth:
  - docs/.meta/source-map.yaml
related_artifacts:
  - refs/scripts/docs/sync_source_map.py
  - refs/scripts/docs/validate_docs.py
---

# Docs Sources of Truth

The authoritative mapping is maintained in:

- `docs/.meta/source-map.yaml`

Every canonical page must map to code files, proto definitions, scripts, or validated reports.

Use the source-map validators before merging docs changes.
