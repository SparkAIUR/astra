## Summary
- 

## Validation
- [ ] `uv run --project refs/scripts python refs/scripts/docs/sync_docs.py`
- [ ] `uv run --project refs/scripts python refs/scripts/docs/validate_docs.py`
- [ ] `npm --prefix docs run docs:build`

## Docs Impact
- [ ] No user-facing behavior changed.
- [ ] User-facing behavior changed and docs were updated in `docs/`.
- [ ] Generated docs artifacts updated when required (`env-vars`, `metric-catalog`, `source-map`).

## Source-of-Truth Alignment
- [ ] New/updated docs pages include frontmatter with `source_of_truth`.
- [ ] Any new config/metric/API surface has matching reference docs.
