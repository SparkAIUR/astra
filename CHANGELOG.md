# Changelog

All notable changes to Astra are recorded here.

## [0.1.1-rc1] - 2026-03-06
### Added
- Public Sparkify/Mintlify documentation set with benchmark, migration, k3s, and Omni guidance.
- Public Omni sandbox manifests with generic Keycloak/OIDC placeholders and migration helpers.
- `halceon/astra-forge` helper-image release path and Omni OIDC proxy support in `astractl`.
- Production single-node k3s deployment automation with Astra-first startup gating and stronger defaults.
- Public maintainer surfaces: `AGENTS.md`, `BENCHMARKS.md`, release runbook, CODEOWNERS, scripts CI, dependency review, and CodeQL workflows.

### Changed
- Public repo is now the intended primary development repo.
- RC tags no longer promote `:latest` image aliases; only stable tags do.
- GitHub release workflow is aligned with crates.io trusted publishing and no longer requires a cargo token for already-configured trusted publishers.
- Public reports now use tracked sanitized benchmark data instead of references to private validation documents.

### Fixed
- Prerelease crate publish skip detection now uses the crates.io API with an explicit user agent.
- Public docs workflows no longer depend on a removed npm lockfile.
- Public benchmark dataset is tracked so docs/report generation succeeds in CI.

## [0.1.0] - 2026-03-05
### Added
- Initial public Astra release with etcd-compatible server, CLI, and migration tooling.
