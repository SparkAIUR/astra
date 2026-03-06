PY := uv run --project refs/scripts python
SPARKIFY := bash refs/scripts/public/run_sparkify.sh
DOCS_DIR := docs
DOCS_OUT := dist
SITE ?= https://sparkaiur.github.io
BASE ?= /astra
TAG ?= v0.1.1-rc1
VERSION ?= 0.1.1-rc1
SPARKIFY_VERSION ?= 0.2.3
OMNI_CHART ?=

.PHONY: docs-sync docs-validate docs-build docs-check public-hygiene reports-generate omni-render k3s-dry-run release-dry-run publish-crates publish-images publish-forge

docs-sync:
	$(PY) refs/scripts/docs/sync_docs.py

docs-validate:
	$(PY) refs/scripts/docs/validate_docs.py

reports-generate:
	$(PY) refs/scripts/public/generate_public_reports.py

public-hygiene:
	$(PY) refs/scripts/public/validate_public_hygiene.py --repo .

docs-build:
	SPARKIFY_VERSION=$(SPARKIFY_VERSION) $(SPARKIFY) build --docs-dir $(DOCS_DIR) --out $(DOCS_OUT) --site $(SITE) --base $(BASE) --strict

docs-check: docs-sync reports-generate docs-validate public-hygiene docs-build

omni-render:
	test -n "$(OMNI_CHART)" || (echo "set OMNI_CHART=/path/to/omni/chart" >&2; exit 1)
	helm template tenant-a $(OMNI_CHART) -n sidero -f refs/sandbox/omni/helm/omni-values.base.yaml -f refs/sandbox/omni/helm/omni-values.instance.example.yaml >/dev/null
	kubectl kustomize refs/sandbox/omni/cluster >/dev/null
	kubectl kustomize refs/sandbox/omni/migration >/dev/null

k3s-dry-run:
	bash refs/scripts/deploy/deploy-k3s-single-node.sh --help >/dev/null
	bash refs/scripts/deploy/deploy-k3s-single-node-remote.sh --help >/dev/null

release-dry-run:
	cargo check --workspace
	bash refs/scripts/release/build-image.sh $(TAG)
	bash refs/scripts/release/build-forge-image.sh $(TAG)
	bash refs/scripts/release/publish-crates.sh --version $(VERSION) --dry-run

publish-crates:
	bash refs/scripts/release/publish-crates.sh --version $(VERSION)

publish-images:
	bash refs/scripts/release/push-final-candidate.sh $(TAG)

publish-forge:
	bash refs/scripts/release/push-forge-image.sh $(TAG)
