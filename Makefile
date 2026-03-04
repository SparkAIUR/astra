REPORT ?= phase4
PY := uv run --project refs/scripts python

REPORT_DOC := refs/docs/$(REPORT)-validation-report.md
REPORT_MSG := refs/docs/$(REPORT)-validation-report-msg.md

DOCS_PY := uv run --project refs/scripts python
DOCS_DIR := docs

.PHONY: report-msg docs-sync docs-validate docs-build
report-msg:
	$(PY) refs/scripts/validation/build_validation_report_msg.py \
		$(REPORT_DOC) \
		--repo-root . \
		--clean-referenced-logs \
		--output $(REPORT_MSG)

docs-sync:
	$(DOCS_PY) refs/scripts/docs/sync_docs.py

docs-validate:
	$(DOCS_PY) refs/scripts/docs/validate_docs.py

docs-build:
	npm --prefix $(DOCS_DIR) run docs:build
