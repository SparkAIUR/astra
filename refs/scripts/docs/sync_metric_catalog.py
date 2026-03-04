#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_METRICS_PATH = REPO_ROOT / "crates/astra-core/src/metrics.rs"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "docs/reference/metrics/metric-catalog.md"


def parse_metrics(metrics_rs: Path) -> list[tuple[str, str, str]]:
    text = metrics_rs.read_text(encoding="utf-8")
    result: dict[str, tuple[str, str]] = {}

    hist_pat = re.compile(
        r'reg\.\w+\.render\(\s*&mut out,\s*"([a-z0-9_]+)",\s*"([^"]+)",\s*\);',
        flags=re.S,
    )
    for m in hist_pat.finditer(text):
        name, help_text = m.group(1), m.group(2)
        result[name] = ("histogram", help_text)

    help_pat = re.compile(r'"# HELP ([a-z0-9_]+) ([^"]+)"')
    type_pat = re.compile(r'"# TYPE ([a-z0-9_]+) ([a-z]+)"')

    helps = {m.group(1): m.group(2) for m in help_pat.finditer(text)}
    types = {m.group(1): m.group(2) for m in type_pat.finditer(text)}

    for name, help_text in helps.items():
        metric_type = types.get(name, "unknown")
        result[name] = (metric_type, help_text)

    rows = [(name, mtype, help_text) for name, (mtype, help_text) in sorted(result.items())]
    return rows


def render_markdown(rows: list[tuple[str, str, str]]) -> str:
    out: list[str] = [
        "---",
        "title: Metric Catalog",
        "summary: Prometheus metric inventory generated from astra-core metrics registry.",
        "audience: operators",
        "status: canonical",
        "last_verified: 2026-03-03",
        "source_of_truth:",
        "  - crates/astra-core/src/metrics.rs",
        "related_artifacts:",
        "  - docs/reference/metrics/slo-sli-reference.md",
        "  - refs/sandbox/grafana/dashboards/astra-overview.json",
        "---",
        "",
        "# Metric Catalog",
        "",
        "Generated from `crates/astra-core/src/metrics.rs`.",
        "",
        "| Metric | Type | Description |",
        "| --- | --- | --- |",
    ]

    for name, mtype, help_text in rows:
        desc = help_text.replace("|", "\\|")
        out.append(f"| `{name}` | `{mtype}` | {desc} |")

    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate docs/reference/metrics/metric-catalog.md")
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    rows = parse_metrics(args.metrics)
    content = render_markdown(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
