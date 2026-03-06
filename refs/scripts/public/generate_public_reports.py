#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA = REPO_ROOT / "docs/research/data/benchmarks.json"
DEFAULT_BENCHMARKS = REPO_ROOT / "BENCHMARKS.md"
DEFAULT_DOC = REPO_ROOT / "docs/research/benchmark-results.mdx"


def load_data(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def render_benchmarks_md(data: dict) -> str:
    out = [
        "# BENCHMARKS",
        "",
        "This file is generated from `docs/research/data/benchmarks.json`.",
        "",
        f"Last updated: `{data.get('last_updated', 'unknown')}`",
        "",
        "## Astra vs etcd snapshots",
        "",
        "| Phase | Scenario | Status | Astra | etcd |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in data.get("astra_vs_etcd", []):
        out.append(
            f"| {row['phase']} | {row['scenario']} | {row['status']} | {row['astra_summary']} | {row['etcd_summary']} |"
        )
    out.extend([
        "",
        "## Feature validations",
        "",
    ])
    for row in data.get("feature_validations", []):
        out.append(f"- **{row['phase']} - {row['scenario']}**: {row['summary']}")
    out.extend([
        "",
        "## Regeneration",
        "",
        "```bash",
        "uv run --project refs/scripts python refs/scripts/public/generate_public_reports.py",
        "```",
        "",
        "## Sources",
        "",
    ])
    for source in data.get("sources", []):
        out.append(f"- `{source}`")
    out.append("")
    return "\n".join(out)



def render_benchmark_doc(data: dict) -> str:
    out = [
        "---",
        "title: Benchmark Results",
        "summary: Public benchmark and validation summary for Astra versus etcd and related control-plane scenarios.",
        "audience: both",
        "status: canonical",
        f"last_verified: {data.get('last_updated', '2026-03-05')}",
        "source_of_truth:",
        "  - BENCHMARKS.md",
        "  - refs/scripts/validation",
        "related_artifacts:",
        "  - /research/benchmark-methodology",
        "---",
        "",
        "# Benchmark Results",
        "",
        "## Astra vs etcd snapshots",
        "",
    ]
    for row in data.get("astra_vs_etcd", []):
        out.extend([
            f"## {row['phase']}: {row['scenario']}",
            "",
            f"- Date: `{row['date']}`",
            f"- Status: `{row['status']}`",
            f"- Workload: {row['workload']}",
            f"- Astra: {row['astra_summary']}",
            f"- etcd: {row['etcd_summary']}",
            f"- Notes: {row['notes']}",
            "",
        ])
    out.extend([
        "## Feature validations",
        "",
    ])
    for row in data.get("feature_validations", []):
        out.extend([
            f"### {row['phase']}: {row['scenario']}",
            "",
            f"- Date: `{row['date']}`",
            f"- Status: `{row['status']}`",
            f"- Summary: {row['summary']}",
            "",
        ])
    out.extend([
        "## Rebuild this page",
        "",
        "```bash",
        "uv run --project refs/scripts python refs/scripts/public/generate_public_reports.py",
        "```",
        "",
    ])
    return "\n".join(out)



def main() -> int:
    parser = argparse.ArgumentParser(description="Generate BENCHMARKS.md and public research benchmark docs.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--benchmarks", type=Path, default=DEFAULT_BENCHMARKS)
    parser.add_argument("--doc", type=Path, default=DEFAULT_DOC)
    args = parser.parse_args()

    data = load_data(args.data)
    args.benchmarks.write_text(render_benchmarks_md(data), encoding="utf-8")
    args.doc.parent.mkdir(parents=True, exist_ok=True)
    args.doc.write_text(render_benchmark_doc(data), encoding="utf-8")
    print(f"wrote {args.benchmarks}")
    print(f"wrote {args.doc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
