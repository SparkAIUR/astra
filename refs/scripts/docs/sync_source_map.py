#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
DEFAULT_OUTPUT = DEFAULT_DOCS_DIR / ".meta/source-map.yaml"


def parse_frontmatter(markdown_path: Path) -> dict:
    text = markdown_path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}
    parts = text.split("\n---\n", 1)
    if len(parts) < 2:
        return {}
    data = yaml.safe_load(parts[0][4:])
    return data if isinstance(data, dict) else {}


def page_from_path(path: Path, docs_dir: Path) -> str:
    rel = path.relative_to(docs_dir).as_posix()
    if rel.endswith("/index.md"):
        rel = rel[: -len("index.md")]
    elif rel.endswith(".md"):
        rel = rel[: -len(".md")]
    if not rel.startswith("/"):
        rel = "/" + rel
    return rel.rstrip("/") or "/"


def build_source_map(docs_dir: Path) -> dict:
    entries = []
    for md in sorted(docs_dir.rglob("*.md")):
        if any(part.startswith(".") for part in md.relative_to(docs_dir).parts):
            continue

        frontmatter = parse_frontmatter(md)
        source_of_truth = frontmatter.get("source_of_truth") or []
        related = frontmatter.get("related_artifacts") or []

        entry = {
            "page": page_from_path(md, docs_dir),
            "file": md.relative_to(REPO_ROOT).as_posix(),
            "title": frontmatter.get("title", ""),
            "status": frontmatter.get("status", ""),
            "last_verified": frontmatter.get("last_verified", ""),
            "source_of_truth": source_of_truth,
            "related_artifacts": related,
        }
        entries.append(entry)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "docs_root": docs_dir.relative_to(REPO_ROOT).as_posix(),
        "entries": entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate docs/.meta/source-map.yaml")
    parser.add_argument("--docs-dir", type=Path, default=DEFAULT_DOCS_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    data = build_source_map(args.docs_dir)
    rendered = yaml.safe_dump(data, sort_keys=False)

    if args.check:
        if not args.output.exists() or args.output.read_text(encoding="utf-8") != rendered:
            print(f"source map is out of date: {args.output}")
            return 1
        print(f"source map ok: {args.output}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
