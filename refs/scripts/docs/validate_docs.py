#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DOCS_DIR = REPO_ROOT / "docs"
DEFAULT_SOURCE_MAP = DEFAULT_DOCS_DIR / ".meta/source-map.yaml"

REQUIRED_KEYS = {
    "title",
    "summary",
    "audience",
    "status",
    "last_verified",
    "source_of_truth",
}

ALLOWED_AUDIENCE = {"operators", "contributors", "both"}
ALLOWED_STATUS = {"canonical", "snapshot", "draft", "deprecated", "archived"}


def parse_frontmatter(path: Path) -> tuple[dict, str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}, text

    parts = text.split("\n---\n", 1)
    if len(parts) < 2:
        return {}, text

    data = yaml.safe_load(parts[0][4:])
    return (data if isinstance(data, dict) else {}), parts[1]


def is_external_target(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "#"))


def resolve_markdown_link(link: str, base_file: Path, docs_dir: Path) -> Path | None:
    if is_external_target(link):
        return None

    rel = link.split("#", 1)[0].split("?", 1)[0].strip()
    if not rel:
        return None

    if rel.startswith("/"):
        candidate = docs_dir / rel.lstrip("/")
    else:
        candidate = (base_file.parent / rel).resolve()

    if candidate.suffix:
        return candidate

    md_candidate = candidate.with_suffix(".md")
    if md_candidate.exists():
        return md_candidate
    index_candidate = candidate / "index.md"
    if index_candidate.exists():
        return index_candidate
    return md_candidate


def validate_source_path(value: str) -> bool:
    if is_external_target(value):
        return True
    path = (REPO_ROOT / value).resolve()
    return path.exists()


def load_source_map(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = data.get("entries") or []
    files: set[str] = set()
    for entry in entries:
        if isinstance(entry, dict) and isinstance(entry.get("file"), str):
            files.add(entry["file"])
    return files


def validate_docs(docs_dir: Path, source_map: Path) -> list[str]:
    errors: list[str] = []
    source_map_files = load_source_map(source_map)

    for md in sorted(docs_dir.rglob("*.md")):
        rel = md.relative_to(REPO_ROOT).as_posix()
        parts = md.relative_to(docs_dir).parts
        if any(part.startswith(".") for part in parts):
            continue
        if parts and parts[0] in {"node_modules"}:
            continue
        if parts and parts[0] in {"dist"}:
            continue

        frontmatter, body = parse_frontmatter(md)
        if not frontmatter:
            errors.append(f"{rel}: missing frontmatter block")
            continue

        missing = REQUIRED_KEYS - set(frontmatter)
        if missing:
            errors.append(f"{rel}: missing required frontmatter keys: {sorted(missing)}")

        audience = str(frontmatter.get("audience", ""))
        if audience and audience not in ALLOWED_AUDIENCE:
            errors.append(f"{rel}: invalid audience '{audience}'")

        status = str(frontmatter.get("status", ""))
        if status and status not in ALLOWED_STATUS:
            errors.append(f"{rel}: invalid status '{status}'")

        source_of_truth = frontmatter.get("source_of_truth")
        if not isinstance(source_of_truth, list) or not source_of_truth:
            errors.append(f"{rel}: source_of_truth must be a non-empty list")
        else:
            for source in source_of_truth:
                if not isinstance(source, str) or not validate_source_path(source):
                    errors.append(f"{rel}: source_of_truth target does not exist: {source}")

        related = frontmatter.get("related_artifacts")
        if related is not None:
            if not isinstance(related, list):
                errors.append(f"{rel}: related_artifacts must be a list")
            else:
                for target in related:
                    if isinstance(target, str) and not validate_source_path(target):
                        errors.append(f"{rel}: related_artifacts target does not exist: {target}")

        if source_map_files and rel not in source_map_files:
            errors.append(f"{rel}: missing from {source_map.relative_to(REPO_ROOT)}")

        for match in re.finditer(r"\[[^\]]+\]\(([^)]+)\)", body):
            link = match.group(1).strip()
            resolved = resolve_markdown_link(link, md, docs_dir)
            if resolved is None:
                continue
            if not resolved.exists():
                errors.append(f"{rel}: dead markdown link '{link}'")

    if source_map.exists() and not source_map_files:
        errors.append(f"{source_map.relative_to(REPO_ROOT)}: entries list is empty")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate docs frontmatter, source links, and source map.")
    parser.add_argument("--docs-dir", type=Path, default=DEFAULT_DOCS_DIR)
    parser.add_argument("--source-map", type=Path, default=DEFAULT_SOURCE_MAP)
    args = parser.parse_args()

    errors = validate_docs(args.docs_dir, args.source_map)
    if errors:
        for err in errors:
            print(f"ERROR: {err}")
        return 1

    print("docs validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
