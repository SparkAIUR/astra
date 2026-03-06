#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE = SCRIPT_DIR.parents[3]
DEFAULT_DEST = DEFAULT_SOURCE.parent / "astra-public"
DEFAULT_MANIFEST = SCRIPT_DIR / "public-export-manifest.yaml"


@dataclass
class RewriteRule:
    pattern: str
    replacement: str


@dataclass
class Bucket:
    name: str
    includes: list[str]
    excludes: list[str]
    message: str


@dataclass
class ExportManifest:
    includes: list[str]
    excludes: list[str]
    rewrites: list[RewriteRule]
    private_reference_patterns: list[str]
    buckets: dict[str, Bucket]


def run_git_ls_files(repo: Path) -> list[str]:
    out = subprocess.check_output(["git", "-C", str(repo), "ls-files"], text=True)
    return [line.strip() for line in out.splitlines() if line.strip()]


def load_manifest(path: Path) -> ExportManifest:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    includes = [str(x) for x in raw.get("includes") or ["**"]]
    excludes = [str(x) for x in raw.get("excludes") or []]
    private_patterns = [str(x) for x in raw.get("private_reference_patterns") or []]

    rewrites: list[RewriteRule] = []
    for row in raw.get("text_rewrites") or []:
        rewrites.append(
            RewriteRule(
                pattern=str(row.get("pattern", "")),
                replacement=str(row.get("replacement", "")),
            )
        )

    buckets: dict[str, Bucket] = {}
    for name, cfg in (raw.get("commit_buckets") or {}).items():
        buckets[str(name)] = Bucket(
            name=str(name),
            includes=[str(x) for x in cfg.get("includes") or []],
            excludes=[str(x) for x in cfg.get("excludes") or []],
            message=str(cfg.get("message") or ""),
        )

    return ExportManifest(
        includes=includes,
        excludes=excludes,
        rewrites=rewrites,
        private_reference_patterns=private_patterns,
        buckets=buckets,
    )


def match_any(path: str, patterns: Iterable[str]) -> bool:
    for pat in patterns:
        if pat.endswith("/**"):
            # Treat "/**" as a directory subtree glob (avoid ".git/**" matching ".gitattributes")
            prefix = pat[: -len("/**")]
            if path == prefix or path.startswith(prefix + "/"):
                return True
            continue
        if fnmatch.fnmatch(path, pat):
            return True
    return False


def apply_rewrites(text: str, rewrites: list[RewriteRule]) -> str:
    out = text
    for r in rewrites:
        if r.pattern:
            out = re.sub(r.pattern, r.replacement, out)
    return out


def is_probably_text(path: Path) -> bool:
    ext = path.suffix.lower()
    return ext in {
        ".md",
        ".mdx",
        ".txt",
        ".yaml",
        ".yml",
        ".json",
        ".toml",
        ".rs",
        ".sh",
        ".py",
        ".mjs",
        ".js",
        ".ts",
        ".proto",
        ".lock",
        "",
    }


def sanitize_docs_frontmatter(dest_repo: Path, private_patterns: list[str]) -> int:
    docs_dir = dest_repo / "docs"
    if not docs_dir.exists():
        return 0

    changed = 0
    for md in sorted(list(docs_dir.rglob("*.md")) + list(docs_dir.rglob("*.mdx"))):
        if "node_modules" in md.parts:
            continue

        text = md.read_text(encoding="utf-8", errors="ignore")
        if not text.startswith("---\n"):
            continue

        parts = text.split("\n---\n", 1)
        if len(parts) < 2:
            continue

        frontmatter = yaml.safe_load(parts[0][4:]) or {}
        if not isinstance(frontmatter, dict):
            continue

        updated = False
        for key in ("source_of_truth", "related_artifacts"):
            value = frontmatter.get(key)
            if not isinstance(value, list):
                continue

            filtered: list[str] = []
            for item in value:
                if not isinstance(item, str):
                    continue
                if any(item.startswith(pat) for pat in private_patterns):
                    updated = True
                    continue
                if item.startswith(("http://", "https://", "mailto:")):
                    filtered.append(item)
                    continue

                candidate = (dest_repo / item).resolve()
                if candidate.exists():
                    filtered.append(item)
                else:
                    updated = True

            frontmatter[key] = filtered

        sot = frontmatter.get("source_of_truth")
        if not isinstance(sot, list) or not sot:
            frontmatter["source_of_truth"] = ["README.md"]
            updated = True

        if updated:
            rendered = "---\n" + yaml.safe_dump(frontmatter, sort_keys=False).rstrip() + "\n---\n" + parts[1]
            md.write_text(rendered, encoding="utf-8")
            changed += 1

    return changed


def sanitize_docs_nav(dest_repo: Path) -> bool:
    docs_json = dest_repo / "docs/docs.json"
    if docs_json.exists():
        data = json.loads(docs_json.read_text(encoding="utf-8"))
        nav = data.get("navigation")
        if not isinstance(nav, list):
            return False

        def page_exists(page: str) -> bool:
            rel = page.lstrip("/")
            for suffix in (".mdx", ".md", "/index.mdx", "/index.md"):
                if (dest_repo / "docs" / f"{rel}{suffix}").exists():
                    return True
            return False

        changed = False
        cleaned_nav = []
        for group in nav:
            if not isinstance(group, dict):
                changed = True
                continue
            pages = group.get("pages")
            if not isinstance(pages, list):
                cleaned_nav.append(group)
                continue
            filtered = [page for page in pages if isinstance(page, str) and page_exists(page)]
            if filtered != pages:
                changed = True
            if filtered:
                updated = dict(group)
                updated["pages"] = filtered
                cleaned_nav.append(updated)
            else:
                changed = True
        if changed:
            data["navigation"] = cleaned_nav
            docs_json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        return changed

    cfg = dest_repo / "docs/.vitepress/config.mjs"
    if not cfg.exists():
        return False

    text = cfg.read_text(encoding="utf-8")
    original = text

    # Remove nav link to Labs
    text = re.sub(r"\n\s*\{ text: 'Labs', link: '/labs/benchmark-methodology' \},?", "", text)

    # Remove Labs and Archive sidebar sections
    text = re.sub(
        r"\n\s*\{\n\s*text: 'Labs and Evidence',[\s\S]*?\n\s*\},",
        "",
        text,
        flags=re.M,
    )
    text = re.sub(
        r"\n\s*\{\n\s*text: 'Archive',[\s\S]*?\n\s*\},",
        "",
        text,
        flags=re.M,
    )

    if text != original:
        cfg.write_text(text, encoding="utf-8")
        return True
    return False


def sanitize_docs_source_map(dest_repo: Path, private_patterns: list[str]) -> bool:
    source_map = dest_repo / "docs/.meta/source-map.yaml"
    if not source_map.exists():
        return False

    data = yaml.safe_load(source_map.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return False

    entries = data.get("entries")
    if not isinstance(entries, list):
        return False

    changed = False
    cleaned_entries: list[dict] = []
    for entry in entries:
        if not isinstance(entry, dict):
            changed = True
            continue

        file_path = entry.get("file")
        if not isinstance(file_path, str):
            changed = True
            continue

        if not (dest_repo / file_path).exists():
            changed = True
            continue

        for key in ("source_of_truth", "related_artifacts"):
            value = entry.get(key)
            if not isinstance(value, list):
                continue

            filtered: list[str] = []
            for item in value:
                if not isinstance(item, str):
                    changed = True
                    continue
                if any((item.startswith(pat) or pat in item) for pat in private_patterns):
                    changed = True
                    continue
                if item.startswith(("http://", "https://", "mailto:")):
                    filtered.append(item)
                    continue

                candidate = (dest_repo / item).resolve()
                if candidate.exists():
                    filtered.append(item)
                else:
                    changed = True

            entry[key] = filtered

        if not entry.get("source_of_truth"):
            entry["source_of_truth"] = ["README.md"]
            changed = True

        cleaned_entries.append(entry)

    if len(cleaned_entries) != len(entries):
        changed = True
    data["entries"] = cleaned_entries

    if changed:
        source_map.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

    return changed


def verify_dest_clean(dest_repo: Path) -> None:
    git_dir = dest_repo / ".git"
    if not git_dir.exists():
        raise SystemExit(f"destination is not a git repo: {dest_repo}")


def collect_source_files(source_repo: Path, manifest: ExportManifest, bucket: Bucket | None) -> list[str]:
    tracked = run_git_ls_files(source_repo)
    includes = manifest.includes
    excludes = list(manifest.excludes)

    if bucket is not None:
        if bucket.includes:
            includes = bucket.includes
        excludes.extend(bucket.excludes)

    selected: list[str] = []
    for rel in tracked:
        if not match_any(rel, includes):
            continue
        if match_any(rel, excludes):
            continue
        selected.append(rel)

    return sorted(selected)


def copy_selected_files(
    source_repo: Path,
    dest_repo: Path,
    rel_paths: list[str],
    rewrites: list[RewriteRule],
    dry_run: bool,
) -> tuple[int, int]:
    copied = 0
    rewritten = 0

    for rel in rel_paths:
        src = source_repo / rel
        dst = dest_repo / rel

        if dry_run:
            copied += 1
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)

        if is_probably_text(src):
            try:
                text = src.read_text(encoding="utf-8")
                out = apply_rewrites(text, rewrites)
                dst.write_text(out, encoding="utf-8")
                copied += 1
                if out != text:
                    rewritten += 1
                continue
            except UnicodeDecodeError:
                pass

        shutil.copy2(src, dst)
        copied += 1

    return copied, rewritten


def remove_stale_files(dest_repo: Path, managed: set[str], dry_run: bool) -> int:
    removed = 0
    tracked_dest = run_git_ls_files(dest_repo)
    for rel in tracked_dest:
        if rel.startswith(".git/"):
            continue
        if rel not in managed:
            p = dest_repo / rel
            if dry_run:
                removed += 1
                continue
            if p.exists():
                p.unlink()
                removed += 1

    if not dry_run:
        # prune empty dirs
        for root, dirs, files in os.walk(dest_repo, topdown=False):
            if ".git" in root.split(os.sep):
                continue
            if dirs or files:
                continue
            path = Path(root)
            if path == dest_repo:
                continue
            try:
                path.rmdir()
            except OSError:
                pass

    return removed


def run_check(dest_repo: Path, manifest: ExportManifest, selected_paths: list[str]) -> list[str]:
    issues: list[str] = []
    private_ref_ignore = [".gitignore", "refs/scripts/public/**", "refs/scripts/tests/**"]
    tracked = set(run_git_ls_files(dest_repo))

    for rel in tracked:
        if match_any(rel, manifest.excludes):
            issues.append(f"excluded file tracked in destination: {rel}")

    absolute_local_patterns = [r"/" r"Volumes/", r"/" r"Users/"]
    private_patterns = manifest.private_reference_patterns

    paths_to_scan = sorted(set(selected_paths).union(tracked))
    for rel in paths_to_scan:
        p = dest_repo / rel
        if not p.exists():
            continue
        if not is_probably_text(p):
            continue
        if match_any(rel, private_ref_ignore):
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        for pat in absolute_local_patterns:
            if re.search(pat, text):
                issues.append(f"absolute local path found in {rel}: {pat}")
                break

        for private in private_patterns:
            if private and private in text:
                issues.append(f"private reference found in {rel}: {private}")
                break

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Export private Astra repo into clean public mirror repo.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    parser.add_argument("--bucket", default="", help="Optional commit bucket name from manifest")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--rewrite", action="store_true", default=False)
    parser.add_argument("--sanitize-docs", action="store_true", default=False)
    args = parser.parse_args()

    source_repo = args.source.resolve()
    dest_repo = args.dest.resolve()
    manifest = load_manifest(args.manifest.resolve())

    verify_dest_clean(dest_repo)

    bucket = None
    if args.bucket:
        bucket = manifest.buckets.get(args.bucket)
        if bucket is None:
            raise SystemExit(f"unknown bucket '{args.bucket}'. Available: {', '.join(sorted(manifest.buckets))}")

    rel_paths = collect_source_files(source_repo, manifest, bucket)
    rewrites = manifest.rewrites if args.rewrite else []
    effective_dest_repo = dest_repo
    temp_check_dir: tempfile.TemporaryDirectory[str] | None = None

    if args.check and not args.dry_run:
        temp_check_dir = tempfile.TemporaryDirectory(prefix="astra-public-export-check-")
        effective_dest_repo = Path(temp_check_dir.name)
        shutil.copytree(dest_repo, effective_dest_repo, dirs_exist_ok=True)

    copied, rewritten = copy_selected_files(
        source_repo=source_repo,
        dest_repo=effective_dest_repo,
        rel_paths=rel_paths,
        rewrites=rewrites,
        dry_run=args.dry_run,
    )

    removed = 0
    if args.mode == "full" and bucket is None:
        removed = remove_stale_files(effective_dest_repo, set(rel_paths), dry_run=args.dry_run)

    docs_sanitized = 0
    nav_sanitized = False
    source_map_sanitized = False
    if args.sanitize_docs and not args.dry_run:
        docs_sanitized = sanitize_docs_frontmatter(effective_dest_repo, manifest.private_reference_patterns)
        nav_sanitized = sanitize_docs_nav(effective_dest_repo)
        source_map_sanitized = sanitize_docs_source_map(effective_dest_repo, manifest.private_reference_patterns)

    print(f"selected_files={len(rel_paths)} copied={copied} rewritten={rewritten} removed={removed}")
    if args.sanitize_docs:
        print(
            "docs_frontmatter_sanitized="
            f"{docs_sanitized} docs_nav_sanitized={int(nav_sanitized)} "
            f"docs_source_map_sanitized={int(source_map_sanitized)}"
        )

    if args.check:
        issues = run_check(effective_dest_repo, manifest, rel_paths)
        if issues:
            for issue in issues:
                print(f"ERROR: {issue}")
            if temp_check_dir is not None:
                temp_check_dir.cleanup()
            return 1
        print("check=ok")

    if temp_check_dir is not None:
        temp_check_dir.cleanup()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
