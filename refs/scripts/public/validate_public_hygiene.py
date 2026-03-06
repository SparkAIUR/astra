#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
from pathlib import Path


TEXT_EXTS = {
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

BANNED_LOCAL_PATTERNS = [
    ("absolute-local-volume", re.compile(r"/" r"Volumes/")),
    ("absolute-local-user", re.compile(r"/" r"Users/")),
    ("file-local-uri", re.compile(r"file:///" r"(Users|Volumes)/")),
]

BANNED_PRIVATE_REFERENCES = [
    "refs/base.md",
    "refs/build-spec-phase",
    "refs/docs/phase",
    "refs/docs/KB.md",
    "refs/docs/RULES.md",
    "refs/docs/STATE.yaml",
    "notes.md",
    "task_plan.md",
]

CONTENT_CHECK_IGNORE_GLOBS = {
    "AGENTS.md",
    ".gitignore",
    "refs/scripts/public/**",
    "refs/scripts/tests/**",
}


def list_tracked_files(repo: Path) -> list[str]:
    out = subprocess.check_output(["git", "-C", str(repo), "ls-files"], text=True)
    return [line.strip() for line in out.splitlines() if line.strip()]


def is_text(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTS


def match_any(path: str, patterns: set[str]) -> bool:
    return any(fnmatch.fnmatch(path, pat) for pat in patterns)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate public repo path hygiene and private-reference leakage.")
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--allow-private-ref", action="append", default=[])
    args = parser.parse_args()

    repo = args.repo.resolve()
    tracked = list_tracked_files(repo)

    allow = set(args.allow_private_ref)
    issues: list[str] = []

    for rel in tracked:
        p = repo / rel
        if not p.exists() or not is_text(p):
            continue

        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        if match_any(rel, CONTENT_CHECK_IGNORE_GLOBS):
            continue

        for label, rx in BANNED_LOCAL_PATTERNS:
            if rx.search(text):
                issues.append(f"{rel}: {label}")
                break

        for needle in BANNED_PRIVATE_REFERENCES:
            if needle in allow:
                continue
            if needle in text:
                issues.append(f"{rel}: private-reference({needle})")
                break

    if issues:
        for issue in issues:
            print(f"ERROR: {issue}")
        return 1

    print("public hygiene check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
