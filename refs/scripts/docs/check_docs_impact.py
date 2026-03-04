#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]

CODE_PREFIXES = (
    "crates/",
    "docker-compose",
    "quickstart.sh",
    "refs/scripts/validation/",
)
DOC_PREFIXES = (
    "docs/",
    "refs/docs/",
)


def get_changed_files(base_ref: str, head_ref: str) -> list[str]:
    cmd = ["git", "diff", "--name-only", f"{base_ref}...{head_ref}"]
    out = subprocess.check_output(cmd, cwd=REPO_ROOT, text=True)
    return [line.strip() for line in out.splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Check whether code changes include documentation updates.")
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--strict", action="store_true", help="Fail when code changed without docs updates.")
    args = parser.parse_args()

    changed = get_changed_files(args.base, args.head)
    code_changed = any(path.startswith(CODE_PREFIXES) for path in changed)
    docs_changed = any(path.startswith(DOC_PREFIXES) for path in changed)

    if code_changed and not docs_changed:
        msg = "docs-impact: code changed but no docs files were updated"
        if args.strict:
            print(f"ERROR: {msg}")
            return 1
        print(f"WARN: {msg}")
    else:
        print("docs-impact: ok")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
