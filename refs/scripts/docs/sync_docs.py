#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def run_step(script_name: str, check: bool = True) -> int:
    cmd = [sys.executable, str(SCRIPT_DIR / script_name)]
    if check:
        return subprocess.run(cmd, check=False).returncode
    return subprocess.run(cmd, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync generated documentation artifacts.")
    parser.add_argument("--check", action="store_true", help="Only verify generated artifacts are up to date.")
    args = parser.parse_args()

    steps = [
        "sync_env_vars_doc.py",
        "sync_metric_catalog.py",
        "sync_source_map.py",
    ]

    for step in steps:
        cmd = [sys.executable, str(SCRIPT_DIR / step)]
        if args.check and step == "sync_source_map.py":
            cmd.append("--check")
        rc = subprocess.run(cmd, check=False).returncode
        if rc != 0:
            return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
