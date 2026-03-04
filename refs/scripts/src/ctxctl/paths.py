from __future__ import annotations

from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[2]  # refs/scripts
REPO_ROOT = SCRIPT_DIR.parents[1]                 # repo root
DOCS_DIR = REPO_ROOT / "refs" / "docs"
DATA_DIR = SCRIPT_DIR / "data"

STATE_PATH = DOCS_DIR / "STATE.yaml"
KB_PATH = DOCS_DIR / "KB.md"
DB_PATH = DATA_DIR / "memory.db"


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
