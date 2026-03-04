#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REFERENCE_TOKEN_RE = re.compile(
    r"`([^`\n]+)`|((?:\./)?(?:[A-Za-z0-9._-]+/)+[A-Za-z0-9._-]+(?:\.[A-Za-z0-9._-]+)?)"
)
RAW_LOG_SUFFIX = "-logs.txt"


@dataclass(frozen=True)
class ReferencedFile:
    display_path: str
    absolute_path: Path


def _display_path(path: Path, repo_root: Path) -> str:
    if path.is_relative_to(repo_root):
        return str(path.relative_to(repo_root))
    return str(path)


def _default_output_path(report_path: Path) -> Path:
    suffix = report_path.suffix or ".md"
    stem = report_path.stem
    return report_path.with_name(f"{stem}-msg{suffix}")


def _normalize_token(token: str) -> str:
    cleaned = token.strip().strip("`").strip("'").strip('"')
    while cleaned and cleaned[-1] in ".,;:)]":
        cleaned = cleaned[:-1]
    while cleaned and cleaned[0] in "([":  # keep leading slash for absolute paths
        cleaned = cleaned[1:]
    return cleaned


def _resolve_candidate(token: str, repo_root: Path) -> ReferencedFile | None:
    normalized = _normalize_token(token)
    if not normalized:
        return None
    if "://" in normalized:
        return None
    if "*" in normalized or "?" in normalized:
        return None
    if "/" not in normalized and "\\" not in normalized:
        return None

    candidate = Path(normalized)
    resolved = candidate if candidate.is_absolute() else (repo_root / candidate)
    resolved = resolved.resolve()

    if not resolved.exists() or not resolved.is_file():
        return None

    return ReferencedFile(display_path=_display_path(resolved, repo_root), absolute_path=resolved)


def _cleaned_log_path(path: Path) -> Path | None:
    if not path.name.endswith(RAW_LOG_SUFFIX):
        return None
    return path.with_name(f"{path.stem}-cleaned{path.suffix}")


def extract_referenced_files(report_text: str, repo_root: Path) -> list[ReferencedFile]:
    seen: set[Path] = set()
    ordered: list[ReferencedFile] = []

    for match in REFERENCE_TOKEN_RE.finditer(report_text):
        token = match.group(1) or match.group(2)
        if token is None:
            continue

        resolved = _resolve_candidate(token, repo_root=repo_root)
        if resolved is None:
            continue
        if resolved.absolute_path in seen:
            continue

        seen.add(resolved.absolute_path)
        ordered.append(resolved)

    return ordered


def _clean_referenced_logs(refs: list[ReferencedFile]) -> int:
    compact_script = Path(__file__).with_name("compact_log.py")
    cleaned_count = 0
    seen: set[Path] = set()

    for ref in refs:
        cleaned_path = _cleaned_log_path(ref.absolute_path)
        if cleaned_path is None:
            continue
        if ref.absolute_path in seen:
            continue
        seen.add(ref.absolute_path)
        if cleaned_path.exists():
            continue

        subprocess.run(
            [
                sys.executable,
                str(compact_script),
                str(ref.absolute_path),
                "--output",
                str(cleaned_path),
            ],
            check=True,
        )
        cleaned_count += 1

    return cleaned_count


def _prefer_cleaned_log_refs(refs: list[ReferencedFile], repo_root: Path) -> list[ReferencedFile]:
    out: list[ReferencedFile] = []
    seen: set[Path] = set()

    for ref in refs:
        preferred = ref
        cleaned_path = _cleaned_log_path(ref.absolute_path)
        if cleaned_path is not None and cleaned_path.exists():
            preferred = ReferencedFile(
                display_path=_display_path(cleaned_path, repo_root),
                absolute_path=cleaned_path,
            )

        if preferred.absolute_path in seen:
            continue
        seen.add(preferred.absolute_path)
        out.append(preferred)

    return out


def render_message_doc(report_text: str, refs: list[ReferencedFile]) -> str:
    parts = ["```markdown", report_text.rstrip("\n"), "```"]
    for ref in refs:
        ref_content = ref.absolute_path.read_text(encoding="utf-8", errors="replace").rstrip("\n")
        parts.extend(["---", f"`{ref.display_path}`", "```", ref_content, "```"])
    return "\n".join(parts).rstrip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="build_validation_report_msg.py",
        description=(
            "Create a token-friendly message markdown file by embedding a validation "
            "report and all referenced files."
        ),
    )
    parser.add_argument("report", type=Path, help="Path to validation report markdown")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output markdown path (default: <report>-msg.md)",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for resolving relative references (default: cwd)",
    )
    parser.add_argument(
        "--clean-referenced-logs",
        action="store_true",
        help=(
            "For each referenced '*-logs.txt', generate a '*-logs-cleaned.txt' file "
            "before rendering the message doc."
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report_path: Path = args.report
    repo_root: Path = args.repo_root.resolve()
    output_path: Path = args.output or _default_output_path(report_path)

    if not report_path.exists() or not report_path.is_file():
        raise SystemExit(f"report file does not exist: {report_path}")

    report_text = report_path.read_text(encoding="utf-8", errors="replace")
    refs = extract_referenced_files(report_text, repo_root=repo_root)
    cleaned_count = 0
    if args.clean_referenced_logs:
        cleaned_count = _clean_referenced_logs(refs)
    refs = _prefer_cleaned_log_refs(refs, repo_root=repo_root)
    rendered = render_message_doc(report_text, refs)
    output_path.write_text(rendered, encoding="utf-8")

    print(
        f"wrote {output_path} "
        f"(report={report_path}, referenced_files={len(refs)}, cleaned_logs={cleaned_count})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
