#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
ISO_TIMESTAMP_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b")
SPACE_RE = re.compile(r"\s+")
LEVEL_RE = re.compile(r"\b(TRACE|DEBUG|INFO|WARN|ERROR)\b")

LEVEL_ORDER = {
    "TRACE": 10,
    "DEBUG": 20,
    "INFO": 30,
    "WARN": 40,
    "ERROR": 50,
}


def _default_output_path(input_path: Path) -> Path:
    suffix = input_path.suffix
    if suffix:
        return input_path.with_name(f"{input_path.stem}-cleaned{suffix}")
    return input_path.with_name(f"{input_path.name}-cleaned")


def _detect_level(line: str) -> int | None:
    match = LEVEL_RE.search(line)
    if match is None:
        return None
    return LEVEL_ORDER[match.group(1)]


def _normalize_line(raw: str, strip_timestamps: bool) -> str:
    cleaned = ANSI_ESCAPE_RE.sub("", raw.rstrip("\n"))
    if strip_timestamps:
        cleaned = ISO_TIMESTAMP_RE.sub("", cleaned)
    cleaned = SPACE_RE.sub(" ", cleaned).strip()
    return cleaned


def _compact_repeated(lines: list[str]) -> list[str]:
    if not lines:
        return []

    compacted: list[str] = []
    current = lines[0]
    count = 1

    for line in lines[1:]:
        if line == current:
            count += 1
            continue

        compacted.append(current)
        if count > 1:
            compacted.append(f"... repeated {count - 1} more times")
        current = line
        count = 1

    compacted.append(current)
    if count > 1:
        compacted.append(f"... repeated {count - 1} more times")

    return compacted


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compact_log.py",
        description=(
            "Normalize noisy logs, drop low-priority lines, and compact repeated "
            "sequential lines for token-efficient review."
        ),
    )
    parser.add_argument("input", type=Path, help="Input log file path")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output path (default: <input>-cleaned.<ext>)",
    )
    parser.add_argument(
        "--min-level",
        choices=["NONE", "TRACE", "DEBUG", "INFO", "WARN", "ERROR"],
        default="INFO",
        help="Drop lines below this level (default: INFO)",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Regex pattern to force-keep matching lines even below --min-level",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Regex pattern to always drop matching lines",
    )
    parser.add_argument(
        "--only-include",
        action="store_true",
        help="Keep only lines that match at least one --include pattern",
    )
    parser.add_argument(
        "--keep-timestamps",
        action="store_true",
        help="Keep ISO timestamps in output (timestamps are stripped by default)",
    )
    parser.add_argument(
        "--no-repeat-compaction",
        action="store_true",
        help="Disable repeated-line compaction output markers",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    input_path: Path = args.input
    if not input_path.exists() or not input_path.is_file():
        raise SystemExit(f"input file does not exist: {input_path}")

    output_path: Path = args.output or _default_output_path(input_path)
    include_patterns = [re.compile(p) for p in args.include]
    exclude_patterns = [re.compile(p) for p in args.exclude]
    min_level = None if args.min_level == "NONE" else LEVEL_ORDER[args.min_level]
    strip_timestamps = not args.keep_timestamps

    filtered: list[str] = []

    for raw in input_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = _normalize_line(raw, strip_timestamps=strip_timestamps)
        if not line:
            continue

        if any(pattern.search(line) for pattern in exclude_patterns):
            continue

        include_hit = any(pattern.search(line) for pattern in include_patterns)
        if args.only_include:
            if not include_hit:
                continue
        else:
            level = _detect_level(line)
            if min_level is not None and level is not None and level < min_level and not include_hit:
                continue

        filtered.append(line)

    output_lines = filtered if args.no_repeat_compaction else _compact_repeated(filtered)
    output_path.write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8")

    print(
        f"wrote {output_path} "
        f"(input={input_path}, kept={len(filtered)}, output_lines={len(output_lines)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
