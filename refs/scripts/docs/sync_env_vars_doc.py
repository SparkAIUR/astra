#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = REPO_ROOT / "crates/astra-core/src/config.rs"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "docs/reference/config/env-vars.md"


@dataclass
class EnvVar:
    name: str
    field: str
    default: str
    source: str
    line: int


def collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def split_top_level_args(raw: str) -> tuple[str, str]:
    depth = 0
    in_str = False
    escaped = False
    for i, ch in enumerate(raw):
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch in "([{":
            depth += 1
            continue
        if ch in ")]}":
            depth = max(0, depth - 1)
            continue
        if ch == "," and depth == 0:
            return raw[:i], raw[i + 1 :]
    return raw, ""


def extract_fn_calls(stmt: str, fn_name: str) -> list[str]:
    out: list[str] = []
    needle = f"{fn_name}("
    i = 0
    while True:
        start = stmt.find(needle, i)
        if start < 0:
            break
        j = start + len(needle)
        depth = 1
        in_str = False
        escaped = False
        while j < len(stmt) and depth > 0:
            ch = stmt[j]
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_str = False
                j += 1
                continue

            if ch == '"':
                in_str = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            j += 1

        if depth == 0:
            out.append(stmt[start + len(needle) : j - 1])
            i = j
        else:
            break
    return out


def infer_default_from_env_var_stmt(stmt: str) -> str:
    m = re.search(r'unwrap_or_else\(\|_\|\s*"([^"]*)"\.to_string\(\)\)', stmt)
    if m:
        return m.group(1)
    m = re.search(r'unwrap_or_else\(\|_\|\s*PathBuf::from\("([^"]*)"\)\)', stmt)
    if m:
        return m.group(1)
    return "(unset)"


def classify(name: str) -> str:
    if name.startswith("ASTRAD_NODE_") or name in {
        "ASTRAD_CLIENT_ADDR",
        "ASTRAD_RAFT_ADDR",
        "ASTRAD_RAFT_ADVERTISE_ADDR",
        "ASTRAD_PEERS",
    }:
        return "Cluster Identity and Networking"
    if name.startswith("ASTRAD_WAL_"):
        return "WAL and Write Path"
    if name.startswith("ASTRAD_PUT_") or name.startswith("ASTRAD_QOS_"):
        return "Batching, QoS, and Adaptation"
    if name.startswith("ASTRAD_LSM_"):
        return "LSM Pressure Control"
    if name.startswith("ASTRAD_BG_IO_"):
        return "Background IO and Tiering"
    if name.startswith("ASTRAD_LIST_") or name.startswith("ASTRAD_READ_"):
        return "Read and LIST Path"
    if name.startswith("ASTRAD_RAFT_"):
        return "Raft Timers and Replication"
    if name.startswith("ASTRAD_GRPC_"):
        return "gRPC Transport"
    if name.startswith("ASTRAD_AUTH_") or name.startswith("ASTRAD_TENANT_"):
        return "Auth and Tenanting"
    if name.startswith("ASTRAD_S3_"):
        return "S3 Tiering"
    if name.startswith("ASTRAD_PROFILE"):
        return "Profiles and Governor"
    if name.startswith("ASTRAD_METRICS"):
        return "Metrics"
    if name.startswith("ASTRAD_CHAOS_"):
        return "Chaos Testing"
    return "General"


def source_priority(source: str) -> int:
    order = {
        "parse_env": 0,
        "parse_csv_bytes": 1,
        "env_var": 2,
        "opt_env": 3,
    }
    return order.get(source, 99)


def normalize_default(raw: str) -> str:
    value = collapse_ws(raw)
    value = value.replace("_usize", "").replace("_u64", "").replace("_u32", "")
    value = value.replace("_i64", "").replace("_u16", "")
    value = value.replace("_", "") if re.fullmatch(r"[0-9_]+", value) else value
    return value


def parse_env_vars(config_path: Path) -> list[EnvVar]:
    text = config_path.read_text(encoding="utf-8")
    rows: dict[str, EnvVar] = {}

    for match in re.finditer(r"let\s+([a-zA-Z0-9_]+)\s*=\s*(.*?);", text, flags=re.S):
        field = match.group(1)
        stmt = match.group(0)
        line = text.count("\n", 0, match.start()) + 1

        parse_env_calls = extract_fn_calls(stmt, "parse_env")
        for call in parse_env_calls:
            left, right = split_top_level_args(call)
            m = re.search(r'"(ASTRAD_[A-Z0-9_]+)"', left)
            if not m:
                continue
            name = m.group(1)
            row = EnvVar(
                name=name,
                field=field,
                default=normalize_default(right) or "(unset)",
                source="parse_env",
                line=line,
            )
            prev = rows.get(name)
            if prev is None or source_priority(row.source) < source_priority(prev.source):
                rows[name] = row

        parse_csv_calls = extract_fn_calls(stmt, "parse_csv_bytes")
        for call in parse_csv_calls:
            left, right = split_top_level_args(call)
            m = re.search(r'"(ASTRAD_[A-Z0-9_]+)"', left)
            if not m:
                continue
            name = m.group(1)
            row = EnvVar(
                name=name,
                field=field,
                default=normalize_default(right) or "(unset)",
                source="parse_csv_bytes",
                line=line,
            )
            prev = rows.get(name)
            if prev is None or source_priority(row.source) < source_priority(prev.source):
                rows[name] = row

        for m in re.finditer(r'env::var\("(ASTRAD_[A-Z0-9_]+)"\)', stmt):
            name = m.group(1)
            row = EnvVar(
                name=name,
                field=field,
                default=infer_default_from_env_var_stmt(stmt),
                source="env_var",
                line=line,
            )
            prev = rows.get(name)
            if prev is None or source_priority(row.source) < source_priority(prev.source):
                rows[name] = row

        for m in re.finditer(r'opt_env\("(ASTRAD_[A-Z0-9_]+)"\)', stmt):
            name = m.group(1)
            row = EnvVar(
                name=name,
                field=field,
                default="(unset)",
                source="opt_env",
                line=line,
            )
            prev = rows.get(name)
            if prev is None or source_priority(row.source) < source_priority(prev.source):
                rows[name] = row

    return sorted(rows.values(), key=lambda row: (classify(row.name), row.name))


def render_markdown(rows: list[EnvVar]) -> str:
    out: list[str] = [
        "---",
        "title: Environment Variables",
        "summary: Runtime configuration surface for astrad sourced from AstraConfig::from_env.",
        "audience: operators",
        "status: canonical",
        "last_verified: 2026-03-03",
        "source_of_truth:",
        "  - crates/astra-core/src/config.rs",
        "related_artifacts:",
        "  - docker-compose.yml",
        "---",
        "",
        "# Environment Variables",
        "",
        "This page is generated from `crates/astra-core/src/config.rs`.",
        "",
        "Legend:",
        "- `Field`: target `AstraConfig` field or local setting variable.",
        "- `Default`: literal default expression used when env var is unset.",
        "",
    ]

    current_group = ""
    for row in rows:
        group = classify(row.name)
        if group != current_group:
            if current_group:
                out.append("")
            out.extend([
                f"## {group}",
                "",
                "| Variable | Field | Default | Source |",
                "| --- | --- | --- | --- |",
            ])
            current_group = group

        default = row.default.replace("|", "\\|").replace("`", "\\`")
        out.append(
            f"| `{row.name}` | `{row.field}` | `{default}` | `{row.source}` |"
        )

    out.extend(
        [
            "",
            "## Notes",
            "",
            "- Optional values (`opt_env`) are represented as `(unset)` and become `None` when not provided.",
            "- Some values are additionally clamped in code (`.max(...)`, `.min(...)`) after parsing.",
            "- `ASTRAD_WAL_BATCH_MAX_ENTRIES` is retained as a fallback alias for `ASTRAD_WAL_MAX_BATCH_REQUESTS`.",
        ]
    )

    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate docs/reference/config/env-vars.md")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    rows = parse_env_vars(args.config)
    content = render_markdown(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
