from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from .db import MemoryRecord, add_kb, add_memory, connect, init_db, query_kb, query_memories
from .kb_ops import append_kb_markdown
from .paths import DB_PATH, KB_PATH, REPO_ROOT, STATE_PATH, ensure_dirs
from .state_ops import load_state, update_state


def _token_estimate(text: str) -> int:
    words = len(text.split())
    return max(1, int(words * 1.3))


def cmd_init(_args: argparse.Namespace) -> int:
    ensure_dirs()
    with connect(DB_PATH) as conn:
        init_db(conn)
    print(f"initialized sqlite at {DB_PATH}")
    return 0


def cmd_state_show(args: argparse.Namespace) -> int:
    state = load_state(Path(args.path))
    if args.format == "json":
        print(json.dumps(state, indent=2))
    else:
        print(yaml.safe_dump(state, sort_keys=False).rstrip())
    return 0


def cmd_state_update(args: argparse.Namespace) -> int:
    ensure_dirs()
    with connect(DB_PATH) as conn:
        init_db(conn)
        state = update_state(
            path=Path(args.path),
            summary=args.summary,
            set_items=args.set_items or [],
            updates=args.updates or [],
            conn=conn,
        )
    print(f"state updated: {state['current'].get('updated_at')}")
    return 0


def cmd_kb_add(args: argparse.Namespace) -> int:
    ensure_dirs()
    with connect(DB_PATH) as conn:
        init_db(conn)
        row_id = add_kb(
            conn,
            entry_type=args.entry_type,
            title=args.title,
            content=args.content,
            evidence=args.evidence or "",
            impact=args.impact or "",
            follow_up=args.follow_up or "",
        )
        add_memory(
            conn,
            MemoryRecord(
                kind=f"kb:{args.entry_type}",
                title=args.title,
                content=args.content,
                source=f"{KB_PATH}",
                tags="kb,knowledge",
            ),
        )

    append_kb_markdown(
        KB_PATH,
        entry_type=args.entry_type,
        title=args.title,
        content=args.content,
        evidence=args.evidence or "",
        impact=args.impact or "",
        follow_up=args.follow_up or "",
    )
    print(f"kb entry added: {row_id}")
    return 0


def cmd_kb_search(args: argparse.Namespace) -> int:
    with connect(DB_PATH) as conn:
        init_db(conn)
        rows = query_kb(conn, args.query or "", limit=args.limit)
    for row in rows:
        print(f"[{row['id']}] {row['entry_type']} | {row['title']} | {row['created_at']}")
    return 0


def cmd_mem_add(args: argparse.Namespace) -> int:
    ensure_dirs()
    with connect(DB_PATH) as conn:
        init_db(conn)
        row_id = add_memory(
            conn,
            MemoryRecord(
                kind=args.kind,
                title=args.title,
                content=args.content,
                source=args.source or "",
                tags=",".join(args.tags or []),
            ),
        )
    print(f"memory entry added: {row_id}")
    return 0


def cmd_mem_query(args: argparse.Namespace) -> int:
    with connect(DB_PATH) as conn:
        init_db(conn)
        rows = query_memories(conn, args.query or "", limit=args.limit)
    for row in rows:
        print(f"[{row['id']}] {row['kind']} | {row['title']} | {row['created_at']}")
    return 0


def cmd_mem_compact(args: argparse.Namespace) -> int:
    with connect(DB_PATH) as conn:
        init_db(conn)
        rows = query_memories(conn, args.query or "", limit=args.limit)

    budget = args.token_budget
    used = 0
    emitted = []

    for row in rows:
        packet = {
            "id": row["id"],
            "kind": row["kind"],
            "title": row["title"],
            "summary": row["content"][:400],
            "source": row["source"],
            "created_at": row["created_at"],
        }
        serialized = json.dumps(packet)
        cost = _token_estimate(serialized)
        if used + cost > budget:
            continue
        emitted.append(packet)
        used += cost

    output = {
        "token_budget": budget,
        "token_estimate": used,
        "count": len(emitted),
        "records": emitted,
    }
    print(json.dumps(output, indent=2))
    return 0


def cmd_sync_markdown_to_db(_args: argparse.Namespace) -> int:
    ensure_dirs()
    with connect(DB_PATH) as conn:
        init_db(conn)

        state_text = STATE_PATH.read_text() if STATE_PATH.exists() else ""
        kb_text = KB_PATH.read_text() if KB_PATH.exists() else ""

        add_memory(
            conn,
            MemoryRecord(
                kind="state_snapshot",
                title="STATE.yaml snapshot",
                content=state_text,
                source=str(STATE_PATH),
                tags="state,yaml",
            ),
        )
        add_memory(
            conn,
            MemoryRecord(
                kind="kb_snapshot",
                title="KB.md snapshot",
                content=kb_text,
                source=str(KB_PATH),
                tags="kb,markdown",
            ),
        )

    print("synced markdown snapshots into sqlite")
    return 0


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    if not text.startswith("---\n"):
        return {}, text
    parts = text.split("\n---\n", 1)
    if len(parts) < 2:
        return {}, text
    frontmatter = yaml.safe_load(parts[0][4:]) or {}
    return (frontmatter if isinstance(frontmatter, dict) else {}), parts[1]


def cmd_docs_sync(args: argparse.Namespace) -> int:
    ensure_dirs()
    docs_dir = Path(args.docs_dir)
    if not docs_dir.exists():
        raise SystemExit(f"docs dir does not exist: {docs_dir}")

    indexed = 0
    with connect(DB_PATH) as conn:
        init_db(conn)
        if args.replace:
            conn.execute("DELETE FROM memories WHERE kind = 'docs_page'")

        for md in sorted(docs_dir.rglob("*.md")):
            parts = md.relative_to(docs_dir).parts
            if any(part.startswith(".") for part in parts):
                continue
            if parts and parts[0] in {"node_modules", "dist"}:
                continue

            text = md.read_text(encoding="utf-8")
            frontmatter, body = _parse_frontmatter(text)
            title = str(frontmatter.get("title") or md.relative_to(docs_dir).as_posix())
            summary = str(frontmatter.get("summary") or "").strip()
            status = str(frontmatter.get("status") or "unknown")
            sources = frontmatter.get("source_of_truth") or []
            body_snippet = " ".join(body.strip().split())[: args.max_chars]

            content = "\n".join(
                [
                    f"summary: {summary}",
                    f"status: {status}",
                    f"source_of_truth: {', '.join(str(s) for s in sources)}",
                    f"snippet: {body_snippet}",
                ]
            )

            add_memory(
                conn,
                MemoryRecord(
                    kind="docs_page",
                    title=title,
                    content=content,
                    source=str(md.resolve().relative_to(REPO_ROOT.resolve())),
                    tags=f"docs,status:{status}",
                ),
            )
            indexed += 1

    print(f"indexed docs pages: {indexed}")
    return 0


def cmd_context_pack(args: argparse.Namespace) -> int:
    state = load_state(Path(args.state_path))
    current = state.get("current", {})
    history = state.get("history", [])

    with connect(DB_PATH) as conn:
        init_db(conn)
        kb_rows = query_kb(conn, args.query or "", limit=args.kb_limit)
        mem_rows = query_memories(conn, args.query or "", limit=args.mem_limit)

    packet: dict[str, object] = {
        "token_budget": args.token_budget,
        "query": args.query or "",
        "state": {
            "project": current.get("project"),
            "phase": current.get("phase"),
            "status": current.get("status"),
            "focus": current.get("focus"),
            "blockers": current.get("blockers"),
            "updated_at": current.get("updated_at"),
            "next_actions": (current.get("next_actions") or [])[: args.next_actions_limit],
        },
        "recent_history": history[-args.history_limit :],
        "knowledge": [],
        "memories": [],
    }

    used = _token_estimate(json.dumps(packet))

    for row in kb_rows:
        item = {
            "id": row["id"],
            "type": row["entry_type"],
            "title": row["title"],
            "content": row["content"][: args.max_entry_chars],
            "evidence": row["evidence"][: args.max_entry_chars],
            "impact": row["impact"][: args.max_entry_chars],
            "created_at": row["created_at"],
        }
        cost = _token_estimate(json.dumps(item))
        if used + cost > args.token_budget:
            continue
        packet["knowledge"].append(item)  # type: ignore[attr-defined]
        used += cost

    for row in mem_rows:
        item = {
            "id": row["id"],
            "kind": row["kind"],
            "title": row["title"],
            "content": row["content"][: args.max_entry_chars],
            "source": row["source"],
            "created_at": row["created_at"],
        }
        cost = _token_estimate(json.dumps(item))
        if used + cost > args.token_budget:
            continue
        packet["memories"].append(item)  # type: ignore[attr-defined]
        used += cost

    packet["token_estimate"] = used
    print(json.dumps(packet, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ctxctl", description="Astra context/state helper CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Initialize sqlite persistence")
    p_init.set_defaults(func=cmd_init)

    p_state = sub.add_parser("state", help="Manage STATE.yaml")
    state_sub = p_state.add_subparsers(dest="state_cmd", required=True)

    p_state_show = state_sub.add_parser("show", help="Show current state")
    p_state_show.add_argument("--path", default=str(STATE_PATH))
    p_state_show.add_argument("--format", choices=["yaml", "json"], default="yaml")
    p_state_show.set_defaults(func=cmd_state_show)

    p_state_update = state_sub.add_parser("update", help="Update current state and append history")
    p_state_update.add_argument("--path", default=str(STATE_PATH))
    p_state_update.add_argument("--summary", required=True)
    p_state_update.add_argument("--set", dest="set_items", action="append", default=[])
    p_state_update.add_argument("--update", dest="updates", action="append", default=[])
    p_state_update.set_defaults(func=cmd_state_update)

    p_kb = sub.add_parser("kb", help="Manage KB")
    kb_sub = p_kb.add_subparsers(dest="kb_cmd", required=True)

    p_kb_add = kb_sub.add_parser("add", help="Add KB entry and persist it")
    p_kb_add.add_argument("--type", dest="entry_type", required=True)
    p_kb_add.add_argument("--title", required=True)
    p_kb_add.add_argument("--content", required=True)
    p_kb_add.add_argument("--evidence", default="")
    p_kb_add.add_argument("--impact", default="")
    p_kb_add.add_argument("--follow-up", default="")
    p_kb_add.set_defaults(func=cmd_kb_add)

    p_kb_search = kb_sub.add_parser("search", help="Search KB entries in sqlite")
    p_kb_search.add_argument("--query", default="")
    p_kb_search.add_argument("--limit", type=int, default=10)
    p_kb_search.set_defaults(func=cmd_kb_search)

    p_mem = sub.add_parser("mem", help="Manage memory records")
    mem_sub = p_mem.add_subparsers(dest="mem_cmd", required=True)

    p_mem_add = mem_sub.add_parser("add", help="Add memory record")
    p_mem_add.add_argument("--kind", required=True)
    p_mem_add.add_argument("--title", required=True)
    p_mem_add.add_argument("--content", required=True)
    p_mem_add.add_argument("--source", default="")
    p_mem_add.add_argument("--tag", dest="tags", action="append", default=[])
    p_mem_add.set_defaults(func=cmd_mem_add)

    p_mem_query = mem_sub.add_parser("query", help="Query memory records")
    p_mem_query.add_argument("--query", default="")
    p_mem_query.add_argument("--limit", type=int, default=10)
    p_mem_query.set_defaults(func=cmd_mem_query)

    p_mem_compact = mem_sub.add_parser("compact-context", help="Emit token-budgeted compact context JSON")
    p_mem_compact.add_argument("--query", default="")
    p_mem_compact.add_argument("--limit", type=int, default=20)
    p_mem_compact.add_argument("--token-budget", type=int, default=1200)
    p_mem_compact.set_defaults(func=cmd_mem_compact)

    p_sync = sub.add_parser("sync", help="Sync markdown and sqlite state")
    sync_sub = p_sync.add_subparsers(dest="sync_cmd", required=True)
    p_sync_m2d = sync_sub.add_parser("markdown-to-db", help="Store markdown snapshots in sqlite")
    p_sync_m2d.set_defaults(func=cmd_sync_markdown_to_db)

    p_docs = sub.add_parser("docs", help="Docs-specific persistence helpers")
    docs_sub = p_docs.add_subparsers(dest="docs_cmd", required=True)

    p_docs_sync = docs_sub.add_parser("sync", help="Index docs markdown into sqlite memory")
    p_docs_sync.add_argument("--docs-dir", default="docs")
    p_docs_sync.add_argument("--max-chars", type=int, default=600)
    p_docs_sync.add_argument("--replace", action="store_true")
    p_docs_sync.set_defaults(func=cmd_docs_sync)

    p_context = sub.add_parser("context", help="Build compact context packets")
    context_sub = p_context.add_subparsers(dest="context_cmd", required=True)
    p_context_pack = context_sub.add_parser("pack", help="Emit token-budgeted packet from state+KB+memory")
    p_context_pack.add_argument("--query", default="")
    p_context_pack.add_argument("--state-path", default=str(STATE_PATH))
    p_context_pack.add_argument("--token-budget", type=int, default=1800)
    p_context_pack.add_argument("--kb-limit", type=int, default=12)
    p_context_pack.add_argument("--mem-limit", type=int, default=20)
    p_context_pack.add_argument("--history-limit", type=int, default=6)
    p_context_pack.add_argument("--next-actions-limit", type=int, default=8)
    p_context_pack.add_argument("--max-entry-chars", type=int, default=320)
    p_context_pack.set_defaults(func=cmd_context_pack)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    code = args.func(args)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
