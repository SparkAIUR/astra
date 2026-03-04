from __future__ import annotations

import json
from pathlib import Path

from ctxctl.db import MemoryRecord, add_memory, connect, init_db, query_memories
from ctxctl.state_ops import update_state


def test_memory_insert_and_query(tmp_path: Path):
    db_path = tmp_path / "test.db"
    with connect(db_path) as conn:
        init_db(conn)
        add_memory(
            conn,
            MemoryRecord(
                kind="decision",
                title="Use unified wal",
                content="Raft log is the sole WAL",
                source="refs/docs/RULES.md",
                tags="raft,wal",
            ),
        )
        rows = query_memories(conn, "unified", limit=10)
        assert len(rows) == 1
        assert rows[0]["title"] == "Use unified wal"


def test_state_update_appends_history(tmp_path: Path):
    state_path = tmp_path / "STATE.yaml"
    with connect(tmp_path / "state.db") as conn:
        init_db(conn)
        state = update_state(
            path=state_path,
            summary="updated status",
            set_items=["phase=bootstrap", "status=in_progress"],
            updates=["created bootstrap files"],
            conn=conn,
        )
    assert state["current"]["phase"] == "bootstrap"
    assert state["current"]["status"] == "in_progress"
    assert len(state["history"]) == 1


def test_token_budget_style_payload(tmp_path: Path):
    db_path = tmp_path / "mem.db"
    with connect(db_path) as conn:
        init_db(conn)
        for i in range(5):
            add_memory(
                conn,
                MemoryRecord(
                    kind="finding",
                    title=f"item-{i}",
                    content="alpha beta gamma delta " * 20,
                    source="test",
                    tags="x",
                ),
            )
        rows = query_memories(conn, "", limit=5)

    payload = {
        "records": [
            {
                "id": row["id"],
                "title": row["title"],
                "summary": row["content"][:40],
            }
            for row in rows
        ]
    }
    text = json.dumps(payload)
    assert "records" in text


def test_default_paths_anchor_to_repo_refs_docs():
    from ctxctl.paths import DOCS_DIR, KB_PATH, STATE_PATH

    assert str(DOCS_DIR).endswith("/refs/docs")
    assert str(KB_PATH).endswith("/refs/docs/KB.md")
    assert str(STATE_PATH).endswith("/refs/docs/STATE.yaml")
