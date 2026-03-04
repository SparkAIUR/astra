from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


@dataclass
class MemoryRecord:
    kind: str
    title: str
    content: str
    source: str
    tags: str


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS memories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
    title,
    content,
    tags,
    content='memories',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
    INSERT INTO memories_fts(rowid, title, content, tags)
    VALUES (new.id, new.title, new.content, new.tags);
END;

CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
    INSERT INTO memories_fts(memories_fts, rowid, title, content, tags)
    VALUES('delete', old.id, old.title, old.content, old.tags);
END;

CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
    INSERT INTO memories_fts(memories_fts, rowid, title, content, tags)
    VALUES('delete', old.id, old.title, old.content, old.tags);
    INSERT INTO memories_fts(rowid, title, content, tags)
    VALUES (new.id, new.title, new.content, new.tags);
END;

CREATE TABLE IF NOT EXISTS kb_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_type TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    evidence TEXT NOT NULL DEFAULT '',
    impact TEXT NOT NULL DEFAULT '',
    follow_up TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS kb_fts USING fts5(
    title,
    content,
    evidence,
    impact,
    follow_up,
    content='kb_entries',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS kb_ai AFTER INSERT ON kb_entries BEGIN
    INSERT INTO kb_fts(rowid, title, content, evidence, impact, follow_up)
    VALUES (new.id, new.title, new.content, new.evidence, new.impact, new.follow_up);
END;

CREATE TRIGGER IF NOT EXISTS kb_ad AFTER DELETE ON kb_entries BEGIN
    INSERT INTO kb_fts(kb_fts, rowid, title, content, evidence, impact, follow_up)
    VALUES('delete', old.id, old.title, old.content, old.evidence, old.impact, old.follow_up);
END;

CREATE TRIGGER IF NOT EXISTS kb_au AFTER UPDATE ON kb_entries BEGIN
    INSERT INTO kb_fts(kb_fts, rowid, title, content, evidence, impact, follow_up)
    VALUES('delete', old.id, old.title, old.content, old.evidence, old.impact, old.follow_up);
    INSERT INTO kb_fts(rowid, title, content, evidence, impact, follow_up)
    VALUES (new.id, new.title, new.content, new.evidence, new.impact, new.follow_up);
END;

CREATE TABLE IF NOT EXISTS state_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    summary TEXT NOT NULL,
    updates TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def connect(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def add_memory(conn: sqlite3.Connection, rec: MemoryRecord) -> int:
    cur = conn.execute(
        """
        INSERT INTO memories(kind, title, content, source, tags, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (rec.kind, rec.title, rec.content, rec.source, rec.tags, utc_now()),
    )
    return int(cur.lastrowid)


def query_memories(conn: sqlite3.Connection, query: str, limit: int = 10) -> list[sqlite3.Row]:
    if query.strip():
        sql = """
        SELECT m.*, bm25(memories_fts) AS score
        FROM memories_fts
        JOIN memories m ON m.id = memories_fts.rowid
        WHERE memories_fts MATCH ?
        ORDER BY score
        LIMIT ?
        """
        return list(conn.execute(sql, (query, limit)))

    sql = """
    SELECT m.*, 0.0 AS score
    FROM memories m
    ORDER BY m.id DESC
    LIMIT ?
    """
    return list(conn.execute(sql, (limit,)))


def add_kb(
    conn: sqlite3.Connection,
    entry_type: str,
    title: str,
    content: str,
    evidence: str,
    impact: str,
    follow_up: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO kb_entries(entry_type, title, content, evidence, impact, follow_up, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (entry_type, title, content, evidence, impact, follow_up, utc_now()),
    )
    return int(cur.lastrowid)


def query_kb(conn: sqlite3.Connection, query: str, limit: int = 10) -> list[sqlite3.Row]:
    if query.strip():
        sql = """
        SELECT k.*, bm25(kb_fts) AS score
        FROM kb_fts
        JOIN kb_entries k ON k.id = kb_fts.rowid
        WHERE kb_fts MATCH ?
        ORDER BY score
        LIMIT ?
        """
        return list(conn.execute(sql, (query, limit)))
    sql = """
    SELECT k.*, 0.0 AS score
    FROM kb_entries k
    ORDER BY k.id DESC
    LIMIT ?
    """
    return list(conn.execute(sql, (limit,)))


def add_state_event(conn: sqlite3.Connection, summary: str, updates: Iterable[str]) -> int:
    cur = conn.execute(
        """
        INSERT INTO state_events(summary, updates, created_at)
        VALUES (?, ?, ?)
        """,
        (summary, "\n".join(updates), utc_now()),
    )
    return int(cur.lastrowid)
