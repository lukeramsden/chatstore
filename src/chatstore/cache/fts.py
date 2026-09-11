"""FTS5 maintenance and query building."""

from __future__ import annotations

import re
import sqlite3

_TOKEN = re.compile(r"\S+")


def literal_query(text: str) -> str:
    """Turn user input into a safe FTS5 query: each whitespace token as a quoted phrase, ANDed."""
    tokens = _TOKEN.findall(text.strip())
    if not tokens:
        return '""'
    return " ".join('"' + t.replace('"', '""') + '"' for t in tokens)


def rebuild(conn: sqlite3.Connection) -> int:
    conn.execute("DELETE FROM messages_fts")
    conn.execute(
        """INSERT INTO messages_fts(rowid, text)
           SELECT m.id, json_extract(r.record, '$.text') FROM messages_idx m JOIN records r ON r.urn = m.urn
           WHERE r.tombstoned=0 AND json_extract(r.record, '$.text') IS NOT NULL"""
    )
    conn.execute("DELETE FROM revisions_fts")
    conn.execute(
        """INSERT INTO revisions_fts(rowid, text)
           SELECT id, json_extract(record, '$.text') FROM revisions
           WHERE entity_kind='messages' AND json_extract(record, '$.text') IS NOT NULL"""
    )
    n = conn.execute("SELECT count(*) FROM messages_fts").fetchone()[0]
    return int(n)


def set_message_text(conn: sqlite3.Connection, message_rowid: int, text: str | None, *, fresh: bool = False) -> None:
    if not fresh:
        conn.execute("DELETE FROM messages_fts WHERE rowid=?", (message_rowid,))
    if text:
        conn.execute("INSERT INTO messages_fts(rowid, text) VALUES (?, ?)", (message_rowid, text))


def add_revision_text(conn: sqlite3.Connection, revision_rowid: int, text: str | None) -> None:
    if text:
        conn.execute("INSERT INTO revisions_fts(rowid, text) VALUES (?, ?)", (revision_rowid, text))


def fts5_available() -> bool:
    try:
        c = sqlite3.connect(":memory:")
        c.execute("CREATE VIRTUAL TABLE t USING fts5(x)")
        c.close()
        return True
    except sqlite3.Error:
        return False
