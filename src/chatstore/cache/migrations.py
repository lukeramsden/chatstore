"""Schema creation and forward migrations."""

from __future__ import annotations

import sqlite3

from .schema import DDL, SCHEMA_VERSION


class IncompatibleCache(Exception):
    pass


def migrate(conn: sqlite3.Connection) -> None:
    for stmt in DDL:
        conn.execute(stmt)
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None:
        conn.execute("INSERT INTO meta(key, value) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
    elif int(row[0]) > SCHEMA_VERSION:
        raise IncompatibleCache(f"cache schema {row[0]} is newer than supported {SCHEMA_VERSION}")
    # Future: elif int(row[0]) < SCHEMA_VERSION: apply stepwise migrations.
    conn.commit()
