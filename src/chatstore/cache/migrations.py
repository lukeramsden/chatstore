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
    if 'observation_id' not in {r[1] for r in conn.execute('PRAGMA table_info(source_observations)')}:
        conn.execute('BEGIN IMMEDIATE')
        try:
            ddl = next(s for s in DDL if 'CREATE TABLE IF NOT EXISTS source_observations (' in s)
            conn.execute(ddl.replace('source_observations (', 'source_observations_v2 ('))
            columns = [r[1] for r in conn.execute('PRAGMA table_info(source_observations)')]
            names = ','.join(columns)
            conn.execute(f'INSERT INTO source_observations_v2(observation_id,{names}) SELECT rowid,{names} FROM source_observations')
            conn.execute('DROP TABLE source_observations')
            conn.execute('ALTER TABLE source_observations_v2 RENAME TO source_observations')
            for stmt in DDL:
                if 'INDEX IF NOT EXISTS observations_' in stmt:
                    conn.execute(stmt)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    if row is not None and int(row[0]) != SCHEMA_VERSION:
        conn.execute("UPDATE meta SET value=? WHERE key='schema_version'", (str(SCHEMA_VERSION),))
    conn.commit()
    from .changefeed import install
    install(conn)
    from .extraction import install as install_extraction
    install_extraction(conn)

    from .import_ranges import install as install_import_ranges
    install_import_ranges(conn)

    from .projection_epoch import install as install_projection_epoch
    install_projection_epoch(conn)

    from .coverage import install as install_coverage
    install_coverage(conn)

    from .capture_coverage import install as install_capture_coverage
    install_capture_coverage(conn)
