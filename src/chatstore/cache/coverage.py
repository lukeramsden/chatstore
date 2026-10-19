"""Incremental message counts and indexed date bounds for sync summaries."""

import sqlite3
from typing import Any


def install(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS messages_scope_time ON messages_idx(source,account_scope,utc_ms) WHERE kind='message'")
    existed = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_counts'").fetchone() is not None
    conn.execute('''CREATE TABLE IF NOT EXISTS message_counts(
        source TEXT NOT NULL,account_scope TEXT NOT NULL,value INTEGER NOT NULL CHECK(value>=0),
        PRIMARY KEY(source,account_scope))''')
    if existed and conn.execute("SELECT 1 FROM meta WHERE key='message_counts_version' AND value='1'").fetchone():
        return
    conn.execute('SAVEPOINT message_counts_install')
    try:
        conn.execute('DELETE FROM message_counts')
        conn.execute("INSERT INTO message_counts SELECT source,account_scope,count(*) FROM messages_idx WHERE kind='message' GROUP BY source,account_scope")
        for action in ('INSERT', 'UPDATE', 'DELETE'):
            conn.execute(f'DROP TRIGGER IF EXISTS count_messages_{action}')
            statements = []
            if action in ('UPDATE', 'DELETE'):
                statements.append("UPDATE message_counts SET value=value-1 WHERE source=OLD.source AND account_scope=OLD.account_scope AND OLD.kind='message';")
            if action in ('INSERT', 'UPDATE'):
                statements.append("INSERT INTO message_counts SELECT NEW.source,NEW.account_scope,1 WHERE NEW.kind='message' ON CONFLICT(source,account_scope) DO UPDATE SET value=value+1;")
            when = "WHEN OLD.source IS NOT NEW.source OR OLD.account_scope IS NOT NEW.account_scope OR OLD.kind IS NOT NEW.kind" if action == 'UPDATE' else ''
            conn.execute(f'CREATE TRIGGER count_messages_{action} AFTER {action} ON messages_idx {when} BEGIN {"".join(statements)} END')
        conn.execute("INSERT INTO meta VALUES('message_counts_version','1') ON CONFLICT(key) DO UPDATE SET value='1'")
        conn.execute('RELEASE message_counts_install')
    except BaseException:
        conn.execute('ROLLBACK TO message_counts_install')
        conn.execute('RELEASE message_counts_install')
        raise


def read(conn: sqlite3.Connection, source: str | None, scope: str | None) -> dict[str, Any]:
    filters, args = [], []
    if source is not None:
        filters.append('c.source=?')
        args.append(source)
    if scope is not None:
        filters.append('c.account_scope=?')
        args.append(scope)
    where = 'WHERE ' + ' AND '.join(filters) if filters else ''
    row = conn.execute(f'''SELECT sum(value),min(first),max(last) FROM (
        SELECT c.value,
            (SELECT utc_ms FROM messages_idx WHERE source=c.source AND account_scope=c.account_scope
             AND kind='message' AND utc_ms IS NOT NULL ORDER BY utc_ms LIMIT 1) first,
            (SELECT utc_ms FROM messages_idx WHERE source=c.source AND account_scope=c.account_scope
             AND kind='message' AND utc_ms IS NOT NULL ORDER BY utc_ms DESC LIMIT 1) last
        FROM message_counts c {where})''', args).fetchone()
    return {'earliest_utc_ms': row[1], 'latest_utc_ms': row[2], 'messages': row[0] or 0}
