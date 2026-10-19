"""Semantic cache generations. Freshness and sync-run writes do not invalidate them."""

import sqlite3


def install(conn: sqlite3.Connection) -> None:
    # Older watch processes retain the global counter until restarted during an upgrade.
    conn.execute('CREATE TABLE IF NOT EXISTS projection_epoch (id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL)')
    conn.execute('INSERT INTO projection_epoch SELECT 1,0 WHERE NOT EXISTS (SELECT 1 FROM projection_epoch)')
    for table in ('records', 'source_observations'):
        for action in ('INSERT', 'UPDATE', 'DELETE'):
            ref = 'OLD' if action == 'DELETE' else 'NEW'
            conditions = [f"{ref}.kind!='sync_runs'"] if table == 'records' else []
            if action == 'UPDATE':
                fields: tuple[str, ...] = ('record', 'revision_digest') if table == 'records' else (
                    'entity_urn', 'source', 'account_scope', 'native_table', 'native_row_ids', 'native_key',
                    'minted', 'fingerprint', 'adapter_version', 'present')
                conditions.append('(' + ' OR '.join(f'OLD.{f} IS NOT NEW.{f}' for f in fields) + ')')
            when = 'WHEN ' + ' AND '.join(conditions) if conditions else ''
            conn.execute(f'''CREATE TRIGGER IF NOT EXISTS epoch_{table}_{action} AFTER {action} ON {table}
                {when} BEGIN UPDATE projection_epoch SET value=value+1 WHERE id=1; END''')

    conn.execute("""CREATE TABLE IF NOT EXISTS source_projection_epochs (
        source TEXT NOT NULL, account_scope TEXT NOT NULL, value INTEGER NOT NULL,
        PRIMARY KEY(source,account_scope))""")
    if conn.execute("SELECT 1 FROM meta WHERE key='source_epoch_version' AND value='1'").fetchone():
        return
    conn.execute('SAVEPOINT source_epoch_install')
    try:
        for table in ('records', 'source_observations'):
            for action in ('INSERT', 'UPDATE', 'DELETE'):
                refs = ('OLD', 'NEW') if action == 'UPDATE' else ('OLD',) if action == 'DELETE' else ('NEW',)
                body = []
                for ref in refs:
                    conditions = [f'{ref}.source IS NOT NULL', f'{ref}.account_scope IS NOT NULL']
                    if table == 'records':
                        conditions.append(f"{ref}.kind!='sync_runs'")
                    if action == 'UPDATE' and ref == 'NEW':
                        pair_changed = 'OLD.source IS NOT NEW.source OR OLD.account_scope IS NOT NEW.account_scope'
                        if table == 'records':
                            pair_changed += " OR OLD.kind='sync_runs'"
                        conditions.append('(' + pair_changed + ')')
                    where = ' AND '.join(conditions) if conditions else '1'
                    pair = f'source={ref}.source AND account_scope={ref}.account_scope'
                    body.append(f"INSERT INTO source_projection_epochs SELECT {ref}.source,{ref}.account_scope,0 WHERE {where} AND NOT EXISTS (SELECT 1 FROM source_projection_epochs WHERE {pair});")
                    body.append(f'UPDATE source_projection_epochs SET value=value+1 WHERE {pair} AND {where};')
                fields = ('kind', 'source', 'account_scope', 'record', 'revision_digest') if table == 'records' else (
                    'entity_urn', 'source', 'account_scope', 'native_table', 'native_row_ids', 'native_key',
                    'minted', 'fingerprint', 'adapter_version', 'present')
                when = 'WHEN ' + ' OR '.join(f'OLD.{f} IS NOT NEW.{f}' for f in fields) if action == 'UPDATE' else ''
                conn.execute(f"DROP TRIGGER IF EXISTS source_epoch_{table}_{action}")
                conn.execute(f"CREATE TRIGGER source_epoch_{table}_{action} AFTER {action} ON {table} {when} BEGIN {''.join(body)} END")
        conn.execute('UPDATE source_projection_epochs SET value=value+1')
        conn.execute("INSERT INTO meta(key,value) VALUES ('source_epoch_version','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        conn.execute('RELEASE source_epoch_install')
    except BaseException:
        conn.execute('ROLLBACK TO source_epoch_install')
        conn.execute('RELEASE source_epoch_install')
        raise


def read_source(conn: sqlite3.Connection, source: str, scope: str) -> int:
    row = conn.execute('SELECT value FROM source_projection_epochs WHERE source=? AND account_scope=?',
                       (source, scope)).fetchone()
    return row[0] if row else 0


def read(conn: sqlite3.Connection) -> int:
    return conn.execute('SELECT value FROM projection_epoch WHERE id=1').fetchone()[0]
