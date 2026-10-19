"""Durable prepared native rows and page ownership, before canonical capture.

This private projection is replaceable, not an archive. A prepared point must be
captured canonically before acknowledgement allows further source changes. Source
cursors here describe the mirror only; they do not prove canonical preservation.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Self

from .page_index import PageIndex
from .pages import SQLitePages
from .source_view import SourceCursor, SourceReadView
from .wal import WalCursor, WalPages
from .wal_image import WalPageImage


class MirrorUnavailable(Exception):
    pass


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _cursor(raw: str) -> SourceCursor:
    value = json.loads(raw)
    wal = value['wal']
    wal['salts'] = tuple(wal['salts'])
    wal['checksum'] = tuple(wal['checksum'])
    return SourceCursor(tuple(value['database_identity']), WalCursor(**wal))


def _encode(row: tuple) -> str:
    return json.dumps([['bytes', base64.b64encode(value).decode('ascii')] if isinstance(value, bytes)
                       else ['float', value.hex()] if isinstance(value, float) else ['value', value]
                       for value in row], separators=(',', ':'), allow_nan=False)


def _same_row(before: tuple | None, after: tuple | None) -> bool:
    if before is None or after is None:
        return before is after
    return len(before) == len(after) and all(type(left) is type(right) and (
        left.hex() == right.hex() if isinstance(left, float) else left == right)
        for left, right in zip(before, after))


def _decode(raw: str) -> tuple:
    return tuple(base64.b64decode(value) if kind == 'bytes' else float.fromhex(value) if kind == 'float'
                 else value for kind, value in json.loads(raw))


@dataclass(frozen=True)
class PreparedCapture:
    token: str
    parent: str | None
    cursor: SourceCursor
    schema_version: int
    namespace: str
    policy_tag: str
    observed_at: str
    context: dict[str, Any]
    baseline: bool
    rows_changed: int


@dataclass(frozen=True)
class NativeChange:
    table: str
    row_id: int
    before: tuple | None
    after: tuple | None


class NativeMirror:
    def __init__(self, path: Path):
        self.path = path
        self.index = PageIndex(path)
        self.conn = self.index.conn
        try:
            self.conn.execute('PRAGMA foreign_keys=OFF')
            self.conn.execute('PRAGMA temp_store=FILE')
            self.conn.execute('PRAGMA temp.cache_size=-8192')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS cs_native_mirror_state(
                id INTEGER PRIMARY KEY CHECK(id=1), token TEXT NOT NULL, parent TEXT,
                cursor TEXT NOT NULL, schema_version INTEGER NOT NULL, namespace TEXT NOT NULL,
                policy_tag TEXT NOT NULL, observed_at TEXT NOT NULL, context TEXT NOT NULL,
                baseline INTEGER NOT NULL, rows_changed INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('prepared','acknowledged')), receipt TEXT)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS cs_native_mirror_tables(
                name TEXT PRIMARY KEY, root INTEGER NOT NULL, rowid_alias TEXT NOT NULL, columns TEXT NOT NULL)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS cs_native_mirror_changes(
                name TEXT NOT NULL,row_id INTEGER NOT NULL,before_row TEXT,
                PRIMARY KEY(name,row_id))''')
        except BaseException:
            self.close()
            raise

    def _state(self):
        return self.conn.execute('SELECT * FROM cs_native_mirror_state WHERE id=1').fetchone()

    @staticmethod
    def _capture(state) -> PreparedCapture:
        return PreparedCapture(state['token'], state['parent'], _cursor(state['cursor']),
            state['schema_version'], state['namespace'], state['policy_tag'], state['observed_at'],
            json.loads(state['context']), bool(state['baseline']), state['rows_changed'])

    def pending(self, *, reader: sqlite3.Connection | None = None) -> PreparedCapture | None:
        if reader is not None and not reader.in_transaction:
            raise MirrorUnavailable('pending native capture requires a pinned supplied reader')
        state = (self.conn if reader is None else reader).execute('SELECT * FROM cs_native_mirror_state WHERE id=1').fetchone()
        return self._capture(state) if state is not None and state['status'] == 'prepared' else None

    def continuation(self) -> PreparedCapture | None:
        state = self._state()
        if state is not None and state['status'] != 'acknowledged':
            raise MirrorUnavailable('prepared native capture must be acknowledged before continuation')
        return self._capture(state) if state is not None else None

    def _publish(self, view: SourceReadView, *, token: str, parent: str | None, namespace: str,
                 policy_tag: str, observed_at: str, context: Mapping[str, Any], baseline: bool, count: int):
        self.conn.execute('''INSERT INTO cs_native_mirror_state VALUES(1,?,?,?,?,?,?,?,?,?,?, 'prepared',NULL)
            ON CONFLICT(id) DO UPDATE SET token=excluded.token,parent=excluded.parent,cursor=excluded.cursor,
            schema_version=excluded.schema_version,namespace=excluded.namespace,policy_tag=excluded.policy_tag,
            observed_at=excluded.observed_at,context=excluded.context,baseline=excluded.baseline,
            rows_changed=excluded.rows_changed,status='prepared',receipt=NULL''',
            (token, parent, json.dumps(asdict(view.cursor), sort_keys=True), view.schema_version,
             namespace, policy_tag, observed_at, json.dumps(dict(context), sort_keys=True, allow_nan=False), baseline, count))

    def initialize(self, view: SourceReadView, tables: Iterable[str], *, namespace: str,
                   policy_tag: str, observed_at: str, context: Mapping[str, Any],
                   validate_rows: Callable[[sqlite3.Connection], None] | None = None) -> PreparedCapture:
        """One full baseline image and row copy; source triggers are never installed."""
        return self._baseline(view, tables, namespace=namespace, policy_tag=policy_tag,
                              observed_at=observed_at, context=context, expected_token=None, validate_rows=validate_rows)

    def rebase(self, view: SourceReadView, tables: Iterable[str], *, expected_token: str,
               namespace: str, policy_tag: str, observed_at: str,
               context: Mapping[str, Any],
               validate_rows: Callable[[sqlite3.Connection], None] | None = None) -> PreparedCapture:
        """Rebuild an acknowledged projection without dropping its canonical parent.

        A pending capture must finish under its original policy before replacement.
        The new full capture reconciles absence canonically after this commit.
        """
        return self._baseline(view, tables, namespace=namespace, policy_tag=policy_tag,
                              observed_at=observed_at, context=context, expected_token=expected_token, validate_rows=validate_rows)

    def _baseline(self, view: SourceReadView, tables: Iterable[str], *, namespace: str,
                  policy_tag: str, observed_at: str, context: Mapping[str, Any],
                  expected_token: str | None,
                  validate_rows: Callable[[sqlite3.Connection], None] | None) -> PreparedCapture:
        if not view.connection.in_transaction:
            raise MirrorUnavailable('baseline source view is no longer pinned')
        token = str(uuid.uuid4())
        specs = []
        for name in sorted(set(tables)):
            if name.lower().startswith(('sqlite_', 'cs_native_')):
                raise MirrorUnavailable('reserved native table name')
            row = view.connection.execute('SELECT rootpage,sql FROM sqlite_schema WHERE type=\'table\' AND name=?', (name,)).fetchone()
            if row is None or row[0] <= 0 or row[1].lstrip().upper().startswith('CREATE VIRTUAL'):
                raise MirrorUnavailable('native mirror requires ordinary rowid tables')
            columns = view.connection.execute(f'PRAGMA table_xinfo({_quote(name)})').fetchall()
            if not columns or any(column[6] for column in columns):
                raise MirrorUnavailable('native generated or hidden columns require reconciliation')
            names = [column[1] for column in columns]
            alias = next((candidate for candidate in ('_rowid_', 'rowid', 'oid') if candidate not in {field.lower() for field in names}), None)
            if alias is None:
                raise MirrorUnavailable('native table shadows every rowid alias')
            specs.append((name, row[0], row[1], alias, names))
        with tempfile.TemporaryDirectory(prefix='chatstore-native-baseline-', dir=self.path.parent) as directory:
            baseline = Path(directory) / 'baseline.sqlite'
            copy = sqlite3.connect(baseline)
            try:
                view.connection.backup(copy)
            finally:
                copy.close()
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                state = self._state()
                if expected_token is None:
                    if state is not None:
                        raise MirrorUnavailable('native mirror is already initialized')
                else:
                    if (state is None or state['status'] != 'acknowledged'
                            or state['token'] != expected_token or state['namespace'] != namespace):
                        raise MirrorUnavailable('native rebase requires the exact acknowledged point and namespace')
                    old_tables = self.conn.execute('SELECT name FROM cs_native_mirror_tables').fetchall()
                    for old in old_tables:
                        self.conn.execute(f'DROP TABLE {_quote(old["name"])}')
                    self.conn.execute('DELETE FROM cs_native_mirror_tables')
                    self.conn.execute('DELETE FROM cs_native_mirror_changes')
                with SQLitePages(baseline) as image:
                    self.index.build(image, {name: root for name, root, *_ in specs}, point=token,
                                     schema_tag=f'{view.schema_version}:{policy_tag}')
                count = 0
                for name, root, sql, alias, columns in specs:
                    self.conn.execute(sql)
                    self.conn.execute('INSERT INTO cs_native_mirror_tables VALUES(?,?,?,?)',
                                      (name, root, alias, json.dumps(columns)))
                    fields = ','.join(map(_quote, [alias, *columns]))
                    source_rows = view.connection.execute(f'SELECT {fields} FROM {_quote(name)}')
                    insert = f'INSERT INTO {_quote(name)}({fields}) VALUES({",".join("?" for _ in range(len(columns) + 1))})'
                    for values in source_rows:
                        self.conn.execute(insert, tuple(values))
                        count += 1
                if validate_rows is not None:
                    validate_rows(self.conn)
                self._publish(view, token=token, parent=expected_token, namespace=namespace, policy_tag=policy_tag,
                              observed_at=observed_at, context=context, baseline=True, count=count)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise
        pending = self.pending()
        assert pending is not None
        return pending

    def prepare(self, view: SourceReadView, wal: WalPages, *, expected_token: str, namespace: str,
                policy_tag: str, observed_at: str, context: Mapping[str, Any],
                validate_rows: Callable[[sqlite3.Connection], None] | None = None) -> PreparedCapture:
        """Prepare rows and page ownership; validate before publishing or roll back."""
        token = str(uuid.uuid4())
        with self.index.snapshot() as old:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                state = self._state()
                if (state is None or state['status'] != 'acknowledged' or state['token'] != expected_token
                        or state['namespace'] != namespace or state['policy_tag'] != policy_tag
                        or state['schema_version'] != view.schema_version
                        or _cursor(state['cursor']).database_identity != view.database_identity
                        or _cursor(state['cursor']).wal != view.changes.after):
                    raise MirrorUnavailable('native mirror continuation point or policy mismatch')
                specs = self.conn.execute('SELECT * FROM cs_native_mirror_tables ORDER BY name').fetchall()
                image = WalPageImage(view, wal, old, expected_point=expected_token,
                                     schema_tag=f'{view.schema_version}:{policy_tag}')
                selected = self.index.update(image, {row['name']: row['root'] for row in specs},
                    view.changes.pages, expected_point=expected_token, point=token,
                    schema_tag=f'{view.schema_version}:{policy_tag}')
                self.conn.execute('DELETE FROM cs_native_mirror_changes')
                count = 0
                for spec in specs:
                    name, alias = spec['name'], spec['rowid_alias']
                    columns = json.loads(spec['columns'])
                    fields = ','.join(map(_quote, columns))
                    candidates = sorted(selected.rows.get(name, ()))
                    if not candidates:
                        continue
                    staged_fields = ['row_id', *(f'value{i}' for i in range(len(columns)))]
                    self.conn.execute('CREATE TEMP TABLE cs_native_pending_values(row_id INTEGER PRIMARY KEY,' +
                                      ','.join(staged_fields[1:]) + ')')
                    stage_insert = 'INSERT INTO temp.cs_native_pending_values VALUES(' + ','.join('?' for _ in staged_fields) + ')'
                    table_count = 0
                    for offset in range(0, len(candidates), 400):
                        batch = candidates[offset:offset + 400]
                        placeholders = ','.join('?' for _ in batch)
                        query = f'SELECT {_quote(alias)},{fields} FROM {_quote(name)} WHERE {_quote(alias)} IN ({placeholders})'
                        before = {row[0]: tuple(row)[1:] for row in self.conn.execute(query, batch)}
                        after = {row[0]: tuple(row)[1:] for row in view.connection.execute(query, batch)}
                        for row_id in batch:
                            previous, current = before.get(row_id), after.get(row_id)
                            if _same_row(previous, current):
                                continue
                            self.conn.execute('INSERT INTO cs_native_mirror_changes VALUES(?,?,?)',
                                (name, row_id, _encode(previous) if previous is not None else None))
                            if current is not None:
                                self.conn.execute(stage_insert, (row_id, *current))
                            table_count += 1
                    # Delete first so swaps of unique keys within this changed set work.
                    self.conn.execute(f'DELETE FROM {_quote(name)} WHERE {_quote(alias)} IN '
                        '(SELECT row_id FROM cs_native_mirror_changes WHERE name=?)', (name,))
                    insert_fields = ','.join(map(_quote, [alias, *columns]))
                    self.conn.execute(f'INSERT INTO {_quote(name)}({insert_fields}) SELECT ' +
                        ','.join(staged_fields) + ' FROM temp.cs_native_pending_values')
                    self.conn.execute('DROP TABLE temp.cs_native_pending_values')
                    count += table_count
                image.check_stable()
                if validate_rows is not None:
                    validate_rows(self.conn)
                self._publish(view, token=token, parent=expected_token, namespace=namespace,
                              policy_tag=policy_tag, observed_at=observed_at, context=context, baseline=False, count=count)
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise
        pending = self.pending()
        assert pending is not None
        return pending

    def changes(self, token: str, *, reader: sqlite3.Connection | None = None) -> Iterator[NativeChange]:
        """Read old/new changed values from one pinned prepared point."""
        with self.index.snapshot() if reader is None else nullcontext(reader) as snapshot:
            if not snapshot.in_transaction:
                raise MirrorUnavailable('native changes require a pinned read snapshot')
            state = snapshot.execute('SELECT token,status FROM cs_native_mirror_state WHERE id=1').fetchone()
            if state is None or state['token'] != token or state['status'] != 'prepared':
                raise MirrorUnavailable('native changes require the exact prepared capture')
            for row in snapshot.execute('SELECT * FROM cs_native_mirror_changes ORDER BY name,row_id'):
                spec = snapshot.execute('SELECT rowid_alias,columns FROM cs_native_mirror_tables WHERE name=?', (row['name'],)).fetchone()
                fields = ','.join(map(_quote, json.loads(spec['columns'])))
                current = snapshot.execute(f'SELECT {fields} FROM {_quote(row["name"])} WHERE {_quote(spec["rowid_alias"])}=?', (row['row_id'],)).fetchone()
                yield NativeChange(row['name'], row['row_id'], _decode(row['before_row']) if row['before_row'] is not None else None,
                                   tuple(current) if current is not None else None)

    def acknowledge(self, token: str, receipt: str) -> None:
        """Call only after the matching canonical capture receipt commits durably."""
        if not receipt:
            raise MirrorUnavailable('canonical capture receipt is required')
        self.conn.execute('BEGIN IMMEDIATE')
        try:
            state = self._state()
            if state is None or state['token'] != token or (state['status'] == 'acknowledged' and state['receipt'] != receipt):
                raise MirrorUnavailable('native capture acknowledgement conflicts with current point')
            self.conn.execute("UPDATE cs_native_mirror_state SET status='acknowledged',receipt=? WHERE id=1", (receipt,))
            self.conn.execute('COMMIT')
        except BaseException:
            self.conn.execute('ROLLBACK')
            raise

    def close(self) -> None:
        self.index.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
