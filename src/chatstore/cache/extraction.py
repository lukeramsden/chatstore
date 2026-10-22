"""Transactional reuse of source extraction units, invalidated by projection changes."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
import sqlite3
import sys
import types
import uuid
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import Any

from ..adapters.base import CapturedUnit, Emit, ExtractStats, ReusedUnit
from ..canonical import records as canonical_records
from ..canonical import schemas as canonical_schemas
from ..config import Config
from ..identity import urn as identity
from .changefeed import append, id_ranges
from .freshness import stamp
from .store import Cache, _j

DDL = [
    """CREATE TABLE IF NOT EXISTS native_units (
        source TEXT NOT NULL, scope TEXT NOT NULL, unit_key TEXT NOT NULL,
        signature TEXT NOT NULL, fingerprint TEXT NOT NULL, valid INTEGER NOT NULL,
        record_count INTEGER NOT NULL, record_ranges TEXT NOT NULL, observation_ranges TEXT NOT NULL,
        counts TEXT NOT NULL, unsupported TEXT NOT NULL,
        PRIMARY KEY(source,scope,unit_key)
    )""",
    """CREATE TABLE IF NOT EXISTS native_unit_members (
        source TEXT NOT NULL, scope TEXT NOT NULL, unit_key TEXT NOT NULL, urn TEXT NOT NULL,
        PRIMARY KEY(source,scope,unit_key,urn)
    )""",
    'CREATE INDEX IF NOT EXISTS native_unit_member_urn ON native_unit_members(urn)',
]


def install(conn: sqlite3.Connection) -> None:
    for sql in DDL:
        conn.execute(sql)
    for table in ('records', 'source_observations'):
        for action in ('insert', 'update', 'delete'):
            urn = 'OLD.urn' if action == 'delete' else 'NEW.urn'
            if table == 'source_observations':
                urn = 'OLD.entity_urn' if action == 'delete' else 'NEW.entity_urn'
            condition = ''
            if action == 'update':
                fields = ('revision_digest', 'record') if table == 'records' else (
                    'native_row_ids', 'native_key', 'minted', 'fingerprint', 'adapter_version', 'present')
                condition = 'WHEN ' + ' OR '.join(f'OLD.{k} IS NOT NEW.{k}' for k in fields)
            conn.execute(f'''CREATE TRIGGER IF NOT EXISTS invalidate_native_{table}_{action}
                AFTER {action.upper()} ON {table} {condition} BEGIN
                UPDATE native_units SET valid=0 WHERE valid=1 AND (source,scope,unit_key) IN (
                    SELECT source,scope,unit_key FROM native_unit_members WHERE urn={urn}); END''')


def _literal(value: Any) -> Any:
    if value is None:
        return ['none']
    if isinstance(value, bool):
        return ['bool', value]
    if isinstance(value, int):
        return ['int', str(value)]
    if isinstance(value, float):
        return ['float', repr(value)]
    if isinstance(value, str):
        return ['str', value]
    if isinstance(value, bytes):
        return ['bytes', len(value), hashlib.sha256(value).hexdigest()]
    if isinstance(value, Decimal):
        return ['decimal', str(value)]
    if isinstance(value, re.Pattern):
        return ['pattern', _literal(value.pattern), value.flags]
    if isinstance(value, uuid.UUID):
        return ['uuid', str(value)]
    if isinstance(value, dict):
        return ['dict', sorted([[_literal(k), _literal(v)] for k, v in value.items()], key=_j)]
    if isinstance(value, (set, frozenset)):
        return [type(value).__name__, sorted((_literal(v) for v in value), key=_j)]
    if isinstance(value, (tuple, list)):
        return [type(value).__name__, [_literal(v) for v in value]]
    raise TypeError


def _code_constant(value: Any) -> Any:
    if isinstance(value, types.CodeType):
        return ['code', value.co_code.hex(), [_code_constant(v) for v in value.co_consts],
                list(value.co_names), list(value.co_varnames), list(value.co_freevars),
                list(value.co_cellvars), value.co_argcount, value.co_posonlyargcount,
                value.co_kwonlyargcount, value.co_flags, value.co_nlocals, value.co_stacksize,
                value.co_exceptiontable.hex()]
    if isinstance(value, (tuple, frozenset)):
        values = [_code_constant(v) for v in value]
        return [type(value).__name__, sorted(values, key=_j) if isinstance(value, frozenset) else values]
    if isinstance(value, slice):
        return ['slice', *[_code_constant(v) for v in (value.start, value.stop, value.step)]]
    if value is Ellipsis:
        return ['ellipsis']
    if isinstance(value, complex):
        return ['complex', repr(value)]
    return _literal(value)


def _default_policy(value: Any) -> Any:
    if isinstance(value, type):
        return ['type', value.__module__, value.__qualname__]
    if isinstance(value, dict):
        return ['dict', sorted([[_default_policy(k), _default_policy(v)] for k, v in value.items()], key=_j)]
    if isinstance(value, (tuple, list)):
        return [type(value).__name__, [_default_policy(v) for v in value]]
    return _literal(value)


def _function_policy(fn: Any) -> Any:
    policies = []
    visited: set[int] = set()
    while True:
        if id(fn) in visited:
            raise RuntimeError('cyclic extraction policy decorator')
        visited.add(id(fn))
        if isinstance(fn, types.FunctionType):
            policies.append([_code_constant(fn.__code__), _default_policy(fn.__defaults__),
                             _default_policy(fn.__kwdefaults__)])
        wrapped = getattr(fn, '__wrapped__', None)
        if wrapped is None:
            return policies
        fn = wrapped


def code_signature(adapter: Any, cfg: Config, *, extra_modules: tuple[types.ModuleType, ...] = ()) -> str:
    # Hash loaded code, not files that may have been replaced while this process was running.
    h = hashlib.sha256(_j({'adapter': adapter.version, 'config': cfg.to_json(), 'format': 6, 'python': list(sys.version_info[:3])}).encode())
    modules = (inspect.getmodule(type(adapter)), canonical_records, canonical_schemas, identity)
    pending = [*modules, *extra_modules]
    visited: set[int] = set()
    while pending:
        module = pending.pop(0)
        if module is None:
            raise RuntimeError('cannot identify extraction implementation')
        if id(module) in visited:
            continue
        visited.add(id(module))
        dependencies = getattr(module, 'POLICY_MODULES', ())
        if any(not isinstance(dependency, types.ModuleType) for dependency in dependencies):
            raise RuntimeError('extraction policy dependencies must be loaded modules')
        pending.extend(dependencies)
        h.update(module.__name__.encode())
        for name, value in sorted(vars(module).items()):
            if isinstance(value, types.FunctionType) or hasattr(value, '__wrapped__'):
                h.update(name.encode())
                h.update(_j(_function_policy(value)).encode())
            elif isinstance(value, type) and value.__module__ == module.__name__:
                for method, fn in sorted(vars(value).items()):
                    if isinstance(fn, (staticmethod, classmethod)):
                        fn = fn.__func__
                    accessors = (fn.fget, fn.fset, fn.fdel) if isinstance(fn, property) else (fn,)
                    for index, accessor in enumerate(accessors):
                        if isinstance(accessor, types.FunctionType) or hasattr(accessor, '__wrapped__'):
                            h.update((name + '.' + method + ':' + str(index)).encode())
                            h.update(_j(_function_policy(accessor)).encode())
            # Schema validators are a runtime cache, not extraction policy.
            elif name.isupper() and name != '_VALIDATORS':
                try:
                    literal = _literal(value)
                except TypeError:
                    continue  # Compiled validators/types are not output-policy literals.
                h.update(name.encode())
                h.update(_j(literal).encode())
    return h.hexdigest()


class ExtractionMemo:
    def __init__(self, cache: Cache, source: str, scope: str, signature: str, *, reuse: bool):
        self.cache, self.source, self.scope, self.signature = cache, source, scope, signature
        self.reuse = reuse

    def _row(self, key: str, fingerprint: str) -> Any:
        return self.cache.conn.execute('''SELECT * FROM native_units WHERE source=? AND scope=? AND unit_key=?
            AND signature=? AND fingerprint=? AND valid=1''',
            (self.source, self.scope, key, self.signature, fingerprint)).fetchone()

    def extract(self, key: str, fingerprint: str, build: Callable[[], Iterator[Emit]],
                stats: ExtractStats) -> Iterator[CapturedUnit | ReusedUnit]:
        row = self._row(key, fingerprint) if self.reuse else None
        if row:
            counts, unsupported = json.loads(row['counts']), json.loads(row['unsupported'])
            for k, n in counts.items():
                stats.bump(k, n)
            for k, n in unsupported.items():
                stats.unsupported[k] = stats.unsupported.get(k, 0) + n
            stats.bump('canonical_units_reused')
            yield ReusedUnit(key, fingerprint, row['record_count'], json.loads(row['record_ranges']),
                             json.loads(row['observation_ranges']), counts, unsupported, build)
        else:
            yield self.capture(key, fingerprint, build, stats)

    def capture(self, key: str, fingerprint: str, build: Callable[[], Iterator[Emit]], stats: ExtractStats) -> CapturedUnit:
        before, unsupported = dict(stats.counts), dict(stats.unsupported)
        errors = sum(e['count'] for e in stats.errors)
        records = list(build())
        counts = {k: n - before.get(k, 0) for k, n in stats.counts.items() if n != before.get(k, 0)}
        unknown = {k: n - unsupported.get(k, 0) for k, n in stats.unsupported.items() if n != unsupported.get(k, 0)}
        stats.bump('canonical_units_rebuilt')
        return CapturedUnit(key, fingerprint, records, counts, unknown,
                            bool(records) and sum(e['count'] for e in stats.errors) == errors)

    def revalidate(self, unit: ReusedUnit, stats: ExtractStats) -> CapturedUnit | ReusedUnit:
        row = self._row(unit.key, unit.fingerprint)
        if row:
            unit.record_ranges = json.loads(row['record_ranges'])
            unit.observation_ranges = json.loads(row['observation_ranges'])
            return unit
        for k, n in unit.counts.items():
            stats.bump(k, -n)
        for k, n in unit.unsupported.items():
            stats.unsupported[k] -= n
        stats.bump('canonical_units_reused', -1)
        return self.capture(unit.key, unit.fingerprint, unit.build, stats)

    def save(self, units: list[CapturedUnit]) -> None:
        """Called after their canonical captures in the same writer transaction."""
        if not units:
            return
        urns = sorted({e.record['urn'] for u in units for e in u.records})
        keys: dict[str, int] = {}
        observations: dict[tuple[str, str], tuple[Any, ...]] = {}
        heads = self.cache.current_heads(urns)
        for i in range(0, len(urns), 500):
            chunk = urns[i:i + 500]
            ph = ','.join('?' * len(chunk))
            keys.update((r[0], r[1]) for r in self.cache.conn.execute(f'SELECT urn,id FROM entity_keys WHERE urn IN ({ph})', chunk))
            observations.update(((r[0], r[1]), tuple(r)[2:]) for r in self.cache.conn.execute(f'''SELECT entity_urn,native_table,observation_id,native_row_ids,native_key,minted,fingerprint,present
                FROM source_observations WHERE entity_urn IN ({ph}) AND source=? AND account_scope=?''',
                [*chunk, self.source, self.scope]))
        for unit in units:
            previous = self.cache.conn.execute('SELECT * FROM native_units WHERE source=? AND scope=? AND unit_key=?',
                                               (self.source, self.scope, unit.key)).fetchone()
            matching = all(heads[e.record['urn']][0] == e.record['revision_digest'] for e in unit.records)
            for e in unit.records:
                if e.observation:
                    o = e.observation
                    actual = observations[(e.record['urn'], o.native_table)]
                    matching = matching and actual[1:] == (_j(o.native_row_ids), o.native_key, int(o.minted), o.fingerprint, 'present')
            if not unit.cacheable or not matching:
                if previous:
                    self.cache.conn.execute('DELETE FROM native_unit_members WHERE source=? AND scope=? AND unit_key=?',
                                            (self.source, self.scope, unit.key))
                    self.cache.conn.execute('DELETE FROM native_units WHERE source=? AND scope=? AND unit_key=?',
                                            (self.source, self.scope, unit.key))
                continue
            record_ranges = _j(id_ranges([keys[e.record['urn']] for e in unit.records]))
            observation_ranges = _j(id_ranges([observations[(e.record['urn'], e.observation.native_table)][0]
                                               for e in unit.records if e.observation]))
            values = (self.source, self.scope, unit.key, self.signature, unit.fingerprint, len(unit.records),
                      record_ranges, observation_ranges, _j(unit.counts), _j(unit.unsupported))
            self.cache.conn.execute("""INSERT INTO native_units VALUES (?,?,?,?,?,1,?,?,?,?,?)
                ON CONFLICT(source,scope,unit_key) DO UPDATE SET signature=excluded.signature,
                fingerprint=excluded.fingerprint,valid=1,record_count=excluded.record_count,
                record_ranges=excluded.record_ranges,observation_ranges=excluded.observation_ranges,
                counts=excluded.counts,unsupported=excluded.unsupported""", values)
            if not self.reuse or previous is None or previous['record_ranges'] != record_ranges:
                self.cache.conn.execute('DELETE FROM native_unit_members WHERE source=? AND scope=? AND unit_key=?',
                                        (self.source, self.scope, unit.key))
                self.cache.conn.executemany('INSERT INTO native_unit_members VALUES (?,?,?,?)',
                                           [(self.source, self.scope, unit.key, urn) for urn in sorted({e.record['urn'] for e in unit.records})])

    def sight(self, units: list[ReusedUnit], at: int, run: str) -> None:
        for kind, operation, attr in (('record', 'record_refresh', 'record_ranges'),
                                      ('observation', 'observation_refresh', 'observation_ranges')):
            spans = sorted(span for u in units for span in getattr(u, attr))
            merged: list[list[int]] = []
            for lo, hi in spans:
                if merged and lo <= merged[-1][1] + 1:
                    merged[-1][1] = max(merged[-1][1], hi)
                else:
                    merged.append([lo, hi])
            if merged:
                stamp(self.cache.conn, kind, merged, at, run)
                append(self.cache.conn, operation, {'ranges': merged, 'observed_at': at, 'sync_run': run})
