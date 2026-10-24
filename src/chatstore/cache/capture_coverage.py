"""Durable coverage attempts; completion must share the canonical receipt transaction."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass

from ..adapters.base import Emit
from .changefeed import append, id_ranges
from .freshness import stamp
from .projection_epoch import read_source
from .store import Cache, _j
from .unit_coverage import CoverageUnavailable, UnitWithdrawal, merge

DDL = [
    '''CREATE TABLE IF NOT EXISTS capture_coverage_points(
        source TEXT NOT NULL,scope TEXT NOT NULL,token TEXT NOT NULL,epoch INTEGER NOT NULL,
        records TEXT NOT NULL,observations TEXT NOT NULL,PRIMARY KEY(source,scope))''',
    '''CREATE TABLE IF NOT EXISTS capture_coverage_attempts(
        token TEXT PRIMARY KEY,binding TEXT NOT NULL,source TEXT NOT NULL,scope TEXT NOT NULL,parent TEXT,
        full INTEGER NOT NULL,epoch INTEGER NOT NULL,withdrawn_records TEXT NOT NULL,withdrawn_observations TEXT NOT NULL,
        retained_records TEXT NOT NULL,retained_observations TEXT NOT NULL,staged_records TEXT NOT NULL,staged_observations TEXT NOT NULL)''',
    'CREATE UNIQUE INDEX IF NOT EXISTS capture_coverage_writer ON capture_coverage_attempts(source,scope)',
]


def install(conn: sqlite3.Connection) -> None:
    for sql in DDL:
        conn.execute(sql)


@dataclass(frozen=True)
class CaptureCoverage:
    cache: Cache
    token: str
    binding: str

    @classmethod
    def begin(cls, cache: Cache, *, source: str, scope: str, token: str, binding: str,
              parent: str | None, full: bool, withdrawal: UnitWithdrawal | None = None):
        if not cache.conn.in_transaction:
            raise CoverageUnavailable('coverage begin requires the canonical writer transaction')
        row = cache.conn.execute('SELECT * FROM capture_coverage_attempts WHERE source=? AND scope=?', (source, scope)).fetchone()
        if row is not None:
            if (row['token'], row['binding'], row['parent']) != (token, binding, parent):
                raise CoverageUnavailable('another prepared coverage attempt owns this namespace')
            result = cls(cache, token, binding)
            result._check()
            return result
        epoch = read_source(cache.conn, source, scope)
        previous = cache.conn.execute('SELECT * FROM capture_coverage_points WHERE source=? AND scope=?', (source, scope)).fetchone()
        retained: dict[str, list[list[int]]]
        withdrawn_records: list[list[int]]
        if full:
            observations = id_ranges([row[0] for row in cache.conn.execute("SELECT observation_id FROM source_observations WHERE source=? AND account_scope=? AND present='present'", (source, scope))])
            withdrawn_records, retained = [], {'record': [], 'observation': []}
        else:
            if previous is None or previous['token'] != parent or previous['epoch'] != epoch:
                raise CoverageUnavailable('prior canonical coverage point or epoch is unavailable')
            if withdrawal is None or (withdrawal.source, withdrawal.scope, withdrawal.epoch) != (source, scope, epoch):
                raise CoverageUnavailable('selective coverage requires matching withdrawal evidence')
            retained = withdrawal.retained(json.loads(previous['records']), json.loads(previous['observations']))
            withdrawn_records = [list(span) for span in withdrawal.record_ranges]
            observations = [list(span) for span in withdrawal.observation_ranges]
        cache.conn.execute('INSERT INTO capture_coverage_attempts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (token, binding, source, scope, parent, full, epoch, _j(withdrawn_records), _j(observations),
             _j(retained['record']), _j(retained['observation']), '[]', '[]'))
        return cls(cache, token, binding)

    @classmethod
    def resume(cls, cache: Cache, *, token: str, binding: str):
        row = cache.conn.execute('SELECT 1 FROM capture_coverage_attempts WHERE token=?', (token,)).fetchone()
        if row is None:
            return None
        result = cls(cache, token, binding)
        result._check()
        return result

    @classmethod
    def restart_full(cls, cache: Cache, *, source: str, scope: str, token: str,
                     binding: str, parent: str | None):
        if not cache.conn.in_transaction:
            raise CoverageUnavailable('coverage recovery requires the writer transaction')
        row = cache.conn.execute('SELECT * FROM capture_coverage_attempts WHERE source=? AND scope=?', (source, scope)).fetchone()
        if row is not None:
            if (row['token'], row['binding'], row['parent']) != (token, binding, parent):
                raise CoverageUnavailable('coverage recovery cannot discard another prepared attempt')
            cache.conn.execute('DELETE FROM capture_coverage_attempts WHERE token=?', (token,))
        return cls.begin(cache, source=source, scope=scope, token=token, binding=binding, parent=parent, full=True)

    @property
    def full(self) -> bool:
        return bool(self._check()['full'])

    def _check(self):
        if not self.cache.conn.in_transaction:
            raise CoverageUnavailable('coverage operations require the canonical writer transaction')
        row = self.cache.conn.execute('SELECT * FROM capture_coverage_attempts WHERE token=?', (self.token,)).fetchone()
        if row is None or row['binding'] != self.binding:
            raise CoverageUnavailable('prepared coverage binding is missing or conflicting')
        if row['epoch'] != read_source(self.cache.conn, row['source'], row['scope']):
            raise CoverageUnavailable('unrelated semantic writer changed the pending canonical coverage')
        return row

    @contextmanager
    def writing(self):
        row = self._check()
        yield
        self.cache.conn.execute('UPDATE capture_coverage_attempts SET epoch=? WHERE token=?',
            (read_source(self.cache.conn, row['source'], row['scope']), self.token))

    def stage(self, records: Iterable[Iterable[int]], observations: Iterable[Iterable[int]]) -> None:
        row = self.cache.conn.execute('SELECT staged_records,staged_observations FROM capture_coverage_attempts WHERE token=? AND binding=?', (self.token, self.binding)).fetchone()
        if not self.cache.conn.in_transaction or row is None:
            raise CoverageUnavailable('coverage staging requires an active prepared writer')
        self.cache.conn.execute('UPDATE capture_coverage_attempts SET staged_records=?,staged_observations=? WHERE token=?',
            (_j(merge([*json.loads(row[0]), *records])), _j(merge([*json.loads(row[1]), *observations])), self.token))

    def stage_records(self, records: list[Emit]) -> None:
        if not records:
            return
        row = self.cache.conn.execute('SELECT source,scope FROM capture_coverage_attempts WHERE token=? AND binding=?', (self.token, self.binding)).fetchone()
        if row is None or not self.cache.conn.in_transaction:
            raise CoverageUnavailable('captured coverage requires its active canonical transaction')
        urns = sorted({item.record['urn'] for item in records})
        ids: dict[str, int] = {}
        evidence: dict[tuple[str, str], int] = {}
        for start in range(0, len(urns), 400):
            batch = urns[start:start + 400]
            placeholders = ','.join('?' for _ in batch)
            ids.update(self.cache.conn.execute(f'SELECT urn,id FROM entity_keys WHERE urn IN ({placeholders})', batch))
            evidence.update(((value[0], value[1]), value[2]) for value in self.cache.conn.execute(
                f'SELECT entity_urn,native_table,observation_id FROM source_observations WHERE entity_urn IN ({placeholders}) AND source=? AND account_scope=?', [*batch, row['source'], row['scope']]))
        if len(ids) != len(urns):
            raise CoverageUnavailable('staged canonical record keys are missing')
        observation_ids = []
        for item in records:
            if item.observation:
                key = (item.record['urn'], item.observation.native_table)
                if key not in evidence:
                    raise CoverageUnavailable('staged canonical source evidence is missing')
                observation_ids.append(evidence[key])
        self.stage(id_ranges(list(ids.values())), id_ranges(observation_ids))

    def finish(self, *, at: int, run: str) -> dict:
        row = self._check()
        staged_records, staged_observations = json.loads(row['staged_records']), json.loads(row['staged_observations'])
        retained_records, retained_observations = json.loads(row['retained_records']), json.loads(row['retained_observations'])
        all_records = merge([*retained_records, *staged_records])
        all_observations = merge([*retained_observations, *staged_observations])
        proof = UnitWithdrawal(row['source'], row['scope'], (), (), tuple(map(tuple, json.loads(row['withdrawn_observations']))), row['epoch'])
        absent = proof.reconcile(self.cache, all_observations, at=at, run=run, expected_epoch=row['epoch'])
        for kind, operation, spans in (('record', 'record_refresh', retained_records),
                                       ('observation', 'observation_refresh', retained_observations)):
            if spans:
                stamp(self.cache.conn, kind, spans, at, run)
                append(self.cache.conn, operation, {'ranges': spans, 'observed_at': at, 'sync_run': run})
        epoch = read_source(self.cache.conn, row['source'], row['scope'])
        self.cache.conn.execute('''INSERT INTO capture_coverage_points VALUES(?,?,?,?,?,?) ON CONFLICT(source,scope)
            DO UPDATE SET token=excluded.token,epoch=excluded.epoch,records=excluded.records,observations=excluded.observations''',
            (row['source'], row['scope'], self.token, epoch, _j(all_records), _j(all_observations)))
        self.cache.conn.execute('DELETE FROM capture_coverage_attempts WHERE token=?', (self.token,))
        return {'record': all_records, 'observation': all_observations, 'absent': absent}
