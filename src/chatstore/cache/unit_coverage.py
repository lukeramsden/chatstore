"""Withdraw affected extraction-unit coverage without enumerating retained history.

The caller owns a prepared point's epoch tracking and the actual new capture's
coverage. These primitives do not publish source checkpoints or canonical receipts.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Self

from .changefeed import id_ranges
from .projection_epoch import read_source
from .store import Cache


class CoverageUnavailable(Exception):
    pass


def merge(spans: Iterable[Iterable[int]]) -> list[list[int]]:
    result: list[list[int]] = []
    for lo, hi in sorted(tuple(span) for span in spans):
        if type(lo) is not int or type(hi) is not int or not 1 <= lo <= hi:
            raise ValueError('coverage ranges must contain positive ordered integer IDs')
        if result and lo <= result[-1][1] + 1:
            result[-1][1] = max(result[-1][1], hi)
        else:
            result.append([lo, hi])
    return result


def subtract(spans: Iterable[Iterable[int]], removed: Iterable[Iterable[int]]) -> list[list[int]]:
    retained, cuts = merge(spans), merge(removed)
    result: list[list[int]] = []
    index = 0
    for lo, hi in retained:
        while index < len(cuts) and cuts[index][1] < lo:
            index += 1
        position, current = lo, index
        while current < len(cuts) and cuts[current][0] <= hi:
            first, last = cuts[current]
            if position < first:
                result.append([position, min(hi, first - 1)])
            position = max(position, last + 1)
            if position > hi:
                break
            current += 1
        if position <= hi:
            result.append([position, hi])
        index = current
    return result


@dataclass(frozen=True)
class UnitWithdrawal:
    source: str
    scope: str
    unit_keys: tuple[str, ...]
    record_ranges: tuple[tuple[int, int], ...]
    observation_ranges: tuple[tuple[int, int], ...]
    epoch: int

    @classmethod
    def take(cls, cache: Cache, source: str, scope: str, keys: Iterable[str], *, expected_epoch: int) -> Self:
        if not cache.conn.in_transaction:
            raise CoverageUnavailable('taking unit coverage requires the canonical writer transaction')
        epoch = read_source(cache.conn, source, scope)
        if epoch != expected_epoch:
            raise CoverageUnavailable('canonical source projection changed before unit withdrawal')
        keys = tuple(sorted(set(keys)))
        records, observations = [], []
        target = 'cs_withdraw_' + uuid.uuid4().hex
        cache.conn.execute(f'CREATE TEMP TABLE {target}(unit_key TEXT PRIMARY KEY)')
        try:
            cache.conn.executemany(f'INSERT INTO {target} VALUES(?)', ((key,) for key in keys))
            rows = list(cache.conn.execute(f'''SELECT unit_key,valid,record_ranges,observation_ranges
                FROM native_units WHERE source=? AND scope=? AND unit_key IN (SELECT unit_key FROM {target})''', (source, scope)))
            if len(rows) != len(keys) or any(not row['valid'] for row in rows):
                raise CoverageUnavailable('affected unit coverage is missing or invalid; full reconciliation required')
            shared = cache.conn.execute(f'''SELECT other.unit_key FROM native_unit_members own
                CROSS JOIN native_unit_members other INDEXED BY native_unit_member_urn
                CROSS JOIN native_units unit
                WHERE other.urn=own.urn AND other.source=own.source AND other.scope=own.scope
                AND unit.source=other.source AND unit.scope=other.scope AND unit.unit_key=other.unit_key
                AND own.source=? AND own.scope=? AND own.unit_key IN (SELECT unit_key FROM {target})
                AND unit.valid=1 AND NOT EXISTS(SELECT 1 FROM {target} WHERE unit_key=other.unit_key) LIMIT 1''', (source, scope)).fetchall()
            if shared:
                raise CoverageUnavailable('another valid unit shares affected records; expand dependency routing')
            for row in rows:
                records.extend(json.loads(row['record_ranges']))
                observations.extend(json.loads(row['observation_ranges']))
        finally:
            cache.conn.execute(f'DROP TABLE {target}')
        return cls(source, scope, keys, tuple((lo, hi) for lo, hi in merge(records)),
                   tuple((lo, hi) for lo, hi in merge(observations)), epoch)

    def with_headers(self, cache: Cache, complete_tables: Iterable[str], *,
                     inferred_keys: Iterable[str] = ()) -> Self:
        if not cache.conn.in_transaction or read_source(cache.conn, self.source, self.scope) != self.epoch:
            raise CoverageUnavailable('header withdrawal requires the original writer epoch')
        records, observations = [], []
        for table, keys in [(table, None) for table in sorted(set(complete_tables))] + [('message.ck_chat_id', sorted(set(inferred_keys)))]:
            batches = [None] if keys is None else [keys[start:start + 400] for start in range(0, len(keys), 400)]
            for batch in batches:
                sql = """SELECT o.observation_id,k.id FROM source_observations o
                    JOIN entity_keys k ON k.urn=o.entity_urn WHERE o.source=? AND o.account_scope=?
                    AND o.native_table=? AND o.present='present'"""
                args = [self.source, self.scope, table]
                if batch is not None:
                    sql += ' AND o.native_key IN (' + ','.join('?' for _ in batch) + ')'
                    args.extend(batch)
                for row in cache.conn.execute(sql, args):
                    observations.append(row[0])
                    records.append(row[1])
        return type(self)(self.source, self.scope, self.unit_keys,
            tuple((lo, hi) for lo, hi in merge([*self.record_ranges, *id_ranges(records)])),
            tuple((lo, hi) for lo, hi in merge([*self.observation_ranges, *id_ranges(observations)])), self.epoch)

    def retained(self, records: Iterable[Iterable[int]], observations: Iterable[Iterable[int]]) -> dict[str, list[list[int]]]:
        records, observations = merge(records), merge(observations)
        if subtract(self.record_ranges, records) or subtract(self.observation_ranges, observations):
            raise CoverageUnavailable('withdrawn unit is outside the supplied prior capture coverage')
        return {'record': subtract(records, self.record_ranges),
                'observation': subtract(observations, self.observation_ranges)}

    def reconcile(self, cache: Cache, seen_observations: Iterable[Iterable[int]], *, at: int,
                  run: str, expected_epoch: int) -> int:
        """Mark only withdrawn evidence absent, in the caller's final transaction.

        `seen_observations` comes from actual newly captured/sighted groups, not
        an assumed whole-source range. It protects shared evidence rebuilt by a
        selected unit. The epoch must include only the caller's known writes.
        """
        if not cache.conn.in_transaction or read_source(cache.conn, self.source, self.scope) != expected_epoch:
            raise CoverageUnavailable('unit absence requires an unchanged expected writer epoch')
        absent = subtract(self.observation_ranges, seen_observations)
        count = 0
        for lo, hi in absent:
            count += cache.conn.execute('''UPDATE source_observations NOT INDEXED
                SET present='absent_from_source',observed_at=?,sync_run=?
                WHERE observation_id BETWEEN ? AND ? AND source=? AND account_scope=? AND present='present' ''',
                (at, run, lo, hi, self.source, self.scope)).rowcount
        return count
