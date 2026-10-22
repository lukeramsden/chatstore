import random
from contextlib import closing

import pytest

from chatstore.adapters.base import Emit, ExtractStats, Observation
from chatstore.cache import Cache, SourceObservation
from chatstore.cache.extraction import ExtractionMemo
from chatstore.cache.projection_epoch import read_source
from chatstore.cache.unit_coverage import CoverageUnavailable, UnitWithdrawal, merge, subtract
from tests.unit.test_cache import SCOPE, msg


def capture(cache, memo, key, rows, at=100, witness=None):
    witness = key if witness is None else witness
    emits = [Emit(value, Observation('message', [witness], witness, value['revision_digest'])) for value in rows]
    unit = memo.capture(key, f'input-{key}-{at}', lambda: iter(emits), ExtractStats())
    with cache.write():
        cache.upsert_many(rows, observed_at=at, sync_run=f'run-{at}')
        cache.observe_many(SourceObservation(value['urn'], 'whatsapp', SCOPE, 'message', [witness], witness,
            value['revision_digest'], '1', at, f'run-{at}') for value in rows)
        memo.save([unit])
    return unit


def test_interval_subtraction_handles_holes_overlaps_and_large_retained_history():
    assert subtract([[1, 10**12]], [[10, 20], [15, 30], [100, 100]]) == [[1, 9], [31, 99], [101, 10**12]]
    rng = random.Random(124)
    for _ in range(100):
        spans = [sorted([rng.randrange(1, 100), rng.randrange(1, 100)]) for _ in range(10)]
        cuts = [sorted([rng.randrange(1, 100), rng.randrange(1, 100)]) for _ in range(5)]
        expand = lambda ranges: {value for lo, hi in ranges for value in range(lo, hi + 1)}
        assert expand(subtract(spans, cuts)) == expand(spans) - expand(cuts)
    with pytest.raises(ValueError):
        merge([[0, 1]])


def test_withdrawal_preserves_content_and_reconciles_only_old_unseen_evidence(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
        removed = msg(1, 'retained historical body', None, None, 1)
        kept = msg(2, 'unchanged history', None, None, 2)
        capture(cache, memo, 'old', [removed])
        capture(cache, memo, 'kept', [kept])
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['old'], expected_epoch=epoch)
        aggregate_records = [list(row) for row in cache.conn.execute('SELECT id,id FROM entity_keys ORDER BY id')]
        aggregate_observations = [list(row) for row in cache.conn.execute('SELECT observation_id,observation_id FROM source_observations ORDER BY observation_id')]
        retained = proof.retained(aggregate_records, aggregate_observations)
        assert len(retained['record']) == 1
        with cache.write():
            assert proof.reconcile(cache, [], at=200, run='deletion', expected_epoch=epoch) == 1
        assert cache.get(removed['urn'])['text'] == 'retained historical body'
        assert cache.observations_of(removed['urn'])[0]['present'] == 'absent_from_source'
        assert cache.observations_of(kept['urn'])[0]['present'] == 'present'
        assert cache.conn.execute("SELECT valid FROM native_units WHERE unit_key='old'").fetchone()[0] == 0


def test_rebuilt_units_keep_seen_observations_and_removed_children_become_absent(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
        one, two = msg(1, 'first', None, None, 1), msg(2, 'retired child', None, None, 2)
        capture(cache, memo, 'group', [one, two])
        with cache.write():
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['group'], expected_epoch=read_source(cache.conn, 'whatsapp', SCOPE))
        capture(cache, memo, 'group', [one], at=200)
        seen = [list(row) for row in cache.conn.execute("SELECT observation_id,observation_id FROM source_observations WHERE sync_run='run-200'")]
        # Unchanged evidence is compactly sighted rather than physically rewritten.
        seen += [list(row) for row in cache.conn.execute("SELECT first_id,last_id FROM freshness_ranges WHERE kind='observation' AND sync_run='run-200'")]
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            assert proof.reconcile(cache, seen, at=200, run='run-200', expected_epoch=epoch) == 1
        assert cache.observations_of(one['urn'])[0]['present'] == 'present'
        assert cache.observations_of(two['urn'])[0]['present'] == 'absent_from_source'
        assert cache.conn.execute("SELECT valid FROM native_units WHERE unit_key='group'").fetchone()[0] == 1


def test_withdrawal_epoch_missing_membership_and_shared_witness_guards(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
        one = msg(1, 'shared content', None, None, 1)
        capture(cache, memo, 'first', [one])
        capture(cache, memo, 'second', [one], witness='first')
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            with pytest.raises(CoverageUnavailable, match='shares affected'):
                UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['first'], expected_epoch=epoch)
            with pytest.raises(CoverageUnavailable, match='missing or invalid'):
                UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['missing'], expected_epoch=epoch)
            with pytest.raises(CoverageUnavailable, match='projection changed'):
                UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['first'], expected_epoch=epoch - 1)
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['first', 'second'], expected_epoch=epoch)
        with cache.write(), pytest.raises(CoverageUnavailable, match='expected writer epoch'):
            proof.reconcile(cache, [], at=200, run='wrong', expected_epoch=epoch - 1)
        assert cache.observations_of(one['urn'])[0]['present'] == 'present'


def test_targeted_absence_and_journal_roll_back_together_even_at_same_timestamp(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
        one = msg(1, 'same clock instant', None, None, 1)
        capture(cache, memo, 'one', [one])
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['one'], expected_epoch=epoch)
        count = cache.conn.execute('SELECT count(*) FROM changes').fetchone()[0]
        with pytest.raises(RuntimeError, match='interrupted final commit'), cache.write():
            assert proof.reconcile(cache, [], at=100, run='same-time-delete', expected_epoch=epoch) == 1
            raise RuntimeError('interrupted final commit')
        assert cache.conn.execute('SELECT count(*) FROM changes').fetchone()[0] == count
        assert cache.observations_of(one['urn'])[0]['present'] == 'present'
        with cache.write():
            assert proof.reconcile(cache, [], at=100, run='same-time-delete', expected_epoch=epoch) == 1


def test_unit_withdrawal_does_not_scan_unrelated_members_or_record_bodies(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
        target = msg(1, 'targeted old evidence', None, None, 1)
        capture(cache, memo, 'target', [target])
        with cache.write():
            cache.conn.executemany('''INSERT INTO native_units VALUES(?,?,?,'policy','native',1,1,'[]','[]','{}','{}')''',
                (('whatsapp', SCOPE, f'unrelated-{key}') for key in range(10000)))
            cache.conn.executemany('INSERT INTO native_unit_members VALUES(?,?,?,?)',
                (('whatsapp', SCOPE, f'unrelated-{key}', f'urn:unrelated:{key}') for key in range(10000)))
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        callbacks, statements = [], []
        cache.conn.set_progress_handler(lambda: callbacks.append(1) or 0, 100)
        cache.conn.set_trace_callback(statements.append)
        with cache.write():
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['target'], expected_epoch=epoch)
            assert proof.reconcile(cache, [], at=200, run='targeted', expected_epoch=epoch) == 1
        cache.conn.set_progress_handler(None, 0)
        cache.conn.set_trace_callback(None)
        assert len(callbacks) * 100 < 5000
        assert not any('FROM records' in statement for statement in statements)
        with pytest.raises(CoverageUnavailable, match='outside the supplied'):
            proof.retained([], [])
