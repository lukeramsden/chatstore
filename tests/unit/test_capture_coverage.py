import json
import uuid
from contextlib import closing

import pytest

from chatstore.adapters.base import Emit, ExtractStats, Observation
from chatstore.cache import Cache, SourceObservation
from chatstore.cache.capture_coverage import CaptureCoverage
from chatstore.cache.extraction import ExtractionMemo
from chatstore.cache.projection_epoch import read_source
from chatstore.cache.unit_coverage import CoverageUnavailable, UnitWithdrawal
from chatstore.canonical.records import record, restamp
from tests.unit.test_cache import SCOPE, msg


def run(at):
    return uuid.UUID(int=at).urn


def unit(memo, key, values, at):
    return memo.capture(key, f'input-{key}-{at}',
        lambda: iter(Emit(value, Observation('message', [key], key, value['revision_digest'])) for value in values), ExtractStats())


def batch(cache, attempt, units, at, memo, headers=()):
    records = [*headers, *(item for value in units for item in value.records)]
    with cache.write(), attempt.writing():
        cache.upsert_many([item.record for item in records], observed_at=at, sync_run=run(at))
        cache.observe_many(SourceObservation(item.record['urn'], 'whatsapp', SCOPE, item.observation.native_table,
            item.observation.native_row_ids, item.observation.native_key, item.observation.fingerprint, '1', at, run(at)) for item in records if item.observation)
        memo.save(units)
        attempt.stage_records(records)


def baseline(cache, *, headers=()):
    memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
    values = [msg(row, f'body-{row}', None, None, row) for row in (1, 2, 3)]
    with cache.write():
        attempt = CaptureCoverage.begin(cache, source='whatsapp', scope=SCOPE, token='base', binding='base-binding', parent=None, full=True)
    batch(cache, attempt, [unit(memo, 'group', values[:2], 100), unit(memo, 'kept', values[2:], 100)], 100, memo, headers)
    with cache.write():
        attempt.finish(at=100, run=run(100))
    return memo, values


def child(cache):
    with cache.write():
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        withdrawal = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['group'], expected_epoch=epoch)
        return CaptureCoverage.begin(cache, source='whatsapp', scope=SCOPE, token='child', binding='child-binding', parent='base', full=False, withdrawal=withdrawal)


def test_partial_batch_reopens_with_original_withdrawal_and_own_updated_epoch(tmp_path):
    path = tmp_path / 'cache.sqlite'
    with closing(Cache(path)) as cache:
        memo, values = baseline(cache)
        attempt = child(cache)
        edited = restamp(dict(values[0], text='retained new body'))
        batch(cache, attempt, [unit(memo, 'group', [edited], 200)], 200, memo)
        assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == 'base'
    with closing(Cache(path)) as cache:
        with cache.write():
            attempt = CaptureCoverage.resume(cache, token='child', binding='child-binding')
            result = attempt.finish(at=200, run=run(200))
            cache.set_checkpoint('whatsapp', SCOPE, 'test-receipt', {'token': 'child'})
        assert result['absent'] == 1
        assert cache.get(values[0]['urn'])['text'] == 'retained new body'
        assert cache.get(values[1]['urn'])['text'] == 'body-2'
        assert cache.get(values[1]['urn'])['observed_at'] == 100
        assert cache.observations_of(values[1]['urn'])[0]['present'] == 'absent_from_source'
        assert cache.observations_of(values[2]['urn'])[0]['observed_at'] == 200
        assert cache.get(values[2]['urn'])['observed_at'] == 200
        assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == 'child'
        assert not cache.conn.execute('SELECT 1 FROM capture_coverage_attempts').fetchone()


def test_finish_receipt_and_sightings_rollback_together(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        _, values = baseline(cache)
        attempt = child(cache)
        before = cache.conn.execute('SELECT count(*) FROM changes').fetchone()[0]
        with pytest.raises(RuntimeError, match='final crash'), cache.write():
            attempt.finish(at=200, run=run(200))
            cache.set_checkpoint('whatsapp', SCOPE, 'test-receipt', {'token': 'child'})
            raise RuntimeError('final crash')
        assert cache.conn.execute('SELECT count(*) FROM changes').fetchone()[0] == before
        assert cache.get_checkpoint('whatsapp', SCOPE, 'test-receipt') is None
        assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == 'base'
        assert cache.observations_of(values[0]['urn'])[0]['present'] == 'present'
        with cache.write():
            result = attempt.finish(at=200, run=run(200))
        assert result['absent'] == 2


def test_unrelated_writer_or_other_point_cannot_reuse_attempt(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        _, values = baseline(cache)
        attempt = child(cache)
        with cache.write():
            with pytest.raises(CoverageUnavailable, match='conflicting'):
                CaptureCoverage.resume(cache, token=attempt.token, binding='wrong-binding')
            with pytest.raises(CoverageUnavailable, match='owns this namespace'):
                CaptureCoverage.begin(cache, source='whatsapp', scope=SCOPE, token='other', binding='other', parent='base', full=False)
        with cache.write():
            cache.upsert(restamp(dict(values[2], text='unrelated writer edit')), observed_at=201)
        with cache.write(), pytest.raises(CoverageUnavailable, match='unrelated semantic writer'):
            CaptureCoverage.resume(cache, token=attempt.token, binding=attempt.binding)
        assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == 'base'


def test_captured_batch_failure_rolls_back_staging_and_expected_epoch(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        _, values = baseline(cache)
        attempt = child(cache)
        old = tuple(cache.conn.execute('SELECT epoch,staged_records,staged_observations FROM capture_coverage_attempts').fetchone())
        with pytest.raises(RuntimeError, match='batch crash'), cache.write(), attempt.writing():
            cache.upsert(restamp(dict(values[0], text='uncommitted')), observed_at=200)
            attempt.stage([[1, 1]], [])
            raise RuntimeError('batch crash')
        assert tuple(cache.conn.execute('SELECT epoch,staged_records,staged_observations FROM capture_coverage_attempts').fetchone()) == old
        with cache.write():
            assert CaptureCoverage.resume(cache, token=attempt.token, binding=attempt.binding)


def test_header_withdrawal_retires_missing_headers_but_resights_message_units(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        header = record('chats', uuid.uuid4().urn, 'whatsapp', SCOPE, native_key='chat-key', kind='direct', assignment='source')
        baseline(cache, headers=[Emit(header, Observation('chat', [1], 'chat-key', 'header-native'))])
        with cache.write():
            epoch = read_source(cache.conn, 'whatsapp', SCOPE)
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, [], expected_epoch=epoch).with_headers(cache, ['chat'])
            attempt = CaptureCoverage.begin(cache, source='whatsapp', scope=SCOPE, token='headers', binding='header-binding', parent='base', full=False, withdrawal=proof)
            result = attempt.finish(at=200, run=run(200))
        assert result['absent'] == 1
        assert cache.get(header['urn'])['observed_at'] == 100
        assert cache.observations_of(header['urn'])[0]['present'] == 'absent_from_source'
        row = cache.conn.execute('SELECT records,observations FROM capture_coverage_points').fetchone()
        assert json.loads(row[0]) and json.loads(row[1])
        assert cache.coverage()['messages'] == 3


def test_clean_coverage_continuation_uses_only_ranges_and_resights_old_heads(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        _, values = baseline(cache)
        statements = []
        with cache.write():
            proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, [], expected_epoch=read_source(cache.conn, 'whatsapp', SCOPE))
            attempt = CaptureCoverage.begin(cache, source='whatsapp', scope=SCOPE, token='clean', binding='clean-binding', parent='base', full=False, withdrawal=proof)
            cache.conn.set_trace_callback(statements.append)
            assert attempt.finish(at=200, run=run(200))['absent'] == 0
            cache.conn.set_trace_callback(None)
        assert not any('FROM records' in sql or 'FROM source_observations' in sql or 'FROM native_units' in sql for sql in statements)
        assert cache.get(values[0]['urn'])['observed_at'] == 200
