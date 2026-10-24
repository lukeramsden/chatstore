import random
from contextlib import closing

from chatstore.cache import Cache
from chatstore.canonical.records import restamp
from chatstore.curation import purge_urns
from tests.unit.test_cache import msg


def reference(cache, source=None, scope=None):
    query = "SELECT min(utc_ms),max(utc_ms),count(*) FROM messages_idx WHERE kind='message'"
    args = []
    for column, value in (('source', source), ('account_scope', scope)):
        if value is not None:
            query += f' AND {column}=?'
            args.append(value)
    row = cache.conn.execute(query, args).fetchone()
    return {'earliest_utc_ms': row[0], 'latest_utc_ms': row[1], 'messages': row[2]}


def test_incremental_counts_and_bounds_match_full_scan_across_changes_and_namespaces(tmp_path):
    rng = random.Random(49)
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        for step in range(100):
            with cache.write():
                row = rng.randrange(12)
                value = msg(row, f'text-{step}', None, None, rng.randrange(-100, 100))
                value.update(source=rng.choice(['messages', 'whatsapp']), account_scope=rng.choice(['one', 'two']), kind=rng.choice(['message', 'message', 'system']))
                if rng.randrange(4) == 0:
                    purge_urns(cache, [value['urn']])
                else:
                    cache.upsert(restamp(value), observed_at=step)
            for source, scope in ((None, None), ('messages', None), (None, 'two'), ('whatsapp', 'one'), ('missing', 'missing')):
                assert cache.coverage(source, scope) == reference(cache, source, scope)


def test_counts_rollback_and_unknown_dates(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        value = msg(1, 'date unknown', None, None, 1)
        value['sent_at'] = None
        with cache.write():
            cache.upsert(restamp(value), observed_at=1)
        assert cache.coverage() == {'messages': 1, 'earliest_utc_ms': None, 'latest_utc_ms': None}
        cache.conn.execute('BEGIN IMMEDIATE')
        cache.conn.execute('DELETE FROM records')
        assert cache.coverage()['messages'] == 0
        cache.conn.execute('ROLLBACK')
        assert cache.coverage()['messages'] == 1


def test_summary_vm_work_is_bounded_and_does_not_deserialize_records(tmp_path):
    with closing(Cache(tmp_path / 'cache.sqlite')) as cache:
        with cache.write():
            cache.upsert_many([msg(row, 'indexed', None, None, row) for row in range(10000)], observed_at=1)
        callbacks = []
        statements = []
        cache.conn.set_progress_handler(lambda: callbacks.append(1) or 0, 100)
        cache.conn.set_trace_callback(statements.append)
        result = cache.coverage('whatsapp')
        cache.conn.set_progress_handler(None, 0)
        cache.conn.set_trace_callback(None)
        assert result['messages'] == 10000
        assert len(callbacks) * 100 < 1000
        assert not any('FROM records' in query for query in statements)


def test_legacy_readonly_fallback_and_one_time_projection_rebuild(tmp_path):
    path = tmp_path / 'cache.sqlite'
    with closing(Cache(path)) as cache, cache.write():
        cache.upsert(msg(1, 'legacy', None, None, 100), observed_at=1)
        cache.conn.execute("DELETE FROM meta WHERE key='message_counts_version'")
    with closing(Cache(path, readonly=True)) as legacy:
        assert not legacy.has_message_counts
        assert legacy.coverage() == reference(legacy)
    with closing(Cache(path)) as migrated:
        assert migrated.has_message_counts
        assert migrated.coverage() == reference(migrated)
        migrated.conn.execute('DROP TABLE message_counts')
    with closing(Cache(path)) as repaired:
        assert repaired.coverage() == reference(repaired)
