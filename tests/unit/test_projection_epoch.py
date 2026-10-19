import pytest

from chatstore.cache import Cache, SourceObservation
from chatstore.cache.projection_epoch import read, read_source
from chatstore.canonical.records import restamp
from chatstore.curation import purge_entity
from tests.unit.test_cache import SCOPE, msg

OTHER_SCOPE = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'


def test_scoped_epochs_ignore_other_sources_scopes_and_freshness(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite')
    try:
        first = msg(1, 'first', None, None, 1)
        other_source = restamp(dict(msg(2, 'other', None, None, 1), source='messages'))
        other_scope = restamp(dict(msg(3, 'other scope', None, None, 1), account_scope=OTHER_SCOPE))
        assert read_source(cache.conn, 'whatsapp', SCOPE) == 0
        with cache.write():
            cache.upsert(first, observed_at=1)
        own = read_source(cache.conn, 'whatsapp', SCOPE)
        legacy = read(cache.conn)
        with cache.write():
            cache.upsert(other_source, observed_at=1)
            cache.upsert(other_scope, observed_at=1)
        assert read_source(cache.conn, 'whatsapp', SCOPE) == own
        assert read_source(cache.conn, 'messages', SCOPE) > 0
        assert read_source(cache.conn, 'whatsapp', OTHER_SCOPE) > 0
        assert read(cache.conn) > legacy  # An already-running older monitor still invalidates.
        with cache.write():
            cache.upsert(first, observed_at=2)
        assert read_source(cache.conn, 'whatsapp', SCOPE) == own
        with pytest.raises(RuntimeError), cache.write():
            cache.upsert(restamp(dict(first, text='rolled back')), observed_at=3)
            assert read_source(cache.conn, 'whatsapp', SCOPE) > own
            raise RuntimeError('interrupted writer')
        assert read_source(cache.conn, 'whatsapp', SCOPE) == own
        with cache.write():
            purge_entity(cache, first['urn'])
        assert read_source(cache.conn, 'whatsapp', SCOPE) > own
    finally:
        cache.close()


def test_provenance_generation_changes_only_for_its_source_and_semantic_evidence(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite')
    try:
        r = msg(1, 'first', None, None, 1)
        obs = SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 1, None)
        with cache.write():
            cache.upsert(r, observed_at=1)
            cache.observe(obs)
        own = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            cache.observe(obs._replace(observed_at=2))
        assert read_source(cache.conn, 'whatsapp', SCOPE) == own
        with cache.write():
            cache.observe(obs._replace(fingerprint='changed', observed_at=3))
        assert read_source(cache.conn, 'whatsapp', SCOPE) > own
        assert read_source(cache.conn, 'messages', SCOPE) == 0
        before = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            cache.conn.execute('UPDATE source_observations SET account_scope=?', (OTHER_SCOPE,))
        assert read_source(cache.conn, 'whatsapp', SCOPE) > before
        assert read_source(cache.conn, 'whatsapp', OTHER_SCOPE) > 0
    finally:
        cache.close()


def test_record_projection_move_invalidates_both_scopes(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite')
    try:
        with cache.write():
            cache.upsert(msg(1, 'first', None, None, 1), observed_at=1)
        before = read_source(cache.conn, 'whatsapp', SCOPE)
        with cache.write():
            cache.conn.execute('UPDATE records SET source=?,account_scope=?', ('messages', OTHER_SCOPE))
        assert read_source(cache.conn, 'whatsapp', SCOPE) > before
        assert read_source(cache.conn, 'messages', OTHER_SCOPE) > 0
    finally:
        cache.close()


def test_global_alias_remains_journaled_without_inventing_a_native_namespace(tmp_path):
    from chatstore.canonical.records import record

    cache = Cache(tmp_path / 'cache.sqlite')
    try:
        before = read(cache.conn)
        with cache.write():
            alias = record('aliases', 'urn:uuid:00000000-0000-0000-0000-00000000000a', None, None,
                           old_urn='urn:uuid:00000000-0000-0000-0000-000000000001',
                           new_urn='urn:uuid:00000000-0000-0000-0000-000000000002',
                           reason='manual', evidence=[], created_at=1)
            cache.upsert(alias, observed_at=1)
        assert read(cache.conn) > before
        assert list(cache.conn.execute('SELECT * FROM source_projection_epochs')) == []
        assert cache.conn.execute("SELECT count(*) FROM changes WHERE operation='record'").fetchone()[0] == 1
    finally:
        cache.close()


def test_existing_scoped_trigger_upgrade_invalidates_old_summaries_once(tmp_path):
    path = tmp_path / 'cache.sqlite'
    cache = Cache(path)
    with cache.write():
        cache.upsert(msg(1, 'before upgrade', None, None, 1), observed_at=1)
        cache.conn.execute("DELETE FROM meta WHERE key='source_epoch_version'")
        cache.conn.execute('DROP TRIGGER source_epoch_records_INSERT')
        cache.conn.execute("""CREATE TRIGGER source_epoch_records_INSERT AFTER INSERT ON records
            BEGIN SELECT RAISE(ABORT,'obsolete scoped trigger'); END""")
    before = read_source(cache.conn, 'whatsapp', SCOPE)
    cache.close()
    cache = Cache(path)
    try:
        assert read_source(cache.conn, 'whatsapp', SCOPE) > before
        with cache.write():
            cache.upsert(msg(2, 'after upgrade', None, None, 2), observed_at=2)
        after = read_source(cache.conn, 'whatsapp', SCOPE)
    finally:
        cache.close()
    cache = Cache(path)
    try:
        assert read_source(cache.conn, 'whatsapp', SCOPE) == after
    finally:
        cache.close()
