"""Bulk checkpoints bound committed WAL growth without invalidating readers."""

import sqlite3
from contextlib import closing

import pytest

from chatstore.cache import Cache, store


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(store, '_BULK_WAL_BYTES', 128 * 1024)
    with closing(Cache(tmp_path / 'cache.sqlite3')) as cache:
        with cache.write():
            cache.conn.execute('CREATE TABLE bulk_payload(id INTEGER PRIMARY KEY,body BLOB)')
        cache.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        cache.conn.execute('PRAGMA wal_autocheckpoint=49')
        yield cache


def test_bulk_reuses_wal_restores_settings_and_keeps_committed_batches_after_failure(cache):
    payload = b'x' * (16 * 1024)
    wal = cache.path.with_name(cache.path.name + '-wal')
    sizes = []
    synchronous = cache.conn.execute('PRAGMA synchronous').fetchone()[0]
    cache_size = cache.conn.execute('PRAGMA cache_size').fetchone()[0]
    with pytest.raises(RuntimeError, match='interrupted batch'), cache.bulk():
        for i in range(24):
            with cache.write():
                cache.conn.execute('INSERT INTO bulk_payload VALUES(?,?)', (i, payload))
            sizes.append(wal.stat().st_size)
        with cache.write():
            cache.conn.execute('INSERT INTO bulk_payload VALUES(999,?)', (payload,))
            raise RuntimeError('interrupted batch')
    assert max(sizes) < 256 * 1024
    assert sum(len(payload) for _ in sizes) > max(sizes)
    assert cache.conn.execute('PRAGMA wal_autocheckpoint').fetchone()[0] == 49
    assert cache.conn.execute('PRAGMA cache_size').fetchone()[0] == cache_size
    assert cache.conn.execute('PRAGMA synchronous').fetchone()[0] == synchronous == 2
    assert wal.stat().st_size == 0
    with closing(sqlite3.connect(cache.path)) as reopened:
        assert reopened.execute('SELECT count(*),sum(length(body)) FROM bulk_payload').fetchone() == (24, len(payload) * 24)
        assert reopened.execute('SELECT 1 FROM bulk_payload WHERE id=999').fetchone() is None


def test_bulk_checkpoint_preserves_pinned_reader_and_catches_up_after_release(cache):
    payload = b'y' * (16 * 1024)
    with cache.write():
        cache.conn.execute('INSERT INTO bulk_payload VALUES(0,?)', (payload,))
    wal = cache.path.with_name(cache.path.name + '-wal')
    with closing(sqlite3.connect(cache.path)) as reader, cache.bulk():
        reader.execute('BEGIN')
        assert reader.execute('SELECT count(*) FROM bulk_payload').fetchone()[0] == 1
        for i in range(1, 17):
            with cache.write():
                cache.conn.execute('INSERT INTO bulk_payload VALUES(?,?)', (i, payload))
        assert wal.stat().st_size > store._BULK_WAL_BYTES
        assert reader.execute('SELECT count(*) FROM bulk_payload').fetchone()[0] == 1
        reader.rollback()
        with cache.write():
            cache.conn.execute('INSERT INTO bulk_payload VALUES(17,?)', (payload,))
    assert wal.stat().st_size == 0
    assert cache.conn.execute('SELECT count(*) FROM bulk_payload').fetchone()[0] == 18
