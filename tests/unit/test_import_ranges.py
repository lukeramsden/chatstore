import random
import sqlite3

import pytest

from chatstore.cache.import_ranges import install, translate


@pytest.fixture
def conn():
    db = sqlite3.connect(':memory:')
    db.executescript('''
        CREATE TABLE records(urn TEXT PRIMARY KEY, revision_digest TEXT);
        CREATE TABLE entity_keys(id INTEGER PRIMARY KEY, urn TEXT);
        CREATE TABLE entity_ids(producer TEXT, entity_id INTEGER, local_id INTEGER, head_digest TEXT,
            PRIMARY KEY(producer,entity_id));
        CREATE TABLE observation_ids(feed_id TEXT,observation_id INTEGER,local_id INTEGER,evidence TEXT,
            PRIMARY KEY(feed_id,observation_id));
        CREATE TABLE source_observations(observation_id INTEGER PRIMARY KEY,entity_urn TEXT,
            source TEXT,account_scope TEXT,native_table TEXT,native_row_ids TEXT,native_key TEXT,
            minted INTEGER,fingerprint TEXT,adapter_version TEXT,present TEXT);
    ''')
    install(db)
    yield db
    db.close()


def seed(db, n):
    db.executemany('INSERT INTO records VALUES (?,?)', [(str(i), 'head') for i in range(1, n + 1)])
    db.executemany('INSERT INTO entity_keys VALUES (?,?)', [(i, str(i)) for i in range(1, n + 1)])
    db.executemany('INSERT INTO entity_ids VALUES (?,?,?,?)', [('p', i, i, 'head') for i in range(1, n + 1)])
    db.commit()


def test_clean_sighting_never_reads_individual_keys(conn):
    seed(conn, 20000)
    assert translate(conn, 'p', 'record', [[1, 20000]]) == [[1, 20000]]
    assert conn.execute('SELECT count(*) FROM import_key_ranges').fetchone()[0] == 1

    def authorize(action, table, *_):
        if action == sqlite3.SQLITE_READ and table in {'records', 'entity_keys', 'entity_ids'}:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(authorize)
    try:
        assert translate(conn, 'p', 'record', [[15, 19998]]) == [[15, 19998]]
    finally:
        conn.set_authorizer(None)


def test_changed_points_split_merge_and_rollback(conn):
    seed(conn, 20)
    assert translate(conn, 'p', 'record', [[1, 20]]) == [[1, 20]]
    conn.commit()
    conn.execute("UPDATE records SET revision_digest='local' WHERE urn='10'")
    assert translate(conn, 'p', 'record', [[1, 20]]) == [[1, 9], [11, 20]]
    conn.rollback()
    assert translate(conn, 'p', 'record', [[1, 20]]) == [[1, 20]]
    conn.execute("UPDATE records SET revision_digest='local' WHERE urn='10'")
    assert translate(conn, 'p', 'record', [[1, 20]]) == [[1, 9], [11, 20]]
    conn.execute("UPDATE records SET revision_digest='head' WHERE urn='10'")
    assert translate(conn, 'p', 'record', [[1, 20]]) == [[1, 20]]
    assert conn.execute('SELECT count(*) FROM import_key_ranges').fetchone()[0] == 1
    conn.execute("DELETE FROM records WHERE urn='1'")
    assert translate(conn, 'p', 'record', [[1, 20]]) == [[2, 20]]
    conn.execute("DELETE FROM entity_ids WHERE entity_id=5")
    with pytest.raises(ValueError, match='unknown producer keys'):
        translate(conn, 'p', 'record', [[1, 20]])


def test_permuted_mapping_matches_individual_reference(conn):
    seed(conn, 100)
    rng = random.Random(10)
    for _ in range(50):
        key, local = rng.randint(1, 100), rng.randint(1, 100)
        conn.execute("UPDATE entity_ids SET local_id=? WHERE producer='p' AND entity_id=?", (local, key))
        conn.execute('UPDATE records SET revision_digest=? WHERE urn=?',
                     (rng.choice(['head', 'other']), str(rng.randint(1, 100))))
        lo, hi = sorted([rng.randint(1, 100), rng.randint(1, 100)])
        expected = {r[0] for r in conn.execute('''SELECT m.local_id FROM entity_ids m
            JOIN entity_keys k ON k.id=m.local_id JOIN records r ON r.urn=k.urn
            WHERE producer='p' AND entity_id BETWEEN ? AND ? AND r.revision_digest=m.head_digest''', (lo, hi))}
        actual = {i for a, b in translate(conn, 'p', 'record', [[lo, hi]]) for i in range(a, b + 1)}
        assert actual == expected


def test_mapping_change_on_one_producer_invalidates_others_local_head(conn):
    seed(conn, 5)
    conn.execute("INSERT INTO entity_ids SELECT 'q',entity_id,local_id,head_digest FROM entity_ids")
    for producer in ('p', 'q'):
        assert translate(conn, producer, 'record', [[1, 5]]) == [[1, 5]]
    conn.execute("UPDATE records SET revision_digest='new' WHERE urn='3'")
    conn.execute("UPDATE entity_ids SET head_digest='new' WHERE producer='p' AND entity_id=3")
    assert translate(conn, 'p', 'record', [[1, 5]]) == [[1, 5]]
    assert translate(conn, 'q', 'record', [[1, 5]]) == [[1, 2], [4, 5]]
