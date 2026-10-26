import random
import sqlite3
from contextlib import closing

import pytest

from chatstore.adapters.page_index import PageIndex
from chatstore.adapters.pages import PageUnavailable, SQLitePages
from chatstore.adapters.wal import read_changes


def snapshot(writer, path):
    with closing(sqlite3.connect(path)) as copy:
        writer.backup(copy)
    return path


def contents(writer):
    return {name: dict(writer.execute(f'SELECT rowid,payload FROM {name}')) for name in ('message', 'other')}


def metadata(index):
    return {table: [tuple(row) for row in index.conn.execute(f'SELECT * FROM cs_native_page_{table} ORDER BY 1')]
            for table in ('roots', 'nodes', 'edges', 'overflow')}


@pytest.fixture(params=[0, 1])
def history(tmp_path, request):
    path = tmp_path / 'source.sqlite'
    writer = sqlite3.connect(path)
    writer.execute('PRAGMA page_size=512')
    writer.execute(f'PRAGMA auto_vacuum={request.param}')
    writer.execute('PRAGMA journal_mode=WAL')
    writer.execute('PRAGMA wal_autocheckpoint=0')
    for name in ('message', 'other'):
        writer.execute(f'CREATE TABLE {name}(id INTEGER PRIMARY KEY,payload BLOB)')
        writer.executemany(f'INSERT INTO {name} VALUES (?,?)', ((i, b'x' * 60) for i in range(1, 2001)))
    writer.commit()
    roots = dict(writer.execute("SELECT name,rootpage FROM sqlite_schema WHERE name IN ('message','other')"))
    cursor = read_changes(path.with_name(path.name + '-wal'), max_frames=None).cursor
    index = PageIndex(tmp_path / 'index.sqlite')
    with SQLitePages(snapshot(writer, tmp_path / 'base.sqlite')) as image:
        index.build(image, roots, point='p0', schema_tag=str(writer.execute('PRAGMA schema_version').fetchone()[0]))
    yield writer, path, roots, cursor, index
    index.close()
    writer.close()


def advance(history, tmp_path, step, expected_before):
    writer, path, roots, cursor, index = history
    change = read_changes(path.with_name(path.name + '-wal'), cursor, max_frames=None)
    after = snapshot(writer, tmp_path / f'image-{step}.sqlite')
    schema_tag = str(writer.execute('PRAGMA schema_version').fetchone()[0])
    with SQLitePages(after) as image:
        if index.conn.execute('SELECT schema_tag FROM cs_native_page_state').fetchone()[0] != schema_tag:
            with pytest.raises(PageUnavailable, match='baseline'):
                index.update(image, roots, change.pages, expected_point=f'p{step - 1}', point=f'p{step}', schema_tag=schema_tag)
            candidates = index.build(image, roots, point=f'p{step}', schema_tag=schema_tag)
        else:
            candidates = index.update(image, roots, change.pages, expected_point=f'p{step - 1}', point=f'p{step}', schema_tag=schema_tag)
    expected_after = contents(writer)
    for name, old in expected_before.items():
        new = expected_after[name]
        changed = {key for key in old.keys() | new.keys() if old.get(key) != new.get(key)}
        assert changed <= candidates.rows.get(name, frozenset())
    with PageIndex(tmp_path / f'reference-{step}.sqlite') as reference, SQLitePages(after) as image:
        reference.build(image, roots, point=f'p{step}', schema_tag=schema_tag)
        assert metadata(index) == metadata(reference)
    return (writer, path, roots, change.cursor, index), candidates


def test_single_old_edit_does_not_walk_unrelated_history(history, tmp_path):
    writer, _, _, _, index = history
    before = contents(writer)
    writer.execute("UPDATE message SET payload=? WHERE id=1", (b'y' * 60,))
    writer.commit()
    _, candidates = advance(history, tmp_path, 1, before)
    assert candidates.tree_pages_read < 10
    assert len(candidates.rows['message']) < 20
    assert 'other' not in candidates.rows
    with SQLitePages(snapshot(writer, tmp_path / 'clean.sqlite')) as image:
        clean = index.update(image, history[2], [], expected_point='p1', point='clean', schema_tag=str(writer.execute('PRAGMA schema_version').fetchone()[0]))
    assert clean.rows == {} and clean.tree_pages_read == 0


def test_splits_removed_subtrees_overflow_reuse_and_truncation(history, tmp_path):
    for step, operation in enumerate(('grow', 'medium_shrink', 'overflow', 'shrink', 'empty', 'reuse', 'vacuum'), 1):
        writer = history[0]
        before = contents(writer)
        if operation == 'grow':
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b'g' * 80) for i in range(2001, 6001)))
        elif operation == 'medium_shrink':
            writer.execute('DELETE FROM message WHERE id>250')
        elif operation == 'overflow':
            writer.execute('UPDATE message SET payload=? WHERE id=1', (b'large' * 20000,))
            writer.execute('UPDATE other SET payload=? WHERE id=10', (b'second' * 15000,))
        elif operation == 'shrink':
            writer.execute('DELETE FROM message WHERE id>10')
            writer.execute('UPDATE message SET payload=? WHERE id=1', (b'small',))
        elif operation == 'empty':
            writer.execute('DELETE FROM message')
        elif operation == 'reuse':
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b'r' * 300) for i in range(-50, 50)))
            writer.execute('DELETE FROM other WHERE id>100')
        else:
            writer.commit()
            writer.execute('VACUUM')
        writer.commit()
        history, _ = advance(history, tmp_path, step, before)


def test_random_native_updates_match_fresh_page_maps(history, tmp_path):
    rng = random.Random(24)
    for step in range(1, 16):
        writer = history[0]
        before = contents(writer)
        for _ in range(10):
            name = rng.choice(['message', 'other'])
            row = rng.randint(-20, 2100)
            if rng.randrange(3) == 0:
                writer.execute(f'DELETE FROM {name} WHERE id=?', (row,))
            else:
                writer.execute(f'INSERT OR REPLACE INTO {name} VALUES (?,?)',
                               (row, bytes([step]) * rng.choice([5, 60, 300, 1800])))
        writer.commit()
        history, _ = advance(history, tmp_path, step, before)


def test_wrong_baseline_or_bad_image_rolls_back_index(history, tmp_path):
    writer, _, roots, _, index = history
    before = metadata(index)
    image_path = snapshot(writer, tmp_path / 'bad.sqlite')
    with SQLitePages(image_path) as image, pytest.raises(PageUnavailable, match='baseline'):
        index.update(image, roots, [], expected_point='wrong', point='p1', schema_tag=str(writer.execute('PRAGMA schema_version').fetchone()[0]))
    assert metadata(index) == before
    leaf = index.conn.execute("SELECT page FROM cs_native_page_nodes WHERE name='message' AND rows!='[]' LIMIT 1").fetchone()[0]
    with image_path.open('r+b') as file:
        file.seek((leaf - 1) * 512)
        file.write(b'\0')
    with SQLitePages(image_path) as image, pytest.raises(PageUnavailable, match='rowid-table'):
        index.update(image, roots, [leaf], expected_point='p0', point='p1', schema_tag=str(writer.execute('PRAGMA schema_version').fetchone()[0]))
    assert metadata(index) == before
    assert index.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == 'p0'


def test_overflow_only_write_finds_owner_without_leaf_wal_frame(history, tmp_path):
    writer, _, _, _, _ = history
    before = contents(writer)
    writer.execute('UPDATE message SET payload=? WHERE id=1', (b'x' * 50000,))
    writer.commit()
    history, _ = advance(history, tmp_path, 1, before)
    writer, path, _, cursor, index = history
    leaf = index.conn.execute('SELECT leaf FROM cs_native_page_overflow WHERE row_id=1 LIMIT 1').fetchone()[0]
    before = contents(writer)
    with writer.blobopen('message', 'payload', 1) as blob:
        blob.seek(20000)
        blob.write(b'Z')
    writer.commit()
    changes = read_changes(path.with_name(path.name + '-wal'), cursor)
    assert leaf not in changes.pages
    _, candidates = advance(history, tmp_path, 2, before)
    assert 1 in candidates.rows['message']
    assert candidates.tree_pages_read < 10


def test_schema_tag_change_requires_explicit_rebuild(history, tmp_path):
    writer, path, roots, cursor, index = history
    before = metadata(index)
    writer.execute("ALTER TABLE message ADD COLUMN extra TEXT DEFAULT 'new context'")
    writer.commit()
    changes = read_changes(path.with_name(path.name + '-wal'), cursor)
    image_path = snapshot(writer, tmp_path / 'new-schema.sqlite')
    schema = str(writer.execute('PRAGMA schema_version').fetchone()[0])
    with SQLitePages(image_path) as image, pytest.raises(PageUnavailable, match='baseline'):
        index.update(image, roots, changes.pages, expected_point='p0', point='p1', schema_tag=schema)
    assert metadata(index) == before
    assert index.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == 'p0'


def test_index_reopen_and_incompatible_metadata_format(history, tmp_path):
    writer, _, roots, _, index = history
    path = tmp_path / 'index.sqlite'
    with PageIndex(path) as reopened, SQLitePages(snapshot(writer, tmp_path / 'reopened.sqlite')) as image:
        result = reopened.update(image, roots, [], expected_point='p0', point='same',
                                 schema_tag=str(writer.execute('PRAGMA schema_version').fetchone()[0]))
        assert result.rows == {} and result.tree_pages_read == 0
        assert metadata(reopened) == metadata(index)
    wrong = tmp_path / 'wrong-index.sqlite'
    with sqlite3.connect(wrong) as db:
        db.execute('CREATE TABLE cs_native_page_state(id INTEGER,point TEXT)')
    with pytest.raises(PageUnavailable, match='index format'):
        PageIndex(wrong)


def test_same_transaction_reuses_freed_tree_pages_for_overflow(history, tmp_path):
    writer = history[0]
    before = contents(writer)
    writer.execute('DELETE FROM message WHERE id>5')
    writer.execute('UPDATE other SET payload=? WHERE id=1', (b'new payload' * 20000,))
    writer.commit()
    history, _ = advance(history, tmp_path, 1, before)
    before = contents(writer)
    writer.execute('UPDATE other SET payload=? WHERE id=1', (b'small',))
    writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b'new leaf' * 30) for i in range(100, 500)))
    writer.commit()
    advance(history, tmp_path, 2, before)


@pytest.mark.parametrize('commit', [False, True])
def test_index_and_mirror_rows_share_outer_transaction_and_old_reader(history, tmp_path, commit):
    writer, _, roots, _, index = history
    index.conn.execute('CREATE TABLE nodes(id INTEGER PRIMARY KEY,payload TEXT)')
    index.conn.execute("INSERT INTO nodes VALUES (1,'old mirror value')")
    before = contents(writer)
    writer.execute('UPDATE message SET payload=? WHERE id=1', (b'new' * 20,))
    writer.commit()
    changes = read_changes(history[1].with_name(history[1].name + '-wal'), history[3])
    with index.snapshot() as old, SQLitePages(snapshot(writer, tmp_path / 'outer.sqlite')) as image:
        assert old.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == 'p0'
        index.conn.execute('BEGIN IMMEDIATE')
        try:
            result = index.update(image, roots, changes.pages, expected_point='p0', point='p1',
                                  schema_tag=str(writer.execute('PRAGMA schema_version').fetchone()[0]))
            assert 1 in result.rows['message']
            assert index.conn.in_transaction
            index.conn.execute("UPDATE nodes SET payload='prepared mirror value' WHERE id=1")
            assert old.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == 'p0'
            assert old.execute('SELECT payload FROM nodes').fetchone()[0] == 'old mirror value'
            index.conn.execute('COMMIT' if commit else 'ROLLBACK')
            assert old.execute('SELECT payload FROM nodes').fetchone()[0] == 'old mirror value'
        finally:
            if index.conn.in_transaction:
                index.conn.rollback()
    assert index.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == ('p1' if commit else 'p0')
    assert index.conn.execute('SELECT payload FROM nodes').fetchone()[0] == ('prepared mirror value' if commit else 'old mirror value')
    assert contents(writer) != before
