import random
import sqlite3
import sys
from contextlib import closing

import pytest

from chatstore.adapters.page_index import PageIndex
from chatstore.adapters.pages import PageUnavailable, SQLitePages
from chatstore.adapters.source_view import SourceReadView, WalSource
from chatstore.adapters.wal import WalPages, read_changes
from chatstore.adapters.wal_image import WalPageImage
from tests.unit import test_page_index
from tests.unit.test_page_index import (
    contents,
    metadata,
    snapshot,
)

history = test_page_index.history


def advance(history, tmp_path, step):
    writer, path, roots, cursor, index = history
    change = read_changes(path.with_name(path.name + '-wal'), cursor, max_frames=None)
    with closing(sqlite3.connect(path)) as reader, index.snapshot() as old, WalPages(path.with_name(path.name + '-wal'), change) as pages:
        reader.execute('BEGIN')
        schema = reader.execute('PRAGMA schema_version').fetchone()[0]
        view = SourceReadView(reader, change, schema, (path.stat().st_dev, path.stat().st_ino), False)
        image = WalPageImage(view, pages, old, expected_point=f'p{step - 1}', schema_tag=str(schema))
        selected = index.update(image, roots, change.pages, expected_point=f'p{step - 1}', point=f'p{step}', schema_tag=str(schema))
        reads = pages.pages_read
    with PageIndex(tmp_path / f'oracle-{step}.sqlite') as oracle, SQLitePages(snapshot(writer, tmp_path / f'after-{step}.sqlite')) as image:
        oracle.build(image, roots, point=f'p{step}', schema_tag=str(schema))
        assert metadata(index) == metadata(oracle)
    return (writer, path, roots, change.cursor, index), selected, reads


def test_wal_overlay_structural_updates_match_full_images(history, tmp_path):
    rng = random.Random(33)
    for step in range(1, 16):
        writer = history[0]
        before = contents(writer)
        if step == 3:
            writer.execute('DELETE FROM message WHERE id>5')
            writer.execute('UPDATE other SET payload=? WHERE id=1', (b'o' * 50000,))
        elif step == 4:
            with writer.blobopen('other', 'payload', 1) as blob:
                blob.seek(20000)
                blob.write(b'Z')
        elif step == 5:
            writer.execute('UPDATE other SET payload=? WHERE id=1', (b'small',))
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b't' * 150) for i in range(100, 400)))
        else:
            for _ in range(10):
                table = rng.choice(['message', 'other'])
                row = rng.randint(-10, 2100)
                writer.execute(f'INSERT OR REPLACE INTO {table} VALUES (?,?)', (row, b'x' * rng.choice([60, 300, 2000])))
        writer.commit()
        history, selected, _ = advance(history, tmp_path, step)
        after = contents(writer)
        for name in before:
            difference = {row for row in before[name].keys() | after[name].keys() if before[name].get(row) != after[name].get(row)}
            assert difference <= selected.rows.get(name, frozenset())


def test_wal_overlay_clean_and_old_edit_read_only_changed_frames(history, tmp_path):
    history, selected, reads = advance(history, tmp_path, 1)
    assert not selected.rows and reads == 0
    writer = history[0]
    writer.execute('UPDATE message SET payload=? WHERE id=1', (b'y' * 60,))
    writer.commit()
    _, selected, reads = advance(history, tmp_path, 2)
    assert 1 in selected.rows['message'] and len(selected.rows['message']) < 20
    assert selected.tree_pages_read < 10 and reads == 1


def test_wal_overlay_rejects_wrong_point_schema_baseline_and_closed_pin(history):
    _, path, _, cursor, index = history
    change = read_changes(path.with_name(path.name + '-wal'), cursor)
    with closing(sqlite3.connect(path)) as reader, index.snapshot() as old, WalPages(path.with_name(path.name + '-wal'), change) as pages:
        reader.execute('BEGIN')
        schema = reader.execute('PRAGMA schema_version').fetchone()[0]
        args = (reader, change, schema, (path.stat().st_dev, path.stat().st_ino))
        view = SourceReadView(*args, False)
        for point, tag in (('wrong', str(schema)), ('p0', 'wrong')):
            with pytest.raises(PageUnavailable, match='baseline or schema'):
                WalPageImage(view, pages, old, expected_point=point, schema_tag=tag)
        with pytest.raises(PageUnavailable, match='continuation'):
            WalPageImage(SourceReadView(*args, True), pages, old, expected_point='p0', schema_tag=str(schema))
        image = WalPageImage(view, pages, old, expected_point='p0', schema_tag=str(schema))
        reader.rollback()
        with pytest.raises(PageUnavailable, match='no longer pinned'):
            image.check_stable()


@pytest.mark.skipif(sys.platform != 'darwin', reason='source coordinator requires macOS vnode monitoring')
def test_coordinated_overlay_keeps_newer_commit_beyond_selected_point(history, tmp_path):
    writer, path, roots, _, index = history
    with WalSource(path) as source:
        with source.view(max_frames=None) as baseline:
            cursor = baseline.cursor
        writer.execute('UPDATE message SET payload=? WHERE id=1', (b'y' * 60,))
        writer.commit()
        with source.view(cursor) as view, index.snapshot() as old, WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
            image = WalPageImage(view, pages, old, expected_point='p0', schema_tag=str(view.schema_version))
            writer.execute('UPDATE message SET payload=? WHERE id=2000', (b'z' * 60,))
            writer.commit()
            selected = index.update(image, roots, view.changes.pages, expected_point='p0', point='p1', schema_tag=str(view.schema_version))
            assert 1 in selected.rows['message'] and 2000 not in selected.rows['message']
            assert view.connection.execute('SELECT payload FROM message WHERE id=2000').fetchone()[0] == b'x' * 60
            cursor = view.cursor
        with source.view(cursor) as view, index.snapshot() as old, WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
            image = WalPageImage(view, pages, old, expected_point='p1', schema_tag=str(view.schema_version))
            selected = index.update(image, roots, view.changes.pages, expected_point='p1', point='p2', schema_tag=str(view.schema_version))
            assert 2000 in selected.rows['message'] and 1 not in selected.rows['message']
