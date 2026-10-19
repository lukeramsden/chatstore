import os
import shutil
import sqlite3
from pathlib import Path

import pytest

from chatstore.adapters.pages import PageUnavailable, SQLitePages


def _image(root: Path, page_size=1024, auto_vacuum=0):
    path = root / 'image.sqlite'
    writer = sqlite3.connect(path)
    writer.execute(f'PRAGMA page_size={page_size}')
    writer.execute(f'PRAGMA auto_vacuum={auto_vacuum}')
    writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,payload BLOB)')
    special = [-(1 << 63), -1, 0, 1, 127, 128, 16383, 16384, (1 << 31) - 1, 1 << 40, (1 << 63) - 1]
    values = [(i, b'x' * (80 + i % 100)) for i in range(1000, 4000)]
    large_sizes = {-1: 90000, 128: 160000, 16384: 70000}
    values += [(i, b'x' * large_sizes[i] if i in large_sizes else b'special') for i in special]
    writer.executemany('INSERT INTO message VALUES (?,?)', values)
    writer.commit()
    root_page = writer.execute("SELECT rootpage FROM sqlite_schema WHERE name='message'").fetchone()[0]
    writer.close()
    return path, root_page


@pytest.mark.parametrize('page_size', [512, 1024, 4096, 65536])
@pytest.mark.parametrize('auto_vacuum', [0, 1])
def test_native_rowids_and_overflow_match_sqlite(tmp_path, page_size, auto_vacuum):
    path, root = _image(tmp_path, page_size, auto_vacuum)
    with sqlite3.connect(path) as sql, SQLitePages(path) as pages:
        expected = [r[0] for r in sql.execute('SELECT rowid FROM message ORDER BY rowid')]
        locations = list(pages.rows(root))
        assert [r.row_id for r in locations] == expected
        large = next(r for r in locations if r.row_id == 128)
        assert large.overflow
        assert pages.bytes_read == 100 + pages.pages_read * page_size
        if sql.execute("SELECT sqlite_compileoption_used('ENABLE_DBSTAT_VTAB')").fetchone()[0]:
            leaves = list(sql.execute("SELECT pageno,path,ncell FROM dbstat WHERE name='message' AND pagetype='leaf' ORDER BY path"))
            native_overflow_rows = list(sql.execute("SELECT pageno,path FROM dbstat WHERE name='message' AND pagetype='overflow' ORDER BY path"))
            position = 0
            for page, path_name, count in leaves:
                assert [r.row_id for r in locations if r.page == page] == expected[position:position + count]
                own_rows = [r for r in locations if r.page == page]
                for ordinal, row in enumerate(own_rows):
                    if row.overflow:
                        prefix = f'{path_name}{ordinal:03x}+'
                        assert row.overflow == tuple(p for p, name in native_overflow_rows if name.startswith(prefix))
                position += count
            native_overflow = {r[0] for r in sql.execute("SELECT pageno FROM dbstat WHERE name='message' AND pagetype='overflow'")}
            assert {p for row in locations for p in row.overflow} == native_overflow
            assert pages.pages_read == sql.execute("SELECT count(*) FROM dbstat WHERE name='message'").fetchone()[0]


def test_deleted_rows_remain_identifiable_from_old_page_locations(tmp_path):
    path, root = _image(tmp_path)
    with SQLitePages(path) as pages:
        before = {r.row_id: r.page for r in pages.rows(root)}
    with sqlite3.connect(path) as sql:
        sql.execute('DELETE FROM message WHERE id IN (1,128,1000,3000)')
        sql.execute("INSERT INTO message VALUES (999,'older backfill')")
    with SQLitePages(path) as pages:
        after = {r.row_id: r.page for r in pages.rows(root)}
    assert set(before) - set(after) == {1, 128, 1000, 3000}
    assert set(after) - set(before) == {999}


def test_corrupt_cell_pointer_and_overflow_cycle_are_rejected(tmp_path):
    path, root = _image(tmp_path)
    with SQLitePages(path) as pages:
        locations = list(pages.rows(root))
        size = pages.page_size
    copy = tmp_path / 'bad-cell.sqlite'
    shutil.copyfile(path, copy)
    with copy.open('r+b') as f:
        f.seek((locations[0].page - 1) * size + 8)
        f.write(b'\0\0')
    with SQLitePages(copy) as pages, pytest.raises(PageUnavailable, match='cell pointer'):
        list(pages.rows(root))
    copy = tmp_path / 'bad-overflow.sqlite'
    shutil.copyfile(path, copy)
    overflow = next(r.overflow for r in locations if r.overflow)
    with copy.open('r+b') as f:
        f.seek((overflow[0] - 1) * size)
        f.write(overflow[0].to_bytes(4, 'big'))
    with SQLitePages(copy) as pages, pytest.raises(PageUnavailable, match='cyclic'):
        list(pages.rows(root))


def test_without_rowid_tables_require_reconciliation(tmp_path):
    path = tmp_path / 'without-rowid.sqlite'
    with sqlite3.connect(path) as sql:
        sql.execute('CREATE TABLE message(id TEXT PRIMARY KEY,payload TEXT) WITHOUT ROWID')
        sql.execute("INSERT INTO message VALUES ('key','text')")
        root = sql.execute("SELECT rootpage FROM sqlite_schema WHERE name='message'").fetchone()[0]
    with SQLitePages(path) as pages, pytest.raises(PageUnavailable, match='rowid-table'):
        list(pages.rows(root))


def test_snapshot_change_with_restored_mtime_is_rejected(tmp_path):
    path, root = _image(tmp_path)
    old_stat = path.stat()
    with SQLitePages(path) as pages:
        rows = pages.rows(root)
        next(rows)
        with path.open('r+b') as f:
            f.seek(-1, 2)
            byte = f.read(1)
            f.seek(-1, 2)
            f.write(bytes([byte[0] ^ 1]))
        os.utime(path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
        with pytest.raises(PageUnavailable, match='image changed'):
            pages.check_stable()
        rows.close()


def test_live_wal_is_not_a_consistent_private_page_image(tmp_path):
    path = tmp_path / 'live.sqlite'
    sql = sqlite3.connect(path)
    try:
        sql.execute('PRAGMA journal_mode=WAL')
        sql.execute('CREATE TABLE message(id INTEGER)')
        sql.commit()
        with pytest.raises(PageUnavailable, match='sidecar'):
            SQLitePages(path)
    finally:
        sql.close()


def test_schema_tree_uses_page_one_header_and_overflow_rules(tmp_path):
    path = tmp_path / 'schema.sqlite'
    with sqlite3.connect(path) as sql:
        sql.execute('PRAGMA page_size=512')
        for i in range(80):
            sql.execute(f"CREATE TABLE t{i}(id INTEGER,payload TEXT DEFAULT '{'x' * 2000}')")
        expected = [row[0] for row in sql.execute('SELECT rowid FROM sqlite_schema ORDER BY rowid')]
    with SQLitePages(path) as pages:
        assert pages.tree_page(1).children
        rows = list(pages.rows(1))
        assert [row.row_id for row in rows] == expected
        assert any(row.overflow for row in rows)


def test_empty_rowid_table_has_no_candidate_rows(tmp_path):
    path = tmp_path / 'empty.sqlite'
    with sqlite3.connect(path) as sql:
        sql.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
        root = sql.execute("SELECT rootpage FROM sqlite_schema WHERE name='message'").fetchone()[0]
    with SQLitePages(path) as pages:
        assert list(pages.rows(root)) == []


def test_tree_walk_exposes_parent_edges_without_reading_payload_overflow(tmp_path):
    path, root = _image(tmp_path)
    with SQLitePages(path) as pages:
        nodes = list(pages.tree_pages(root))
        assert nodes[0].number == root and nodes[0].children
        assert {node.number for node in nodes} == {root} | {child for node in nodes for child in node.children}
        assert pages.pages_read == len(nodes)
        assert any(cell.overflow for node in nodes for cell in node.cells)
    with sqlite3.connect(path) as sql:
        expected = [row[0] for row in sql.execute('SELECT rowid FROM message ORDER BY rowid')]
    assert [cell.row_id for node in nodes for cell in node.cells] == expected


def test_wal_created_after_open_invalidates_page_image(tmp_path):
    path, _ = _image(tmp_path)
    with SQLitePages(path) as pages:
        path.with_name(path.name + '-wal').write_bytes(b'unapplied private-image changes')
        with pytest.raises(PageUnavailable, match='sidecar'):
            pages.check_stable()


def test_malformed_parent_separator_cannot_hide_leaf_rows(tmp_path):
    path, root = _image(tmp_path)
    with SQLitePages(path) as pages:
        node = pages.tree_page(root)
        assert node.children and node.separators[0] > 0
        size = pages.page_size
        data = pages.read(root)
        cell = int.from_bytes(data[12:14], 'big')
    with path.open('r+b') as f:
        f.seek((root - 1) * size + cell + 4)
        f.write(b'\0')
    with SQLitePages(path) as pages, pytest.raises(PageUnavailable, match='parent bounds'):
        list(pages.rows(root))
