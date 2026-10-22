import shutil
import sqlite3
import sys

import pytest

from chatstore.adapters.source_view import WalSource, _read_index
from chatstore.adapters.wal import WalUnavailable

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS source identity monitoring')


@pytest.fixture
def source(tmp_path):
    path = tmp_path.resolve() / 'source.sqlite'
    writer = sqlite3.connect(path)
    writer.execute('PRAGMA journal_mode=WAL')
    writer.execute('PRAGMA wal_autocheckpoint=0')
    writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
    writer.executemany('INSERT INTO message VALUES (?,?)', ((i, 'old-' + 'x' * 100) for i in range(1, 1001)))
    writer.commit()
    yield path, writer
    writer.close()


def test_candidate_matches_readonly_sqlite_view_and_old_edit(source):
    path, writer = source
    with WalSource(path) as source_view:
        with source_view.view() as baseline:
            assert baseline.baseline_required
            assert baseline.connection.execute('SELECT count(*) FROM message').fetchone()[0] == 1000
            cursor = baseline.cursor
            assert cursor.database_identity == (path.stat().st_dev, path.stat().st_ino)
            with pytest.raises(sqlite3.OperationalError, match='readonly'):
                baseline.connection.execute("UPDATE message SET text='forbidden' WHERE id=1")
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            baseline.connection.execute('SELECT 1')
        writer.execute("UPDATE message SET text='historical edit' WHERE id=1")
        writer.commit()
        with source_view.view(cursor) as changed:
            assert not changed.baseline_required
            assert changed.changes.commits == 1 and changed.changes.pages
            assert changed.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == 'historical edit'
            cursor = changed.cursor
        with source_view.view(cursor) as clean:
            assert clean.changes.pages == frozenset()
            assert clean.changes.frames_read == 0 and clean.changes.bytes_read <= 128


def test_commit_after_yield_keeps_sqlite_snapshot_and_next_cursor_sees_it(source):
    path, writer = source
    with WalSource(path) as source_view:
        with source_view.view() as view:
            old = view.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0]
            cursor = view.cursor
            writer.execute("UPDATE message SET text='after yielded view' WHERE id=1")
            writer.commit()
            assert view.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == old
        with source_view.view(cursor) as next_view:
            assert next_view.changes.commits == 1
            assert next_view.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == 'after yielded view'


def test_spilled_uncommitted_tail_is_excluded_by_sqlite_commit_bound(source):
    path, writer = source
    with WalSource(path) as source_view:
        with source_view.view() as view:
            cursor = view.cursor
        writer.execute('PRAGMA cache_size=1')
        writer.execute('PRAGMA cache_spill=1')
        writer.executemany('INSERT INTO message VALUES (?,?)', ((i, 'pending-' + 'y' * 100) for i in range(1001, 2001)))
        with source_view.view(cursor) as pending:
            assert pending.changes.pages == frozenset()
            assert pending.cursor == cursor
            assert pending.connection.execute('SELECT count(*) FROM message').fetchone()[0] == 1000
        writer.commit()
        with source_view.view(cursor) as committed:
            assert committed.changes.commits == 1
            assert committed.connection.execute('SELECT count(*) FROM message').fetchone()[0] == 2000


def test_restarted_wal_uses_committed_extent_and_rejects_old_cursor(source):
    path, writer = source
    with WalSource(path) as source_view:
        with source_view.view() as initial:
            cursor = initial.cursor
        assert writer.execute('PRAGMA wal_checkpoint(RESTART)').fetchone()[0] == 0
        writer.execute("UPDATE message SET text='new log' WHERE id=1")
        writer.commit()
        with pytest.raises(WalUnavailable, match='reset or replaced'), source_view.view(cursor):
            pytest.fail('old WAL cursor accepted after restart')
        with source_view.view() as restarted:
            assert restarted.baseline_required
            assert len(restarted.changes.pages) < restarted.changes.cursor.database_pages
            assert restarted.changes.cursor.frame < cursor.wal.frame
            assert restarted.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == 'new log'


def test_consumer_failure_propagates_and_releases_reader_lock(source):
    path, writer = source
    with WalSource(path) as source_view:
        with pytest.raises(RuntimeError, match='capture interrupted'), source_view.view() as view:
            cursor = view.cursor
            raise RuntimeError('capture interrupted')
        assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        assert cursor.wal.frame > 0


def test_torn_or_corrupt_index_header_requires_reconciliation(source, tmp_path):
    path, _ = source
    shm = path.with_name(path.name + '-shm')
    copy = tmp_path / 'index.copy'
    shutil.copyfile(shm, copy)
    assert _read_index(copy).frame > 0
    with copy.open('r+b') as f:
        f.seek(40)
        raw = f.read(1)
        for offset in (40, 88):
            f.seek(offset)
            f.write(bytes([raw[0] ^ 1]))
    with pytest.raises(WalUnavailable, match='index checksum'):
        _read_index(copy)
    shutil.copyfile(shm, copy)
    with copy.open('r+b') as f:
        f.seek(8)
        raw = f.read(1)
        f.seek(8)
        f.write(bytes([raw[0] ^ 1]))
    with pytest.raises(WalUnavailable, match='incomplete or changing'):
        _read_index(copy)


def test_concurrent_commit_while_pinning_never_returns_mixed_view(source, monkeypatch):
    import chatstore.adapters.source_view as module

    path, writer = source
    with WalSource(path) as source_view:
        original = module._read_index
        injected = False

        def concurrent(*args, **kwargs):
            nonlocal injected
            result = original(*args, **kwargs)
            if not injected:
                injected = True
                writer.execute("UPDATE message SET text='during pin' WHERE id=1")
                writer.commit()
            return result

        with monkeypatch.context() as patch:
            patch.setattr(module, '_read_index', concurrent)
            with pytest.raises(WalUnavailable, match='source changed'), source_view.view():
                pytest.fail('mixed source view escaped')
        assert injected
        with source_view.view() as stable:
            assert stable.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == 'during pin'


def test_commit_during_wal_validation_stays_beyond_pinned_candidate(source, monkeypatch):
    import chatstore.adapters.source_view as module

    path, writer = source
    with WalSource(path) as source_view:
        with source_view.view() as baseline:
            cursor = baseline.cursor
        original = module.read_changes

        def concurrent(*args, **kwargs):
            result = original(*args, **kwargs)
            writer.execute("UPDATE message SET text='during WAL validation' WHERE id=1")
            writer.commit()
            return result

        with monkeypatch.context() as patch:
            patch.setattr(module, 'read_changes', concurrent)
            with source_view.view(cursor) as pinned:
                assert pinned.cursor == cursor and not pinned.changes.pages
                assert pinned.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0].startswith('old-')
        with source_view.view(cursor) as following:
            assert following.changes.commits == 1
            assert following.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == 'during WAL validation'


def test_database_rename_and_restore_cannot_evade_identity_guard(source, monkeypatch):
    import chatstore.adapters.source_view as module

    path, _ = source
    original = module._read_index
    moved = path.with_name('moved.sqlite')
    with WalSource(path) as source_view:
        injected = False

        def rename_and_restore(*args):
            nonlocal injected
            result = original(*args)
            if not injected:
                injected = True
                path.rename(moved)
                moved.rename(path)
            return result

        monkeypatch.setattr(module, '_read_index', rename_and_restore)
        with pytest.raises(WalUnavailable, match='source changed'), source_view.view():
            pytest.fail('identity transition escaped')


def test_closed_coordinator_and_symlinks_fail_closed(source):
    path, _ = source
    source_view = WalSource(path)
    source_view.close()
    with pytest.raises(WalUnavailable, match='closed'), source_view.view():
        pytest.fail('closed coordinator reused')
    link = path.with_name('linked.sqlite')
    link.symlink_to(path)
    with pytest.raises(WalUnavailable, match='identity monitoring unavailable'):
        WalSource(link)


def test_prior_cursor_is_bound_to_database_identity(source, tmp_path):
    path, writer = source
    with WalSource(path) as source_view, source_view.view() as baseline:
        cursor = baseline.cursor
    replacement = tmp_path / 'replacement.sqlite'
    copy = sqlite3.connect(replacement)
    try:
        writer.backup(copy)
    finally:
        copy.close()
    writer.close()
    replacement.replace(path)
    with WalSource(path) as source_view, pytest.raises(WalUnavailable, match='database identity changed'), source_view.view(cursor):
        pytest.fail('cursor from a replaced database accepted')


@pytest.mark.parametrize('page_size', [512, 65536])
def test_index_page_size_matches_pinned_native_view(tmp_path, page_size):
    path = tmp_path.resolve() / 'page-size.sqlite'
    writer = sqlite3.connect(path)
    try:
        writer.execute(f'PRAGMA page_size={page_size}')
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
        writer.execute("INSERT INTO message VALUES (1,'row')")
        writer.commit()
        with WalSource(path) as source_view, source_view.view() as view:
            assert view.changes.cursor.page_size == page_size
            assert view.connection.execute('SELECT text FROM message').fetchone()[0] == 'row'
    finally:
        writer.close()


def test_non_wal_source_requires_reconciliation(tmp_path):
    path = tmp_path.resolve() / 'rollback-mode.sqlite'
    writer = sqlite3.connect(path)
    try:
        writer.execute('CREATE TABLE message(id INTEGER)')
        writer.commit()
        with WalSource(path) as source_view, pytest.raises(WalUnavailable, match='source view unavailable'), source_view.view():
            pytest.fail('rollback-journal database treated as a WAL feed')
    finally:
        writer.close()


def test_backup_page_locations_stay_bound_to_pinned_view(source, tmp_path):
    from chatstore.adapters.pages import SQLitePages

    path, writer = source
    image = tmp_path / 'pinned.sqlite'
    with WalSource(path) as source_view:
        with source_view.view() as pinned:
            cursor = pinned.cursor
            table_root = pinned.connection.execute("SELECT rootpage FROM sqlite_schema WHERE name='message'").fetchone()[0]
            old_text = pinned.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0]
            writer.execute('UPDATE message SET text=? WHERE id=1', ('y' * len(old_text),))
            writer.commit()
            copy = sqlite3.connect(image)
            try:
                pinned.connection.backup(copy)
            finally:
                copy.close()
        with SQLitePages(image) as pages:
            locations = list(pages.rows(table_root))
            leaf = next(row.page for row in locations if row.row_id == 1)
            assert len(locations) == 1000
        copy = sqlite3.connect(image)
        try:
            assert copy.execute('SELECT text FROM message WHERE id=1').fetchone()[0] == old_text
        finally:
            copy.close()
        with source_view.view(cursor) as following:
            assert leaf in following.changes.pages
            assert following.connection.execute('SELECT text FROM message WHERE id=1').fetchone()[0] != old_text
