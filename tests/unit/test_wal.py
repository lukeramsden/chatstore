import shutil
import sqlite3

import pytest

from chatstore.adapters.wal import WalUnavailable, read_changes


@pytest.fixture
def source(tmp_path):
    path = tmp_path / 'source.sqlite'
    writer = sqlite3.connect(path)
    writer.execute('PRAGMA page_size=1024')
    writer.execute('PRAGMA journal_mode=WAL')
    writer.execute('PRAGMA wal_autocheckpoint=0')
    writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
    writer.executemany('INSERT INTO message VALUES (?,?)', ((i, 'old-' + 'x' * 100) for i in range(1, 2001)))
    writer.commit()
    wal = path.with_name(path.name + '-wal')
    yield writer, wal
    writer.close()


def test_historical_edit_delete_backfill_and_clean_cursor_are_bounded(source):
    writer, wal = source
    initial = read_changes(wal, max_frames=None)
    assert initial.commits >= 1 and len(initial.pages) > 100
    writer.execute("UPDATE message SET text='edited historical row' WHERE id=1")
    writer.commit()
    writer.execute('DELETE FROM message WHERE id=2')
    writer.commit()
    writer.execute("INSERT INTO message VALUES (0,'backfilled older row')")
    writer.commit()
    delta = read_changes(wal, initial.cursor)
    assert delta.commits == 3
    assert 0 < len(delta.pages) < 10
    assert delta.frames_read < 10
    assert delta.cursor.frame > initial.cursor.frame
    clean = read_changes(wal, delta.cursor, max_frames=0)
    assert clean.pages == frozenset() and clean.commits == 0 and clean.frames_read == 0
    assert clean.bytes_read <= 128  # WAL contains hundreds of historical frames.
    assert clean.cursor == delta.cursor


def test_uncommitted_spill_does_not_advance_candidate(source):
    writer, wal = source
    initial = read_changes(wal, max_frames=None)
    writer.execute('PRAGMA cache_size=1')
    writer.execute('PRAGMA cache_spill=1')
    writer.executemany('INSERT INTO message VALUES (?,?)', ((i, 'pending-' + 'y' * 100) for i in range(2001, 4001)))
    try:
        pending = read_changes(wal, initial.cursor)
    except WalUnavailable:
        # SQLite may defer checksums on spilled frames until commit. Such a
        # tail must force reconciliation, never become an accepted cursor.
        pending = None
    if pending is not None:
        assert pending.pending_frames > 0
        assert pending.cursor == initial.cursor
        assert pending.pages == frozenset() and pending.commits == 0
    writer.commit()
    committed = read_changes(wal, initial.cursor)
    assert committed.commits == 1 and committed.cursor.frame > initial.cursor.frame
    assert len(committed.pages) > 100


def test_reset_and_frame_limit_require_reconciliation(source):
    writer, wal = source
    with pytest.raises(WalUnavailable, match='frame limit'):
        read_changes(wal, max_frames=1)
    initial = read_changes(wal, max_frames=None)
    assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    writer.execute("UPDATE message SET text='after reset' WHERE id=1")
    writer.commit()
    with pytest.raises(WalUnavailable, match='reset or replaced'):
        read_changes(wal, initial.cursor)


def test_header_and_frame_corruption_never_yield_a_candidate(source, tmp_path):
    _, wal = source
    copy = tmp_path / 'corrupt.wal'
    shutil.copyfile(wal, copy)
    with copy.open('r+b') as f:
        f.seek(24)
        f.write(b'\0\0\0\0')
    with pytest.raises(WalUnavailable, match='header checksum'):
        read_changes(copy, max_frames=None)
    shutil.copyfile(wal, copy)
    with copy.open('r+b') as f:
        f.seek(-1, 2)
        original = f.read(1)
        f.seek(-1, 2)
        f.write(bytes([original[0] ^ 1]))
    with pytest.raises(WalUnavailable, match='frame checksum'):
        read_changes(copy, max_frames=None)


def test_explicit_committed_extent_excludes_stale_or_partial_tail(source, tmp_path):
    _, wal = source
    initial = read_changes(wal, max_frames=None)
    copy = tmp_path / 'tail.wal'
    shutil.copyfile(wal, copy)
    with copy.open('ab') as f:
        f.write(b'stale partial tail')
    with pytest.raises(WalUnavailable, match='partial WAL frame'):
        read_changes(copy, max_frames=None)
    bounded = read_changes(copy, through_frame=initial.cursor.frame, max_frames=None)
    assert bounded.pages == initial.pages and bounded.cursor.frame == initial.cursor.frame
    with pytest.raises(WalUnavailable, match='outside sampled extent'):
        read_changes(copy, through_frame=initial.cursor.frame + 1, max_frames=None)


def test_replacement_or_disappeared_log_is_not_a_continuation(source, tmp_path):
    _, wal = source
    cursor = read_changes(wal, max_frames=None).cursor
    replacement = tmp_path / 'replacement.wal'
    shutil.copyfile(wal, replacement)
    with pytest.raises(WalUnavailable, match='reset or replaced'):
        read_changes(replacement, cursor)
    with pytest.raises(WalUnavailable, match='unavailable'):
        read_changes(tmp_path / 'missing.wal', cursor)


@pytest.mark.parametrize('page_size', [512, 65536])
def test_native_sqlite_page_sizes(tmp_path, page_size):
    path = tmp_path / f'pages-{page_size}.sqlite'
    writer = sqlite3.connect(path)
    try:
        writer.execute(f'PRAGMA page_size={page_size}')
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
        writer.execute("INSERT INTO message VALUES (1,'old')")
        writer.commit()
        wal = path.with_name(path.name + '-wal')
        baseline = read_changes(wal)
        assert baseline.cursor.page_size == page_size
        writer.execute("UPDATE message SET text='new' WHERE id=1")
        writer.commit()
        delta = read_changes(wal, baseline.cursor)
        assert delta.commits == 1 and delta.pages
    finally:
        writer.close()


def test_append_during_read_stays_after_returned_candidate(source, monkeypatch):
    from pathlib import Path

    writer, wal = source
    baseline = read_changes(wal, max_frames=None)
    real_open = Path.open
    injected = False

    class AppendOnRead:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def fileno(self):
            return self.handle.fileno()

        def seek(self, *args):
            return self.handle.seek(*args)

        def read(self, *args):
            nonlocal injected
            value = self.handle.read(*args)
            if not injected:
                injected = True
                writer.execute("UPDATE message SET text='committed during read' WHERE id=1")
                writer.commit()
            return value

    def open_with_append(path, *args, **kwargs):
        handle = real_open(path, *args, **kwargs)
        return AppendOnRead(handle) if path == wal else handle

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', open_with_append)
        candidate = read_changes(wal, baseline.cursor)
    assert injected
    assert candidate.cursor == baseline.cursor and not candidate.pages
    next_candidate = read_changes(wal, candidate.cursor)
    assert next_candidate.commits == 1 and next_candidate.pages


def test_committed_frame_descriptors_select_latest_body_and_allow_later_append(source):
    from chatstore.adapters.wal import WalPages

    writer, wal = source
    baseline = read_changes(wal, max_frames=None)
    for value in ('first', 'second'):
        writer.execute('UPDATE message SET text=? WHERE id=1', (value,))
        writer.commit()
    delta = read_changes(wal, baseline.cursor)
    assert delta.frames.keys() == delta.pages
    assert len(delta.frames) < delta.frames_read
    with pytest.raises(TypeError):
        delta.frames[1] = None
    with WalPages(wal, delta) as pages:
        bodies = {page: pages.read(page) for page in delta.pages}
        assert any(b'second' in body for body in bodies.values())
        writer.execute("UPDATE message SET text='later' WHERE id=1")
        writer.commit()
        assert {page: pages.read(page) for page in delta.pages} == bodies
        with pytest.raises(WalUnavailable, match='no committed frame'):
            pages.read(0)
        assert pages.pages_read == 2 * len(delta.pages)


def test_selected_page_corruption_and_reset_fail_closed(source):
    from chatstore.adapters.wal import WalPages

    writer, wal = source
    baseline = read_changes(wal, max_frames=None)
    writer.execute("UPDATE message SET text='selected' WHERE id=1")
    writer.commit()
    delta = read_changes(wal, baseline.cursor)
    page, frame = next(iter(delta.frames.items()))
    with WalPages(wal, delta) as pages:
        with wal.open('r+b') as handle:
            offset = 32 + (frame.number - 1) * (24 + delta.cursor.page_size) + 24 + 500
            handle.seek(offset)
            original = handle.read(1)
            handle.seek(offset)
            handle.write(bytes([original[0] ^ 1]))
        with pytest.raises(WalUnavailable, match='selected WAL frame changed'):
            pages.read(page)
        with wal.open('r+b') as handle:
            handle.seek(offset)
            handle.write(original)
        assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        writer.execute("UPDATE message SET text='new epoch' WHERE id=1")
        writer.commit()
        with pytest.raises(WalUnavailable, match='reset or replaced'):
            pages.check_stable()
