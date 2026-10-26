import os
import sqlite3
import sys
from pathlib import Path

import pytest

from chatstore.adapters.monitor import MonitoringUnavailable, SourceMonitor

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS vnode monitoring')


def test_open_media_handle_changes_are_detected_and_require_capture(tmp_path):
    path = tmp_path.resolve() / 'media'
    path.write_bytes(b'original')
    with SourceMonitor() as monitor, path.open('r+b', buffering=0) as writer:
        monitor.watch(path)
        assert monitor.accept_capture(monitor.token())
        assert monitor.unchanged()
        for _ in range(5):
            writer.seek(0)
            writer.write(b'changed!')
            os.fsync(writer.fileno())
            assert not monitor.unchanged()
            assert not monitor.unchanged()  # Polling never acknowledges an uncaptured write.
            assert monitor.accept_capture(monitor.token())
            assert monitor.unchanged()


def test_database_historical_commit_and_replacement(tmp_path):
    path = tmp_path.resolve() / 'chat.sqlite'
    with sqlite3.connect(path) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('CREATE TABLE messages(id INTEGER PRIMARY KEY,text TEXT)')
        writer.execute("INSERT INTO messages VALUES (1,'old')")
        writer.commit()
        with SourceMonitor() as monitor:
            monitor.database(path)
            assert monitor.accept_capture(monitor.token())
            before = monitor.token()
            writer.execute("UPDATE messages SET text='historical edit' WHERE id=1")
            writer.commit()
            assert not monitor.unchanged()
            assert not monitor.accept_capture(before)
            assert monitor.accept_capture(monitor.token())
            assert monitor.unchanged()
            path.rename(path.with_suffix('.replaced'))
            assert not monitor.unchanged()
            assert monitor.failure == 'monitored database identity changed'


def test_registration_changes_and_restart_cannot_reuse_capture(tmp_path):
    path = tmp_path.resolve() / 'new-media'
    with SourceMonitor() as first, SourceMonitor() as second:
        token = first.token()
        first.watch(path)
        assert not first.accept_capture(token)
        token = first.token()
        assert first.accept_capture(token)
        assert not second.accept_capture(token)
        path.write_bytes(b'newly downloaded')
        assert not first.unchanged()
        before = first.token()
        first.watch(path)
        assert not first.accept_capture(before)
        assert first.accept_capture(first.token())
        assert first.unchanged()
    assert not first.unchanged()


def test_replaced_media_is_rearmed_before_capture_can_be_accepted(tmp_path):
    root = tmp_path.resolve()
    media = root / 'media'
    media.write_bytes(b'old')
    with SourceMonitor() as monitor:
        monitor.watch(media)
        assert monitor.accept_capture(monitor.token())
        replacement = root / 'replacement'
        replacement.write_bytes(b'new')
        replacement.replace(media)
        assert not monitor.unchanged()
        before = monitor.token()
        monitor.watch(media)
        assert not monitor.accept_capture(before)
        assert monitor.accept_capture(monitor.token())
        with media.open('r+b', buffering=0) as writer:
            writer.write(b'edit')
            assert not monitor.unchanged()


def test_symlink_target_writes_and_retargeting_are_monitored(tmp_path):
    root = tmp_path.resolve()
    target, other, link = root / 'target', root / 'other', root / 'link'
    target.write_bytes(b'original')
    other.write_bytes(b'other')
    link.symlink_to(target.name)
    with SourceMonitor() as monitor:
        monitor.watch(link)
        assert monitor.accept_capture(monitor.token())
        target.write_bytes(b'edited')
        assert monitor.dirty_paths() == {link}
        assert monitor.accept_capture(monitor.token())
        link.unlink()
        link.symlink_to(other.name)
        assert monitor.dirty_paths() == {link}
        assert target not in monitor._files
        assert monitor.accept_capture(monitor.token())
        other.write_bytes(b'edited again')
        assert monitor.dirty_paths() == {link}
        monitor.unwatch(link)
        assert not monitor._files
        assert not monitor._request_targets


def test_symlink_ancestor_and_missing_target_creation(tmp_path):
    root = tmp_path.resolve()
    directory, alias = root / 'directory', root / 'alias'
    directory.mkdir()
    alias.symlink_to(directory.name)
    media = alias / 'missing'
    with SourceMonitor() as monitor:
        monitor.watch(media)
        assert monitor.accept_capture(monitor.token())
        (directory / 'missing').write_bytes(b'newly downloaded')
        assert monitor.dirty_paths() == {media}
        assert monitor.accept_capture(monitor.token())
        (directory / 'missing').write_bytes(b'changed bytes')
        assert monitor.dirty_paths() == {media}


def test_symlink_chain_replacement_requires_new_target_coverage(tmp_path):
    root = tmp_path.resolve()
    first, second, missing = root / 'first', root / 'second', root / 'missing'
    first.symlink_to(second.name)
    second.symlink_to(missing.name)
    with SourceMonitor() as monitor:
        monitor.watch(first)
        assert monitor.accept_capture(monitor.token())
        missing.write_bytes(b'downloaded')
        assert monitor.dirty_paths() == {first}
        assert monitor.accept_capture(monitor.token())
        missing.write_bytes(b'edited')
        assert monitor.dirty_paths() == {first}


def test_missing_database_fails_closed(tmp_path):
    with SourceMonitor() as monitor:
        monitor.database(tmp_path.resolve() / 'missing.sqlite')
        assert monitor.failure
        assert not monitor.unchanged()


def test_closed_monitor_cannot_be_reused():
    monitor = SourceMonitor()
    monitor.close()
    monitor.close()
    with pytest.raises(MonitoringUnavailable, match='closed'):
        monitor.watch(Path('/'))
    assert monitor.token() is None


def test_database_replacement_cannot_rearm_an_old_sqlite_handle(tmp_path):
    path = tmp_path.resolve() / 'source.sqlite'
    with sqlite3.connect(path) as writer:
        writer.execute('CREATE TABLE message(id INTEGER)')
    with SourceMonitor() as monitor:
        monitor.database(path)
        assert monitor.accept_capture(monitor.token())
        path.unlink()
        with sqlite3.connect(path) as writer:
            writer.execute('CREATE TABLE message(id INTEGER)')
        monitor.database(path)
        assert monitor.failure == 'monitored database identity changed'
        assert not monitor.unchanged()


def test_unchanged_poll_does_not_stat_or_open_each_media_path(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    for i in range(100):
        (root / f'media-{i}').write_bytes(b'bytes')
    with SourceMonitor() as monitor:
        for i in range(100):
            monitor.watch(root / f'media-{i}')
        assert monitor.accept_capture(monitor.token())

        def forbidden(*args, **kwargs):
            raise AssertionError('unchanged poll enumerated filesystem dependencies')

        with monkeypatch.context() as patch:
            patch.setattr(os, 'stat', forbidden)
            patch.setattr(os, 'lstat', forbidden)
            patch.setattr(os, 'open', forbidden)
            assert monitor.unchanged()
            assert monitor.unchanged()


def test_registration_resource_failure_never_accepts_capture(tmp_path, monkeypatch):
    path = tmp_path.resolve() / 'media'
    path.write_bytes(b'bytes')
    with SourceMonitor() as monitor:
        before = monitor.token()

        def exhausted(*args, **kwargs):
            raise OSError('descriptor limit reached')

        with monkeypatch.context() as patch:
            patch.setattr(os, 'open', exhausted)
            monitor.watch(path)
        assert monitor.failure
        assert not monitor.accept_capture(before)
        assert not monitor.unchanged()


def test_unrelated_ancestor_entries_do_not_invalidate_known_dependencies(tmp_path):
    root = tmp_path.resolve()
    media = root / 'source' / 'media'
    media.parent.mkdir()
    media.write_bytes(b'bytes')
    with SourceMonitor() as monitor:
        monitor.watch(media)
        assert monitor.accept_capture(monitor.token())
        unrelated = root / 'unrelated-directory'
        unrelated.mkdir()
        assert monitor.unchanged()
        unrelated.rmdir()
        assert monitor.unchanged()
        media.parent.rename(root / 'moved-source')
        assert not monitor.unchanged()


def test_dirty_paths_are_sparse_and_repeated_polling_never_clears_them(tmp_path):
    root = tmp_path.resolve()
    paths = [root / f'media-{index}' for index in range(100)]
    for path in paths:
        path.write_bytes(b'old')
    with SourceMonitor() as monitor:
        for path in paths:
            monitor.watch(path)
        assert monitor.dirty_paths() == frozenset(paths)
        assert monitor.accept_capture(monitor.token())
        assert monitor.dirty_paths() == frozenset()
        with paths[37].open('r+b', buffering=0) as writer:
            writer.write(b'new')
            os.fsync(writer.fileno())
        assert monitor.dirty_paths() == frozenset({paths[37]})
        assert monitor.dirty_paths() == frozenset({paths[37]})
        before = monitor.token()
        paths[38].write_bytes(b'changed during capture')
        assert not monitor.accept_capture(before)
        assert monitor.dirty_paths() == frozenset({paths[37], paths[38]})
        assert monitor.accept_capture(monitor.token())
        assert monitor.dirty_paths() == frozenset()


def test_dirty_path_poll_rearms_replacement_for_subsequent_open_handle_writes(tmp_path):
    root = tmp_path.resolve()
    path = root / 'media'
    path.write_bytes(b'old')
    with SourceMonitor() as monitor:
        monitor.watch(path)
        assert monitor.accept_capture(monitor.token())
        replacement = root / 'replacement'
        replacement.write_bytes(b'new')
        replacement.replace(path)
        before_rearm = monitor.token()
        assert not monitor.accept_capture(before_rearm)
        assert monitor.dirty_paths() == frozenset({path})
        assert monitor.accept_capture(monitor.token())
        with path.open('r+b', buffering=0) as writer:
            writer.write(b'edit')
            os.fsync(writer.fileno())
            assert monitor.dirty_paths() == frozenset({path})


def test_missing_child_creation_does_not_dirty_existing_siblings(tmp_path):
    root = tmp_path.resolve()
    existing = root / 'existing'
    missing = root / 'missing'
    existing.write_bytes(b'bytes')
    with SourceMonitor() as monitor:
        monitor.watch(existing)
        monitor.watch(missing)
        assert monitor.accept_capture(monitor.token())
        missing.write_bytes(b'downloaded')
        assert monitor.dirty_paths() == frozenset({missing})
        assert monitor.accept_capture(monitor.token())
        unrelated = root / 'unrelated'
        unrelated.write_bytes(b'not a dependency')
        assert monitor.dirty_paths() == frozenset()
        assert monitor.unchanged()
        with missing.open('r+b', buffering=0) as writer:
            writer.write(b'new bytes')
            assert monitor.dirty_paths() == frozenset({missing})


def test_new_missing_sibling_registration_always_invalidates_capture(tmp_path):
    root = tmp_path.resolve()
    with SourceMonitor() as monitor:
        monitor.watch(root / 'missing-a')
        assert monitor.accept_capture(monitor.token())
        before = monitor.token()
        monitor.watch(root / 'missing-b')
        assert not monitor.accept_capture(before)
        assert monitor.dirty_paths() == frozenset({root / 'missing-b'})


def test_clean_dirty_path_poll_never_stats_or_opens_dependencies(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    with SourceMonitor() as monitor:
        for index in range(100):
            path = root / f'file-{index}'
            path.write_bytes(b'bytes')
            monitor.watch(path)
        assert monitor.accept_capture(monitor.token())
        def forbidden(*args, **kwargs):
            raise AssertionError('clean dirty-path poll enumerated dependencies')
        with monkeypatch.context() as patch:
            patch.setattr(os, 'stat', forbidden)
            patch.setattr(os, 'lstat', forbidden)
            patch.setattr(os, 'open', forbidden)
            assert monitor.dirty_paths() == frozenset()
            assert monitor.dirty_paths() == frozenset()


def test_dirty_paths_include_database_commits_and_fail_closed_after_close(tmp_path):
    path = tmp_path.resolve() / 'chat.sqlite'
    with sqlite3.connect(path) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('CREATE TABLE message(body TEXT)')
        writer.commit()
        with SourceMonitor() as monitor:
            monitor.database(path)
            assert monitor.accept_capture(monitor.token())
            writer.execute("INSERT INTO message VALUES('new')")
            writer.commit()
            assert monitor.dirty_paths() == frozenset({path})
        assert monitor.dirty_paths() is None


def test_retiring_media_releases_handles_and_preserves_shared_watch_coverage(tmp_path):
    root = tmp_path.resolve()
    kept, retired = root / 'kept', root / 'retired'
    kept.write_bytes(b'kept')
    retired.write_bytes(b'retired')
    with SourceMonitor() as monitor:
        monitor.watch(kept)
        monitor.watch(retired)
        assert monitor.accept_capture(monitor.token())
        count = len(monitor._files)
        before = monitor.token()
        monitor.unwatch(retired)
        assert len(monitor._files) == count - 1
        assert not monitor.accept_capture(before)
        assert monitor.accept_capture(monitor.token())
        retired.write_bytes(b'not a dependency now')
        assert monitor.dirty_paths() == frozenset()
        kept.write_bytes(b'still covered')
        assert monitor.dirty_paths() == frozenset({kept})
        monitor.unwatch(kept)
        assert not monitor._files and not monitor._routes and not monitor._dependencies


def test_retiring_missing_child_removes_parent_write_subscription(tmp_path):
    root = tmp_path.resolve()
    kept = root / 'kept'
    missing = root / 'missing'
    kept.write_bytes(b'bytes')
    with SourceMonitor() as monitor:
        monitor.watch(kept)
        monitor.watch(missing)
        monitor.unwatch(missing)
        assert monitor.accept_capture(monitor.token())
        missing.write_bytes(b'no longer observed')
        assert monitor.dirty_paths() == frozenset()
        assert monitor.unchanged()


def test_manual_rearm_before_event_poll_keeps_replacement_dirty(tmp_path):
    root = tmp_path.resolve()
    path = root / 'media'
    path.write_bytes(b'old')
    with SourceMonitor() as monitor:
        monitor.watch(path)
        assert monitor.accept_capture(monitor.token())
        replacement = root / 'replacement'
        replacement.write_bytes(b'new')
        replacement.replace(path)
        monitor.watch(path)  # Closing the old FD may remove its queued event.
        assert monitor.dirty_paths() == frozenset({path})
        assert monitor.accept_capture(monitor.token())
        with path.open('r+b', buffering=0) as writer:
            writer.write(b'new write')
            assert monitor.dirty_paths() == frozenset({path})


def test_symlink_retarget_keeps_shared_target_owners(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    target, other, link = root / 'target', root / 'other', root / 'link'
    target.write_bytes(b'original')
    other.write_bytes(b'other')
    link.symlink_to(target)
    with SourceMonitor() as monitor:
        monitor.watch(link)
        monitor.watch(target)
        assert monitor.accept_capture(monitor.token())
        link.unlink()
        link.symlink_to(other)
        assert link in monitor.dirty_paths()
        assert monitor.accept_capture(monitor.token())
        monitor.unwatch(link)
        assert target in monitor._files
        target.write_bytes(b'changed')
        assert monitor.dirty_paths() == {target}
        assert monitor.accept_capture(monitor.token())
        def forbidden(*args, **kwargs):
            raise AssertionError('clean poll inspected paths')
        monkeypatch.setattr(Path, 'lstat', forbidden)
        monkeypatch.setattr(os, 'open', forbidden)
        assert monitor.dirty_paths() == set()


def test_watch_registration_race_fails_closed(tmp_path, monkeypatch):
    target = tmp_path.resolve() / 'file'
    target.write_bytes(b'original')
    original = os.open
    def replace_before_open(path, flags):
        if path == target:
            target.unlink()
            target.write_bytes(b'replaced')
        return original(path, flags)
    with SourceMonitor() as monitor:
        monkeypatch.setattr(os, 'open', replace_before_open)
        monitor.watch(target)
        assert monitor.failure == 'watch target changed during registration'
        assert not monitor.accept_capture(monitor.token())


def test_database_symlink_swap_during_registration_fails_closed(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    database, other = root / 'database', root / 'other'
    database.write_bytes(b'original')
    other.write_bytes(b'other')
    with SourceMonitor() as monitor:
        original = monitor.watch
        def swap(path):
            database.unlink()
            database.symlink_to(other)
            original(path)
        monkeypatch.setattr(monitor, 'watch', swap)
        monitor.database(database)
        assert monitor.failure == 'symlink database identity is unsupported'
        assert not monitor.accept_capture(monitor.token())
