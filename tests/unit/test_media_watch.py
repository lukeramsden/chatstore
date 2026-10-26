import os
import sys

import pytest

from chatstore.adapters.media_watch import MediaWatch
from chatstore.adapters.monitor import MonitoringUnavailable, SourceMonitor

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS vnode monitoring')


def test_shared_path_routes_all_owners_and_retires_only_last(tmp_path):
    path = tmp_path.resolve() / 'shared'
    path.write_bytes(b'first')
    with SourceMonitor() as monitor:
        media = MediaWatch(monitor)
        media.update([(1, str(path)), (2, str(path))])
        assert media.dirty_ids() == {1, 2}
        assert monitor.accept_capture(monitor.token())
        media.update([(1, None)])
        with path.open('r+b', buffering=0) as writer:
            writer.write(b'other')
            os.fsync(writer.fileno())
        assert media.dirty_ids() == {2}
        media.update([(2, None)])
        assert media.dirty_ids() == set()
        assert path not in monitor._requests


def test_changed_filename_and_late_download(tmp_path):
    root = tmp_path.resolve()
    old, new = root / 'old', root / 'new'
    old.write_bytes(b'old')
    with SourceMonitor() as monitor:
        media = MediaWatch(monitor)
        media.update([(7, str(old)), (8, None)])
        assert monitor.accept_capture(monitor.token())
        media.update([(7, str(new))])
        assert media.dirty_ids() == {7}
        assert monitor.accept_capture(monitor.token())
        new.write_bytes(b'downloaded')
        assert media.dirty_ids() == {7}
        assert monitor.accept_capture(monitor.token())
        assert media.dirty_ids() == set()
        assert old not in monitor._requests


def test_clean_poll_does_not_enumerate_attachments(tmp_path, monkeypatch):
    path = tmp_path.resolve() / 'file'
    path.write_bytes(b'x')
    with SourceMonitor() as monitor:
        media = MediaWatch(monitor)
        media.update((row_id, str(path)) for row_id in range(1000))
        assert monitor.accept_capture(monitor.token())
        def forbidden(*args, **kwargs):
            raise AssertionError('clean poll inspected a path')
        monkeypatch.setattr(type(path), 'lstat', forbidden)
        monkeypatch.setattr(os, 'open', forbidden)
        assert media.dirty_ids() == set()


def test_watch_failure_never_returns_clean_coverage(tmp_path, monkeypatch):
    target = tmp_path.resolve() / 'target'
    target.write_bytes(b'x')
    def denied(*args, **kwargs):
        raise PermissionError('watch denied')
    with SourceMonitor() as monitor:
        media = MediaWatch(monitor)
        monkeypatch.setattr(os, 'open', denied)
        with pytest.raises(MonitoringUnavailable, match='registration failed'):
            media.update([(1, str(target))])
        with pytest.raises(MonitoringUnavailable):
            media.dirty_ids()
