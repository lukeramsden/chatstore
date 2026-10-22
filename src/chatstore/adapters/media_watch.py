"""Process-local attachment ownership for a live source monitor."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from ..paths import DataDir
from .media_capture import MediaUnavailable
from .monitor import MonitoringUnavailable, SourceMonitor
from .prepared_media import prepare_media


class MediaWatch:
    def __init__(self, monitor: SourceMonitor) -> None:
        self.monitor = monitor
        self._paths: dict[int, Path] = {}
        self._owners: dict[Path, set[int]] = {}

    def update(self, attachments: Iterable[tuple[int, str | None]]) -> None:
        """Apply changed rows; a missing filename also retires deleted rows.

        Register new paths before capture. Callers must also capture every changed
        attachment row, including a new owner of an already watched shared path,
        and acknowledge only after stable media retention.
        """
        for row_id, filename in attachments:
            path = Path(filename).expanduser().absolute() if filename else None
            old = self._paths.get(row_id)
            if old == path:
                continue
            if path is not None:
                if path not in self._owners:
                    self.monitor.watch(path)
                    if self.monitor.failure:
                        raise MonitoringUnavailable(self.monitor.failure)
                    self._owners[path] = set()
                self._owners[path].add(row_id)
                self._paths[row_id] = path
            else:
                self._paths.pop(row_id, None)
            if old is not None:
                owners = self._owners[old]
                owners.remove(row_id)
                if not owners:
                    self.monitor.unwatch(old)
                    del self._owners[old]
                    if self.monitor.failure:
                        raise MonitoringUnavailable(self.monitor.failure)

    def dirty_ids(self) -> frozenset[int]:
        paths = self.monitor.dirty_paths()
        if paths is None:
            raise MonitoringUnavailable(self.monitor.failure or 'media monitoring unavailable')
        return frozenset(row_id for path in paths for row_id in self._owners.get(path, ()))


class PreparedMediaWatch:
    """Bind live file coverage to one private mirror and retained page point.

    A new instance reconciles all files. Failed or rolled-back publication cannot
    lend its coverage to a different parent point on the next attempt.
    """

    def __init__(self, root: Path, dd: DataDir) -> None:
        self.root = root
        self.dd = dd
        self._monitor: SourceMonitor | None = None
        self._watch: MediaWatch | None = None
        self._binding: tuple[str, str] | None = None

    def prepare(self, conn: sqlite3.Connection, *, full: bool = False) -> None:
        if not conn.in_transaction:
            raise MediaUnavailable('media watcher requires the native writer transaction')
        database = str(conn.execute('PRAGMA database_list').fetchone()[2])
        parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
        if point is None:
            raise MediaUnavailable('media watcher requires a candidate page point')
        reconcile = full or parent is None or self._binding != (database, parent[0])
        try:
            if reconcile:
                self.close()
                self._monitor = SourceMonitor()
                self._watch = MediaWatch(self._monitor)
                rows = conn.execute('SELECT ROWID,filename FROM attachment')
            else:
                rows = conn.execute("""SELECT changes.row_id,attachment.filename
                    FROM cs_native_mirror_changes changes LEFT JOIN attachment
                    ON attachment.ROWID=changes.row_id WHERE changes.name='attachment'""")
            assert self._watch is not None and self._monitor is not None
            self._watch.update((row[0], row[1]) for row in rows)
            dirty = self._watch.dirty_ids()
            before = self._monitor.token()
            dirty |= self._watch.dirty_ids()
            prepare_media(conn, self.root, self.dd, full=reconcile, dirty_ids=dirty)
            if not self._monitor.accept_capture(before):
                raise MediaUnavailable('attachment files changed during media preparation')
            self._binding = (database, point[0])
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._monitor is not None:
            self._monitor.close()
        self._monitor = None
        self._watch = None
        self._binding = None
