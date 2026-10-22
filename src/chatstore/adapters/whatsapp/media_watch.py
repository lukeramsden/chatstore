"""Bind WhatsApp file monitoring to one prepared native point."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ...paths import DataDir
from .. import media_watch
from ..media_capture import MediaUnavailable
from ..media_watch import MediaWatch
from ..monitor import SourceMonitor
from . import media
from .media import filename, prepare

POLICY_MODULES = (media, media_watch)


class PreparedWhatsAppMediaWatch:
    def __init__(self, root: Path, dd: DataDir):
        self.root, self.dd = root, dd
        self._monitor: SourceMonitor | None = None
        self._watch: MediaWatch | None = None
        self._binding: tuple[str, str] | None = None

    def prepare(self, conn: sqlite3.Connection, *, full: bool = False) -> None:
        if not conn.in_transaction:
            raise MediaUnavailable('WhatsApp media watcher requires the native writer transaction')
        database = str(conn.execute('PRAGMA database_list').fetchone()[2])
        parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
        if point is None:
            raise MediaUnavailable('WhatsApp media watcher requires a candidate page point')
        reconcile = full or parent is None or self._binding != (database, parent[0])
        try:
            if reconcile:
                self.close()
                self._monitor = SourceMonitor()
                self._watch = MediaWatch(self._monitor)
                rows = conn.execute('SELECT Z_PK,ZMEDIALOCALPATH FROM ZWAMEDIAITEM')
            else:
                rows = conn.execute('''SELECT changes.row_id,media.ZMEDIALOCALPATH
                    FROM cs_native_mirror_changes changes LEFT JOIN ZWAMEDIAITEM media
                    ON media.Z_PK=changes.row_id WHERE changes.name='ZWAMEDIAITEM' ''')
            assert self._watch is not None and self._monitor is not None
            self._watch.update((row[0], filename(self.root, row[1])) for row in rows)
            dirty = self._watch.dirty_ids()
            before = self._monitor.token()
            dirty |= self._watch.dirty_ids()
            prepare(conn, self.root, self.dd, full=reconcile, dirty_ids=dirty)
            if not self._monitor.accept_capture(before):
                raise MediaUnavailable('WhatsApp files changed during media preparation')
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
