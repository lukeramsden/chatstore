"""Durable file-change coverage for native media evidence, without live watchers.

The ledger stores an `lstat` fingerprint for every file path referenced by the
mirror, bound to the same native point as the retained media observations. A
later process compares fresh fingerprints with the ledger and re-hashes only
rows whose fingerprint changed, plus rows whose native row changed. Nothing is
held open between polls.

Soundness rests on the kernel updating `ctime` on every content or metadata
write (userspace cannot set it) and on `(dev, ino, birthtime)` exposing
replace-by-rename. Files whose timestamps fall close to the scan time are
stored as racy and re-hashed next run, as git does for its index. Any
mismatch, unreadable state, foreign point or clock rollback reconciles every
file instead of trusting the ledger.
"""

from __future__ import annotations

import errno
import os
import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path

from ..paths import DataDir
from . import prepared_media
from .media_capture import MediaUnavailable
from .whatsapp import media as whatsapp_media

POLICY_MODULES = (prepared_media, whatsapp_media)

FORMAT = 1
RACY_WINDOW_NS = 2_000_000_000
# Absence and permission errors are stable observations (captured as missing).
# Anything else, such as EIO, is re-captured every run until it resolves.
_ABSENT = frozenset((errno.ENOENT, errno.ENOTDIR, errno.ELOOP, errno.EACCES, errno.EPERM))


def fingerprint(path: str) -> str:
    try:
        st = os.lstat(path)
    except OSError as exc:
        return f'errno:{exc.errno}'
    birth = getattr(st, 'st_birthtime_ns', None)
    if birth is None:
        birth = getattr(st, 'st_birthtime', '')
    return f'{st.st_dev}:{st.st_ino}:{st.st_mode}:{st.st_size}:{st.st_mtime_ns}:{st.st_ctime_ns}:{birth}'


def _volatile(value: str) -> bool:
    return value.startswith('errno:') and int(value[6:]) not in _ABSENT


def now_ns() -> int:
    """Scan clock for racy-clean decisions; compare only with values it returned."""
    return time.time_ns()


def recent(value: str, started_ns: int) -> bool:
    """True when a fingerprint's timestamps are too close to the scan to prove cleanliness."""
    if value.startswith('errno:'):
        return False
    _, _, _, _, mtime, ctime, _ = value.split(':', 6)
    return max(int(mtime), int(ctime)) >= started_ns - RACY_WINDOW_NS


class PreparedMediaLedger:
    """Bind persisted file fingerprints to one private mirror and retained point.

    `prepare` runs as the native mirror's pre-commit validator, so the ledger,
    media observations and page point commit or roll back together. Source
    queries live in methods so that the extraction policy hashes them.
    """

    def __init__(self, root: Path, dd: DataDir) -> None:
        self.root, self.dd = root, dd

    def _rows(self, conn: sqlite3.Connection) -> Iterable[tuple[int, str]]:
        raise NotImplementedError

    def _path(self, name: str) -> str | None:
        raise NotImplementedError

    def _evidence_point(self, conn: sqlite3.Connection) -> tuple[str, int] | None:
        raise NotImplementedError

    def _captured(self, conn: sqlite3.Connection) -> set[int]:
        raise NotImplementedError

    def _capture(self, conn: sqlite3.Connection, *, full: bool, dirty_ids: set[int] | None) -> None:
        raise NotImplementedError

    def prepare(self, conn: sqlite3.Connection, *, full: bool = False) -> None:
        if not conn.in_transaction:
            raise MediaUnavailable('media ledger requires the native writer transaction')
        point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
        if point is None:
            raise MediaUnavailable('media ledger requires a candidate page point')
        started = now_ns()
        conn.execute('''CREATE TABLE IF NOT EXISTS cs_native_media_ledger(
            row_id INTEGER PRIMARY KEY,path TEXT NOT NULL,fingerprint TEXT NOT NULL,racy INTEGER NOT NULL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS cs_native_media_ledger_state(
            id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,format INTEGER NOT NULL,captured_ns INTEGER NOT NULL)''')
        full = full or not self._continues(conn, started)
        current: dict[int, str] = {}
        for row_id, name in self._rows(conn):
            path = self._path(name)
            if path is not None:
                current[row_id] = path
        seen = {row_id: fingerprint(path) for row_id, path in current.items()}
        dirty: set[int] | None = None
        if not full:
            dirty = set(current)
            for row_id, path, value, racy in conn.execute('SELECT row_id,path,fingerprint,racy FROM cs_native_media_ledger'):
                if current.get(row_id) == path and seen[row_id] == value and not racy and not _volatile(value):
                    dirty.discard(row_id)
                else:
                    dirty.add(row_id)
        # Bracket every hash with lstat: bytes are bound to the fingerprint only
        # if the file looked the same before and after it was read.
        for attempt in range(2):
            conn.execute('SAVEPOINT cs_media_ledger_capture')
            self._capture(conn, full=full, dirty_ids=dirty)
            captured = current.keys() if full else self._captured(conn) & current.keys()
            moved = {row_id: value for row_id in captured
                     if (value := fingerprint(current[row_id])) != seen[row_id]}
            if not moved:
                conn.execute('RELEASE cs_media_ledger_capture')
                break
            # Capture rebinds the media point, so a retry must start from the parent.
            conn.execute('ROLLBACK TO cs_media_ledger_capture')
            conn.execute('RELEASE cs_media_ledger_capture')
            if attempt:
                raise MediaUnavailable('media files changed during capture')
            seen.update(moved)
        self._write(conn, point[0], started, current, seen, captured, full)

    def _continues(self, conn: sqlite3.Connection, started: int) -> bool:
        parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        state = conn.execute('SELECT point,format,captured_ns FROM cs_native_media_ledger_state WHERE id=1').fetchone()
        if parent is None or state is None or (state[0], state[1]) != (parent[0], FORMAT) or started < state[2]:
            return False
        return self._evidence_point(conn) == (parent[0], 1)

    @staticmethod
    def _write(conn: sqlite3.Connection, point: str, started: int, current: dict[int, str],
               seen: dict[int, str], captured: Iterable[int], full: bool) -> None:
        if full:
            conn.execute('DELETE FROM cs_native_media_ledger')
        else:
            stale = [row[0] for row in conn.execute('SELECT row_id FROM cs_native_media_ledger') if row[0] not in current]
            conn.executemany('DELETE FROM cs_native_media_ledger WHERE row_id=?', ((row_id,) for row_id in stale))
        conn.executemany('''INSERT INTO cs_native_media_ledger VALUES(?,?,?,?) ON CONFLICT(row_id) DO UPDATE
            SET path=excluded.path,fingerprint=excluded.fingerprint,racy=excluded.racy''',
            ((row_id, current[row_id], seen[row_id], int(recent(seen[row_id], started))) for row_id in captured))
        conn.execute('''INSERT INTO cs_native_media_ledger_state VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE
            SET point=excluded.point,format=excluded.format,captured_ns=excluded.captured_ns''', (point, FORMAT, started))


def _point(conn: sqlite3.Connection, table: str) -> tuple[str, int] | None:
    if not conn.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (table,)).fetchone():
        return None
    row = conn.execute(f'SELECT point,format FROM {table} WHERE id=1').fetchone()
    return None if row is None else (row[0], row[1])


class MessagesMediaLedger(PreparedMediaLedger):
    def _rows(self, conn: sqlite3.Connection) -> Iterable[tuple[int, str]]:
        return conn.execute("SELECT ROWID,filename FROM attachment WHERE filename>''")

    def _path(self, name: str) -> str | None:
        # Relative names are never opened: they would resolve against the cwd.
        return str(Path(name).expanduser()) if name.startswith(('~', '/')) else None

    def _evidence_point(self, conn: sqlite3.Connection) -> tuple[str, int] | None:
        return _point(conn, 'cs_native_media_state')

    def _captured(self, conn: sqlite3.Connection) -> set[int]:
        return {row[0] for row in conn.execute('SELECT row_id FROM cs_native_media_changes')}

    def _capture(self, conn: sqlite3.Connection, *, full: bool, dirty_ids: set[int] | None) -> None:
        prepared_media.prepare_media(conn, self.root, self.dd, full=full, dirty_ids=dirty_ids)


class WhatsAppMediaLedger(PreparedMediaLedger):
    def _rows(self, conn: sqlite3.Connection) -> Iterable[tuple[int, str]]:
        return conn.execute("SELECT Z_PK,ZMEDIALOCALPATH FROM ZWAMEDIAITEM WHERE ZMEDIALOCALPATH>''")

    def _path(self, name: str) -> str | None:
        return whatsapp_media.filename(self.root, name)

    def _evidence_point(self, conn: sqlite3.Connection) -> tuple[str, int] | None:
        return _point(conn, 'cs_native_wa_media_state')

    def _captured(self, conn: sqlite3.Connection) -> set[int]:
        return {row[0] for row in conn.execute('SELECT row_id FROM cs_native_wa_media_changes')}

    def _capture(self, conn: sqlite3.Connection, *, full: bool, dirty_ids: set[int] | None) -> None:
        whatsapp_media.prepare(conn, self.root, self.dd, full=full, dirty_ids=dirty_ids)
