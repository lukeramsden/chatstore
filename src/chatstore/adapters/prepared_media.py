"""Point-bound Messages media observations inside the native mirror transaction."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..paths import DataDir
from . import media_capture
from .base import IncompatibleSource
from .media_capture import CapturedMedia, capture_attachment

POLICY_MODULES = (media_capture,)


def prepare_media(conn: sqlite3.Connection, root: Path, dd: DataDir, *, full: bool,
                  dirty_ids: Iterable[int] | None = None) -> None:
    """Called as the mirror's pre-commit validator, never after publication.

    Full baselines enumerate attachments. Deltas visit changed native attachment
    rows plus explicit file-watcher IDs; unchanged file evidence needs a reliable
    watcher before a production caller may retain it.
    """
    if not conn.in_transaction:
        raise IncompatibleSource('media preparation requires the native writer transaction')
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
    if point is None:
        raise IncompatibleSource('media preparation has no candidate page point')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_media(row_id INTEGER PRIMARY KEY,observation TEXT NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_media_state(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,format INTEGER NOT NULL)')
    ids: Iterator[int]
    if full:
        conn.execute('DELETE FROM cs_native_media')
        ids = (row[0] for row in conn.execute('SELECT ROWID FROM attachment'))
    else:
        if dirty_ids is None:
            raise IncompatibleSource('media delta requires an explicit complete file-change set')
        parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        previous = conn.execute('SELECT point,format FROM cs_native_media_state WHERE id=1').fetchone()
        if parent is None or previous is None or (previous[0], previous[1]) != (parent[0], 1):
            raise IncompatibleSource('media continuation has no matching parent evidence')
        selected = set(dirty_ids)
        selected.update(row[0] for row in conn.execute("SELECT row_id FROM cs_native_mirror_changes WHERE name='attachment'"))
        ids = iter(sorted(selected))
    for row_id in ids:
        row = conn.execute('SELECT filename,total_bytes FROM attachment WHERE ROWID=?', (row_id,)).fetchone()
        if row is None:
            conn.execute('DELETE FROM cs_native_media WHERE row_id=?', (row_id,))
            continue
        captured = capture_attachment({'filename': row[0], 'total_bytes': row[1]}, root, dd)
        conn.execute('INSERT INTO cs_native_media VALUES(?,?) ON CONFLICT(row_id) DO UPDATE SET observation=excluded.observation',
                     (row_id, json.dumps(asdict(captured), sort_keys=True)))
    conn.execute('INSERT INTO cs_native_media_state VALUES(1,?,1) ON CONFLICT(id) DO UPDATE SET point=excluded.point,format=1', (point[0],))


def read_media(conn: sqlite3.Connection, token: str, attachment: dict[str, Any]) -> tuple[str, str | None, int | None, str | None]:
    row = conn.execute("""SELECT media.observation FROM cs_native_media media
        CROSS JOIN cs_native_media_state state CROSS JOIN cs_native_mirror_state native
        WHERE media.row_id=? AND state.id=1 AND state.point=? AND state.format=1
        AND native.id=1 AND native.token=?""", (attachment.get('rowid'), token, token)).fetchone()
    if row is None:
        raise IncompatibleSource('prepared attachment has no retained observation at this native point')
    captured = CapturedMedia(**json.loads(row[0]))
    if captured.filename != attachment.get('filename'):
        raise IncompatibleSource('prepared attachment filename differs from retained media evidence')
    return captured.probe
