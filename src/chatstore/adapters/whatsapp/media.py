"""Point-bound WhatsApp file evidence and live file-change coverage."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from ...paths import DataDir
from .. import media_capture
from ..base import IncompatibleSource
from ..media_capture import CapturedMedia, capture_attachment

POLICY_MODULES = (media_capture,)


def filename(root: Path, relative: str | None) -> str | None:
    if not relative or relative.startswith('/') or '..' in relative.split('/'):
        return None
    return str(root.absolute() / relative)


def prepare(conn: sqlite3.Connection, root: Path, dd: DataDir, *, full: bool,
            dirty_ids: Iterable[int] | None = None) -> None:
    """Retain stable bytes before the candidate native point is published."""
    if not conn.in_transaction:
        raise IncompatibleSource('WhatsApp media preparation requires the native writer transaction')
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
    if point is None:
        raise IncompatibleSource('WhatsApp media preparation requires a candidate page point')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_wa_media(row_id INTEGER PRIMARY KEY,observation TEXT NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_wa_media_state(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,format INTEGER NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_wa_media_changes(row_id INTEGER PRIMARY KEY)')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_wa_media_coverage(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,full INTEGER NOT NULL)')
    conn.execute('DELETE FROM cs_native_wa_media_changes')
    ids: Iterator[int]
    if full:
        conn.execute('DELETE FROM cs_native_wa_media')
        ids = (row[0] for row in conn.execute('SELECT Z_PK FROM ZWAMEDIAITEM'))
    else:
        if dirty_ids is None:
            raise IncompatibleSource('WhatsApp media delta requires complete file-change coverage')
        parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        previous = conn.execute('SELECT point,format FROM cs_native_wa_media_state WHERE id=1').fetchone()
        if parent is None or previous is None or (previous[0], previous[1]) != (parent[0], 1):
            raise IncompatibleSource('WhatsApp media continuation requires matching parent evidence')
        selected = set(dirty_ids)
        selected.update(row[0] for row in conn.execute("SELECT row_id FROM cs_native_mirror_changes WHERE name='ZWAMEDIAITEM'"))
        ids = iter(sorted(selected))
    for row_id in ids:
        if not full:
            conn.execute('INSERT INTO cs_native_wa_media_changes VALUES(?)', (row_id,))
        row = conn.execute('SELECT ZMEDIALOCALPATH,ZFILESIZE FROM ZWAMEDIAITEM WHERE Z_PK=?', (row_id,)).fetchone()
        if row is None:
            conn.execute('DELETE FROM cs_native_wa_media WHERE row_id=?', (row_id,))
            continue
        relative, size = row
        path = filename(root, relative)
        if not relative:
            captured = CapturedMedia(relative, 'not_downloaded', None, size, None)
        elif path is None:
            captured = CapturedMedia(relative, 'unknown', None, size, None)
        else:
            captured = replace(capture_attachment({'filename': path, 'total_bytes': size}, root, dd), filename=relative)
        conn.execute('INSERT INTO cs_native_wa_media VALUES(?,?) ON CONFLICT(row_id) DO UPDATE SET observation=excluded.observation',
                     (row_id, json.dumps(asdict(captured), sort_keys=True)))
    conn.execute('INSERT INTO cs_native_wa_media_state VALUES(1,?,1) ON CONFLICT(id) DO UPDATE SET point=excluded.point,format=1', (point[0],))
    conn.execute('INSERT INTO cs_native_wa_media_coverage VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET point=excluded.point,full=excluded.full',
                 (point[0], int(full)))


def read(conn: sqlite3.Connection, token: str, item: dict[str, Any]) -> tuple[str, str | None, int | None]:
    row = conn.execute('''SELECT media.observation FROM cs_native_wa_media media
        CROSS JOIN cs_native_wa_media_state state CROSS JOIN cs_native_mirror_state native
        WHERE media.row_id=? AND state.id=1 AND state.point=? AND state.format=1
        AND native.id=1 AND native.token=?''', (item.get('Z_PK'), token, token)).fetchone()
    if row is None:
        raise IncompatibleSource('WhatsApp media has no retained observation at this native point')
    captured = CapturedMedia(**json.loads(row[0]))
    if captured.filename != item.get('ZMEDIALOCALPATH'):
        raise IncompatibleSource('WhatsApp media path differs from retained evidence')
    return captured.availability, captured.sha256, captured.size

