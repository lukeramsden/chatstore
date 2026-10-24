"""Disjoint latest-sighting intervals; historical sightings remain in the journal."""

from __future__ import annotations

import sqlite3
from typing import Any


def time_sql(kind: str, key: str) -> str:
    """One predecessor lookup, including gaps; never scan backwards through other ranges."""
    if kind not in ('record', 'observation'):
        raise ValueError(kind)
    return f"""(SELECT CASE WHEN last_id >= {key} THEN observed_at END
        FROM freshness_ranges WHERE kind='{kind}' AND first_id <= {key}
        ORDER BY first_id DESC LIMIT 1)"""


def lookup(conn: sqlite3.Connection, kind: str, key: int, observation: dict[str, Any]) -> dict[str, Any]:
    row = conn.execute('''SELECT last_id,observed_at,sync_run FROM freshness_ranges
        WHERE kind=? AND first_id<=? ORDER BY first_id DESC LIMIT 1''', (kind, key)).fetchone()
    if row and row[0] >= key and row[1] >= observation['observed_at']:
        return observation | {'observed_at': row[1], 'sync_run': row[2]}
    return observation


def stamp(conn: sqlite3.Connection, kind: str, spans: list[list[int]], at: int, run: str | None) -> None:
    """Replace only covered, older projection metadata. Never delete journal evidence."""
    for lo, hi in spans:
        predecessor = conn.execute('''SELECT first_id FROM freshness_ranges
            WHERE kind=? AND first_id<=? ORDER BY first_id DESC LIMIT 1''', (kind, lo)).fetchone()
        start = predecessor[0] if predecessor else lo
        old = [tuple(r) for r in conn.execute('''SELECT first_id,last_id,observed_at,sync_run
            FROM freshness_ranges WHERE kind=? AND first_id BETWEEN ? AND ? AND last_id>=?
            ORDER BY first_id''', (kind, start, hi, lo))]
        pieces: list[tuple[int, int, int, str | None]] = []
        position = lo
        for first, last, observed, sync_run in old:
            if first < lo:
                pieces.append((first, lo - 1, observed, sync_run))
            if first > position:
                pieces.append((position, first - 1, at, run))
            a, b = max(lo, first), min(hi, last)
            pieces.append((a, b, observed, sync_run) if observed > at else (a, b, at, run))
            position = b + 1
            if last > hi:
                pieces.append((hi + 1, last, observed, sync_run))
        if position <= hi:
            pieces.append((position, hi, at, run))
        # Include adjacent intervals so stable full-scan batches coalesce across batch boundaries.
        left = conn.execute('''SELECT first_id,last_id,observed_at,sync_run FROM freshness_ranges
            WHERE kind=? AND last_id=?''', (kind, pieces[0][0] - 1)).fetchone()
        right = conn.execute('''SELECT first_id,last_id,observed_at,sync_run FROM freshness_ranges
            WHERE kind=? AND first_id=?''', (kind, pieces[-1][1] + 1)).fetchone()
        removed = [r[0] for r in old]
        if left and tuple(left)[2:] == pieces[0][2:]:
            removed.append(left[0])
            pieces.insert(0, tuple(left))
        if right and tuple(right)[2:] == pieces[-1][2:]:
            removed.append(right[0])
            pieces.append(tuple(right))
        merged: list[tuple[int, int, int, str | None]] = []
        for piece in pieces:
            if merged and merged[-1][1] + 1 == piece[0] and merged[-1][2:] == piece[2:]:
                previous = merged.pop()
                merged.append((previous[0], piece[1], piece[2], piece[3]))
            else:
                merged.append(piece)
        if merged == old:
            continue
        conn.executemany('DELETE FROM freshness_ranges WHERE kind=? AND first_id=?',
                         [(kind, first) for first in removed])
        conn.executemany('INSERT INTO freshness_ranges VALUES (?,?,?,?,?)',
                         [(kind, *piece) for piece in merged])


def forget(conn: sqlite3.Connection, kind: str, keys: list[int]) -> None:
    """Remove projection metadata when an explicit purge removes its canonical record."""
    for key in sorted(set(keys)):
        row = conn.execute('''SELECT first_id,last_id,observed_at,sync_run FROM freshness_ranges
            WHERE kind=? AND first_id<=? ORDER BY first_id DESC LIMIT 1''', (kind, key)).fetchone()
        if not row or row[1] < key:
            continue
        first, last, at, run = row
        conn.execute('DELETE FROM freshness_ranges WHERE kind=? AND first_id=?', (kind, first))
        if first < key:
            conn.execute('INSERT INTO freshness_ranges VALUES (?,?,?,?,?)', (kind, first, key - 1, at, run))
        if last > key:
            conn.execute('INSERT INTO freshness_ranges VALUES (?,?,?,?,?)', (kind, key + 1, last, at, run))
