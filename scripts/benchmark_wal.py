#!/usr/bin/env python3
"""Measure WAL inspection on synthetic history; not end-to-end sync or capture proof."""

import argparse
import json
import sqlite3
import tempfile
import time
from contextlib import closing, nullcontext
from pathlib import Path

from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import read_changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--coordinated', action='store_true', help='Include pinned SQLite source-view coordination (macOS).')
    args = parser.parse_args()
    if args.rows < 1:
        parser.error('--rows must be positive')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-wal-bench-') as directory:
        path = Path(directory).resolve() / 'synthetic.sqlite'
        wal = path.with_name(path.name + '-wal')
        with closing(sqlite3.connect(path)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, 'x' * 100) for i in range(1, args.rows + 1)))
            writer.commit()
            cursor = None

            with (WalSource(path) if args.coordinated else nullcontext()) as coordinator:
                def measure(label):
                    nonlocal cursor
                    wall, cpu = time.perf_counter(), time.process_time()
                    if coordinator is not None:
                        with coordinator.view(cursor, max_frames=None) as view:
                            change, next_cursor = view.changes, view.cursor
                    else:
                        change = read_changes(wal, cursor, max_frames=None)
                        next_cursor = change.cursor
                    results.append({'stage': label, 'wall_seconds': time.perf_counter() - wall,
                        'cpu_seconds': time.process_time() - cpu, 'logical_bytes_read': change.bytes_read,
                        'frames_read': change.frames_read, 'changed_pages': len(change.pages),
                        'commits': change.commits, 'wal_bytes': wal.stat().st_size})
                    cursor = next_cursor
    
                measure('initial-wal-validation')
                measure('no-change')
                writer.execute("UPDATE message SET text='historical edit' WHERE id=1")
                writer.commit()
                measure('historical-edit')
                writer.execute('INSERT INTO message VALUES (?,?)', (args.rows + 1, 'new message'))
                writer.commit()
                measure('new-row')
                writer.execute('DELETE FROM message WHERE id=1')
                writer.commit()
                measure('historical-delete')

    scope = ('Synthetic pinned SQLite view/WAL coordination; reported bytes cover WAL only, excluding index/SQLite metadata reads.'
             if args.coordinated else 'Synthetic WAL parsing only; excludes source-view coordination.')
    print(json.dumps({'rows': args.rows, 'coordinated': args.coordinated,
        'scope': scope + ' Excludes setup, SQL mutation and sync/archive work. Logical reads, not kernel storage IO. No energy measurement.',
        'measurements': results}, indent=2))


if __name__ == '__main__':
    main()
