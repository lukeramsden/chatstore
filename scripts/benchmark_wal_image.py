#!/usr/bin/env python3
"""Synthetic coordinated WAL-to-row selection, without per-update full images."""

import argparse
import json
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

from chatstore.adapters.page_index import PageIndex
from chatstore.adapters.pages import SQLitePages
from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import WalPages
from chatstore.adapters.wal_image import WalPageImage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=100000)
    args = parser.parse_args()
    if args.rows < 2:
        parser.error('--rows must be at least two')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-wal-image-bench-') as directory:
        root = Path(directory).resolve()
        path = root / 'native.sqlite'
        with closing(sqlite3.connect(path)) as writer, PageIndex(root / 'index.sqlite') as index:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,payload BLOB)')
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b'x' * 100) for i in range(1, args.rows + 1)))
            writer.commit()
            with WalSource(path) as source:
                wall, cpu = time.perf_counter(), time.process_time()
                with source.view(max_frames=None) as view:
                    roots = dict(view.connection.execute("SELECT name,rootpage FROM sqlite_schema WHERE name='message'"))
                    with closing(sqlite3.connect(root / 'baseline.sqlite')) as copy:
                        view.connection.backup(copy)
                    with SQLitePages(root / 'baseline.sqlite') as image:
                        index.build(image, roots, point='p0', schema_tag=str(view.schema_version))
                    cursor = view.cursor
                results.append({'stage': 'baseline-view-full-image-and-index',
                    'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                    'image_bytes': (root / 'baseline.sqlite').stat().st_size})
                operations = [('old-edit', 'UPDATE message SET payload=? WHERE id=1', (b'y' * 100,), 1),
                    ('new-row', 'INSERT INTO message VALUES (?,?)', (args.rows + 1, b'n' * 100), args.rows + 1),
                    ('old-delete', 'DELETE FROM message WHERE id=2', (), 2),
                    ('large-payload', 'UPDATE message SET payload=? WHERE id=1', (b'o' * 50000,), 1),
                    ('overflow-only', None, (), 1), ('clean', None, (), None)]
                for step, (label, sql, values, expected) in enumerate(operations, 1):
                    if sql:
                        writer.execute(sql, values)
                        writer.commit()
                    elif label == 'overflow-only':
                        with writer.blobopen('message', 'payload', 1) as blob:
                            blob.seek(20000)
                            blob.write(b'Z')
                        writer.commit()
                    wall, cpu = time.perf_counter(), time.process_time()
                    with source.view(cursor) as view, index.snapshot() as old, WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
                        image = WalPageImage(view, pages, old, expected_point=f'p{step - 1}', schema_tag=str(view.schema_version))
                        selected = index.update(image, roots, view.changes.pages, expected_point=f'p{step - 1}', point=f'p{step}', schema_tag=str(view.schema_version))
                        candidates = sorted(selected.rows.get('message', ()))
                        if expected is not None:
                            assert expected in candidates
                        else:
                            assert not candidates
                        # Match the indexed-row read needed by a future native mirror.
                        rows = []
                        for offset in range(0, len(candidates), 500):
                            batch = candidates[offset:offset + 500]
                            placeholders = ','.join('?' * len(batch))
                            rows.extend(view.connection.execute(f'SELECT id,payload FROM message WHERE id IN ({placeholders})', batch))
                        results.append({'stage': label,
                            'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                            'candidate_rows': len(candidates), 'source_rows_returned': len(rows),
                            'tree_nodes_visited': selected.tree_pages_read, 'wal_pages_retrieved': pages.pages_read,
                            'wal_discovery_bytes': view.changes.bytes_read, 'wal_page_reader_bytes': pages.bytes_read})
                        cursor = view.cursor
    print(json.dumps({'rows': args.rows,
        'scope': 'Synthetic coordinated WAL validation, persistent page ownership update and indexed candidate SQL reads. Full image only at baseline; no per-update image copies. Excludes source setup/mutation, durable native mirror, canonical extraction, sync/archive work, kernel IO and energy. SQLite/index/WAL-index reads additional to WAL byte counters.',
        'measurements': results}, indent=2))


if __name__ == '__main__':
    main()
