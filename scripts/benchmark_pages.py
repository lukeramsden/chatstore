#!/usr/bin/env python3
"""Synthetic row-page mapping microbenchmark, including snapshot preparation costs.

The delta path uses already prepared private images. It does not implement native
incremental snapshot maintenance or adapter extraction, and is not a sync benchmark.
"""

import argparse
import json
import sqlite3
import tempfile
import time
from collections import defaultdict
from contextlib import closing
from pathlib import Path

from chatstore.adapters.page_index import PageIndex
from chatstore.adapters.pages import SQLitePages
from chatstore.adapters.wal import read_changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--persistent-index', action='store_true')
    args = parser.parse_args()
    if args.rows < 1:
        parser.error('--rows must be positive')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-pages-bench-') as directory:
        root = Path(directory)
        source = root / 'source.sqlite'
        old, new = root / 'before.sqlite', root / 'after.sqlite'
        with closing(sqlite3.connect(source)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,payload BLOB)')
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b'x' * 100) for i in range(1, args.rows + 1)))
            writer.commit()
            schema_tag = str(writer.execute('PRAGMA schema_version').fetchone()[0])
            table_root = writer.execute("SELECT rootpage FROM sqlite_schema WHERE name='message'").fetchone()[0]
            cursor = read_changes(source.with_name(source.name + '-wal'), max_frames=None).cursor

            def prepare(path, label):
                wall, cpu = time.perf_counter(), time.process_time()
                with closing(sqlite3.connect(path)) as copy:
                    writer.backup(copy)
                results.append({'stage': label, 'wall_seconds': time.perf_counter() - wall,
                    'cpu_seconds': time.process_time() - cpu, 'image_bytes': path.stat().st_size})

            prepare(old, 'prepare-baseline-image')
            wall, cpu = time.perf_counter(), time.process_time()
            index = defaultdict(list)
            persistent = PageIndex(root / 'index.sqlite') if args.persistent_index else None
            with SQLitePages(old) as pages:
                if persistent:
                    baseline = persistent.build(pages, {'message': table_root}, point='p0', schema_tag=schema_tag)
                    indexed_rows = sum(map(len, baseline.rows.values()))
                else:
                    for row in pages.rows(table_root):
                        index[row.page].append(row.row_id)
                    indexed_rows = sum(map(len, index.values()))
                results.append({'stage': 'build-baseline-page-map', 'wall_seconds': time.perf_counter() - wall,
                    'cpu_seconds': time.process_time() - cpu, 'pages_read': pages.pages_read,
                    'logical_bytes_read': pages.bytes_read, 'rows_indexed': indexed_rows})
            writer.execute('UPDATE message SET payload=? WHERE id=1', (b'y' * 100,))
            writer.commit()
            change = read_changes(source.with_name(source.name + '-wal'), cursor)
            prepare(new, 'prepare-changed-image')
            wall, cpu = time.perf_counter(), time.process_time()
            candidates = set()
            with SQLitePages(new) as pages:
                if persistent:
                    selected = persistent.update(pages, {'message': table_root}, change.pages,
                        expected_point='p0', point='p1', schema_tag=schema_tag)
                    candidates.update(selected.rows.get('message', ()))
                else:
                    for page in change.pages:
                        candidates.update(index[page])
                        leaf = pages.tree_page(page)
                        assert not leaf.children, 'fixture edit unexpectedly changed tree structure'
                        candidates.update(cell.row_id for cell in leaf.cells)
                logical_bytes, page_reads = pages.bytes_read, pages.pages_read
                pages.check_stable()
            ordered = sorted(candidates)
            placeholders = ','.join('?' * len(ordered))
            query = f'SELECT id,payload FROM message WHERE id IN ({placeholders})'
            with closing(sqlite3.connect(old.as_uri() + '?mode=ro', uri=True)) as prior:
                before_rows = dict(prior.execute(query, ordered))
            after_rows = dict(writer.execute(query, ordered))
            changed = {key for key in before_rows.keys() | after_rows.keys() if before_rows.get(key) != after_rows.get(key)}
            assert changed == {1}
            results.append({'stage': 'decode-changed-leaf-and-query-candidates',
                'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                'pages_read': page_reads, 'logical_bytes_read': logical_bytes,
                'candidate_rows': len(candidates), 'changed_rows': len(changed)})
            if persistent:
                wall, cpu = time.perf_counter(), time.process_time()
                with SQLitePages(new) as pages:
                    clean = persistent.update(pages, {'message': table_root}, (),
                        expected_point='p1', point='p2', schema_tag=schema_tag)
                    assert not clean.rows
                    results.append({'stage': 'clean-index-continuation',
                        'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                        'pages_read': pages.pages_read, 'logical_bytes_read': pages.bytes_read})
                persistent.close()
    print(json.dumps({'rows': args.rows, 'persistent_index': args.persistent_index,
        'scope': 'Synthetic baseline mapping and changed-leaf selection only. Snapshot copies shown separately; their incremental replacement is not implemented. Decoder logical reads exclude SQLite candidate-query IO, WAL parsing, capture and archives. No energy measurement.',
        'measurements': results}, indent=2))


if __name__ == '__main__':
    main()
