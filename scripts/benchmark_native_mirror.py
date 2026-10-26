#!/usr/bin/env python3
"""Synthetic durable native-mirror preparation; canonical capture is excluded."""

import argparse
import json
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import WalPages


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
        with closing(sqlite3.connect(path)) as writer, NativeMirror(root / 'mirror.sqlite') as mirror:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,payload BLOB)')
            writer.executemany('INSERT INTO message VALUES (?,?)', ((i, b'x' * 100) for i in range(1, args.rows + 1)))
            writer.commit()
            with WalSource(path) as source:
                wall, cpu = time.perf_counter(), time.process_time()
                with source.view(max_frames=None) as view:
                    capture = mirror.initialize(view, ['message'], namespace='synthetic/account',
                        policy_tag='bench-v1', observed_at='2026-10-18T10:00:00Z', context={})
                    cursor = capture.cursor
                results.append({'stage': 'baseline-view-image-index-and-native-rows',
                    'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                    'native_rows_prepared': capture.rows_changed})
                mirror.acknowledge(capture.token, 'synthetic-benchmark-baseline')
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
                    with source.view(cursor) as view, WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
                        capture = mirror.prepare(view, pages, expected_token=capture.token,
                            namespace='synthetic/account', policy_tag='bench-v1',
                            observed_at=f'2026-10-18T10:{step * 5:02d}:00Z', context={})
                        changes = list(mirror.changes(capture.token))
                        assert {change.row_id for change in changes} == ({expected} if expected is not None else set())
                        results.append({'stage': label,
                            'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
                            'native_rows_prepared': capture.rows_changed, 'wal_pages_retrieved': pages.pages_read,
                            'wal_discovery_bytes': view.changes.bytes_read, 'wal_page_reader_bytes': pages.bytes_read})
                        cursor = capture.cursor
                    # The harness has no canonical cache. These stand-in receipts
                    # permit measuring later preparations, not preservation proof.
                    mirror.acknowledge(capture.token, f'synthetic-benchmark-{step}')
    print(json.dumps({'rows': args.rows,
        'scope': 'Synthetic coordinated WAL validation, persistent ownership, native-row comparison and durable prepared mirror transaction; includes reading retained old/new changes. Full image only at baseline. Canonical capture, media bytes, sync/archive work, source setup/mutation, kernel IO and energy excluded. Uses stand-in benchmark acknowledgements, not canonical receipts. SQLite/index/WAL-index reads additional to WAL byte counters.',
        'measurements': results}, indent=2))


if __name__ == '__main__':
    main()
