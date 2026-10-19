#!/usr/bin/env python3
"""Synthetic WAL -> native preparation -> dependency routing -> Messages extraction."""

import argparse
import json
import resource
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chatstore.adapters.base import ExtractStats
from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.messages.dependencies import route_changes
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import WalPages
from chatstore.config import Config
from chatstore.prepared_sync import PreparedSync
from tests.fixtures.synthetic import build_messages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=int, default=100000)
    args = parser.parse_args()
    if args.messages < 7 or args.messages >= 500000:
        parser.error('--messages must be between seven and 499999')
    helper = Path(__file__).resolve().parents[1] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        parser.error('build the development Rust helper first')
    measurements = []
    with tempfile.TemporaryDirectory(prefix='chatstore-message-routing-') as directory:
        root = Path(directory).resolve()
        native = build_messages(root / 'native', extra_messages=args.messages - 7)
        cfg = Config(source_paths={'messages': str(native)}, helper_path=str(helper))
        adapter = MessagesAdapter()
        path = native / 'chat.db'
        with closing(sqlite3.connect(path)) as writer, NativeMirror(root / 'mirror.sqlite') as mirror:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('PRAGMA user_version=42')
            writer.commit()
            with WalSource(path) as source:
                wall, cpu = time.perf_counter(), time.process_time()
                children = resource.getrusage(resource.RUSAGE_CHILDREN)
                with source.view(max_frames=None) as view:
                    tables = [row[0] for row in view.connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
                    capture = mirror.initialize(view, tables, namespace='messages/synthetic-scope', policy_tag='benchmark', observed_at='2026-10-18T10:00:00Z', context={})
                stats = ExtractStats()
                count = sum(1 for _ in adapter.extract(PreparedSync(mirror, capture, 'chat').snapshot(), 'synthetic-scope', cfg, stats, None, full=False))
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                measurements.append({'stage': 'baseline-native-preparation-and-full-extraction',
                    'wall_seconds': time.perf_counter() - wall, 'parent_cpu_seconds': time.process_time() - cpu,
                    'helper_cpu_seconds': after.ru_utime + after.ru_stime - children.ru_utime - children.ru_stime,
                    'native_rows_changed': capture.rows_changed, 'canonical_records_emitted': count})
                mirror.acknowledge(capture.token, 'synthetic-benchmark-baseline')
                operations = [
                    ('clean', None, (), set()),
                    ('old-edit', "UPDATE message SET text='routed historical edit' WHERE ROWID=1", (), {1}),
                    ('new-message', 'insert', (), {500000}),
                    ('chat-title', "UPDATE chat SET display_name='routed chat title' WHERE ROWID=1", (), set()),
                    ('attachment-metadata', "UPDATE attachment SET mime_type='video/mp4' WHERE ROWID=1", (), {3}),
                    ('old-delete', 'DELETE FROM message WHERE ROWID=1', (), {1}),
                ]
                for step, (label, sql, values, expected) in enumerate(operations, 1):
                    if sql == 'insert':
                        writer.row_factory = sqlite3.Row
                        row = dict(writer.execute('SELECT * FROM message WHERE ROWID=1').fetchone())
                        row.update(ROWID=500000, guid='CCCCCCCC-0000-4000-8000-000000500000', text='routed new message')
                        fields = ','.join('"' + key + '"' for key in row)
                        writer.execute(f'INSERT INTO message({fields}) VALUES({",".join("?" for _ in row)})', list(row.values()))
                    elif sql:
                        writer.execute(sql, values)
                    writer.commit()
                    wall, cpu = time.perf_counter(), time.process_time()
                    children = resource.getrusage(resource.RUSAGE_CHILDREN)
                    with source.view(capture.cursor) as view, WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
                        capture = mirror.prepare(view, pages, expected_token=capture.token,
                            namespace=capture.namespace, policy_tag=capture.policy_tag,
                            observed_at=f'2026-10-18T10:{step * 5:02d}:00Z', context={})
                        wal_bytes = view.changes.bytes_read + pages.bytes_read
                    selection = route_changes(mirror, capture)
                    assert selection.rowids == expected
                    stats = ExtractStats(selected_message_rowids=set(selection.rowids))
                    count = sum(1 for _ in adapter.extract(PreparedSync(mirror, capture, 'chat').snapshot(), 'synthetic-scope', cfg, stats, None, full=False))
                    assert not stats.errors, stats.errors
                    after = resource.getrusage(resource.RUSAGE_CHILDREN)
                    measurements.append({'stage': label, 'wall_seconds': time.perf_counter() - wall,
                        'parent_cpu_seconds': time.process_time() - cpu,
                        'helper_cpu_seconds': after.ru_utime + after.ru_stime - children.ru_utime - children.ru_stime,
                        'native_rows_changed': capture.rows_changed, 'routed_message_rows': len(selection.rowids),
                        'canonical_messages_emitted': stats.counts.get('messages', 0), 'canonical_records_emitted': count,
                        'wal_logical_bytes_read': wal_bytes})
                    # Canonical cache commits are outside this component harness.
                    mirror.acknowledge(capture.token, f'synthetic-benchmark-{step}')
    print(json.dumps({'messages': args.messages,
        'scope': 'Synthetic coordinated WAL validation, atomic native-mirror preparation, automatic old/new dependency routing, and selected Messages adapter extraction. Includes helper launch/SQL/decoding and canonical construction; baseline includes full image/rows/index setup. Excludes canonical cache writes and retained-unit freshness/deletion reconciliation, durable media preparation, archives, source setup/mutation, kernel IO and energy. Acknowledgements use stand-in benchmark receipt IDs, not canonical receipts. Logical byte counts cover WAL only; SQLite/index/WAL-index reads are additional.',
        'measurements': measurements}, indent=2))


if __name__ == '__main__':
    main()
