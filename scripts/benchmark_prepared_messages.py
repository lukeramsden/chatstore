#!/usr/bin/env python3
"""Synthetic WAL -> native preparation -> selective canonical capture and acknowledgement."""

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

from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import WalPages
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, policy_signature, run_prepared_sync
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
        dd = DataDir(root / 'data')
        dd.create()
        ident = Identity()
        scope, _ = ident.ensure_scope('messages')
        with closing(sqlite3.connect(path)) as writer, NativeMirror(root / 'mirror.sqlite') as mirror, closing(Cache(dd.cache)) as cache:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('DELETE FROM message_attachment_join')
            writer.execute('DELETE FROM attachment')
            writer.execute('PRAGMA user_version=42')
            writer.commit()
            with WalSource(path) as source:
                wall, cpu = time.perf_counter(), time.process_time()
                children = resource.getrusage(resource.RUSAGE_CHILDREN)
                with source.view(max_frames=None) as view:
                    tables = [row[0] for row in view.connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
                    capture = mirror.initialize(view, tables, namespace=f'messages/{scope}', policy_tag=policy_signature(adapter, cfg), observed_at='2026-10-18T10:00:00Z', context={})
                outcome = run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, capture, 'chat'))
                assert outcome.exit_code == 0, outcome.run
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                measurements.append({'stage': 'baseline-native-and-canonical-capture',
                    'wall_seconds': time.perf_counter() - wall, 'parent_cpu_seconds': time.process_time() - cpu,
                    'helper_cpu_seconds': after.ru_utime + after.ru_stime - children.ru_utime - children.ru_stime,
                    'native_rows_changed': capture.rows_changed, 'canonical_written': outcome.run['counts']['written']})
                operations = [
                    ('clean', None, (), set()),
                    ('old-edit', "UPDATE message SET text='routed historical edit' WHERE ROWID=1", (), {1}),
                    ('new-message', 'insert', (), {500000}),
                    ('chat-title', "UPDATE chat SET display_name='routed chat title' WHERE ROWID=1", (), set()),
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
                    outcome = run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, capture, 'chat'))
                    assert outcome.exit_code == 0, outcome.run
                    expected_messages = len(expected) if label != 'old-delete' else 0
                    assert outcome.run['counts'].get('messages', 0) == expected_messages
                    after = resource.getrusage(resource.RUSAGE_CHILDREN)
                    measurements.append({'stage': label, 'wall_seconds': time.perf_counter() - wall,
                        'parent_cpu_seconds': time.process_time() - cpu,
                        'helper_cpu_seconds': after.ru_utime + after.ru_stime - children.ru_utime - children.ru_stime,
                        'native_rows_changed': capture.rows_changed,
                        'canonical_messages_emitted': outcome.run['counts'].get('messages', 0),
                        'canonical_written': outcome.run['counts']['written'],
                        'absent_withdrawn': outcome.run['counts'].get('absent_from_source:withdrawn', 0),
                        'wal_logical_bytes_read': wal_bytes})
                    assert mirror.pending() is None
                    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == capture.token
    print(json.dumps({'messages': args.messages,
        'scope': 'Synthetic text-only Messages source with coordinated WAL validation, atomic native preparation, automatic routing, selected adapter extraction, canonical cache batches, durable coverage/epoch tracking, retained-unit sightings, targeted absence, canonical receipt/checkpoints and mirror acknowledgement. Baseline includes full image/native rows/index setup. File-backed media, WhatsApp, archive/encryption/import/delivery, source setup/mutation, kernel IO and energy excluded. WAL logical bytes exclude SQLite/index/WAL-index reads. CPU splits parent and helper processes.',
        'measurements': measurements}, indent=2))


if __name__ == '__main__':
    main()
