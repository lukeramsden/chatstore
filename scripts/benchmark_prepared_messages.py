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
from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, policy_signature, run_prepared_sync
from scripts.benchmark_incremental import io_counters
from tests.fixtures.synthetic import build_messages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=int, default=100000)
    parser.add_argument('--pipeline', action='store_true')
    args = parser.parse_args()
    if args.messages < 7 or args.messages >= 500000:
        parser.error('--messages must be between seven and 499999')
    helper = Path(__file__).resolve().parents[1] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        parser.error('build the development Rust helper first')
    measurements = []
    def resources(before):
        after = io_counters()
        result = {'lifetime_peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
        if before is not None and after is not None:
            result.update({key: after[key] - before[key] for key in before})
        return result

    with tempfile.TemporaryDirectory(prefix='chatstore-message-routing-') as directory:
        root = Path(directory).resolve()
        native = build_messages(root / 'native', extra_messages=args.messages - 7)
        cfg = Config(source_paths={'messages': str(native)}, helper_path=str(helper))
        adapter = MessagesAdapter()
        path = native / 'chat.db'
        dd = DataDir(root / 'data')
        dd.create()
        restored = DataDir(root / 'restored')
        restored.create()
        target_ident = Identity()
        ident = Identity()
        scope, _ = ident.ensure_scope('messages')
        with closing(sqlite3.connect(path)) as writer, NativeMirror(root / 'mirror.sqlite') as mirror, closing(Cache(dd.cache)) as cache, closing(Cache(restored.cache)) as target:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('DELETE FROM message_attachment_join')
            writer.execute('DELETE FROM attachment')
            writer.execute('PRAGMA user_version=42')
            writer.commit()
            def pipeline(label):
                if not args.pipeline:
                    return
                paths = []
                events = 0
                before = io_counters()
                wall, cpu = time.perf_counter(), time.process_time()
                while True:
                    result = export_changes(cache, dd, cfg, ident, consumer='benchmark', output=dd.exports,
                                            password='synthetic-benchmark-only', media='text')
                    if result['action'] == 'written':
                        paths.append(Path(result['path']))
                        events += result['counts']['changes']
                    if not result['has_more']:
                        break
                measurements.append({'stage': 'export-verified-' + label,
                    'wall_seconds': time.perf_counter() - wall, 'parent_cpu_seconds': time.process_time() - cpu,
                    **resources(before), 'archives': len(paths), 'journal_events': events,
                    'ciphertext_bytes': sum(item.stat().st_size for item in paths)})
                print(f"export-verified-{label}: {measurements[-1]['wall_seconds']:.3f}s", file=sys.stderr, flush=True)
                before = io_counters()
                wall, cpu = time.perf_counter(), time.process_time()
                for item in paths:
                    result = import_archive(target, restored, target_ident, item, 'synthetic-benchmark-only')
                    assert result.action == 'imported', result
                measurements.append({'stage': 'import-verified-' + label,
                    'wall_seconds': time.perf_counter() - wall, 'parent_cpu_seconds': time.process_time() - cpu,
                    **resources(before), 'archives': len(paths)})
                print(f"import-verified-{label}: {measurements[-1]['wall_seconds']:.3f}s", file=sys.stderr, flush=True)
            with WalSource(path) as source:
                before = io_counters()
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
                    'native_rows_changed': capture.rows_changed, 'canonical_written': outcome.run['counts']['written'], **resources(before)})
                print(f"sync-baseline: {measurements[-1]['wall_seconds']:.3f}s", file=sys.stderr, flush=True)
                pipeline('baseline')
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
                    before = io_counters()
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
                        'wal_logical_bytes_read': wal_bytes, **resources(before)})
                    print(f"sync-{label}: {measurements[-1]['wall_seconds']:.3f}s", file=sys.stderr, flush=True)
                    pipeline(label)
                    assert mirror.pending() is None
                    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == capture.token
                if args.pipeline:
                    edited = next(row for row in cache.iter_records('messages') if row['text'] == 'routed historical edit')
                    assert target.get(edited['urn'])['revision_digest'] == edited['revision_digest']
                    assert {row['revision_digest'] for row in target.revisions_of(edited['urn'])} == {
                        row['revision_digest'] for row in cache.revisions_of(edited['urn'])}
                    assert target.observations_of(edited['urn'])[0]['present'] == 'absent_from_source'
                    assert any(row['text'] == 'routed new message' for row in target.iter_records('messages'))
    print(json.dumps({'messages': args.messages, 'pipeline': args.pipeline,
        'scope': 'Synthetic text-only Messages source with coordinated WAL validation, atomic native preparation, automatic routing, selected adapter extraction, canonical cache batches, durable coverage/epoch tracking, retained-unit sightings, targeted absence, canonical receipt/checkpoints and mirror acknowledgement. Baseline includes full image/native rows/index setup. Optional pipeline measures encrypted text-only export with built-in verification and verified import separately, then checks historical revisions, source absence and a newly inserted message in the isolated importer. File-backed media, WhatsApp, remote delivery, source setup/mutation and energy excluded. Storage counters are process-attributed; buffered attribution can cross stage boundaries and cached reads can be zero. RSS is lifetime parent peak, including baseline, not an incremental-only peak or helper RSS. Ciphertext bytes are potential transfer bytes, not measured network traffic. WAL logical bytes exclude SQLite/index/WAL-index reads. CPU splits parent and helper processes.',
        'measurements': measurements}, indent=2))


if __name__ == '__main__':
    main()
