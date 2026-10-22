#!/usr/bin/env python3
"""Measure the production native WhatsApp collector and optional encrypted pipeline."""

import argparse
import hashlib
import json
import platform
import random
import resource
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir
from chatstore.sync_session import SyncSession
from scripts.benchmark_incremental import io_counters
from tests.fixtures.synthetic import build_whatsapp

PASSWORD = 'synthetic-benchmark-only'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=int, default=100000)
    parser.add_argument('--aliases', type=int, default=1000)
    parser.add_argument('--pipeline', action='store_true')
    parser.add_argument('--file-media', action='store_true')
    parser.add_argument('--media-items', type=int, default=1, help='Total native media rows, including rows without file paths.')
    parser.add_argument('--media-files', type=int, default=1, help='Distinct file paths when --file-media is enabled.')
    parser.add_argument('--media-bytes', type=int, default=256 * 1024, help='Bytes per synthetic media file.')
    parser.add_argument('--chats', type=int, default=3)
    parser.add_argument('--wal-resets', action='store_true')
    parser.add_argument('--checkpoint-setup', action='store_true', help='Checkpoint fixture construction and commit a fresh WAL frame before timings.')
    parser.add_argument('--archive-media', choices=('text', 'available-media'), default='text')
    parser.add_argument('--batch-limit', type=int, default=5000)
    parser.add_argument('--sqlite-stats', action='store_true')
    parser.add_argument('--sample-resources', action='store_true', help='Sample parent resident memory, footprint and canonical WAL file sizes every 10 ms.')
    parser.add_argument('--disable-cache-spill', action='store_true')
    parser.add_argument('--database-layout', action='store_true')
    args = parser.parse_args()
    if sys.platform != 'darwin' or args.messages < 8 or args.aliases < 0 or not 1 <= args.batch_limit <= 100000:
        parser.error('requires macOS, at least eight messages, nonnegative aliases, and batch limit 1..100000')
    if not 1 <= args.media_items <= max(1, args.messages - 7) or not 1 <= args.media_files <= args.media_items:
        parser.error('media rows must fit the extra messages, with 1..media-items file paths')
    if args.media_bytes < 1 or args.chats < 3 or (args.media_files > 1 and not args.file_media):
        parser.error('requires positive media bytes, at least three chats, and --file-media for multiple files')
    measurements = []
    layouts = {}
    connections = {}
    def sqlite_stats():
        if not args.sqlite_stats:
            return {}
        result = {}
        for name, cache in connections.items():
            result[name] = {key: cache.conn.execute('SELECT chatstore_benchmark_db_status(?)', (code,)).fetchone()[0]
                            for key, code in (('cache_bytes', 1), ('cache_misses', 8), ('cache_writes', 9), ('cache_spills', 12))}
            result[name]['database_bytes'] = cache.path.stat().st_size
            wal = cache.path.with_name(cache.path.name + '-wal')
            result[name]['wal_bytes'] = wal.stat().st_size if wal.exists() else 0
        return result
    def measure(stage, operation):
        sqlite_before = sqlite_stats()
        before = io_counters()
        peaks = {}
        stop = threading.Event()
        sampler = None
        def sample():
            values = io_counters(include_memory=True) or {}
            for name in ('resident_bytes', 'physical_footprint_bytes'):
                if name in values:
                    peaks[name] = max(peaks.get(name, 0), values[name])
            for name, cache in connections.items():
                wal = cache.path.with_name(cache.path.name + '-wal')
                try:
                    size = wal.stat().st_size
                except FileNotFoundError:
                    size = 0
                key = name + '_wal_bytes'
                peaks[key] = max(peaks.get(key, 0), size)
            peaks['samples'] = peaks.get('samples', 0) + 1
        def sampling():
            try:
                while not stop.wait(0.01):
                    sample()
            except OSError as exc:
                peaks['error'] = type(exc).__name__
        if args.sample_resources:
            sample()
            sampler = threading.Thread(target=sampling, daemon=True)
            sampler.start()
        wall, cpu = time.perf_counter(), time.process_time()
        try:
            result, details = operation()
            elapsed = {'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu}
        finally:
            stop.set()
            if sampler is not None:
                sampler.join()
        if args.sample_resources:
            sample()
            elapsed['sampled_peaks'] = peaks

        after = io_counters()
        if before is not None and after is not None:
            elapsed.update({key: after[key] - before[key] for key in before})
        sqlite_after = sqlite_stats()
        if sqlite_after:
            elapsed['sqlite'] = {name: {
                **{key: values[key] - sqlite_before[name][key] for key in ('cache_misses', 'cache_writes', 'cache_spills')},
                **{key + '_after': values[key] for key in ('cache_bytes', 'database_bytes', 'wal_bytes')},
            } for name, values in sqlite_after.items()}
        measurements.append({'stage': stage, **elapsed,
                             'lifetime_peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, **details})
        print(f'{stage}: {elapsed["wall_seconds"]:.3f}s wall, {elapsed["cpu_seconds"]:.3f}s CPU', file=sys.stderr, flush=True)
        return result
    with tempfile.TemporaryDirectory(prefix='chatstore-native-wa-') as directory:
        root = Path(directory).resolve()
        extension = root / 'sqlite_stats.dylib'
        if args.sqlite_stats:
            subprocess.run(['/usr/bin/cc', '-dynamiclib', '-Wall', '-Wextra', '-Werror',
                            str(Path(__file__).with_name('sqlite_benchmark_stats.c')), '-o', str(extension)], check=True)
        native = build_whatsapp(root / 'source', extra_messages=args.messages - 8)
        media_path = native / 'Message/Media/x/pic.jpg'
        if args.file_media:
            media_path.write_bytes(random.Random(7).randbytes(args.media_bytes))
        dd, restored = DataDir(root / 'data'), DataDir(root / 'restored')
        dd.create()
        restored.create()
        cfg, ident, target_ident = Config(source_paths={'whatsapp': str(native)}), Identity(), Identity()
        ident.ensure_scope('whatsapp')
        adapter = WhatsAppAdapter()
        with (closing(sqlite3.connect(native / 'ChatStorage.sqlite')) as writer,
              closing(sqlite3.connect(native / 'LID.sqlite')) as lid,
              closing(Cache(dd.cache)) as cache, closing(Cache(restored.cache)) as target, SyncSession() as session):
            connections.update(producer=cache, importer=target)
            for connection in (cache.conn, target.conn):
                if args.sqlite_stats:
                    connection.enable_load_extension(True)
                    try:
                        connection.load_extension(str(extension))
                    finally:
                        connection.enable_load_extension(False)
                if args.disable_cache_spill:
                    connection.execute('PRAGMA cache_spill=OFF')
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            chat_columns = [row[1] for row in writer.execute('PRAGMA table_info(ZWACHATSESSION)')]
            chat_template = dict(zip(chat_columns, writer.execute('SELECT * FROM ZWACHATSESSION WHERE Z_PK=1').fetchone(), strict=True))
            chat_ids = [1, 2, 3]
            for i in range(args.chats - 3):
                row = dict(chat_template, Z_PK=100+i, ZCONTACTJID=f'benchmark-{i}@s.whatsapp.net', ZPARTNERNAME=f'Contact {i}')
                writer.execute(f"INSERT INTO ZWACHATSESSION({','.join(chat_columns)}) VALUES({','.join('?' for _ in chat_columns)})",
                               [row[column] for column in chat_columns])
                chat_ids.append(row['Z_PK'])
            if args.chats > 3:
                for i, (message_id,) in enumerate(writer.execute('SELECT Z_PK FROM ZWAMESSAGE WHERE Z_PK>=100 ORDER BY Z_PK')):
                    writer.execute('UPDATE ZWAMESSAGE SET ZCHATSESSION=? WHERE Z_PK=?', (chat_ids[i % len(chat_ids)], message_id))
            media_columns = [row[1] for row in writer.execute('PRAGMA table_info(ZWAMEDIAITEM)')]
            media_template = dict(zip(media_columns, writer.execute('SELECT * FROM ZWAMEDIAITEM WHERE Z_PK=1').fetchone(), strict=True))
            media_directory = native / 'Message/Media/benchmark'
            if args.file_media and args.media_files > 1:
                media_directory.mkdir(parents=True)
            for item_id, (message_id,) in enumerate(writer.execute('SELECT Z_PK FROM ZWAMESSAGE WHERE Z_PK>=100 ORDER BY Z_PK LIMIT ?', (args.media_items - 1,)), 2):
                relative = None
                if args.file_media and item_id <= args.media_files:
                    relative = f'Media/benchmark/{item_id}.bin'
                    (native / 'Message' / relative).write_bytes(random.Random(item_id + 10).randbytes(args.media_bytes))
                row = dict(media_template, Z_PK=item_id, ZMESSAGE=message_id, ZMEDIALOCALPATH=relative,
                           ZFILESIZE=args.media_bytes if relative else None)
                writer.execute(f"INSERT INTO ZWAMEDIAITEM({','.join(media_columns)}) VALUES({','.join('?' for _ in media_columns)})",
                               [row[column] for column in media_columns])
                writer.execute('UPDATE ZWAMESSAGE SET ZMEDIAITEM=? WHERE Z_PK=?', (item_id, message_id))
                if relative:
                    writer.execute('UPDATE ZWAMESSAGE SET ZMESSAGETYPE=1,ZTEXT=NULL WHERE Z_PK=?', (message_id,))
            if args.file_media:
                writer.execute('UPDATE ZWAMEDIAITEM SET ZFILESIZE=? WHERE Z_PK=1', (media_path.stat().st_size,))
            else:
                writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
            writer.commit()
            lid.executemany('INSERT INTO ZWAZACCOUNT VALUES(?,?,?)',
                            ((100 + row, f'{800000000 + row}@lid', f'+1556{row:07d}') for row in range(args.aliases)))
            lid.commit()
            if args.checkpoint_setup:
                assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
                version = writer.execute('PRAGMA user_version').fetchone()[0]
                writer.execute(f'PRAGMA user_version={version + 1}')
                writer.commit()
            stages = [
                ('baseline', None), ('clean', None),
                ('new-message', 'new-message'), ('clean-after-new', None),
                *([('wal-reset-clean', 'reset-clean'), ('wal-reset-old-edit', 'reset-edit')] if args.wal_resets else []),
                ('old-edit', "UPDATE ZWAMESSAGE SET ZTEXT='benchmark historical edit' WHERE Z_PK=1"),
                ('group-event', "UPDATE ZWAGROUPMEMBERSCHANGE SET ZMEMBERJIDS='benchmark@s.whatsapp.net' WHERE Z_PK=1"),
                ('group-title', "UPDATE ZWACHATSESSION SET ZPARTNERNAME='benchmark group title' WHERE Z_PK=2"),
                ('chat-archive', 'UPDATE ZWACHATSESSION SET ZARCHIVED=1-ZARCHIVED WHERE Z_PK=1'),
                ('chat-title', "UPDATE ZWACHATSESSION SET ZPARTNERNAME='benchmark title' WHERE Z_PK=1"),
                ('auxiliary-noop', 'auxiliary-noop'), ('auxiliary-edit', 'auxiliary'),
                *([('file-only-edit', 'file-edit'), ('clean-after-file', None),
                   ('file-missing', 'file-missing'), ('file-download', 'file-download')] if args.file_media else []),
                ('old-delete', 'DELETE FROM ZWAMESSAGE WHERE Z_PK=1'),
                ('clean-after-changes', None),
            ]
            for label, mutation in stages:
                if mutation == 'new-message':
                    columns = [row[1] for row in writer.execute('PRAGMA table_info(ZWAMESSAGE)')]
                    values = dict(zip(columns, writer.execute('SELECT * FROM ZWAMESSAGE WHERE Z_PK=1').fetchone(), strict=True))
                    values.update(
                        Z_PK=writer.execute('SELECT max(Z_PK)+1 FROM ZWAMESSAGE').fetchone()[0],
                        ZSORT=writer.execute('SELECT max(ZSORT)+1 FROM ZWAMESSAGE').fetchone()[0],
                        ZMESSAGEDATE=writer.execute('SELECT max(ZMESSAGEDATE)+1 FROM ZWAMESSAGE').fetchone()[0],
                        ZSTANZAID='BENCHMARK-NEW-MESSAGE', ZTEXT='benchmark newly arrived message')
                    writer.execute(f"INSERT INTO ZWAMESSAGE({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                                   [values[column] for column in columns])
                    writer.commit()
                elif mutation in ('reset-clean', 'reset-edit'):
                    assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
                    if mutation == 'reset-clean':
                        version = writer.execute('PRAGMA user_version').fetchone()[0]
                        writer.execute(f'PRAGMA user_version={version + 1}')
                    else:
                        writer.execute("UPDATE ZWAMESSAGE SET ZTEXT='historical edit after WAL reset' WHERE Z_PK=1")
                    writer.commit()
                elif mutation == 'auxiliary-noop':
                    lid.execute('PRAGMA user_version=1')
                    lid.commit()
                elif mutation == 'auxiliary':
                    lid.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+15550009900' WHERE Z_PK=1")
                    lid.commit()
                elif mutation == 'file-edit':
                    media_path.write_bytes(random.Random(8).randbytes(args.media_bytes))
                elif mutation == 'file-missing':
                    media_path.unlink()
                elif mutation == 'file-download':
                    media_path.write_bytes(random.Random(9).randbytes(args.media_bytes))
                elif mutation:
                    writer.execute(mutation)
                    writer.commit()
                def capture(label=label):
                    outcome = collect_native(adapter, dd, cfg, ident, cache, auxiliary_session=session.native_auxiliary(cfg),
                        media_watch=session.native_media(cfg, dd, source='whatsapp') if args.file_media else None)
                    assert outcome is not None and outcome.exit_code == 0, outcome
                    counts = outcome.run['counts']
                    if label.startswith('clean') or label == 'auxiliary-noop':
                        assert counts.get('prepared_coverage_reused') == 1
                    if label.startswith('wal-reset-'):
                        captured = outcome.run['snapshot']['prepared_native']['capture']
                        assert captured['context']['native_reconciled'] and not captured['baseline']
                        assert counts.get('messages', 0) == (0 if label == 'wal-reset-clean' else 1), counts
                    if label in ('file-only-edit', 'file-missing', 'file-download'):
                        assert counts.get('messages') == 1 and counts.get('prepared_headers_reused') == 1, counts
                    if label in ('group-title', 'chat-archive'):
                        assert counts.get('chats') == 1 and counts.get('prepared_headers_reused') == 1, counts
                        assert counts.get('messages', 0) == 0, counts
                    if label == 'group-event':
                        assert counts.get('events') == 1 and counts.get('prepared_headers_reused') == 1, counts
                        assert counts.get('messages', 0) == 0, counts
                    if label == 'new-message':
                        assert outcome.run['mode'] == 'incremental'
                        assert counts.get('messages') == 1 and counts.get('canonical_units_rebuilt') == 1, counts
                    return outcome, {'counts': counts, 'mode': outcome.run['mode']}
                measure('sync-' + label, capture)
                if not args.pipeline:
                    continue
                def export():
                    paths = []
                    journal_events = 0
                    while True:
                        result = export_changes(cache, dd, cfg, ident, consumer='benchmark', output=dd.exports,
                                                password=PASSWORD, limit=args.batch_limit, media=args.archive_media)
                        if result['action'] == 'written':
                            paths.append(Path(result['path']))
                            journal_events += result['counts']['changes']
                        if not result['has_more']:
                            break
                    return paths, {'archives': len(paths), 'journal_events': journal_events,
                                   'ciphertext_bytes': sum(path.stat().st_size for path in paths)}
                paths = measure('export-verified-' + label, export)
                def importing(paths=paths):
                    for path in paths:
                        result = import_archive(target, restored, target_ident, path, PASSWORD)
                        assert result.action == 'imported', result
                    return None, {'archives': len(paths)}
                measure('import-verified-' + label, importing)
            assert any(row['text'] == 'benchmark historical edit' for row in cache.iter_records('messages'))
            assert any(row['text'] == 'benchmark newly arrived message' for row in cache.iter_records('messages'))
            if args.pipeline:
                assert any(row['text'] == 'benchmark newly arrived message' for row in target.iter_records('messages'))
                assert any(row['text'] == 'benchmark historical edit' for row in target.iter_records('messages'))
            if args.file_media and args.pipeline:
                available = {row['filename']: row['blob_sha256'] for row in target.iter_records('attachments') if row['availability'] == 'available'}
                assert len(available) == args.media_files, (len(available), args.media_files)
                paths = [media_path, *(media_directory / f'{item}.bin' for item in range(2, args.media_files + 1))]
                for file_path in paths:
                    payload = file_path.read_bytes()
                    digest = hashlib.sha256(payload).hexdigest()
                    assert available[file_path.name] == digest
                    if args.archive_media == 'available-media':
                        from chatstore.paths import blob_path
                        assert blob_path(restored, digest).read_bytes() == payload
            if args.database_layout:
                for name, database in connections.items():
                    layouts[name] = {
                        'page_size': database.conn.execute('PRAGMA page_size').fetchone()[0],
                        'page_count': database.conn.execute('PRAGMA page_count').fetchone()[0],
                        'freelist_count': database.conn.execute('PRAGMA freelist_count').fetchone()[0],
                        'objects': [dict(row) for row in database.conn.execute('''
                            SELECT d.name,coalesce(s.type,'system') AS type,count(*) AS pages,
                                   sum(d.pgsize) AS bytes,sum(d.payload) AS payload_bytes,sum(d.unused) AS unused_bytes
                            FROM dbstat d LEFT JOIN sqlite_schema s ON s.name=d.name
                            GROUP BY d.name,s.type ORDER BY bytes DESC''')],
                        'journal_operations': [dict(row) for row in database.conn.execute('''
                            SELECT operation,count(*) AS events,sum(length(CAST(data AS BLOB))) AS encoded_data_bytes
                            FROM changes GROUP BY operation ORDER BY encoded_data_bytes DESC''')],
                    }
    print(json.dumps({'messages': args.messages, 'additional_aliases': args.aliases, 'batch_limit': args.batch_limit,
        'media_items': args.media_items, 'media_files': args.media_files if args.file_media else 0, 'media_bytes': args.media_bytes, 'chats': args.chats,
        'setup_checkpointed': args.checkpoint_setup,
        'file_media': args.file_media, 'archive_media': args.archive_media, 'wal_resets': args.wal_resets,
        'resource_sampling': args.sample_resources,
        'sqlite_stats': args.sqlite_stats, 'cache_spill_disabled': args.disable_cache_spill, 'platform': platform.platform(),
        'scope': (f'Synthetic source includes {args.media_files} watched deterministic files of {args.media_bytes} bytes each. ' if args.file_media else 'Synthetic source has no file-backed media. ') + 'Production collector with retained auxiliary monitoring, native preparation, canonical coverage and acknowledgement. Optional export includes encryption and built-in exact-ciphertext verification; import includes its trust-boundary verification. Excludes fixture setup/mutation, network transfer/remote proof and energy. File population and byte sizes are explicit fixture parameters; synthetic file bytes do not establish real-media type/content diversity. Storage counters are Darwin process-attributed I/O; cached reads can be zero and buffered attribution can cross stage boundaries. Optional SQLite counters cover only canonical producer/importer Cache connections, not native mirrors or source readers; cache_writes/spills are page counts and cache_bytes is a gauge. Disabling spills is an explicit benchmark-only experiment. RSS is the lifetime peak including fixture/baseline, not incremental-only peak. Ciphertext bytes are potential transfer bytes, not measured network traffic.',
        'resource_sampling_scope': 'Optional 10 ms sampling records parent resident memory/physical footprint and producer/importer WAL file-size maxima, not a guaranteed instantaneous peak or native-mirror WAL size. Sampling overhead is included in stage CPU/wall while active; resident gauges include allocations retained from earlier stages. Lifetime peak RSS remains a separate high-water mark.',
        'measurements': measurements, 'database_layout': layouts,
        'reset_scope': 'Optional reset stages checkpoint/truncate the source WAL and commit a fresh frame before capture; these source-writer mutations are outside timings. Capture includes a full image backup, native page-graph rebuild and ordered raw-row comparison. This is not bounded reset discovery. Empty or invalid WALs retain ordinary fallback.',
        'layout_scope': 'Optional dbstat and journal inspection runs after all timed stages; it reads database pages and is diagnostic work, not part of a regular collector poll.'}, indent=2))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    main()
