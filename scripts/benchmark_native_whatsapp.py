#!/usr/bin/env python3
"""Measure the production native WhatsApp collector and optional encrypted pipeline."""

import argparse
import json
import platform
import resource
import sqlite3
import subprocess
import sys
import tempfile
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
    parser.add_argument('--batch-limit', type=int, default=5000)
    parser.add_argument('--sqlite-stats', action='store_true')
    parser.add_argument('--disable-cache-spill', action='store_true')
    parser.add_argument('--database-layout', action='store_true')
    args = parser.parse_args()
    if sys.platform != 'darwin' or args.messages < 8 or args.aliases < 0 or not 1 <= args.batch_limit <= 100000:
        parser.error('requires macOS, at least eight messages, nonnegative aliases, and batch limit 1..100000')
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
        wall, cpu = time.perf_counter(), time.process_time()
        result, details = operation()
        elapsed = {'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu}
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
            writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
            writer.commit()
            lid.executemany('INSERT INTO ZWAZACCOUNT VALUES(?,?,?)',
                            ((100 + row, f'{800000000 + row}@lid', f'+1556{row:07d}') for row in range(args.aliases)))
            lid.commit()
            stages = [
                ('baseline', None), ('clean', None),
                ('new-message', 'new-message'), ('clean-after-new', None),
                ('old-edit', "UPDATE ZWAMESSAGE SET ZTEXT='benchmark historical edit' WHERE Z_PK=1"),
                ('chat-title', "UPDATE ZWACHATSESSION SET ZPARTNERNAME='benchmark title' WHERE Z_PK=1"),
                ('auxiliary-edit', 'auxiliary'), ('old-delete', 'DELETE FROM ZWAMESSAGE WHERE Z_PK=1'),
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
                elif mutation == 'auxiliary':
                    lid.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+15550009900' WHERE Z_PK=1")
                    lid.commit()
                elif mutation:
                    writer.execute(mutation)
                    writer.commit()
                def capture(label=label):
                    outcome = collect_native(adapter, dd, cfg, ident, cache, auxiliary_session=session.native_auxiliary(cfg))
                    assert outcome is not None and outcome.exit_code == 0, outcome
                    counts = outcome.run['counts']
                    if label.startswith('clean'):
                        assert counts.get('prepared_coverage_reused') == 1
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
                                                password=PASSWORD, limit=args.batch_limit)
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
        'sqlite_stats': args.sqlite_stats, 'cache_spill_disabled': args.disable_cache_spill, 'platform': platform.platform(),
        'scope': 'Production collector with retained auxiliary monitoring, native preparation, canonical coverage and acknowledgement. Synthetic source has no file-backed media. Optional export includes encryption and built-in exact-ciphertext verification; import includes its trust-boundary verification. Excludes fixture setup/mutation, network transfer/remote proof, WhatsApp media and energy. Storage counters are Darwin process-attributed I/O; cached reads can be zero and buffered attribution can cross stage boundaries. Optional SQLite counters cover only canonical producer/importer Cache connections, not native mirrors or source readers; cache_writes/spills are page counts and cache_bytes is a gauge. Disabling spills is an explicit benchmark-only experiment. RSS is the lifetime peak including fixture/baseline, not incremental-only peak. Ciphertext bytes are potential transfer bytes, not measured network traffic.',
        'measurements': measurements, 'database_layout': layouts,
        'layout_scope': 'Optional dbstat and journal inspection runs after all timed stages; it reads database pages and is diagnostic work, not part of a regular collector poll.'}, indent=2))


if __name__ == '__main__':
    main()
