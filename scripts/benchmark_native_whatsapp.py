#!/usr/bin/env python3
"""Measure the production native WhatsApp collector and optional encrypted pipeline."""

import argparse
import json
import platform
import resource
import sqlite3
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
    args = parser.parse_args()
    if sys.platform != 'darwin' or args.messages < 8 or args.aliases < 0 or not 1 <= args.batch_limit <= 100000:
        parser.error('requires macOS, at least eight messages, nonnegative aliases, and batch limit 1..100000')
    measurements = []
    def measure(stage, operation):
        before = io_counters()
        wall, cpu = time.perf_counter(), time.process_time()
        result, details = operation()
        elapsed = {'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu}
        after = io_counters()
        if before is not None and after is not None:
            elapsed.update({key: after[key] - before[key] for key in before})
        measurements.append({'stage': stage, **elapsed,
                             'lifetime_peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, **details})
        print(f'{stage}: {elapsed["wall_seconds"]:.3f}s wall, {elapsed["cpu_seconds"]:.3f}s CPU', file=sys.stderr, flush=True)
        return result
    with tempfile.TemporaryDirectory(prefix='chatstore-native-wa-') as directory:
        root = Path(directory).resolve()
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
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
            writer.commit()
            lid.executemany('INSERT INTO ZWAZACCOUNT VALUES(?,?,?)',
                            ((100 + row, f'{800000000 + row}@lid', f'+1556{row:07d}') for row in range(args.aliases)))
            lid.commit()
            stages = [
                ('baseline', None), ('clean', None),
                ('old-edit', "UPDATE ZWAMESSAGE SET ZTEXT='benchmark historical edit' WHERE Z_PK=1"),
                ('chat-title', "UPDATE ZWACHATSESSION SET ZPARTNERNAME='benchmark title' WHERE Z_PK=1"),
                ('auxiliary-edit', 'auxiliary'), ('old-delete', 'DELETE FROM ZWAMESSAGE WHERE Z_PK=1'),
                ('clean-after-changes', None),
            ]
            for label, mutation in stages:
                if mutation == 'auxiliary':
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
                    return outcome, {'counts': counts, 'mode': outcome.run['mode']}
                measure('sync-' + label, capture)
                if not args.pipeline:
                    continue
                def export():
                    paths = []
                    while True:
                        result = export_changes(cache, dd, cfg, ident, consumer='benchmark', output=dd.exports,
                                                password=PASSWORD, limit=args.batch_limit)
                        if result['action'] == 'written':
                            paths.append(Path(result['path']))
                        if not result['has_more']:
                            break
                    return paths, {'archives': len(paths), 'ciphertext_bytes': sum(path.stat().st_size for path in paths)}
                paths = measure('export-verified-' + label, export)
                def importing(paths=paths):
                    for path in paths:
                        result = import_archive(target, restored, target_ident, path, PASSWORD)
                        assert result.action == 'imported', result
                    return None, {'archives': len(paths)}
                measure('import-verified-' + label, importing)
            assert any(row['text'] == 'benchmark historical edit' for row in cache.iter_records('messages'))
            if args.pipeline:
                assert any(row['text'] == 'benchmark historical edit' for row in target.iter_records('messages'))
    print(json.dumps({'messages': args.messages, 'additional_aliases': args.aliases, 'batch_limit': args.batch_limit, 'platform': platform.platform(),
        'scope': 'Production collector with retained auxiliary monitoring, native preparation, canonical coverage and acknowledgement. Synthetic source has no file-backed media. Optional export includes encryption and built-in exact-ciphertext verification; import includes its trust-boundary verification. Excludes fixture setup/mutation, network transfer/remote proof, WhatsApp media and energy. Storage counters are Darwin process-attributed I/O; cached reads can be zero and buffered attribution can cross stage boundaries. RSS is the lifetime peak including fixture/baseline, not incremental-only peak. Ciphertext bytes are potential transfer bytes, not measured network traffic.',
        'measurements': measurements}, indent=2))


if __name__ == '__main__':
    main()
