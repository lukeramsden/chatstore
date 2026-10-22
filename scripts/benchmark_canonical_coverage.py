#!/usr/bin/env python3
"""Synthetic canonical summary and targeted-unit withdrawal work."""

import argparse
import json
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chatstore.cache import Cache
from chatstore.cache.extraction import ExtractionMemo
from chatstore.cache.projection_epoch import read_source
from chatstore.cache.unit_coverage import UnitWithdrawal
from tests.unit.test_cache import SCOPE, msg
from tests.unit.test_unit_coverage import capture


def measure(cache, label, action):
    callbacks = []
    cache.conn.set_progress_handler(lambda: callbacks.append(1) or 0, 100)
    wall, cpu = time.perf_counter(), time.process_time()
    value = action()
    elapsed, used = time.perf_counter() - wall, time.process_time() - cpu
    cache.conn.set_progress_handler(None, 0)
    return {'stage': label, 'wall_seconds': elapsed, 'cpu_seconds': used,
            'sqlite_vm_steps_floor': len(callbacks) * 100, 'result': value}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=int, default=100000)
    args = parser.parse_args()
    if args.messages < 1:
        parser.error('--messages must be positive')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-canonical-coverage-') as directory, closing(Cache(Path(directory) / 'cache.sqlite')) as cache:
        with cache.bulk():
            for start in range(1, args.messages + 1, 1000):
                with cache.write():
                    cache.upsert_many([msg(row, 'synthetic retained history', None, None, row) for row in range(start, min(start + 1000, args.messages + 1))], observed_at=100)
        def full_summary():
            row = cache.conn.execute("SELECT min(utc_ms),max(utc_ms),count(*) FROM messages_idx WHERE kind='message' AND source='whatsapp' AND account_scope=?", (SCOPE,)).fetchone()
            return {'earliest_utc_ms': row[0], 'latest_utc_ms': row[1], 'messages': row[2]}
        results.append(measure(cache, 'legacy-summary-scan', full_summary))
        results.append(measure(cache, 'maintained-count-indexed-summary', lambda: cache.coverage('whatsapp', SCOPE)))
        assert results[0]['result'] == results[1]['result']
        memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'benchmark', reuse=True)
        capture(cache, memo, 'target', [msg(0, 'target old evidence', None, None, 0)])
        with cache.write():
            cache.conn.executemany("INSERT INTO native_units VALUES(?,?,?,'benchmark','native',1,1,'[]','[]','{}','{}')",
                (('whatsapp', SCOPE, f'unrelated-{row}') for row in range(args.messages)))
            cache.conn.executemany('INSERT INTO native_unit_members VALUES(?,?,?,?)',
                (('whatsapp', SCOPE, f'unrelated-{row}', f'urn:unrelated:{row}') for row in range(args.messages)))
        epoch = read_source(cache.conn, 'whatsapp', SCOPE)
        def withdraw():
            with cache.write():
                proof = UnitWithdrawal.take(cache, 'whatsapp', SCOPE, ['target'], expected_epoch=epoch)
                return proof.reconcile(cache, [], at=200, run='benchmark-withdrawal', expected_epoch=epoch)
        results.append(measure(cache, 'one-unit-withdrawal-with-unrelated-memberships', withdraw))
        assert results[-1]['result'] == 1
    print(json.dumps({'messages': args.messages,
        'scope': 'Synthetic canonical cache components only. Summary compares the previous scan with maintained counts and per-namespace indexed date bounds. Withdrawal includes writer commit, journal/invalidations and one missing observation among unrelated unit memberships. Setup, source/mirror extraction, full selective-capture lifecycle, archives, kernel IO and energy excluded. Timings include SQLite progress-handler instrumentation; VM counters are floors at 100-instruction resolution.',
        'measurements': results}, indent=2))


if __name__ == '__main__':
    main()
