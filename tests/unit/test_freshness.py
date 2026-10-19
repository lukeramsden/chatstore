"""Latest-sighting projection correctness, including gaps, stale imports and range splits."""

import random
from itertools import pairwise

from chatstore.cache import Cache
from chatstore.cache.freshness import lookup, stamp


def test_interval_projection_matches_independent_per_key_history(tmp_path):
    cache = Cache(tmp_path / 'fresh.sqlite3')
    rng = random.Random(46)
    expected = {}
    try:
        for step in range(250):
            lo, hi = sorted(rng.sample(range(1, 60), 2))
            at = rng.randrange(1, 100)
            run = f'run-{step}'
            with cache.write():
                stamp(cache.conn, 'record', [[lo, hi]], at, run)
            for key in range(lo, hi + 1):
                if at >= expected.get(key, (0, None))[0]:
                    expected[key] = (at, run)
            ranges = [tuple(r) for r in cache.conn.execute('''SELECT first_id,last_id,observed_at,sync_run
                FROM freshness_ranges WHERE kind='record' ORDER BY first_id''')]
            for a, b in pairwise(ranges):
                assert a[1] < b[0]
                assert a[1] + 1 != b[0] or a[2:] != b[2:]
            for key in range(1, 61):
                actual = lookup(cache.conn, 'record', key, {'observed_at': 0, 'sync_run': None})
                assert (actual['observed_at'], actual['sync_run']) == expected.get(key, (0, None))
    finally:
        cache.close()


def test_consecutive_batches_coalesce_without_rewriting_canonical_rows(tmp_path):
    cache = Cache(tmp_path / 'fresh.sqlite3')
    try:
        with cache.write():
            for start in range(1, 10001, 100):
                stamp(cache.conn, 'observation', [[start, start + 99]], 10, 'one-scan')
        assert [tuple(r) for r in cache.conn.execute('SELECT * FROM freshness_ranges')] == [
            ('observation', 1, 10000, 10, 'one-scan')]
        with cache.write():
            stamp(cache.conn, 'observation', [[100, 200]], 20, 'newer')
            stamp(cache.conn, 'observation', [[1, 10000]], 15, 'older')
        assert [tuple(r) for r in cache.conn.execute('SELECT * FROM freshness_ranges ORDER BY first_id')] == [
            ('observation', 1, 99, 15, 'older'), ('observation', 100, 200, 20, 'newer'),
            ('observation', 201, 10000, 15, 'older')]
    finally:
        cache.close()


def test_effective_head_time_preserves_signed_timestamps_and_uncovered_gaps(tmp_path):
    from tests.unit.test_cache import msg
    cache = Cache(tmp_path / 'fresh.sqlite3')
    records = [msg(i, 'signed observation time', None, None, i) for i in range(3)]
    try:
        with cache.write():
            for rec, at in zip(records, (-300, -200, -100), strict=True):
                cache.upsert(rec, observed_at=at)
            cache.upsert(records[0], observed_at=-250)
            cache.upsert(records[2], observed_at=-50)
        heads = cache.current_heads(r['urn'] for r in records)
        assert [heads[r['urn']][1] for r in records] == [-250, -200, -50]
        assert [cache.get(r['urn'])['observed_at'] for r in records] == [-250, -200, -50]
    finally:
        cache.close()
