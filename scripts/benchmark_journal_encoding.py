#!/usr/bin/env python3
"""Isolated, lossless journal-storage experiment; never changes production encoding."""

import argparse
import hashlib
import json
import platform
import resource
import sqlite3
import struct
import sys
import tempfile
import time
import zlib
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.sync import run_sync
from scripts.benchmark_incremental import io_counters
from tests.fixtures.synthetic import build_whatsapp

MAGIC = b'CSJ1\0'
HEADER = struct.Struct('>5sQ32s')
MAX_RAW_BYTES = 64 * 1024 * 1024


def encode(value: str) -> str | bytes:
    if len(value) < 512:
        return value
    raw = value.encode('utf-8')
    if len(raw) > MAX_RAW_BYTES:
        return value
    packed = zlib.compress(raw, level=1)
    if len(packed) + HEADER.size + 64 >= len(raw):
        return value
    return HEADER.pack(MAGIC, len(raw), hashlib.sha256(raw).digest()) + packed


def decode(value: str | bytes) -> str:
    if isinstance(value, str):
        return value
    if len(value) < HEADER.size:
        raise ValueError('truncated journal encoding header')
    magic, size, digest = HEADER.unpack(value[:HEADER.size])
    if magic != MAGIC or size > MAX_RAW_BYTES:
        raise ValueError('unsupported journal encoding or decoded size')
    decoder = zlib.decompressobj()
    raw = decoder.decompress(value[HEADER.size:], size + 1)
    if len(raw) != size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('invalid compressed journal length or stream')
    if hashlib.sha256(raw).digest() != digest:
        raise ValueError('journal encoding checksum mismatch')
    return raw.decode('utf-8')


def measure(action):
    before = io_counters()
    wall, cpu = time.perf_counter(), time.process_time()
    result = action()
    counters = {'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu}
    after = io_counters()
    if before is not None and after is not None:
        counters.update({key: after[key] - before[key] for key in before})
    counters['lifetime_peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return result, counters


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=int, default=2000)
    parser.add_argument('--repeats', type=int, default=10)
    args = parser.parse_args()
    if sys.platform != 'darwin' or args.messages < 8 or args.repeats < 1:
        parser.error('requires macOS, at least eight source rows, and positive repeats')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-journal-encoding-') as directory:
        root = Path(directory).resolve()
        native = build_whatsapp(root / 'source', extra_messages=args.messages - 8)
        dd = DataDir(root / 'data')
        dd.create()
        cfg, ident = Config(source_paths={'whatsapp': str(native)}), Identity()
        ident.ensure_scope('whatsapp')
        with closing(Cache(dd.cache)) as cache:
            synced = run_sync(WhatsAppAdapter(), dd, cfg, ident, cache)
            assert synced.exit_code == 0, synced.run
            templates = [tuple(row) for row in cache.conn.execute('SELECT operation,data FROM changes ORDER BY seq')]
        count = len(templates) * args.repeats
        for compressed in (False, True):
            label = 'compressed' if compressed else 'plain'
            with closing(sqlite3.connect(root / (label + '.sqlite'), isolation_level=None)) as conn:
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA synchronous=FULL')
                conn.execute('PRAGMA cache_size=-65536')
                conn.execute('PRAGMA wal_autocheckpoint=0')
                conn.execute('CREATE TABLE changes(seq INTEGER PRIMARY KEY AUTOINCREMENT,operation TEXT NOT NULL,data TEXT NOT NULL)')
                def write(compressed=compressed, conn=conn):
                    for start in range(0, count, 5000):
                        conn.execute('BEGIN IMMEDIATE')
                        try:
                            for index in range(start, min(start + 5000, count)):
                                operation, value = templates[index % len(templates)]
                                conn.execute('INSERT INTO changes VALUES(?,?,?)',
                                    (index + 1, operation, encode(value) if compressed else value))
                            conn.execute('COMMIT')
                        except BaseException:
                            conn.execute('ROLLBACK')
                            raise
                    conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                _, writing = measure(write)
                def read(conn=conn):
                    digest = hashlib.sha256()
                    seen = 0
                    for seq, operation, value in conn.execute('SELECT seq,operation,data FROM changes ORDER BY seq'):
                        expected_op, expected = templates[(seq - 1) % len(templates)]
                        text = decode(value)
                        assert operation == expected_op and text == expected
                        digest.update(struct.pack('>Q', seq) + operation.encode() + text.encode())
                        seen += 1
                    assert seen == count
                    return digest.hexdigest()
                digest, reading = measure(read)
                size = conn.execute('PRAGMA page_count').fetchone()[0] * conn.execute('PRAGMA page_size').fetchone()[0]
                encoded_bytes = conn.execute('SELECT sum(length(CAST(data AS BLOB))) FROM changes').fetchone()[0]
                results.append({'encoding': label, 'events': count, 'database_bytes': size,
                                'stored_payload_bytes': encoded_bytes, 'roundtrip_sha256': digest,
                                'write_and_checkpoint': writing, 'read_and_exact_roundtrip': reading})
                print(f'{label}: {size} database bytes, {writing["wall_seconds"]:.3f}s write, {reading["wall_seconds"]:.3f}s read',
                      file=sys.stderr, flush=True)
        assert results[0]['roundtrip_sha256'] == results[1]['roundtrip_sha256']
    print(json.dumps({'messages': args.messages, 'repeats': args.repeats, 'platform': platform.platform(),
        'scope': 'Isolated journal table populated from real adapter-produced synthetic journal templates. Repeats preserve representative per-event structure but are not distinct source histories. FULL durability, 64 MiB cache, 5,000-row commits, no automatic checkpoints, final truncate checkpoint. Includes codec cost and exact UTF-8/sequence roundtrip checks. Excludes production triggers, canonical/index writes, archive/encryption/import, network and energy. RSS is process lifetime peak including template setup. This experimental encoding is not used by application caches.',
        'results': results}, indent=2))


if __name__ == '__main__':
    main()
