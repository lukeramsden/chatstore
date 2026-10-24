#!/usr/bin/env python3
"""Synthetic translation microbenchmark; no source apps or personal cache access."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time

from chatstore.cache.changefeed import id_ranges
from chatstore.cache.import_ranges import install, translate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--keys', type=int, default=100000)
    args = parser.parse_args()
    if args.keys < 3:
        parser.error('--keys must be at least 3')
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
        CREATE TABLE records(urn TEXT PRIMARY KEY,revision_digest TEXT);
        CREATE TABLE entity_keys(id INTEGER PRIMARY KEY,urn TEXT);
        CREATE TABLE entity_ids(producer TEXT,entity_id INTEGER,local_id INTEGER,head_digest TEXT,
            PRIMARY KEY(producer,entity_id));
        CREATE TABLE observation_ids(feed_id TEXT,observation_id INTEGER,local_id INTEGER,evidence TEXT,
            PRIMARY KEY(feed_id,observation_id));
        CREATE TABLE source_observations(observation_id INTEGER PRIMARY KEY,entity_urn TEXT,
            source TEXT,account_scope TEXT,native_table TEXT,native_row_ids TEXT,native_key TEXT,
            minted INTEGER,fingerprint TEXT,adapter_version TEXT,present TEXT);
    ''')
    install(conn)
    conn.executemany('INSERT INTO records VALUES (?,?)', ((str(i), 'head') for i in range(1, args.keys + 1)))
    conn.executemany('INSERT INTO entity_keys VALUES (?,?)', ((i, str(i)) for i in range(1, args.keys + 1)))
    conn.executemany('INSERT INTO entity_ids VALUES (?,?,?,?)', (('p', i, i, 'head') for i in range(1, args.keys + 1)))
    conn.commit()
    results = []

    def measure(label, fn):
        wall, cpu = time.perf_counter(), time.process_time()
        result = fn()
        results.append({'stage': label, 'wall_seconds': time.perf_counter() - wall,
                        'cpu_seconds': time.process_time() - cpu, 'output_intervals': len(result)})
        return result

    def legacy():
        rows = conn.execute('''SELECT m.local_id,r.revision_digest=m.head_digest FROM entity_ids m
            JOIN entity_keys k ON k.id=m.local_id LEFT JOIN records r ON r.urn=k.urn
            WHERE m.producer='p' AND m.entity_id BETWEEN 1 AND ?''', (args.keys,)).fetchall()
        assert len(rows) == args.keys
        return id_ranges([r[0] for r in rows if r[1]])

    ranges = [[1, args.keys]]
    assert measure('legacy-clean', legacy) == ranges
    assert measure('initialize-projection', lambda: translate(conn, 'p', 'record', ranges)) == ranges
    assert measure('interval-clean', lambda: translate(conn, 'p', 'record', ranges)) == ranges
    middle = args.keys // 2
    conn.execute("UPDATE records SET revision_digest='local' WHERE urn=?", (str(middle),))
    expected = [[1, middle - 1], [middle + 1, args.keys]]
    assert measure('interval-one-divergent-head', lambda: translate(conn, 'p', 'record', ranges)) == expected
    assert measure('legacy-one-divergent-head', legacy) == expected
    conn.execute("UPDATE records SET revision_digest='head' WHERE urn=?", (str(middle),))
    assert measure('interval-head-restored', lambda: translate(conn, 'p', 'record', ranges)) == ranges
    print(json.dumps({'keys': args.keys, 'scope': 'in-memory synthetic record-key translation only; excludes archive IO and capture',
                      'energy_joules': None, 'measurements': results}, indent=2))
    conn.close()


if __name__ == '__main__':
    main()
