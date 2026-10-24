#!/usr/bin/env python3
"""Synthetic Messages adapter selection; native preparation/routing are excluded."""

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
from chatstore.adapters.messages import adapter as messages
from chatstore.config import Config
from tests.fixtures.synthetic import build_messages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--messages', type=int, default=100000)
    args = parser.parse_args()
    if args.messages < 7:
        parser.error('--messages must be at least seven')
    helper = Path(__file__).resolve().parents[1] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        parser.error('build the development Rust helper first')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-message-selection-') as directory:
        root = Path(directory).resolve()
        native = build_messages(root / 'native', extra_messages=args.messages - 7)
        config = Config(source_paths={'messages': str(native)}, helper_path=str(helper))
        adapter = messages.MessagesAdapter()
        wall = time.perf_counter()
        snap = adapter.snapshot(config, root / 'staging')
        results.append({'stage': 'prepare-full-private-snapshot', 'wall_seconds': time.perf_counter() - wall,
                        'image_bytes': snap.files['chat'].size})
        footer = {}
        original = messages.decode
        def measured_decode(*args, **kwargs):
            for value in original(*args, **kwargs):
                if value['type'] == 'footer':
                    footer.clear()
                    footer.update(value)
                yield value
        messages.decode = measured_decode

        def measure(label, selected):
            stats = ExtractStats(selected_message_rowids=selected)
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            wall, cpu = time.perf_counter(), time.process_time()
            count = sum(1 for _ in adapter.extract(snap, 'synthetic-scope', config, stats, None, full=False))
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            assert not stats.errors, stats.errors
            results.append({'stage': label, 'wall_seconds': time.perf_counter() - wall,
                'parent_cpu_seconds': time.process_time() - cpu,
                'helper_cpu_seconds': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                'canonical_records_emitted': count, 'messages_emitted': stats.counts.get('messages', 0),
                'helper_messages_decoded': footer['messages'], 'query_fullscan_steps': footer['query_fullscan_steps'],
                'query_vm_steps': footer['query_vm_steps']})

        measure('baseline-full-adapter-and-index-build', None)
        measure('explicit-empty-selection', set())
        path = snap.files['chat'].snapshot_path
        with closing(sqlite3.connect(path)) as writer:
            writer.execute("UPDATE message SET text='selected old edit' WHERE ROWID=1")
            writer.commit()
            measure('explicit-old-row-selection', {1})
            writer.row_factory = sqlite3.Row
            row = dict(writer.execute('SELECT * FROM message WHERE ROWID=1').fetchone())
            row.update(ROWID=500000, guid='BBBBBBBB-0000-4000-8000-000000500000', text='selected new message')
            writer.execute(f'INSERT INTO message({",".join(row)}) VALUES({",".join("?" for _ in row)})', list(row.values()))
            writer.commit()
            measure('explicit-new-row-selection', {500000})
            writer.execute('DELETE FROM message WHERE ROWID=1')
            writer.commit()
            measure('explicit-deleted-row-selection', {1})
        snap.cleanup()
    print(json.dumps({'messages': args.messages,
        'scope': 'Synthetic adapter extraction with explicit caller-supplied row selection. Initial snapshot and private dependency index construction shown separately. Includes helper launch/SQL/decoding, Python context reads, canonical record construction and media probes for selected rows. Excludes WAL/native-mirror preparation, dependency routing, canonical cache commits, archives, source mutation/setup, kernel IO and energy. Fullscan/VM counters describe the primary message query, not every helper SQL statement. CPU splits parent and child helpers.',
        'measurements': results}, indent=2))


if __name__ == '__main__':
    main()
