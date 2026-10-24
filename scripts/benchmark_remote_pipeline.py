#!/usr/bin/env python3
"""Build synthetic immutable batches, then measure their import on a separate host.

Transport is orchestrated separately so its wall time and SSH byte counters are not
confused with importer CPU/I/O. Never uses configured personal data directories.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import resource
import shutil
import sys
import time
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache, SourceObservation
from chatstore.canonical.records import record, restamp, timestamp
from chatstore.config import Config, Identity
from chatstore.identity.urn import derive_urn
from chatstore.paths import DataDir
from scripts.benchmark_incremental import io_counters

PASSWORD = 'synthetic-remote-benchmark-only'
SCOPE = '11111111-2222-4333-8444-555555555555'
STAGES = ('baseline', 'clean', 'new-message', 'old-edit', 'context', 'old-delete')


def urn(kind, key):
    return derive_urn('whatsapp', SCOPE, kind, key)


def message(number, text=None):
    return record('messages', urn('message', f'm{number}'), 'whatsapp', SCOPE,
        native_key=f'm{number}', chat_urn=urn('chat', 'c1'), chat_assignment='source_column',
        sender_urn=urn('identity', 'i1'), is_from_me=False, transport='whatsapp', kind='message',
        sent_at=timestamp(1_500_000_000_000 + number * 2_000_000, 'ms', '1970-01-01T00:00:00Z'),
        received_at=None, edited_at=None, retracted_at=None, state='unknown', reply_to_urn=None,
        reply_to_native=None, text=text or f'synthetic historical message {number} café',
        has_attachments=False, part_count=1, source_flags={}, tombstone=None)


def measured(label, operation):
    before = io_counters()
    wall, cpu = time.perf_counter(), time.process_time()
    details = operation()
    result = {'stage': label, 'wall_seconds': time.perf_counter() - wall,
              'cpu_seconds': time.process_time() - cpu, **details}
    after = io_counters()
    if before is not None and after is not None:
        result.update({key: after[key] - before[key] for key in before})
    result['lifetime_peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
    print(json.dumps(result), flush=True)
    return result


def prepare(root, count):
    root.mkdir(parents=True, exist_ok=False)
    source = DataDir(root / 'producer')
    source.create()
    cfg, ident = Config(), Identity()
    measurements = []
    with closing(Cache(source.cache)) as cache:
        def seed():
            with cache.bulk():
                with cache.write():
                    cache.upsert(record('chats', urn('chat', 'c1'), 'whatsapp', SCOPE,
                        native_key='c1', kind='direct', observed_name='Synthetic conversation',
                        service='whatsapp', assignment='source', is_archived=False, created_at=None), observed_at=100)
                    cache.upsert(record('identities', urn('identity', 'i1'), 'whatsapp', SCOPE,
                        native_key='i1', kind='jid_phone', address='15550000001@s.whatsapp.net',
                        service='whatsapp', is_me=False, observed_names=[]), observed_at=100)
                for start in range(1, count + 1, 1000):
                    with cache.write():
                        for number in range(start, min(start + 1000, count + 1)):
                            msg = message(number)
                            cache.upsert(msg, observed_at=100)
                            cache.observe(SourceObservation(msg['urn'], 'whatsapp', SCOPE,
                                'message', [number], f'm{number}', msg['revision_digest'], 'synthetic/1', 100, None))
            return {'messages': count}
        measurements.append(measured('seed-canonical-history', seed))
        for index, stage in enumerate(STAGES):
            at = 200 + index
            if stage != 'baseline':
                with cache.write():
                    if stage == 'clean':
                        cache.upsert(message(1), observed_at=at)
                    elif stage == 'new-message':
                        msg = message(count + 1, 'synthetic newly arrived message')
                        cache.upsert(msg, observed_at=at)
                        cache.observe(SourceObservation(msg['urn'], 'whatsapp', SCOPE, 'message',
                            [count + 1], f'm{count + 1}', msg['revision_digest'], 'synthetic/1', at, None))
                    elif stage == 'old-edit':
                        msg = message(1, 'synthetic historical edit')
                        cache.upsert(msg, observed_at=at)
                        cache.observe(SourceObservation(msg['urn'], 'whatsapp', SCOPE, 'message',
                            [1], 'm1', msg['revision_digest'], 'synthetic/1', at, None))
                    elif stage == 'context':
                        current = cache.get(urn('chat', 'c1'))
                        cache.upsert(restamp(dict(current, observed_name='Synthetic renamed conversation')), observed_at=at)
                    elif stage == 'old-delete':
                        cache.upsert(restamp(dict(message(1), text=None, tombstone={'reason': 'source_deleted'})), observed_at=at)
                        cache.observe(SourceObservation(urn('message', 'm1'), 'whatsapp', SCOPE,
                            'message', [1], 'm1', message(1, 'synthetic historical edit')['revision_digest'],
                            'synthetic/1', at, None, present='absent_from_source'))
            def exporting(stage=stage):
                receipts = []
                while True:
                    batch = export_changes(cache, source, cfg, ident, consumer='remote-benchmark',
                        output=root / 'objects', password=PASSWORD, media='text')
                    if batch['action'] == 'written':
                        receipts.append({'file': Path(batch['path']).name, 'receipt': batch['receipt']})
                    if not batch['has_more']:
                        break
                metadata = {'stage': stage, 'messages': count, 'objects': receipts}
                (root / f'{stage}.json').write_text(json.dumps(metadata) + '\n')
                return {'archives': len(receipts), 'ciphertext_bytes': sum(r['receipt']['ciphertext_bytes'] for r in receipts)}
            measurements.append(measured('export-verified-' + stage, exporting))
    shutil.rmtree(source.root)
    (root / 'preparation.json').write_text(json.dumps({'messages': count, 'measurements': measurements,
        'scope': 'Synthetic canonical history, not native capture. Producer cache removed before remote import.'}, indent=2) + '\n')


def receiving(root, stage, reject=False):
    metadata = json.loads((root / f'{stage}.json').read_text())
    target = DataDir(root / ('rejected' if reject else 'restored'))
    target.create()
    with closing(Cache(target.cache)) as cache:
        ident = Identity()
        imported = 0
        for entry in metadata['objects']:
            if shutil.disk_usage(root).free < 2 * 1024**3:
                raise RuntimeError('benchmark disk reserve reached; production data left untouched')
            path = root / 'objects' / entry['file']
            if not reject:
                with path.open('rb') as payload:
                    assert 'sha256:' + hashlib.file_digest(payload, 'sha256').hexdigest() == entry['receipt']['ciphertext_sha256']
            result = import_archive(cache, target, ident, path, PASSWORD)
            if reject:
                assert result.action in ('invalid', 'wrong_password', 'schema_error'), result
                assert cache.counts() == {}
                assert cache.conn.execute('SELECT count(*) FROM feed_imports').fetchone()[0] == 0
                return {'rejected_partial_archive': True}
            assert result.action in ('imported', 'already_imported') and result.conflicts == 0, result
            imported += result.action == 'imported'
            row = cache.conn.execute('SELECT ciphertext_sha256 FROM verified_imports WHERE export_id=?', (result.export_id,)).fetchone()
            assert row[0] == entry['receipt']['ciphertext_sha256']
        return {'archives': len(metadata['objects']), 'newly_imported': imported,
                'ciphertext_bytes': sum(r['receipt']['ciphertext_bytes'] for r in metadata['objects'])}


def verify(root, stage):
    metadata = json.loads((root / f'{stage}.json').read_text())
    with closing(Cache(root / 'restored' / 'cache.sqlite3', readonly=True)) as cache:
        assert cache.get(urn('message', 'm1')) is not None
        if stage == 'old-delete':
            assert cache.get(urn('message', 'm1'))['tombstone']
            assert len(cache.revisions_of(urn('message', 'm1'))) == 3
            assert cache.get(urn('message', f'm{metadata["messages"] + 1}'))['text'] == 'synthetic newly arrived message'
            assert cache.get(urn('chat', 'c1'))['observed_name'] == 'Synthetic renamed conversation'
            assert cache.observations_of(urn('message', 'm1'))[0]['present'] == 'absent_from_source'
            assert cache.observations_of(urn('message', 'm2'))[0]['present'] == 'present'
            assert cache.conn.execute("SELECT count(*) FROM revisions_fts WHERE revisions_fts MATCH '\"synthetic historical edit\"'").fetchone()[0] == 1
            assert cache.counts()['messages'] == metadata['messages'] + 1



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('prepare', 'import', 'reject', 'retry'))
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--messages', type=int, default=100000)
    parser.add_argument('--stage', choices=STAGES, default='baseline')
    args = parser.parse_args()
    if args.messages < 2:
        parser.error('messages must be at least two')
    if args.operation == 'prepare':
        prepare(args.root, args.messages)
    else:
        result = measured(args.operation + '-' + args.stage,
            lambda: receiving(args.root, args.stage, reject=args.operation == 'reject'))
        if args.operation != 'reject':
            verify(args.root, args.stage)
        if args.operation == 'retry':
            assert result['newly_imported'] == 0
        (args.root / f'{args.operation}-{args.stage}-result.json').write_text(json.dumps({**result,
            'assertions_passed': True,
            'platform': platform.platform(), 'python': platform.python_version()}) + '\n')


if __name__ == '__main__':
    main()
