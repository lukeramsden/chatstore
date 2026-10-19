"""Bounded immutable changefeed archives, with independent durable consumer cursors."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from ..cache.store import Cache, SourceObservation, _j
from ..canonical.records import now_ms
from ..config import Config, Identity, save_identity
from ..media import locate_blob
from ..paths import DataDir
from . import container
from . import manifest as M

FORMAT = 'chatstore-changefeed-v1'


def _sync_file(path: Path) -> None:
    with path.open('rb') as f:
        os.fsync(f.fileno())
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def export_changes(cache: Cache, dd: DataDir, cfg: Config, ident: Identity, *, consumer: str,
                   output: Path, password: str, media: str = 'text', limit: int = 5000) -> dict[str, Any]:
    """Publish one bounded batch. A crash before cursor commit leaves a recoverable immutable file.

    The write lock serializes publishers with sync and other exporters. It is bounded by `limit`,
    and avoids races between the journal boundary, immutable publication and cursor commit.
    """
    if not consumer or not 1 <= limit <= 100000:
        raise ValueError('consumer must be nonempty and limit must be between 1 and 100000')
    export_set, _ = ident.ensure_export_set()
    save_identity(dd, ident)
    policy = _j({'media': media, 'export_set': export_set})
    with cache.write():
        from ..cache.changefeed import capture_identity
        capture_identity(cache.conn, ident)
        cache.conn.execute('''INSERT OR IGNORE INTO export_consumers
            (consumer,feed_id,sequence,policy) VALUES (?,?,0,?)''', (consumer, uuid.uuid4().urn, policy))
    with cache.write():
        cursor = cache.conn.execute('SELECT * FROM export_consumers WHERE consumer=?', (consumer,)).fetchone()
        if cursor['policy'] != policy:
            raise ValueError('consumer policy changed; use a new consumer name to create a new baseline')
        start = cursor['sequence']
        rows = cache.conn.execute('SELECT seq,operation,data FROM changes WHERE seq>? ORDER BY seq LIMIT ?',
                                  (start, limit)).fetchall()
        if not rows:
            return {'kind': 'delta', 'action': 'unchanged', 'consumer': consumer,
                    'sequence': start, 'has_more': False}
        end = rows[-1]['seq']
        stream = cursor['feed_id']
        producer = cache.conn.execute("SELECT value FROM meta WHERE key='feed_id'").fetchone()[0]
        output.mkdir(parents=True, exist_ok=True)
        label = f"delta-{stream.removeprefix('urn:uuid:')}-{start:020d}-{end:020d}"
        dst = output / f'chatstore-{label}.zip'
        work = dd.staging / uuid.uuid4().hex
        work.mkdir(mode=0o700, parents=True)
        try:
            receipt: dict[str, Any] | None
            events = [{'seq': r['seq'], 'operation': r['operation'], 'data': json.loads(r['data'])} for r in rows]
            if dst.exists():
                # Recovery after durable publication and before SQLite cursor commit.
                from .verify import load_archive, verification_receipt
                loaded = load_archive(dst, password, work)
                m = loaded.manifest
                receipt = verification_receipt(loaded)
                if loaded.records['changes'] != events or m.get('feed') != {
                    'producer': producer, 'stream': stream, 'after': start, 'through': end,
                    'parent': cursor['export_id'], 'parent_sha256': cursor['ciphertext_sha256'],
                } or m.get('attachment_policy') != media or m.get('export_set') != export_set:
                    raise RuntimeError(f'existing batch failed recovery validation: {dst}')
            else:
                body = (_j(e) + '\n' for e in events)
                data = ''.join(body).encode('utf-8')
                export_id = uuid.uuid4().urn
                m = M.build_manifest(
                    export_id=export_id, export_set=export_set, created_at=now_ms(),
                    bucket=M.Bucket('delta', label), revision=1, supersedes=[], lineage=[export_id],
                    account_scopes=[{'source': s, **v} for s, v in sorted(ident.scopes.items())],
                    scope_mappings=list(ident.scope_mappings), observation={}, coverage={'status': 'journal'},
                    adapter_versions={}, counts={'changes': len(events)}, attachment_policy=media,
                    media={}, payload_digest='', limits=dict(container.DEFAULT_LIMITS), members=[], content_digest='')
                m['archive_format'] = FORMAT
                m['feed'] = {'producer': producer, 'stream': stream, 'after': start, 'through': end,
                             'parent': cursor['export_id'], 'parent_sha256': cursor['ciphertext_sha256']}
                m['requires'] = [cursor['export_id']] if cursor['export_id'] else []
                tar = container.TarBuilder(work / 'payload.tar')
                try:
                    tar.add_bytes('records/changes.jsonl', data)
                    # Only attachment events in this batch can introduce media. Historical bytes
                    # already shipped remain in their original immutable archive.
                    blobs: dict[str, dict[str, Any]] = {}
                    for e in events:
                        r = e['data'].get('record', {})
                        if r.get('entity') == 'attachments' and r.get('availability') == 'available' and r.get('blob_sha256'):
                            blobs[r['blob_sha256']] = r
                    for sha, rec in sorted(blobs.items()):
                        if media != 'available-media':
                            continue
                        p = locate_blob(dd, cfg, rec)
                        if p is None:
                            raise FileNotFoundError(f'available media {sha} could not be read; checkpoint retained')
                        tar.add_file(f'blobs/sha256/{sha[:2]}/{sha}', p, expected_sha256=sha)
                    checksums = dict(tar.checksums)
                    m['counts']['blob_files'] = len(checksums) - 1
                    m['members'] = list(checksums)
                    m['payload_digest'] = 'sha256:' + container.members_digest(checksums)
                    tar.add_bytes('manifest.json', M.manifest_bytes(m))
                    tar.add_bytes('checksums.json', _j(checksums).encode())
                finally:
                    tar.finish(hash_file=False)
                container.write_encrypted_zip(dst, work / 'payload.tar', password)
                from .verify import verify_archive
                verified = verify_archive(dst, password, staging=work)
                if not verified.ok:
                    dst.unlink()
                    raise RuntimeError('new batch failed verification: ' + '; '.join(verified.problems))
                receipt = verified.receipt
            _sync_file(dst)
            if receipt is None:
                raise RuntimeError('verification did not produce a receipt')
            digest = receipt['ciphertext_sha256']
            cache.conn.execute('''INSERT INTO exports(export_id,export_set,kind,bucket_label,revision,
                content_digest,lineage,path,created_at) VALUES (?,?,'delta',?,1,?,?,?,?)''',
                (m['export_id'], export_set, label, m['payload_digest'], _j([m['export_id']]), str(dst.resolve()), m['created_at']))
            cache.conn.execute('''UPDATE export_consumers SET sequence=?,export_id=?,ciphertext_sha256=?
                WHERE consumer=?''', (end, m['export_id'], digest, consumer))
            cache.conn.execute('INSERT INTO verified_exports VALUES (?,?)', (m['export_id'], _j(receipt)))
            return {'kind': 'delta', 'action': 'written', 'path': str(dst), 'consumer': consumer,
                    'sequence': end, 'export_id': m['export_id'], 'counts': m['counts'], 'receipt': receipt,
                    'has_more': bool(cache.conn.execute('SELECT 1 FROM changes WHERE seq>? LIMIT 1', (end,)).fetchone())}
        finally:
            shutil.rmtree(work, ignore_errors=True)


def validate_event(event: Any) -> str | None:
    from .verify import _check_observation, _check_row
    if not isinstance(event, dict) or type(event.get('seq')) is not int or not 1 <= event['seq'] <= 2**63 - 1:
        return 'change needs an integer sequence'
    op, d = event.get('operation'), event.get('data')
    if not isinstance(d, dict):
        return 'change data must be an object'
    if d.get('sync_run') is not None and not isinstance(d['sync_run'], str):
        return 'change sync_run must be a string or null'
    if op == 'entity_key' or (op == 'record' and 'entity_id' in d):
        if type(d.get('entity_id')) is not int or not 1 <= d['entity_id'] <= 2**63 - 1:
            return 'entity key needs a positive stable id'
        if op == 'entity_key':
            from .verify import SHA_RE
            if not isinstance(d.get('urn'), str) or not d['urn'].startswith('urn:uuid:') or not SHA_RE.fullmatch(str(d.get('revision_digest'))):
                return 'entity key needs an urn and revision digest'
            if 'observed_at' in d and type(d['observed_at']) is not int:
                return 'entity key observation time must be an integer'
            return None
    if op == 'identity_metadata':
        if not isinstance(d.get('scopes'), dict) or not isinstance(d.get('scope_mappings'), list):
            return 'identity metadata needs scopes and scope mappings'
        return None
    if op == 'revision':
        return _check_row('revisions', d, True)
    if op == 'record':
        r = d.get('record')
        if not isinstance(r, dict) or r.get('entity') not in M.RECORD_ORDER:
            return 'change record has unknown entity'
        if type(d.get('observed_at')) is not int:
            return 'change observed_at must be an integer'
        from .verify import SHA_RE
        if d.get('previous_digest') is not None and not SHA_RE.fullmatch(str(d['previous_digest'])):
            return 'change previous_digest must be a SHA-256 digest or null'
        return _check_row(r['entity'], r, True)
    if op == 'observation':
        if type(d.get('observation_id')) is not int or not 1 <= d['observation_id'] <= 2**63 - 1:
            return 'observation needs a positive stable id'
        return _check_observation(d)
    if op in ('observation_refresh', 'record_refresh'):
        spans = d.get('ranges')
        if type(d.get('observed_at')) is not int or not isinstance(spans, list):
            return 'refresh needs observed_at and ranges'
        if any(not isinstance(s, list) or len(s) != 2 or any(type(n) is not int for n in s)
               or s[0] < 1 or s[0] > s[1] or s[1] > 2**63 - 1 for s in spans):
            return 'invalid refresh ranges'
        return None
    return f'unknown change operation {op!r}'


def apply_events(cache: Cache, manifest: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, int]:
    """Replay only after authenticated, contiguous parent validation, inside the import transaction."""
    producer = manifest['feed']['producer']
    known = dict(cache.conn.execute('''SELECT sequence,event_sha256 FROM applied_changes
        WHERE producer=? AND sequence BETWEEN ? AND ?''', (producer, events[0]['seq'], events[-1]['seq'])))
    hashes = {e['seq']: 'sha256:' + hashlib.sha256(_j(e).encode()).hexdigest() for e in events}
    if any(known[seq] != hashes[seq] for seq in known):
        raise container.InvalidArchive('producer reused a journal sequence with different captured evidence')
    outcomes: dict[str, int] = {}
    for e in events:
        if e['seq'] in known:
            continue
        op, d = e['operation'], e['data']
        if op == 'identity_metadata':
            from ..cache.changefeed import append
            append(cache.conn, op, d)
        elif op == 'revision':
            cache.add_revision_row(d)
        elif op == 'record':
            r = dict(d['record'])
            # A feed carries the exact prior head. Unrelated local edits remain conflicts.
            prior = {d['previous_digest']} if d.get('previous_digest') else set()
            out = cache.upsert(r, observed_at=d['observed_at'], sync_run=d.get('sync_run'),
                               origin='import', prior_digests=prior)
            outcomes[out] = outcomes.get(out, 0) + 1
            if 'entity_id' in d:
                _bind_entity(cache, producer, d['entity_id'], r['urn'], r['revision_digest'])
        elif op == 'entity_key':
            local = _bind_entity(cache, producer, d['entity_id'], d['urn'], d['revision_digest'])
            current = cache.conn.execute('SELECT revision_digest FROM records WHERE urn=?', (d['urn'],)).fetchone()
            if 'observed_at' in d and current and current[0] == d['revision_digest']:
                from ..cache.changefeed import append
                from ..cache.freshness import stamp
                stamp(cache.conn, 'record', [[local, local]], d['observed_at'], d.get('sync_run'))
                append(cache.conn, 'record_refresh', {'ranges': [[local, local]],
                       'observed_at': d['observed_at'], 'sync_run': d.get('sync_run')})
        elif op == 'observation':
            cache.merge_observations([SourceObservation(
                d['entity_urn'], d['source'], d['account_scope'], d['native_table'], d['native_row_ids'],
                d.get('native_key'), d['fingerprint'], d['adapter_version'], d['observed_at'],
                d.get('sync_run'), d['minted'], d['present'])])
            local = cache.conn.execute('''SELECT rowid FROM source_observations WHERE
                entity_urn=? AND source=? AND account_scope=? AND native_table=?''',
                (d['entity_urn'], d['source'], d['account_scope'], d['native_table'])).fetchone()[0]
            previous = cache.conn.execute('SELECT local_id,evidence FROM observation_ids WHERE feed_id=? AND observation_id=?',
                                          (producer, d['observation_id'])).fetchone()
            identity = ('entity_urn', 'source', 'account_scope', 'native_table')
            if previous and previous[0] != local:
                previous_evidence = json.loads(previous[1]) if previous[1] else {}
                if any(previous_evidence.get(k) != d[k] for k in identity):
                    raise container.InvalidArchive('producer reused an observation key for another entity')
            evidence = {k: d.get(k) for k in (*identity, 'native_row_ids', 'native_key', 'minted', 'fingerprint', 'adapter_version', 'present')}
            cache.conn.execute('INSERT INTO observation_ids VALUES (?,?,?,?) ON CONFLICT(feed_id,observation_id) DO UPDATE SET local_id=excluded.local_id,evidence=excluded.evidence',
                               (producer, d['observation_id'], local, _j(evidence)))
        elif op in ('observation_refresh', 'record_refresh'):
            from ..cache.changefeed import append
            from ..cache.freshness import stamp
            from ..cache.import_ranges import translate
            kind = 'observation' if op == 'observation_refresh' else 'record'
            try:
                spans = translate(cache.conn, producer, kind, d['ranges'])
            except ValueError as exc:
                raise container.InvalidArchive(str(exc)) from exc
            if spans:
                stamp(cache.conn, kind, spans, d['observed_at'], d.get('sync_run'))
                append(cache.conn, op, {'ranges': spans, 'observed_at': d['observed_at'], 'sync_run': d.get('sync_run')})
    from ..cache.import_ranges import translate
    for kind in ('record', 'observation'):
        translate(cache.conn, producer, kind, [])
    cache.conn.executemany('INSERT INTO applied_changes VALUES (?,?,?)',
                           [(producer, seq, digest) for seq, digest in hashes.items() if seq not in known])
    return outcomes


def _bind_entity(cache: Cache, producer: str, entity_id: int, urn: str, digest: str) -> int:
    key = cache.conn.execute('SELECT id FROM entity_keys WHERE urn=?', (urn,)).fetchone()
    local = key[0] if key else cache.conn.execute('INSERT INTO entity_keys(urn) VALUES (?) RETURNING id', (urn,)).fetchone()[0]
    previous = cache.conn.execute('SELECT local_id FROM entity_ids WHERE producer=? AND entity_id=?',
                                  (producer, entity_id)).fetchone()
    if previous and previous[0] != local:
        raise container.InvalidArchive('producer reused an entity key for another entity')
    cache.conn.execute('INSERT INTO entity_ids VALUES (?,?,?,?) ON CONFLICT(producer,entity_id) DO UPDATE SET head_digest=excluded.head_digest',
                       (producer, entity_id, local, digest))
    return local
