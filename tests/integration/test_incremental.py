"""Preservation and failure boundaries of immutable changefeed batches."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from chatstore.archive import incremental as I
from chatstore.archive.import_ import import_archive
from chatstore.archive.verify import load_archive, verify_archive
from chatstore.cache import Cache, SourceObservation
from chatstore.canonical.records import restamp
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from tests.unit.test_cache import SCOPE, msg


@pytest.fixture
def feed(tmp_path):
    dd = DataDir(tmp_path / 'source')
    dd.create()
    cache = Cache(dd.cache)
    cfg, ident = Config(), Identity()
    r = msg(1, 'captured before expiry', None, None, 1_000_000)
    with cache.write():
        cache.upsert(r, observed_at=100)
        cache.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 100, None))
    yield cache, dd, cfg, ident, r
    cache.close()


def export(feed, consumer='remote', limit=5000):
    cache, dd, cfg, ident, _ = feed
    return I.export_changes(cache, dd, cfg, ident, consumer=consumer, output=dd.exports,
                            password='test-password', limit=limit)


def restore(tmp_path):
    dd = DataDir(tmp_path / 'isolated')
    dd.create()
    return Cache(dd.cache), dd, Identity()


def test_baseline_delta_expiry_cold_restore_and_receipt(feed, tmp_path):
    cache, _, _, _, r = feed
    baseline = export(feed)
    assert baseline['receipt']['ciphertext_sha256'] == 'sha256:' + hashlib.sha256(Path(baseline['path']).read_bytes()).hexdigest()
    assert baseline['receipt']['export_id'] == baseline['export_id']
    # No-change work must never enumerate records/revisions/observations or serialize history.
    statements = []
    cache.conn.set_trace_callback(statements.append)
    assert export(feed)['action'] == 'unchanged'
    cache.conn.set_trace_callback(None)
    assert not any('FROM records' in s or 'FROM revisions' in s or 'FROM source_observations' in s for s in statements)
    # Edit, reversion and tombstone all occur before the next export; retain every captured state.
    with cache.write():
        edited = restamp(dict(r, text='historical edit'))
        cache.upsert(edited, observed_at=200)
        cache.upsert(r, observed_at=300)
        deleted = restamp(dict(r, text=None, tombstone={'reason': 'source_deleted'}))
        cache.upsert(deleted, observed_at=400)
        cache.mark_absent('whatsapp', SCOPE, 'message', 500, None)
    delta = export(feed)
    assert delta['receipt']['feed']['parent'] == baseline['export_id']
    assert delta['counts']['changes'] <= 8
    dst, target, ident = restore(tmp_path)
    try:
        assert import_archive(dst, target, ident, Path(delta['path']), 'test-password').action == 'branch_conflict'
        assert dst.counts() == {}
        for item in (baseline, delta):
            result = import_archive(dst, target, ident, Path(item['path']), 'test-password')
            assert result.action == 'imported', result
            assert result.conflicts == 0
        assert dst.get(r['urn'])['tombstone']
        assert {rv['revision_digest'] for rv in dst.revisions_of(r['urn'])} == {rv['revision_digest'] for rv in cache.revisions_of(r['urn'])}
        assert dst.revision_record(r['urn'], edited['revision_digest'])['text'] == 'historical edit'
        assert dst.observations_of(r['urn'])[0]['present'] == 'absent_from_source'
        assert dst.conn.execute("SELECT count(*) FROM revisions_fts WHERE revisions_fts MATCH 'captured'").fetchone()[0] == 1
        assert import_archive(dst, target, ident, Path(delta['path']), 'test-password').action == 'already_imported'
    finally:
        dst.close()


def test_refresh_coverage_without_record_rewrites(feed, tmp_path):
    cache, _, _, _, r = feed
    baseline = export(feed)
    with cache.write():
        cache.upsert(r, observed_at=200)
        cache.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 200, None))
    delta = export(feed)
    assert delta['counts']['changes'] == 2
    assert cache.conn.execute('SELECT observed_at FROM records WHERE urn=?', (r['urn'],)).fetchone()[0] == 100
    assert cache.conn.execute('SELECT observed_at FROM source_observations').fetchone()[0] == 100
    assert cache.get(r['urn'])['observed_at'] == 200
    work = tmp_path / 'work'
    work.mkdir()
    loaded = load_archive(Path(delta['path']), 'test-password', work)
    assert {e['operation'] for e in loaded.records['changes']} == {'observation_refresh', 'record_refresh'}
    dst, target, ident = restore(tmp_path)
    try:
        for item in (baseline, delta):
            assert import_archive(dst, target, ident, Path(item['path']), 'test-password').action == 'imported'
        assert dst.observations_of(r['urn'])[0]['observed_at'] == 200
    finally:
        dst.close()


def test_independent_consumers_bounded_batches_and_migration(feed, tmp_path):
    cache, dd, _, _, r = feed
    a = export(feed, 'a', limit=1)
    assert a['sequence'] == 1 and a['has_more']
    b = export(feed, 'b')
    assert b['sequence'] > a['sequence']
    paths = [a['path']]
    while a['has_more']:
        a = export(feed, 'a', limit=1)
        paths.append(a['path'])
    assert a['sequence'] == b['sequence']
    assert export(feed, 'a')['action'] == 'unchanged'
    dst, target, target_ident = restore(tmp_path)
    try:
        for p in paths:
            result = import_archive(dst, target, target_ident, Path(p), 'test-password')
            assert result.action == 'imported', result
        assert dst.get(r['urn'])['text'] == r['text']
    finally:
        dst.close()
    # Existing pre-journal caches are seeded once, with all historical revisions.
    with cache.write():
        for name in ('records', 'revisions', 'source_observations'):
            for action in ('insert', 'update'):
                cache.conn.execute(f'DROP TRIGGER journal_{name}_{action}')
        cache.conn.execute("DELETE FROM meta WHERE key='feed_id'")
        cache.conn.execute('DELETE FROM changes')
    cache.close()
    migrated = Cache(dd.cache)
    try:
        events = [json.loads(row[0]) for row in migrated.conn.execute("SELECT data FROM changes WHERE operation='revision'")]
        assert r['revision_digest'] in {e['revision_digest'] for e in events}
        count = migrated.conn.execute('SELECT count(*) FROM changes').fetchone()[0]
        another = Cache(dd.cache)
        assert another.conn.execute('SELECT count(*) FROM changes').fetchone()[0] == count
        another.close()
    finally:
        migrated.close()


def test_sync_rollback_and_export_crash_recovery(feed, monkeypatch):
    cache, _, _, _, r = feed
    n = cache.conn.execute('SELECT max(seq) FROM changes').fetchone()[0]
    with pytest.raises(RuntimeError), cache.write():
        cache.upsert(restamp(dict(r, text='rolled back')), observed_at=200)
        raise RuntimeError('sync interrupted')
    assert cache.conn.execute('SELECT max(seq) FROM changes').fetchone()[0] == n
    sync = I._sync_file
    def crash(path):
        sync(path)
        raise RuntimeError('crash after publication')
    monkeypatch.setattr(I, '_sync_file', crash)
    with pytest.raises(RuntimeError, match='after publication'):
        export(feed)
    cursor = cache.conn.execute('SELECT sequence FROM export_consumers').fetchone()[0]
    assert cursor == 0
    paths = list(feed[1].exports.glob('*.zip'))
    assert len(paths) == 1
    original = paths[0].read_bytes()
    monkeypatch.setattr(I, '_sync_file', sync)
    recovered = export(feed)
    assert paths[0].read_bytes() == original
    assert Path(recovered['path']) == paths[0]
    assert recovered['sequence'] == cache.conn.execute('SELECT max(seq) FROM changes').fetchone()[0]


def test_failed_verification_keeps_cursor_and_policy_is_immutable(feed, monkeypatch):
    from chatstore.archive import verify as V
    original = V.verify_archive
    monkeypatch.setattr(V, 'verify_archive', lambda *a, **k: V.VerifyResult('', False, problems=['fault']))
    with pytest.raises(RuntimeError, match='failed verification'):
        export(feed)
    assert feed[0].conn.execute('SELECT sequence FROM export_consumers').fetchone()[0] == 0
    assert not list(feed[1].exports.glob('*.zip'))
    monkeypatch.setattr(V, 'verify_archive', original)
    result = export(feed)
    assert verify_archive(Path(result['path']), 'wrong').ok is False
    cache, dd, cfg, ident, _ = feed
    with pytest.raises(ValueError, match='policy changed'):
        I.export_changes(cache, dd, cfg, ident, consumer='remote', output=dd.exports,
                         password='test-password', media='available-media')


def test_context_and_media_only_mutations(feed, tmp_path):
    from chatstore.canonical.records import record
    from chatstore.identity.urn import derive_urn
    cache, _, _, _, r = feed
    baseline = export(feed)
    identity = record('identities', derive_urn('whatsapp', SCOPE, 'identity', 'new'), 'whatsapp', SCOPE,
                      native_key='new', kind='jid_phone', address='new', service=None, is_me=False,
                      observed_names=[{'name': 'Updated identity', 'origin': 'push_name'}])
    attachment = record('attachments', derive_urn('whatsapp', SCOPE, 'attachment', 'a'), 'whatsapp', SCOPE,
                        native_key='a', message_urn=r['urn'], part_index=0, filename='a.txt', mime_type='text/plain',
                        uti=None, declared_size=5, availability='not_downloaded', blob_sha256=None,
                        source_path_hint=None, is_sticker=False, kind='document')
    with cache.write():
        cache.upsert(identity, observed_at=200)
        cache.upsert(attachment, observed_at=200)
    context_delta = export(feed)
    with cache.write():
        cache.upsert(restamp(dict(attachment, availability='missing')), observed_at=300)
    media_delta = export(feed)
    assert media_delta['counts']['changes'] == 2
    dst, target, ident = restore(tmp_path)
    try:
        for item in (baseline, context_delta, media_delta):
            result = import_archive(dst, target, ident, Path(item['path']), 'test-password')
            assert result.action == 'imported', result.problems
        assert dst.get(identity['urn'])['observed_names'][0]['name'] == 'Updated identity'
        assert dst.get(attachment['urn'])['availability'] == 'missing'
        assert len(dst.revisions_of(attachment['urn'])) == 2
    finally:
        dst.close()


def test_interrupted_import_is_atomic(feed, tmp_path, monkeypatch):
    batch = export(feed)
    dst, dd, ident = restore(tmp_path)
    original = I.apply_events
    def crash(cache, manifest, events):
        original(cache, manifest, events)
        raise RuntimeError('import interrupted')
    monkeypatch.setattr(I, 'apply_events', crash)
    try:
        with pytest.raises(RuntimeError, match='interrupted'):
            import_archive(dst, dd, ident, Path(batch['path']), 'test-password')
        assert dst.counts() == {}
        assert dst.conn.execute('SELECT count(*) FROM feed_imports').fetchone()[0] == 0
        monkeypatch.setattr(I, 'apply_events', original)
        assert import_archive(dst, dd, ident, Path(batch['path']), 'test-password').action == 'imported'
    finally:
        dst.close()


def test_remote_durability_requires_metadata_and_remote_bytes(feed, monkeypatch):
    import io

    from chatstore.archive import delivery as D
    baseline = export(feed)
    cache, _, _, _, r = feed
    with cache.write():
        cache.upsert(restamp(dict(r, text='later')), observed_at=200)
    delta = export(feed)
    calls = []
    objects = {}
    for batch in (baseline, delta):
        objects[batch['export_id'] + '/receipt'] = json.dumps(batch['receipt']).encode()
        objects[batch['export_id'] + '/archive'] = Path(batch['path']).read_bytes()
    def remote(url):
        calls.append(url)
        return io.BytesIO(objects[url])
    monkeypatch.setattr(D, '_open_remote', remote)
    def confirm(batch, destination='linux'):
        return D.confirm(cache, destination=destination, export_id=batch['export_id'],
                         receipt_url=batch['export_id'] + '/receipt', archive_url=batch['export_id'] + '/archive')
    assert len(D.pending(cache, 'remote', 'linux')) == 2
    with pytest.raises(ValueError, match='preceding batches'):
        confirm(delta)
    assert cache.conn.execute('SELECT count(*) FROM delivery_checkpoints').fetchone()[0] == 0
    # A populated local cache never substitutes for either published object.
    objects[baseline['export_id'] + '/receipt'] = b'{}'
    with pytest.raises(ValueError, match='receipt does not match'):
        confirm(baseline)
    objects[baseline['export_id'] + '/receipt'] = json.dumps(baseline['receipt']).encode()
    saved = objects[baseline['export_id'] + '/archive']
    objects[baseline['export_id'] + '/archive'] = bytes(len(saved))
    with pytest.raises(ValueError, match='ciphertext does not match'):
        confirm(baseline)
    assert len(D.pending(cache, 'remote', 'linux')) == 2
    objects[baseline['export_id'] + '/archive'] = saved
    confirm(baseline)
    confirm(delta)
    assert D.pending(cache, 'remote', 'linux') == []
    assert len(D.pending(cache, 'remote', 'another-destination')) == 2
    n = len(calls)
    confirm(baseline)  # idempotent reproof still retrieves both objects and cannot regress the head
    assert len(calls) == n + 2
    assert cache.conn.execute('SELECT sequence FROM delivery_checkpoints').fetchone()[0] == delta['sequence']


def test_next_poll_captures_old_source_edits_absence_context_and_media(tmp_path):
    import sqlite3

    from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
    from chatstore.sync import run_sync
    from tests.fixtures.synthetic import build_whatsapp
    root = build_whatsapp(tmp_path / 'wa')
    # A checkpoint beyond the old 5,000-row look-back must not hide row 1 mutations.
    with sqlite3.connect(root / 'ChatStorage.sqlite') as source:
        source.execute('''INSERT INTO ZWAMESSAGE SELECT 10000,Z_ENT,Z_OPT,ZFLAGS,ZGROUPEVENTTYPE,
            ZISFROMME,ZMESSAGEERRORSTATUS,ZMESSAGESTATUS,ZMESSAGETYPE,10000,ZSPOTLIGHTSTATUS,ZSTARRED,
            ZCHATSESSION,ZGROUPMEMBER,ZMEDIAITEM,ZPARENTMESSAGE,ZMESSAGEDATE,ZSENTDATE,ZFROMJID,ZPUSHNAME,
            'high-checkpoint','newer',ZTOJID FROM ZWAMESSAGE WHERE Z_PK=1''')
    dd = DataDir(tmp_path / 'data')
    dd.create()
    ident, cfg = Identity(), Config(source_paths={'whatsapp': str(root)})
    ident.ensure_scope('whatsapp')
    cache = Cache(dd.cache)
    try:
        assert run_sync(WhatsAppAdapter(), dd, cfg, ident, cache).exit_code == 0
        first = I.export_changes(cache, dd, cfg, ident, consumer='linux', output=dd.exports, password='pw')
        with sqlite3.connect(root / 'ChatStorage.sqlite') as source:
            source.execute("UPDATE ZWAMESSAGE SET ZTEXT='old edit found on next poll' WHERE Z_PK=1")
            source.execute('DELETE FROM ZWAMESSAGE WHERE Z_PK=8')
            source.execute("UPDATE ZWACHATSESSION SET ZPARTNERNAME='Renamed' WHERE Z_PK=1")
        (root / 'Message' / 'Media' / 'x' / 'pic.jpg').unlink()
        outcome = run_sync(WhatsAppAdapter(), dd, cfg, ident, cache)
        assert outcome.exit_code == 0 and outcome.run['mode'] == 'incremental'
        second = I.export_changes(cache, dd, cfg, ident, consumer='linux', output=dd.exports, password='pw')
        assert second['receipt']['feed']['parent'] == first['export_id']
        old = next(m for m in cache.iter_records('messages') if m.get('text') == 'old edit found on next poll')
        assert len(cache.revisions_of(old['urn'])) == 2
        absent = next(m for m in cache.iter_records('messages') if m.get('text') == 'direct from lid')
        assert cache.observations_of(absent['urn'])[0]['present'] == 'absent_from_source'
        assert next(cache.iter_records('attachments'))['availability'] == 'missing'
        dst, target, target_ident = restore(tmp_path)
        try:
            for batch in (first, second):
                result = import_archive(dst, target, target_ident, Path(batch['path']), 'pw')
                assert result.action == 'imported' and result.conflicts == 0, result
            assert dst.get(old['urn'])['text'] == 'old edit found on next poll'
            assert dst.observations_of(absent['urn'])[0]['present'] == 'absent_from_source'
            assert dst.get(old['chat_urn'])['observed_name'] == 'Renamed'
        finally:
            dst.close()
    finally:
        cache.close()


def test_legacy_cache_migration_retains_observations_and_nonreused_ids(tmp_path):
    import sqlite3

    from chatstore.cache.schema import DDL
    path = tmp_path / 'legacy.sqlite3'
    conn = sqlite3.connect(path, isolation_level=None)
    for stmt in DDL:
        conn.execute(stmt.replace('        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,\n', '')
                     .replace('UNIQUE (entity_urn, source, account_scope, native_table)',
                              'PRIMARY KEY (entity_urn, source, account_scope, native_table)'))
    conn.execute("INSERT INTO meta VALUES ('schema_version','1')")
    r = msg(1, 'legacy capture', None, None, 0)
    obs = SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fp', '1', 100, None)
    conn.execute(Cache._OBSERVE_SQL, obs.row())
    conn.close()
    # Read-only queries work before the writable migration too.
    readonly = Cache(path, readonly=True)
    assert readonly.observations_of(r['urn'])[0]['observed_at'] == 100
    readonly.close()
    cache = Cache(path)
    try:
        assert cache.observations_of(r['urn'])[0]['native_row_ids'] == [1]
        first_id = cache.conn.execute('SELECT observation_id FROM source_observations').fetchone()[0]
        with cache.write():
            cache.conn.execute('DELETE FROM source_observations')
            cache.observe(obs._replace(entity_urn=msg(2, 'new', None, None, 0)['urn']))
        assert cache.conn.execute('SELECT observation_id FROM source_observations').fetchone()[0] > first_id
        evidence = [json.loads(r[0]) for r in cache.conn.execute("SELECT data FROM changes WHERE operation='observation'")]
        assert {r['observation_id'] for r in evidence} == {first_id, first_id + 1}
    finally:
        cache.close()


def test_identity_only_change_and_forwarded_freshness_survive(feed, tmp_path):
    cache, _, cfg, ident, r = feed
    first = export(feed)
    ident.scope_mappings.append({'from': 'a', 'to': 'b', 'created_at': 123})
    metadata = export(feed)
    assert metadata['counts']['changes'] == 1
    with cache.write():
        cache.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 200, None))
    refreshed = export(feed)
    dst, target, target_ident = restore(tmp_path)
    try:
        for batch in (first, metadata, refreshed):
            assert import_archive(dst, target, target_ident, Path(batch['path']), 'test-password').action == 'imported'
        assert target_ident.scope_mappings == ident.scope_mappings
        # Re-export from a restored cache preserves freshness and provenance for a third machine.
        forwarded = I.export_changes(dst, target, cfg, target_ident, consumer='third', output=target.exports,
                                      password='test-password')
        third = DataDir(tmp_path / 'third')
        third.create()
        c = Cache(third.cache)
        try:
            result = import_archive(c, third, Identity(), Path(forwarded['path']), 'test-password')
            assert result.action == 'imported' and result.conflicts == 0
            assert c.observations_of(r['urn'])[0]['observed_at'] == 200
        finally:
            c.close()
    finally:
        dst.close()


def test_retry_hashes_exact_ciphertext_without_redecrypting(feed, tmp_path, monkeypatch):
    from chatstore.archive import import_ as imports
    batch = export(feed)
    dst, target, ident = restore(tmp_path)
    path = Path(batch['path'])
    try:
        assert import_archive(dst, target, ident, path, 'test-password').action == 'imported'
        def unexpected_decrypt(*args, **kwargs):
            raise AssertionError('already authenticated exact ciphertext must not be decrypted again')
        monkeypatch.setattr(imports, 'load_archive', unexpected_decrypt)
        assert import_archive(dst, target, ident, path, 'test-password').action == 'already_imported'
        content = bytearray(path.read_bytes())
        content[len(content) // 2] ^= 1
        path.write_bytes(content)
        with pytest.raises(AssertionError, match='authenticated exact ciphertext'):
            import_archive(dst, target, ident, path, 'test-password')
    finally:
        dst.close()


def test_overlapping_consumers_do_not_replay_or_conflict(feed, tmp_path):
    cache, _, _, _, r = feed
    first = export(feed, 'a')
    with cache.write():
        cache.upsert(restamp(dict(r, text='next state')), observed_at=200)
    next_a = export(feed, 'a')
    all_b = export(feed, 'b')
    dst, dd, ident = restore(tmp_path)
    try:
        for batch in (first, next_a):
            assert import_archive(dst, dd, ident, Path(batch['path']), 'test-password').conflicts == 0
        captured = dst.conn.execute('SELECT count(*) FROM changes').fetchone()[0]
        result = import_archive(dst, dd, ident, Path(all_b['path']), 'test-password')
        assert result.action == 'imported' and result.conflicts == 0
        assert result.outcomes == {}
        assert dst.conn.execute('SELECT count(*) FROM changes').fetchone()[0] == captured
        assert dst.get(r['urn'])['text'] == 'next state'
        assert len(dst.revisions_of(r['urn'])) == 2
    finally:
        dst.close()


def test_divergent_clone_cannot_reuse_producer_sequence(feed, tmp_path):
    cache, _, cfg, ident, r = feed
    first = export(feed, 'a')
    clone_dd = DataDir(tmp_path / 'clone')
    clone_dd.create()
    clone = Cache(clone_dd.cache)
    cache.conn.backup(clone.conn)
    try:
        with cache.write():
            cache.upsert(restamp(dict(r, text='first branch')), observed_at=200)
        second = export(feed, 'a')
        with clone.write():
            clone.upsert(restamp(dict(r, text='divergent branch')), observed_at=200)
        fork = I.export_changes(clone, clone_dd, cfg, ident, consumer='fork', output=clone_dd.exports,
                                password='test-password')
        dst, dd, target_ident = restore(tmp_path)
        try:
            for batch in (first, second):
                assert import_archive(dst, dd, target_ident, Path(batch['path']), 'test-password').action == 'imported'
            n = dst.conn.execute('SELECT count(*) FROM changes').fetchone()[0]
            result = import_archive(dst, dd, target_ident, Path(fork['path']), 'test-password')
            assert result.action == 'invalid' and 'reused a journal sequence' in result.problems[0]
            assert dst.get(r['urn'])['text'] == 'first branch'
            assert dst.conn.execute('SELECT count(*) FROM changes').fetchone()[0] == n
            assert dst.conn.execute('SELECT count(*) FROM feed_imports').fetchone()[0] == 1
        finally:
            dst.close()
    finally:
        clone.close()


def test_interrupted_sync_keeps_captured_batch_and_running_provenance(tmp_path):
    from types import SimpleNamespace

    from chatstore.adapters.base import Diagnosis, Emit
    from chatstore.sync import BATCH, run_sync
    class InterruptedAdapter:
        source = 'whatsapp'
        version = 'test'
        def diagnose(self, cfg):
            return Diagnosis('whatsapp', '', True, True, True)
        def snapshot(self, cfg, staging):
            return SimpleNamespace(manifest=dict, cleanup=lambda: None)
        def extract(self, snapshot, scope, cfg, stats, checkpoints, *, full):
            for n in range(BATCH):
                yield Emit(msg(n, 'captured batch', None, None, n))
            raise KeyboardInterrupt
    dd = DataDir(tmp_path / 'interrupted')
    dd.create()
    cache, ident = Cache(dd.cache), Identity()
    ident.ensure_scope('whatsapp')
    try:
        with pytest.raises(KeyboardInterrupt):
            run_sync(InterruptedAdapter(), dd, Config(), ident, cache)
        assert cache.counts()['messages'] == BATCH
        run = cache.latest_sync_runs()[0]
        assert run['status'] == 'running' and run['finished_at'] is None
        assert cache.get_checkpoint('whatsapp', ident.scope_for('whatsapp'), 'adapter') is None
        captured = next(cache.iter_records('messages'))
        assert captured['sync_run'] == run['urn']
        assert cache.get(captured['sync_run']) is not None
    finally:
        cache.close()


def test_record_sightings_preserve_import_order_without_touching_content(feed, tmp_path):
    cache, _, _, _, r = feed
    first = export(feed)
    with cache.write():
        cache.upsert(r, observed_at=300)
    refreshed = export(feed)
    assert cache.get(r['urn'])['observed_at'] == 300
    assert cache.conn.execute('SELECT observed_at FROM records WHERE urn=?', (r['urn'],)).fetchone()[0] == 100
    stale = restamp(dict(r, text='older observation from same history'))
    with cache.write():
        assert cache.upsert(stale, observed_at=200, prior_digests={r['revision_digest']}, origin='import') == 'older'
    dst, dd, ident = restore(tmp_path)
    try:
        for batch in (first, refreshed):
            assert import_archive(dst, dd, ident, Path(batch['path']), 'test-password').action == 'imported'
        assert dst.get(r['urn'])['observed_at'] == 300
        with dst.write():
            assert dst.upsert(stale, observed_at=200, prior_digests={r['revision_digest']}, origin='import') == 'older'
    finally:
        dst.close()


def test_producer_sighting_does_not_refresh_a_divergent_local_head(feed, tmp_path):
    cache, _, _, _, r = feed
    first = export(feed)
    with cache.write():
        cache.upsert(r, observed_at=300)
    refreshed = export(feed)
    dst, dd, ident = restore(tmp_path)
    try:
        assert import_archive(dst, dd, ident, Path(first['path']), 'test-password').action == 'imported'
        with dst.write():
            dst.upsert(restamp(dict(r, text='independent local content')), observed_at=200)
        assert import_archive(dst, dd, ident, Path(refreshed['path']), 'test-password').action == 'imported'
        assert dst.get(r['urn'])['text'] == 'independent local content'
        assert dst.get(r['urn'])['observed_at'] == 200
    finally:
        dst.close()


def test_missing_sighting_key_rejects_batch_without_advancing_import(feed, tmp_path):
    from chatstore.cache.changefeed import append
    first = export(feed)
    with feed[0].write():
        append(feed[0].conn, 'record_refresh', {'ranges': [[12345, 12345]], 'observed_at': 300, 'sync_run': None})
    invalid = export(feed)
    dst, dd, ident = restore(tmp_path)
    try:
        assert import_archive(dst, dd, ident, Path(first['path']), 'test-password').action == 'imported'
        result = import_archive(dst, dd, ident, Path(invalid['path']), 'test-password')
        assert result.action == 'invalid' and 'unknown producer keys' in result.problems[0]
        assert dst.conn.execute('SELECT sequence FROM feed_imports').fetchone()[0] == first['sequence']
    finally:
        dst.close()


def test_old_feed_upgrades_keys_and_keeps_freshness_replayable(feed, tmp_path):
    cache, dd, cfg, ident, r = feed
    # Emulate a feed written before canonical entity IDs and compact local sightings existed.
    with cache.write():
        cache.conn.execute("UPDATE changes SET data=json_remove(data,'$.entity_id') WHERE operation='record'")
    first = export(feed)
    with cache.write():
        cache.conn.execute("UPDATE records SET observed_at=250 WHERE urn=?", (r['urn'],))
        cache.conn.execute("DELETE FROM meta WHERE key='sightings_version'")
        cache.conn.execute('DROP TABLE freshness_ranges')
        cache.conn.execute('DROP TABLE entity_ids')
        cache.conn.execute('DROP TABLE entity_keys')
    cache.close()
    upgraded = Cache(dd.cache)
    try:
        assert upgraded.get(r["urn"])["observed_at"] == 250
        second = I.export_changes(upgraded, dd, cfg, ident, consumer='remote', output=dd.exports, password='test-password')
        dst, target, target_ident = restore(tmp_path)
        try:
            for batch in (first, second):
                result = import_archive(dst, target, target_ident, Path(batch['path']), 'test-password')
                assert result.action == 'imported' and result.conflicts == 0, result
            assert dst.get(r['urn'])['observed_at'] == 250
        finally:
            dst.close()
    finally:
        upgraded.close()


def test_purging_projection_cannot_refresh_a_later_different_head(feed):
    from chatstore.curation import purge_entity
    cache, _, _, _, r = feed
    with cache.write():
        cache.upsert(r, observed_at=300)
    key = cache.conn.execute('SELECT id FROM entity_keys WHERE urn=?', (r['urn'],)).fetchone()[0]
    with cache.write():
        purge_entity(cache, r['urn'])
        cache.upsert(restamp(dict(r, text='newly restored content')), observed_at=200)
    assert cache.get(r['urn'])['observed_at'] == 200
    assert cache.conn.execute('SELECT id FROM entity_keys WHERE urn=?', (r['urn'],)).fetchone()[0] == key


def test_sighting_does_not_refresh_divergent_local_observation_evidence(feed, tmp_path):
    cache, _, _, _, r = feed
    first = export(feed)
    with cache.write():
        cache.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 300, None))
    refreshed = export(feed)
    dst, dd, ident = restore(tmp_path)
    try:
        assert import_archive(dst, dd, ident, Path(first['path']), 'test-password').action == 'imported'
        with dst.write():
            dst.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [42], 'm1', 'fingerprint', '1', 200, None))
        assert import_archive(dst, dd, ident, Path(refreshed['path']), 'test-password').action == 'imported'
        observed = dst.observations_of(r['urn'])[0]
        assert observed['native_row_ids'] == [42] and observed['observed_at'] == 200
    finally:
        dst.close()


def test_observation_binding_survives_projection_purge_and_recapture(feed, tmp_path):
    from chatstore.curation import purge_entity
    cache, _, _, _, r = feed
    first = export(feed)
    with cache.write():
        cache.upsert(restamp(dict(r, text='new capture')), observed_at=300)
        cache.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [2], 'm1', 'new-fingerprint', '1', 300, None))
    second = export(feed)
    dst, dd, ident = restore(tmp_path)
    try:
        assert import_archive(dst, dd, ident, Path(first['path']), 'test-password').action == 'imported'
        with dst.write():
            purge_entity(dst, r['urn'])
        result = import_archive(dst, dd, ident, Path(second['path']), 'test-password')
        assert result.action == 'imported' and result.conflicts == 0, result
        assert dst.get(r['urn'])['text'] == 'new capture'
        assert dst.observations_of(r['urn'])[0]['native_row_ids'] == [2]
    finally:
        dst.close()


def test_clean_sighting_import_uses_intervals_without_reading_entity_mappings(feed, tmp_path):
    import sqlite3

    from chatstore.cache.changefeed import refresh_records

    source, _, _, _, r = feed
    baseline = export(feed)
    dst, dd, ident = restore(tmp_path)
    try:
        assert import_archive(dst, dd, ident, Path(baseline['path']), 'test-password').action == 'imported'
        assert dst.conn.execute('SELECT count(*) FROM import_range_ready').fetchone()[0] == 2
        with source.write():
            refresh_records(source.conn, [r['urn']], 300, None)
            source.observe(SourceObservation(r['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 300, None))
        sighting = export(feed)

        def authorize(action, table, *_):
            if action == sqlite3.SQLITE_READ and table in {
                'records', 'source_observations', 'entity_ids', 'observation_ids', 'entity_keys',
            }:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        dst.conn.set_authorizer(authorize)
        try:
            result = import_archive(dst, dd, ident, Path(sighting['path']), 'test-password')
            assert result.action == 'imported', result
        finally:
            dst.conn.set_authorizer(None)
        assert dst.get(r['urn'])['urn'] == r['urn']
        assert dst.conn.execute("SELECT observed_at FROM freshness_ranges WHERE kind='record'").fetchone()[0] == 300
        assert dst.conn.execute("SELECT observed_at FROM freshness_ranges WHERE kind='observation'").fetchone()[0] == 300
    finally:
        dst.close()


def test_large_event_limit_splits_on_utf8_member_bytes_without_sequence_gaps(feed, monkeypatch):
    cache, dd, _, _, _ = feed
    with cache.write():
        for number in range(10, 20):
            cache.upsert(msg(number, '字' * 500, None, None, number))
    monkeypatch.setitem(I.container.DEFAULT_LIMITS, 'max_member_bytes', 8192)
    sequences = []
    batches = 0
    while True:
        result = export(feed, limit=100000)
        assert result['action'] == 'written'
        batches += 1
        loaded = load_archive(Path(result['path']), 'test-password', dd.staging)
        try:
            events = loaded.records['changes']
            assert sum(len((I._j(event) + '\n').encode('utf-8')) for event in events) <= 8192
            sequences.extend(event['seq'] for event in events)
        finally:
            shutil.rmtree(loaded.tar_path.parent)
        if not result['has_more']:
            break
    assert batches > 1
    assert sequences == [row[0] for row in cache.conn.execute('SELECT seq FROM changes ORDER BY seq')]
    assert export(feed, limit=100000)['action'] == 'unchanged'


def test_single_oversized_event_retains_consumer_checkpoint(feed, monkeypatch):
    cache, dd, _, _, _ = feed
    baseline = export(feed)
    checkpoint = cache.conn.execute("SELECT sequence FROM export_consumers WHERE consumer='remote'").fetchone()[0]
    with cache.write():
        cache.upsert(msg(10, '字' * 2000, None, None, 10))
    monkeypatch.setitem(I.container.DEFAULT_LIMITS, 'max_member_bytes', 4096)
    with pytest.raises(ValueError, match='exceeds archive member byte limit'):
        export(feed, limit=100000)
    assert cache.conn.execute("SELECT sequence FROM export_consumers WHERE consumer='remote'").fetchone()[0] == checkpoint
    assert list(dd.exports.glob('*.zip')) == [Path(baseline['path'])]
