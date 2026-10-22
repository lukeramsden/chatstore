"""Source reuse must check native dependencies and preserve exact capture/freshness semantics."""

from __future__ import annotations

import sqlite3

import pytest

from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.cache import Cache
from chatstore.canonical.records import now_ms, restamp
from chatstore.config import Config, Identity
from chatstore.curation import purge_entity
from chatstore.paths import DataDir
from chatstore.sync import run_sync
from tests.fixtures.synthetic import build_messages, build_whatsapp


def _source(name, tmp_path):
    root = (build_whatsapp if name == 'whatsapp' else build_messages)(tmp_path / name)
    adapter = WhatsAppAdapter() if name == 'whatsapp' else MessagesAdapter()
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cfg, ident = Config(source_paths={name: str(root)}), Identity()
    ident.ensure_scope(name)
    cache = Cache(dd.cache)
    def sync(*, full=False):
        outcome = run_sync(adapter, dd, cfg, ident, cache, full=full)
        assert outcome.exit_code == 0, outcome.run
        return outcome.run['counts']
    initial = sync()
    first = next(m for m in cache.iter_records('messages') if m['text'] == ('hello from alice' if name == 'whatsapp' else 'hello dave'))
    yield name, root, adapter, dd, cfg, ident, cache, sync, initial, first
    cache.close()


@pytest.fixture(params=['whatsapp', 'messages'])
def source(request, tmp_path):
    yield from _source(request.param, tmp_path)


@pytest.fixture
def messages_source(tmp_path):
    yield from _source('messages', tmp_path)


@pytest.fixture
def whatsapp_source(tmp_path):
    yield from _source('whatsapp', tmp_path)


def _add_message(name, root, text):
    table, key = ('ZWAMESSAGE', 'Z_PK') if name == 'whatsapp' else ('message', 'ROWID')
    with sqlite3.connect(root / ('ChatStorage.sqlite' if name == 'whatsapp' else 'chat.db')) as native:
        native.row_factory = sqlite3.Row
        row = dict(native.execute(f'SELECT * FROM {table} WHERE {key}=1').fetchone())
        row[key] = 50000
        if name == 'whatsapp':
            row.update(ZSTANZAID='new-unit-50000', ZSORT=50000, ZTEXT=text)
        else:
            row.update(guid='BBBBBBBB-0000-4000-8000-000000050000', text=text)
        native.execute(f"INSERT INTO {table}({','.join(row)}) VALUES ({','.join('?' * len(row))})", list(row.values()))


def test_unchanged_scan_reuses_units_but_preserves_coverage_and_counts(source):
    name, _, _, _, _, _, cache, sync, initial, first = source
    before = len(cache.revisions_of(first['urn']))
    counts = sync()
    assert counts.get('canonical_units_rebuilt', 0) == 0
    assert counts['canonical_units_reused'] == initial['messages']
    assert counts['messages'] == initial['messages']
    assert counts['unsupported'] == initial['unsupported']
    assert counts['written']['inserted'] == counts['written']['updated'] == 0
    assert len(cache.revisions_of(first['urn'])) == before
    observations = cache.observations_of(first['urn'])
    assert observations[0]['present'] == 'present'
    assert observations[0]['sync_run'] == cache.latest_sync_runs()[0]['urn']
    assert cache.get(first['urn'])['sync_run'] == cache.latest_sync_runs()[0]['urn']
    assert counts.get('absent_from_source:' + ('ZWAMESSAGE' if name == 'whatsapp' else 'message'), 0) == 0


def test_old_text_edit_reconstructs_only_changed_unit(source):
    name, root, _, _, _, _, cache, sync, initial, first = source
    with sqlite3.connect(root / ('ChatStorage.sqlite' if name == 'whatsapp' else 'chat.db')) as native:
        native.execute("UPDATE ZWAMESSAGE SET ZTEXT='years-old edit' WHERE Z_PK=1" if name == 'whatsapp'
                       else "UPDATE message SET text='years-old edit' WHERE ROWID=1")
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert counts['canonical_units_reused'] == initial['messages'] - 1
    assert cache.get(first['urn'])['text'] == 'years-old edit'
    assert len(cache.revisions_of(first['urn'])) == 2


def test_context_rename_is_captured_without_rebuilding_message_units(source):
    name, root, _, _, _, _, cache, sync, initial, first = source
    with sqlite3.connect(root / ('ChatStorage.sqlite' if name == 'whatsapp' else 'chat.db')) as native:
        native.execute("UPDATE ZWACHATSESSION SET ZPARTNERNAME='Updated context' WHERE Z_PK=1" if name == 'whatsapp'
                       else "UPDATE chat SET display_name='Updated context' WHERE ROWID=1")
    counts = sync()
    assert counts.get('canonical_units_rebuilt', 0) == 0
    assert counts['canonical_units_reused'] == initial['messages']
    assert cache.get(first['chat_urn'])['observed_name'] == 'Updated context'


def test_media_bytes_and_availability_are_checked_even_with_unchanged_database(source):
    name, root, _, _, _, _, cache, sync, initial, _ = source
    media = root / ('Message/Media/x/pic.jpg' if name == 'whatsapp' else 'Attachments/ab/01/IMG_0001.HEIC')
    attachment = next(cache.iter_records('attachments'))
    old_sha = attachment['blob_sha256']
    original = media.read_bytes()
    media.write_bytes(bytes(b ^ 1 for b in original))  # identical size, different actual bytes
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert counts['canonical_units_reused'] == initial['messages'] - 1
    assert cache.get(attachment['urn'])['blob_sha256'] != old_sha
    assert len(cache.revisions_of(attachment['urn'])) == 2
    media.unlink()
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert cache.get(attachment['urn'])['availability'] == 'missing'


def test_deleted_native_row_is_not_resighted_by_cached_unit(source):
    name, root, _, _, _, _, cache, sync, initial, first = source
    with sqlite3.connect(root / ('ChatStorage.sqlite' if name == 'whatsapp' else 'chat.db')) as native:
        native.execute('DELETE FROM ZWAMESSAGE WHERE Z_PK=1' if name == 'whatsapp' else 'DELETE FROM message WHERE ROWID=1')
    counts = sync()
    assert counts['canonical_units_reused'] == initial['messages'] - 1
    assert cache.observations_of(first['urn'])[0]['present'] == 'absent_from_source'
    assert cache.get(first['urn'])['text'] == first['text']


def test_projection_edits_and_purge_invalidate_reuse(source):
    name, _, _, _, _, _, cache, sync, initial, first = source
    with cache.write():
        cache.upsert(restamp(dict(first, text='independent cached edit')), observed_at=now_ms())
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert counts['canonical_units_reused'] == initial['messages'] - 1
    assert cache.get(first['urn'])['text'] == first['text']
    with cache.write():
        purge_entity(cache, first['urn'])
    counts = sync()
    # Purging a reaction target also purges its event, owned by another Messages unit.
    assert counts['canonical_units_rebuilt'] == (2 if name == 'messages' else 1)
    assert cache.get(first['urn'])['text'] == first['text']
    if name == 'messages':
        assert any(e['kind'] == 'reaction' and e['target_urn'] == first['urn'] for e in cache.iter_records('events'))
    assert sync().get('canonical_units_rebuilt', 0) == 0


def test_full_mode_forces_reconstruction(source):
    _, _, _, _, _, _, _, sync, initial, _ = source
    counts = sync(full=True)
    assert counts.get('canonical_units_reused', 0) == 0
    assert counts['canonical_units_rebuilt'] == initial['messages']


def test_memo_and_canonical_capture_roll_back_together(source, monkeypatch):
    from chatstore.cache.extraction import ExtractionMemo
    name, root, adapter, dd, cfg, ident, cache, sync, _, first = source
    before = cache.get(first['urn'])['revision_digest']
    checkpoint = cache.get_checkpoint(name, ident.scope_for(name), 'adapter')
    with sqlite3.connect(root / ('ChatStorage.sqlite' if name == 'whatsapp' else 'chat.db')) as native:
        native.execute("UPDATE ZWAMESSAGE SET ZTEXT='interrupted new content' WHERE Z_PK=1" if name == 'whatsapp'
                       else "UPDATE message SET text='interrupted new content' WHERE ROWID=1")
    _add_message(name, root, 'new row before interrupted capture')
    original = ExtractionMemo.save
    def crash(self, units):
        original(self, units)
        if units:
            raise RuntimeError('interrupted memo capture')
    monkeypatch.setattr(ExtractionMemo, 'save', crash)
    with pytest.raises(RuntimeError, match='interrupted memo capture'):
        run_sync(adapter, dd, cfg, ident, cache)
    assert cache.get(first['urn'])['revision_digest'] == before
    assert cache.get_checkpoint(name, ident.scope_for(name), 'adapter') == checkpoint
    monkeypatch.setattr(ExtractionMemo, 'save', original)
    assert sync()['canonical_units_rebuilt'] == 2
    assert cache.get(first['urn'])['text'] == 'interrupted new content'
    assert any(m['text'] == 'new row before interrupted capture' for m in cache.iter_records('messages'))


def test_whatsapp_data_items_and_parent_keys_are_native_dependencies(tmp_path):
    root = build_whatsapp(tmp_path / 'wa')
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cache, ident = Cache(dd.cache), Identity()
    ident.ensure_scope('whatsapp')
    cfg, adapter = Config(source_paths={'whatsapp': str(root)}), WhatsAppAdapter()
    def sync():
        result = run_sync(adapter, dd, cfg, ident, cache)
        assert result.exit_code == 0
        return result.run['counts']
    try:
        sync()
        first = next(m for m in cache.iter_records('messages') if m['text'] == 'hello from alice')
        child = next(m for m in cache.iter_records('messages') if m['text'] == 'hi alice, see you tomorrow')
        with sqlite3.connect(root / 'ChatStorage.sqlite') as native:
            native.execute("INSERT INTO ZWAMESSAGEDATAITEM VALUES (1,0,1,1,'link title','summary','https://example.test')")
            native.execute('UPDATE ZWAMESSAGE SET ZPARENTMESSAGE=1 WHERE Z_PK=2')
        assert sync()['canonical_units_rebuilt'] == 2
        assert cache.get(first['urn'])['part_count'] == 2
        old_reply = cache.get(child['urn'])['reply_to_urn']
        with sqlite3.connect(root / 'ChatStorage.sqlite') as native:
            native.execute("UPDATE ZWAMESSAGE SET ZSTANZAID='corrected-parent-key' WHERE Z_PK=1")
        assert sync()['canonical_units_rebuilt'] == 2
        assert cache.get(child['urn'])['reply_to_urn'] != old_reply
        assert cache.get(old_reply)['text'] == first['text']
    finally:
        cache.close()


def test_messages_sender_context_changes_are_dependencies(messages_source):
    _, root, _, _, _, _, cache, sync, initial, first = messages_source
    old_sender = first['sender_urn']
    with sqlite3.connect(root / 'chat.db') as native:
        native.execute("UPDATE handle SET id='+15550007777' WHERE ROWID=1")
    counts = sync()
    assert 0 < counts['canonical_units_rebuilt'] < initial['messages']
    assert counts['canonical_units_reused'] > 0
    assert cache.get(first['urn'])['sender_urn'] != old_sender


def test_output_policy_change_invalidates_reuse(whatsapp_source, monkeypatch):
    from chatstore.adapters.whatsapp import adapter as WA
    _, _, _, _, _, _, cache, sync, initial, _ = whatsapp_source
    old = next(m for m in cache.iter_records('messages') if m['kind'] == 'unknown:99')
    monkeypatch.setitem(WA.CONTENT_TYPES, 99, 'text')
    counts = sync()
    assert counts['canonical_units_rebuilt'] == initial['messages']
    assert counts.get('canonical_units_reused', 0) == 0
    assert cache.get(old['urn'])['kind'] == 'message'
    assert len(cache.revisions_of(old['urn'])) == 2


def test_historical_receipt_status_change_is_not_text_or_time_filtered(source):
    name, root, _, _, _, _, cache, sync, initial, _ = source
    target = next(m for m in cache.iter_records('messages') if 'see you tomorrow' in (m.get('text') or ''))
    with sqlite3.connect(root / ('ChatStorage.sqlite' if name == 'whatsapp' else 'chat.db')) as native:
        native.execute('UPDATE ZWAMESSAGE SET ZMESSAGESTATUS=5 WHERE Z_PK=2' if name == 'whatsapp'
                       else 'UPDATE message SET date_read=700100000000000000 WHERE ROWID=2')
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert counts['canonical_units_reused'] == initial['messages'] - 1
    current = cache.get(target['urn'])
    assert current['text'] == target['text']
    assert current['state'] == ('delivered' if name == 'whatsapp' else 'read')
    assert len(cache.revisions_of(target['urn'])) == 2


def test_one_new_message_only_rebuilds_the_new_unit(source):
    name, root, _, _, _, _, cache, sync, initial, _ = source
    _add_message(name, root, 'new message after baseline')
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert counts['canonical_units_reused'] == initial['messages']
    assert any(m['text'] == 'new message after baseline' for m in cache.iter_records('messages'))


def test_decimal_timestamp_policy_is_part_of_loaded_code_signature(messages_source, monkeypatch):
    from decimal import Decimal

    from chatstore.canonical import records
    _, _, _, _, _, _, cache, sync, initial, first = messages_source
    monkeypatch.setitem(records._UNIT_DIV, 'ns', Decimal(500000))
    counts = sync()
    assert counts['canonical_units_rebuilt'] == initial['messages']
    assert cache.get(first['urn'])['sent_at']['utc_ms'] != first['sent_at']['utc_ms']


def test_full_reconcile_repairs_unit_dependency_projection(source):
    _, _, _, _, _, _, cache, sync, _, first = source
    with cache.write():
        cache.conn.execute('DELETE FROM native_unit_members WHERE urn=?', (first['urn'],))
    sync(full=True)
    with cache.write():
        cache.upsert(restamp(dict(first, text='later local override')), observed_at=now_ms())
    counts = sync()
    assert counts['canonical_units_rebuilt'] == 1
    assert cache.get(first['urn'])['text'] == first['text']
