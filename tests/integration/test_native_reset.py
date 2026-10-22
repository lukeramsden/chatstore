import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir
from chatstore.sync_session import SyncSession
from tests.fixtures.synthetic import build_messages, build_whatsapp

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS pinned native views')


@pytest.fixture(params=['messages', 'whatsapp'])
def history(request, tmp_path):
    source = request.param
    root = (build_messages if source == 'messages' else build_whatsapp)(tmp_path / source, extra_messages=100)
    dd = DataDir(tmp_path / 'data')
    dd.create()
    helper = Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if source == 'messages' and not helper.is_file():
        pytest.skip('build Messages helper first')
    cfg, ident = Config(source_paths={source: str(root)}, helper_path=str(helper)), Identity()
    ident.ensure_scope(source)
    adapter = MessagesAdapter() if source == 'messages' else WhatsAppAdapter()
    path = root / ('chat.db' if source == 'messages' else 'ChatStorage.sqlite')
    with closing(sqlite3.connect(path)) as writer, closing(Cache(dd.cache)) as cache, SyncSession() as session:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        if source == 'messages':
            writer.execute('DELETE FROM message_attachment_join')
            writer.execute('DELETE FROM attachment')
        else:
            writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
        writer.commit()
        def poll():
            return collect_native(adapter, dd, cfg, ident, cache,
                auxiliary_session=session.native_auxiliary(cfg) if source == 'whatsapp' else None)
        yield source, root, dd, cfg, ident, writer, cache, poll


def test_reset_reconciles_actual_changes_retries_offline_and_cold_restores(history, tmp_path, monkeypatch):
    source, root, dd, cfg, ident, writer, cache, poll = history
    initial = poll()
    assert initial is not None and initial.exit_code == 0
    archives = []
    def export():
        result = export_changes(cache, dd, cfg, ident, consumer='reset-restore', output=dd.exports, password='test-password')
        assert result['action'] == 'written' and not result['has_more']
        archives.append(Path(result['path']))
    export()
    assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    writer.execute('PRAGMA user_version=2')
    writer.commit()
    clean = poll()
    assert clean is not None and clean.exit_code == 0
    capture = clean.run['snapshot']['prepared_native']['capture']
    assert capture['context']['native_reconciled'] and not capture['baseline'] and capture['rows_changed'] == 0
    assert clean.run['mode'] == 'incremental' and clean.run['counts'].get('messages', 0) == 0
    export()
    assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    table, key, text = ('message', 'ROWID', 'text') if source == 'messages' else ('ZWAMESSAGE', 'Z_PK', 'ZTEXT')
    columns = [row[1] for row in writer.execute(f'PRAGMA table_info({table})')]
    values = dict(zip(columns, writer.execute(f'SELECT * FROM {table} WHERE {key}=1').fetchone(), strict=True))
    if source == 'messages':
        values.update(ROWID=999999, guid='00000000-0000-0000-0000-000000099999', text='old backfill after reset', date=1)
    else:
        values.update(Z_PK=999999, ZSTANZAID='RESET-BACKFILL', ZTEXT='old backfill after reset', ZMESSAGEDATE=1)
    writer.execute(f'UPDATE {table} SET {text}=? WHERE {key}=1', ('historical reset edit',))
    writer.execute(f'DELETE FROM {table} WHERE {key}=2')
    writer.execute(f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                   [values[column] for column in columns])
    if source == 'messages':
        writer.execute('INSERT INTO chat_message_join(chat_id,message_id) VALUES(1,999999)')
    writer.commit()
    def interrupt(*args, **kwargs):
        raise RuntimeError('interrupt reset canonical capture')
    with monkeypatch.context() as patch:
        patch.setattr(cache, 'upsert_many', interrupt)
        with pytest.raises(RuntimeError, match='interrupt reset'):
            poll()
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        pending = mirror.pending()
        assert pending is not None and pending.context['native_reconciled'] and not pending.baseline
    root.rename(tmp_path / 'expired-source')
    retried = poll()
    assert retried is not None and retried.exit_code == 0 and retried.run['mode'] == 'incremental'
    assert retried.run['snapshot']['prepared_native']['capture']['token'] == pending.token
    assert retried.run['counts']['messages'] < initial.run['counts']['messages']
    messages = list(cache.iter_records('messages'))
    edited = next(row for row in messages if row['text'] == 'historical reset edit')
    assert len(cache.revisions_of(edited['urn'])) == 2
    assert any(row['text'] == 'old backfill after reset' for row in messages)
    export()
    restored = DataDir(tmp_path / 'cold-restore')
    restored.create()
    with closing(Cache(restored.cache)) as target:
        target_ident = Identity()
        for archive in archives:
            assert import_archive(target, restored, target_ident, archive, 'test-password').action == 'imported'
        assert target.get(edited['urn'])['text'] == edited['text']
        assert len(target.revisions_of(edited['urn'])) == 2
        assert any(row['text'] == 'old backfill after reset' for row in target.iter_records('messages'))


def test_empty_truncated_wal_keeps_ordinary_fallback_without_advancing_native_point(history):
    _, _, dd, _, _, writer, _, poll = history
    initial = poll()
    assert initial is not None and initial.exit_code == 0
    token = initial.run['snapshot']['prepared_native']['capture']['token']
    assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    assert poll() is None
    with NativeMirror(next((dd.root / 'native').glob('*.sqlite3'))) as mirror:
        assert mirror.pending() is None and mirror.continuation().token == token
