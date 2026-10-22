import json
import sqlite3
import sys
from contextlib import closing

import pytest

from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.cache import Cache
from chatstore.cli.app import main
from chatstore.config import load_config, load_identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir
from tests.fixtures.synthetic import build_whatsapp

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS guarded native collector')


@pytest.fixture
def collector(tmp_path, monkeypatch):
    root = build_whatsapp(tmp_path / 'whatsapp', extra_messages=50)
    dd = DataDir(tmp_path / 'data')
    monkeypatch.setenv('CHATSTORE_DATA_DIR', str(dd.root))
    with closing(sqlite3.connect(root / 'ChatStorage.sqlite')) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
        writer.commit()
        yield root, dd, writer


def cli(capsys, *args):
    code = main(['--json', *args])
    result = json.loads(capsys.readouterr().out)
    assert code == 0, result
    return result['data']


def test_cli_whatsapp_dispatch_preserves_old_changes_and_optional_creation(collector, capsys):
    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    initial = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    capture = initial['snapshot']['prepared_native']['capture']
    assert capture['baseline']
    assert capture['context']['whatsapp_auxiliary']['files']['ContactsV2'] == {'present': False}
    clean = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert clean['counts'].get('messages', 0) == 0
    writer.execute("UPDATE ZWAMESSAGE SET ZTEXT='old edit through WhatsApp CLI' WHERE Z_PK=1")
    writer.commit()
    edited = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert edited['mode'] == 'incremental' and edited['counts']['messages'] == 1
    with closing(sqlite3.connect(root / 'ContactsV2.sqlite')) as contacts:
        contacts.execute('CREATE TABLE ZWAADDRESSBOOKCONTACT(ZLID TEXT,ZWHATSAPPID TEXT)')
        contacts.execute("INSERT INTO ZWAADDRESSBOOKCONTACT VALUES('123456@lid','15550000000@s.whatsapp.net')")
        contacts.commit()
    auxiliary = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert auxiliary['counts'].get('messages', 0) == 0 and auxiliary['counts']['aliases'] == 3
    assert auxiliary['snapshot']['prepared_native']['capture']['context']['whatsapp_auxiliary']['files']['ContactsV2']['present']
    writer.execute('DELETE FROM ZWAMESSAGE WHERE Z_PK=1')
    writer.commit()
    deleted = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert deleted['counts'].get('messages', 0) == 0
    with closing(Cache(dd.cache)) as cache:
        first = next(row for row in cache.iter_records('messages') if row['text'] == 'old edit through WhatsApp CLI')
        assert cache.observations_of(first['urn'])[0]['present'] == 'absent_from_source'
        assert len(cache.revisions_of(first['urn'])) == 2


def test_cli_whatsapp_pending_retry_does_not_open_expired_sources(collector, capsys, monkeypatch):
    root, dd, _ = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    def interrupted(*args, **kwargs):
        raise RuntimeError('interrupted WhatsApp canonical write')
    with closing(Cache(dd.cache)) as cache, monkeypatch.context() as patch:
        patch.setattr(cache, 'upsert_many', interrupted)
        with pytest.raises(RuntimeError, match='interrupted WhatsApp'):
            collect_native(WhatsAppAdapter(), dd, load_config(dd), load_identity(dd), cache)
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        pending = mirror.pending()
        assert pending is not None
    for name in ('ChatStorage', 'LID'):
        (root / f'{name}.sqlite').rename(root / f'offline-{name}.sqlite')
    retried = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert retried['snapshot']['prepared_native']['capture']['token'] == pending.token
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation() == pending


def test_file_path_transition_rolls_back_native_candidate_before_fallback(collector, capsys):
    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    initial = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    token = initial['snapshot']['prepared_native']['capture']['token']
    writer.execute("UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH='Media/x/pic.jpg' WHERE Z_PK=1")
    writer.commit()
    with closing(Cache(dd.cache)) as cache:
        assert collect_native(WhatsAppAdapter(), dd, load_config(dd), load_identity(dd), cache) is None
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation().token == token
        assert mirror.conn.execute('SELECT ZMEDIALOCALPATH FROM ZWAMEDIAITEM WHERE Z_PK=1').fetchone()[0] is None
    fallback = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert 'prepared_native' not in fallback['snapshot']
    with closing(Cache(dd.cache)) as cache:
        assert next(cache.iter_records('attachments'))['availability'] == 'available'
