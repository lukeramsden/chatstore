import sqlite3
from contextlib import closing
from functools import partial
from pathlib import Path

import pytest

from chatstore.adapters.base import IncompatibleSource
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.wal import WalPages
from chatstore.adapters.whatsapp import aliases
from chatstore.adapters.whatsapp.adapter import REQUIRED, WhatsAppAdapter
from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, policy_signature, run_prepared_sync
from tests.fixtures.synthetic import build_whatsapp
from tests.unit.test_native_mirror import view


@pytest.fixture
def history(tmp_path):
    root = build_whatsapp(tmp_path / 'whatsapp')
    dd = DataDir(tmp_path / 'data')
    dd.create()
    adapter, cfg, ident = WhatsAppAdapter(), Config(source_paths={'whatsapp': str(root)}), Identity()
    scope, _ = ident.ensure_scope('whatsapp')
    path = root / 'ChatStorage.sqlite'
    with closing(sqlite3.connect(root / 'ContactsV2.sqlite')) as contacts:
        contacts.execute('CREATE TABLE ZWAADDRESSBOOKCONTACT(ZLID TEXT,ZWHATSAPPID TEXT)')
        contacts.execute("INSERT INTO ZWAADDRESSBOOKCONTACT VALUES('111222333@lid','15550009999@s.whatsapp.net')")
        contacts.commit()
    with (closing(sqlite3.connect(path)) as writer, closing(sqlite3.connect(root / 'LID.sqlite')) as lid,
          closing(sqlite3.connect(root / 'ContactsV2.sqlite')) as contacts,
          NativeMirror(tmp_path / 'mirror.sqlite') as mirror, closing(Cache(dd.cache)) as cache):
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('PRAGMA user_version=1')
        writer.commit()
        lid.execute('BEGIN')
        contacts.execute('BEGIN')
        sources = {'LID': lid, 'ContactsV2': contacts}
        expected = aliases.pairs(aliases.collect(sources))
        with view(path) as source:
            capture = mirror.initialize(source, REQUIRED, namespace=f'whatsapp/{scope}',
                policy_tag=policy_signature(adapter, cfg), observed_at='2026-10-18T10:00:00Z',
                context={'whatsapp_aliases': {'format': 1}}, validate_rows=partial(aliases.prepare, sources=sources))
        lid.rollback()
        contacts.rollback()
        yield root, dd, adapter, cfg, ident, writer, lid, mirror, cache, capture, expected


def test_prepared_aliases_survive_source_expiry_and_cold_restore(history, tmp_path):
    root, dd, adapter, cfg, ident, _, _, mirror, cache, capture, expected = history
    (root / 'LID.sqlite').rename(root / 'offline-lid.sqlite')
    (root / 'ContactsV2.sqlite').rename(root / 'offline-contacts.sqlite')
    (root / 'ChatStorage.sqlite').rename(root / 'offline-chat.sqlite')
    prepared = PreparedSync(mirror, capture, 'ChatStorage')
    assert adapter._alias_pairs(prepared.snapshot()) == expected
    result = run_prepared_sync(adapter, dd, cfg, ident, cache, prepared)
    assert result.exit_code == 0, result.run
    assert result.run['counts']['aliases'] == 2
    expected_aliases = {row['urn']: row['revision_digest'] for row in cache.iter_records('aliases')}
    exported = export_changes(cache, dd, cfg, ident, consumer='alias-restore', output=dd.exports,
                              password='test-password', media='text')
    target = DataDir(tmp_path / 'restored')
    target.create()
    with closing(Cache(target.cache)) as restored:
        result = import_archive(restored, target, Identity(), Path(exported['path']), 'test-password')
        assert result.action == 'imported'
        assert {row['urn']: row['revision_digest'] for row in restored.iter_records('aliases')} == expected_aliases


def test_alias_point_and_writer_requirements_fail_closed(history):
    _, _, _, _, _, _, lid, mirror, _, capture, _ = history
    with pytest.raises(IncompatibleSource, match='matching native point'):
        aliases.read(mirror.conn, 'other-token')
    with pytest.raises(IncompatibleSource, match='writer and pinned'):
        aliases.prepare(mirror.conn, sources={'LID': lid, 'ContactsV2': None})
    assert mirror.pending() == capture


def test_rejected_auxiliary_update_rolls_back_with_native_point(history):
    root, _, _, _, _, _, lid, mirror, _, capture, expected = history
    mirror.acknowledge(capture.token, 'test-receipt')
    lid.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+1 555 999 0000' WHERE Z_PK=1")
    lid.commit()
    lid.execute('BEGIN')
    def reject(conn):
        aliases.prepare(conn, sources={'LID': lid, 'ContactsV2': None})
        raise RuntimeError('reject after auxiliary capture')
    try:
        path = root / 'ChatStorage.sqlite'
        with (view(path, capture.cursor) as source,
              WalPages(path.with_name(path.name + '-wal'), source.changes) as pages,
              pytest.raises(RuntimeError, match='reject after auxiliary')):
            mirror.prepare(source, pages, expected_token=capture.token, namespace=capture.namespace,
                policy_tag=capture.policy_tag, observed_at='2026-10-18T10:15:00Z',
                context={'whatsapp_aliases': {'format': 1}}, validate_rows=reject)
    finally:
        lid.rollback()
    assert mirror.pending() is None and mirror.continuation() == capture
    assert aliases.read(mirror.conn, capture.token) == expected
