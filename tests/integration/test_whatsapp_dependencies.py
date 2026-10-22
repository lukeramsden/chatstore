import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest

from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.wal import WalPages
from chatstore.adapters.whatsapp.adapter import REQUIRED
from chatstore.adapters.whatsapp.dependencies import RoutingUnavailable, route_changes
from tests.fixtures.synthetic import build_whatsapp
from tests.unit.test_native_mirror import view


@pytest.fixture
def history(tmp_path):
    root = build_whatsapp(tmp_path / 'whatsapp', extra_messages=100)
    path = root / 'ChatStorage.sqlite'
    with closing(sqlite3.connect(path)) as writer, NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('UPDATE ZWAMESSAGE SET ZPARENTMESSAGE=1 WHERE Z_PK=2')
        writer.execute('CREATE TABLE unknown_info(value TEXT)')
        writer.execute("INSERT INTO unknown_info VALUES('initial')")
        writer.commit()
        with view(path) as source:
            baseline = mirror.initialize(source, [*REQUIRED, 'unknown_info'], namespace='whatsapp/test',
                policy_tag='routing-v1', observed_at='2026-10-18T10:00:00Z', context={})
        mirror.acknowledge(baseline.token, 'baseline')
        yield path, writer, mirror, baseline


def prepare(history):
    path, _, mirror, parent = history
    with view(path, parent.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        return mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z', context={})


@pytest.mark.parametrize(('sql', 'expected', 'old_key'), [
    ("UPDATE ZWAMESSAGE SET ZTEXT='edited' WHERE Z_PK=2", {2, 3}, '1:3EB0AAAA0002'),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=2', {2, 3}, '1:3EB0AAAA0002'),
    ("UPDATE ZWAMESSAGE SET ZSTANZAID='renamed' WHERE Z_PK=1", {1, 2, 3}, '1:3EB0AAAA0001'),
    ("UPDATE ZWAMESSAGE SET ZSTANZAID='3EB0AAAA0001' WHERE Z_PK=2", {1, 2, 3}, '1:3EB0AAAA0002'),
    ('UPDATE ZWAMEDIAITEM SET ZMESSAGE=2 WHERE Z_PK=1', {2, 3, 4}, '1:3EB0AAAA0002'),
    ("INSERT INTO ZWAMESSAGEDATAITEM(Z_PK,ZMESSAGE,ZTITLE) VALUES(1,3,'changed metadata')", {2, 3}, '1:3EB0AAAA0002'),
    ("UPDATE ZWACHATSESSION SET ZPARTNERNAME='title only' WHERE Z_PK=1", set(), None),
])
def test_routes_stanza_siblings_old_keys_replies_and_both_media_owners(history, sql, expected, old_key):
    _, writer, mirror, _ = history
    writer.execute(sql)
    writer.commit()
    capture = prepare(history)
    selected = route_changes(mirror, capture)
    assert selected.rowids == expected
    if old_key is not None:
        assert old_key in selected.old_unit_keys
    if not expected:
        assert selected.header_tables == {'ZWACHATSESSION'}


def test_chat_identity_change_routes_all_affected_groups_and_not_other_chats(history):
    _, writer, mirror, _ = history
    writer.execute("UPDATE ZWACHATSESSION SET ZCONTACTJID='new@s.whatsapp.net' WHERE Z_PK=1")
    writer.commit()
    selected = route_changes(mirror, prepare(history))
    assert selected.rowids == {1, 2, 3, 4, *range(100, 200)}


def test_file_only_dirty_owner_routes_complete_group(history):
    _, _, mirror, _ = history
    selected = route_changes(mirror, prepare(history), media_message_ids=[3])
    assert selected.rowids == {2, 3}
    assert selected.new_unit_keys == {'1:3EB0AAAA0002'}


def test_unknown_table_and_wrong_point_refuse_reuse(history):
    _, writer, mirror, parent = history
    with pytest.raises(RoutingUnavailable, match='baseline'):
        route_changes(mirror, parent)
    writer.execute("UPDATE unknown_info SET value='changed'")
    writer.commit()
    capture = prepare(history)
    with pytest.raises(RoutingUnavailable, match='exact pending'):
        route_changes(mirror, replace(capture, token='wrong'))
    with pytest.raises(RoutingUnavailable, match='unclassified'):
        route_changes(mirror, capture)


@pytest.mark.parametrize(('sql', 'expected'), [
    ("UPDATE ZWAMESSAGE SET ZTEXT='text only' WHERE Z_PK=1", True),
    ("UPDATE ZWAMESSAGE SET ZFROMJID='987654321@lid' WHERE Z_PK=1", True),
    ("UPDATE ZWAMESSAGE SET ZFROMJID='new@lid' WHERE Z_PK=1", False),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=2', True),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=1', False),  # Last incoming owner recipient.
    ('UPDATE ZWAMESSAGE SET ZISFROMME=0 WHERE Z_PK=2', False),
    ("UPDATE ZWACHATSESSION SET ZPARTNERNAME='changed title' WHERE Z_PK=1", False),
    ('UPDATE ZWAMEDIAITEM SET ZMESSAGE=2 WHERE Z_PK=1', True),
    ("INSERT INTO ZWAMESSAGE(Z_PK,ZFROMJID,ZTOJID,ZISFROMME) VALUES(999,'15550001111@s.whatsapp.net','15550009999@s.whatsapp.net',0)", True),
    ("INSERT INTO ZWAMESSAGE(Z_PK,ZFROMJID,ZTOJID,ZISFROMME) VALUES(999,'new@lid',NULL,0)", False),
])
def test_header_input_proof_checks_distinct_jids_and_owner_membership(history, sql, expected):
    from chatstore.adapters.whatsapp.adapter import _message_jids
    from chatstore.adapters.whatsapp.dependencies import headers_unchanged

    _, writer, mirror, parent = history
    for field, incoming in (('ZFROMJID', False), ('ZTOJID', False), ('ZTOJID', True)):
        list(_message_jids(mirror.conn, field, incoming=incoming))
    assert not headers_unchanged(mirror, parent)
    writer.execute(sql)
    writer.commit()
    capture = prepare(history)
    assert headers_unchanged(mirror, capture) is expected
    with pytest.raises(RoutingUnavailable, match='exact pending'):
        headers_unchanged(mirror, replace(capture, token='wrong'))


def test_missing_header_indexes_require_rebuild_without_constructing_them(history):
    from chatstore.adapters.whatsapp.dependencies import headers_unchanged

    assert not headers_unchanged(history[2], prepare(history))
    assert not history[2].conn.execute("SELECT 1 FROM sqlite_schema WHERE name LIKE 'cs_jid_%'").fetchone()
