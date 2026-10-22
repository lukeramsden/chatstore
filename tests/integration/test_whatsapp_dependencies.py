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
