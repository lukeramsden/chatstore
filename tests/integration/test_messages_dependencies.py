import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from chatstore.adapters.base import ExtractStats
from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.messages.dependencies import (
    CK_IDENTIFIER,
    RoutingUnavailable,
    route_changes,
)
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.wal import WalPages
from chatstore.config import Config
from chatstore.prepared_sync import PreparedSync
from tests.fixtures.synthetic import build_messages
from tests.unit.test_native_mirror import view


@pytest.fixture
def history(tmp_path, request):
    helper = Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        pytest.skip('build the development Rust helper first')
    root = build_messages(tmp_path / 'messages', extra_messages=30)
    path = root / 'chat.db'
    cfg = Config(source_paths={'messages': str(root)}, helper_path=str(helper))
    adapter = MessagesAdapter()
    with closing(sqlite3.connect(path)) as writer, NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        if getattr(request, 'param', None) == 'duplicate-guid':
            sql = writer.execute("SELECT sql FROM sqlite_schema WHERE name='message'").fetchone()[0]
            writer.execute('ALTER TABLE message RENAME TO old_message')
            writer.execute(sql.replace('guid TEXT UNIQUE', 'guid TEXT'))
            writer.execute('INSERT INTO message SELECT * FROM old_message')
            writer.execute('DROP TABLE old_message')
            writer.execute('UPDATE message SET guid=(SELECT guid FROM message WHERE ROWID=1) WHERE ROWID=2')
        writer.execute("CREATE TABLE unknown_info(value TEXT)")
        writer.execute("INSERT INTO unknown_info VALUES('baseline')")
        writer.execute("UPDATE message SET ck_chat_id='SMS;-;+15550009999' WHERE ROWID=100")
        writer.execute('DELETE FROM chat_message_join WHERE message_id=100')
        writer.commit()
        with view(path) as source:
            tables = [row[0] for row in source.connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
            baseline = mirror.initialize(source, tables, namespace='messages/test-scope', policy_tag='routing-v1', observed_at='2026-10-18T10:00:00Z', context={})
        before = units(adapter, cfg, mirror, baseline)
        mirror.acknowledge(baseline.token, 'routing-test-baseline')
        yield adapter, cfg, mirror, baseline, writer, path, before


def units(adapter, cfg, mirror, capture):
    class Outputs:
        def __init__(self):
            self.values = defaultdict(dict)

        def extract(self, key, fingerprint, build, stats):
            for item in build():
                self.values[key][item.record['urn']] = item.record['revision_digest']
                yield item
    output = Outputs()
    stats = ExtractStats(memo=output)
    snap = PreparedSync(mirror, capture, 'chat').snapshot()
    list(adapter.extract(snap, 'test-scope', cfg, stats, None, full=False))
    return dict(output.values)


def prepare(history):
    _, _, mirror, baseline, _, path, _ = history
    with view(path, baseline.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        return mirror.prepare(source, pages, expected_token=baseline.token, namespace=baseline.namespace,
            policy_tag=baseline.policy_tag, observed_at='2026-10-18T10:15:00Z', context={})


@pytest.mark.parametrize('operation', [
    'body', 'guid', 'delete', 'reuse', 'reply', 'handle', 'handle-name', 'subject-handle', 'chat-guid', 'chat-name',
    'chat-identifier', 'chat-service', 'chat-delete', 'chat-insert', 'membership', 'chat-join',
    'stub-support', 'attachment', 'attachment-owner', 'attachment-unjoin', 'recoverable', 'ck-chat',
])
def test_native_dependency_routes_cover_every_changed_full_extraction_unit(history, operation):
    adapter, cfg, mirror, _, writer, _, before = history
    if operation == 'body':
        writer.execute("UPDATE message SET text='old historical edit' WHERE ROWID=1")
    elif operation == 'guid':
        writer.execute("UPDATE message SET guid='CCCCCCCC-0000-4000-8000-000000000001' WHERE ROWID=1")
    elif operation == 'delete':
        writer.execute('DELETE FROM message WHERE ROWID=1')
    elif operation == 'reuse':
        writer.row_factory = sqlite3.Row
        row = dict(writer.execute('SELECT * FROM message WHERE ROWID=1').fetchone())
        writer.execute('DELETE FROM message WHERE ROWID=1')
        row.update(guid='CCCCCCCC-0000-4000-8000-000000000001', text='reused native row')
        fields = ','.join('"' + key + '"' for key in row)
        writer.execute(f'INSERT INTO message({fields}) VALUES({",".join("?" for _ in row)})', list(row.values()))
    elif operation == 'reply':
        writer.execute("UPDATE message SET thread_originator_guid='AAAAAAAA-0000-4000-8000-000000000002' WHERE ROWID=7")
    elif operation in ('handle', 'subject-handle'):
        if operation == 'subject-handle':
            writer.execute('UPDATE message SET other_handle=3 WHERE ROWID=2')
            handle = 3
        else:
            handle = 1
        writer.execute("UPDATE handle SET id='changed-native-identity' WHERE ROWID=?", (handle,))
    elif operation == 'handle-name':
        writer.execute("UPDATE handle SET uncanonicalized_id='new source name' WHERE ROWID=1")
    elif operation == 'chat-name':
        writer.execute("UPDATE chat SET display_name='new chat title' WHERE ROWID=1")
    elif operation == 'chat-guid':
        writer.execute("UPDATE chat SET guid='changed-chat-guid' WHERE ROWID=1")
    elif operation == 'chat-identifier':
        writer.execute("UPDATE chat SET chat_identifier='different-native-identifier' WHERE ROWID=1")
    elif operation == 'chat-service':
        writer.execute("UPDATE chat SET service_name='SMS' WHERE ROWID=1")
    elif operation == 'chat-delete':
        writer.execute('DELETE FROM chat WHERE ROWID=1')
    elif operation == 'chat-insert':
        writer.execute("INSERT INTO chat(ROWID,guid,chat_identifier,service_name,style,is_archived) VALUES(3,'new-chat','+15550009999','SMS',45,0)")
    elif operation == 'membership':
        writer.execute('UPDATE chat_handle_join SET handle_id=3 WHERE ROWID=1')
    elif operation == 'chat-join':
        writer.execute('UPDATE chat_message_join SET message_id=5 WHERE ROWID=1')
    elif operation == 'stub-support':
        writer.execute('INSERT INTO chat_message_join(chat_id,message_id) VALUES(1,6)')
    elif operation == 'attachment':
        writer.execute("UPDATE attachment SET mime_type='video/mp4' WHERE ROWID=1")
    elif operation == 'attachment-owner':
        writer.execute('INSERT INTO message_attachment_join(message_id,attachment_id) VALUES(1,1)')
    elif operation == 'attachment-unjoin':
        writer.execute('DELETE FROM message_attachment_join WHERE message_id=3')
    elif operation == 'recoverable':
        writer.execute('INSERT INTO chat_recoverable_message_join VALUES(1,1,100,0)')
    elif operation == 'ck-chat':
        writer.execute("UPDATE message SET ck_chat_id='SMS;-;changed-stub' WHERE ROWID=6")
    writer.commit()
    pending = prepare(history)
    plan = route_changes(mirror, pending)
    after = units(adapter, cfg, mirror, pending)
    changed = {key for key in before.keys() | after.keys() if before.get(key) != after.get(key)}
    assert changed & before.keys() <= plan.old_unit_keys
    assert changed & after.keys() <= plan.new_unit_keys
    if operation == 'body':
        assert plan.rowids == {1}
    elif operation == 'reply':
        assert {1, 2, 7} <= plan.rowids
    elif operation == 'stub-support':
        assert {6, 100} <= plan.rowids
    elif operation == 'attachment-owner':
        assert {1, 3} <= plan.rowids
    elif operation in ('handle-name', 'chat-name'):
        assert not plan.rowids and plan.header_tables
    elif operation == 'membership':
        assert not plan.rowids and 'chat_handle_join' in plan.header_tables
    assert plan.token == pending.token


def test_clean_and_explicit_media_changes_use_small_selections(history):
    _, _, mirror, _, _, _, _ = history
    pending = prepare(history)
    clean = route_changes(mirror, pending)
    assert not clean.rowids and not clean.old_unit_keys and not clean.new_unit_keys
    media = route_changes(mirror, pending, media_attachment_ids=[1])
    assert media.rowids == {3}


def test_unclassified_changes_and_wrong_point_require_reconciliation(history):
    _, _, mirror, baseline, writer, _, _ = history
    with pytest.raises(RoutingUnavailable, match='baseline'):
        route_changes(mirror, baseline)
    writer.execute("UPDATE unknown_info SET value='unclassified change'")
    writer.commit()
    pending = prepare(history)
    with pytest.raises(RoutingUnavailable, match='unclassified'):
        route_changes(mirror, pending)
    with pytest.raises(RoutingUnavailable, match='exact pending'):
        route_changes(mirror, replace(pending, observed_at='wrong-point-metadata'))


@pytest.mark.parametrize('value', [None, '', 'plain', ';', 'a;b', 'a;b;c', 'a;b;c;d', 'SMS;-;+15550009999'])
def test_indexed_inference_identifier_matches_adapter_split(value):
    with closing(sqlite3.connect(':memory:')) as db:
        db.execute('CREATE TABLE message(ck_chat_id TEXT)')
        db.execute('INSERT INTO message VALUES(?)', (value,))
        actual = db.execute('SELECT ' + CK_IDENTIFIER + ' FROM message').fetchone()[0]
    assert actual == (value.split(';', 2)[-1] if value is not None else None)



@pytest.mark.parametrize('history', ['duplicate-guid'], indirect=True)
def test_metadata_routing_includes_other_witnesses_of_the_same_message_guid(history):
    _, _, mirror, _, writer, _, _ = history
    writer.execute("UPDATE handle SET id='changed sender' WHERE ROWID=1")
    writer.commit()
    pending = prepare(history)
    selection = route_changes(mirror, pending)
    assert {1, 2} <= selection.rowids


def test_dependency_queries_use_persistent_reverse_indexes(history):
    _, _, mirror, _, _, _, _ = history
    with mirror.index.snapshot() as reader:
        for sql, value, index in (
            ('SELECT ROWID FROM message WHERE guid IN (?)', 'guid', 'cs_route_message_guid'),
            ('SELECT ROWID FROM message WHERE other_handle IN (?)', 1, 'cs_route_message_subject'),
            ('SELECT message_id FROM chat_message_join WHERE chat_id IN (?)', 1, 'cs_route_chat_members'),
            ('SELECT message_id FROM message_attachment_join WHERE attachment_id IN (?)', 1, 'cs_attachment_owner_message'),
            ('SELECT ROWID FROM message WHERE (' + CK_IDENTIFIER + ') IN (?)', '+15550009999', 'cs_route_ck_identifier'),
        ):
            plan = ' '.join(row[3] for row in reader.execute('EXPLAIN QUERY PLAN ' + sql, (value,)))
            assert index in plan or (index == 'cs_route_message_guid' and 'SEARCH ' in plan and 'INDEX ' in plan), plan


def test_router_uses_one_old_native_snapshot_when_another_point_commits(history, monkeypatch):
    _, _, mirror, baseline, writer, path, _ = history
    original_guid = writer.execute('SELECT guid FROM message WHERE ROWID=1').fetchone()[0]
    writer.execute("UPDATE message SET text='first prepared point' WHERE ROWID=1")
    writer.commit()
    first = prepare(history)
    changes = mirror.changes
    next_point = []
    def commit_between_state_and_changes(token, *, reader=None):
        mirror.acknowledge(first.token, 'routing-test-first')
        writer.execute("UPDATE message SET guid='CCCCCCCC-0000-4000-8000-000000000001' WHERE ROWID=1")
        writer.commit()
        with view(path, first.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
            next_point.append(mirror.prepare(source, pages, expected_token=first.token,
                namespace=baseline.namespace, policy_tag=baseline.policy_tag, observed_at='2026-10-18T10:30:00Z', context={}))
        yield from changes(token, reader=reader)
    monkeypatch.setattr(mirror, 'changes', commit_between_state_and_changes)
    selection = route_changes(mirror, first)
    assert selection.token == first.token
    assert selection.new_unit_keys == {original_guid}
    assert mirror.pending().token == next_point[0].token != selection.token
