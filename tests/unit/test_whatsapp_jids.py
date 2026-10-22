import sqlite3
from contextlib import closing

import pytest

from chatstore.adapters.base import ExtractStats
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter, _message_jids


@pytest.mark.parametrize(('field', 'incoming'), [('ZFROMJID', False), ('ZTOJID', False), ('ZTOJID', True)])
def test_seeked_jids_match_distinct_after_edits_and_deletions(field, incoming):
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('CREATE TABLE ZWAMESSAGE(ZFROMJID TEXT,ZTOJID TEXT,ZISFROMME INTEGER)')
        conn.executemany('INSERT INTO ZWAMESSAGE VALUES(?,?,?)', [
            ('a@lid', 'b@lid', 0), ('a@lid', 'b@lid', 0),
            ('', '', 0), (None, None, 0), ('é@lid', 'z@lid', 1),
            ('文@lid', 'q@lid', None), ('null-state@lid', 'b@lid', 1),
        ])
        for sql in (None, "UPDATE ZWAMESSAGE SET ZTOJID='new@lid',ZFROMJID='new-from@lid' WHERE rowid=1",
                    'DELETE FROM ZWAMESSAGE WHERE rowid=2'):
            if sql:
                conn.execute(sql)
            expected = [row[0] for row in conn.execute(
                f'SELECT DISTINCT {field} FROM ZWAMESSAGE WHERE {field} IS NOT NULL' +
                (' AND ZISFROMME=0' if incoming else '') + f' ORDER BY {field}')]
            assert list(_message_jids(conn, field, incoming=incoming)) == expected


def test_owner_detection_keeps_alias_closure_and_ignores_outgoing_recipients():
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('CREATE TABLE ZWAMESSAGE(ZTOJID TEXT,ZISFROMME INTEGER)')
        conn.executemany('INSERT INTO ZWAMESSAGE VALUES(?,?)', [
            ('15550000001@s.whatsapp.net', 0), ('15550000001@s.whatsapp.net', 0),
            ('15550000002@s.whatsapp.net', 1), ('', 0), (None, 0),
        ])
        pair = ('100@lid', '15550000001@s.whatsapp.net')
        assert WhatsAppAdapter()._owner_jids(conn, {pair: []}, ExtractStats()) == set(pair)
        conn.execute('DELETE FROM ZWAMESSAGE WHERE ZISFROMME=0')
        stats = ExtractStats()
        assert WhatsAppAdapter()._owner_jids(conn, {pair: []}, stats) == set()
        assert any('owner JID not detected' in note for note in stats.notes)


def test_warm_jid_discovery_work_scales_with_distinct_values_not_message_count():
    counts = []
    for size in (100, 100000):
        with closing(sqlite3.connect(':memory:')) as conn:
            conn.execute('CREATE TABLE ZWAMESSAGE(ZFROMJID TEXT,ZTOJID TEXT,ZISFROMME INTEGER)')
            conn.executemany('INSERT INTO ZWAMESSAGE VALUES(?,?,?)',
                ((f'{row % 10}@lid', f'{row % 3}@lid', row % 2) for row in range(size)))
            def discover():
                return (list(_message_jids(conn, 'ZFROMJID')),
                        list(_message_jids(conn, 'ZTOJID')),
                        list(_message_jids(conn, 'ZTOJID', incoming=True)))
            expected = discover()  # Private indexes are built once per mirror/schema.
            steps = []
            conn.set_progress_handler(lambda steps=steps: steps.append(1) or 0, 100)
            assert discover() == expected
            counts.append(len(steps))
    assert max(counts) < 20, counts
    assert counts[1] <= counts[0] + 2, counts
