import json
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import asdict, replace

import pytest

from chatstore.adapters.native_mirror import MirrorUnavailable, NativeMirror
from chatstore.adapters.source_view import SourceReadView
from chatstore.adapters.wal import WalPages, read_changes


@contextmanager
def view(path, cursor=None):
    with closing(sqlite3.connect(path)) as reader:
        reader.execute('BEGIN')
        schema = reader.execute('PRAGMA schema_version').fetchone()[0]
        changes = read_changes(path.with_name(path.name + '-wal'), cursor.wal if cursor else None, max_frames=None)
        stat = path.stat()
        yield SourceReadView(reader, changes, schema, (stat.st_dev, stat.st_ino), cursor is None)


@pytest.fixture
def native(tmp_path):
    path = tmp_path / 'native.sqlite'
    writer = sqlite3.connect(path)
    writer.execute('PRAGMA page_size=512')
    writer.execute('PRAGMA journal_mode=WAL')
    writer.execute('PRAGMA wal_autocheckpoint=0')
    writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,body BLOB,name TEXT UNIQUE,number REAL)')
    writer.execute('CREATE TABLE metadata(key TEXT,value BLOB)')
    writer.execute('CREATE TABLE audit(value TEXT)')
    writer.execute("CREATE TRIGGER native_trigger AFTER INSERT ON message BEGIN INSERT INTO audit VALUES('source action'); END")
    writer.executemany('INSERT INTO message VALUES(?,?,?,?)', ((row, b'x' * 60, f'name-{row}', row / 10) for row in range(1, 1001)))
    writer.execute("INSERT INTO metadata VALUES('context',?)", (b'\0\xff',))
    writer.commit()
    yield path, writer
    writer.close()


def initialize(mirror, path):
    with view(path) as source:
        return mirror.initialize(source, ['message', 'metadata', 'audit'], namespace='test/account',
            policy_tag='adapter-v1', observed_at='2026-10-18T10:00:00Z', context={'media': ['retained-probe']})


def prepare(mirror, path, previous):
    with view(path, previous.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        return mirror.prepare(source, pages, expected_token=previous.token, namespace='test/account',
            policy_tag='adapter-v1', observed_at='2026-10-18T10:15:00Z', context={'media': ['next-probe']})


def rows(db, table):
    return [tuple(row) for row in db.execute(f'SELECT rowid,* FROM {table} ORDER BY rowid')]


def test_baseline_is_durable_pending_without_installing_source_triggers(native, tmp_path):
    path, writer = native
    mirror_path = tmp_path / 'mirror.sqlite'
    with NativeMirror(mirror_path) as mirror:
        capture = initialize(mirror, path)
        assert capture.baseline and capture.parent is None and capture.rows_changed == 2001
        assert capture.context == {'media': ['retained-probe']}
        for name in ('message', 'metadata', 'audit'):
            assert rows(mirror.conn, name) == rows(writer, name)
        assert mirror.conn.execute("SELECT count(*) FROM sqlite_schema WHERE type='trigger'").fetchone()[0] == 0
        assert list(mirror.changes(capture.token)) == []
        with pytest.raises(MirrorUnavailable, match='acknowledged'):
            mirror.continuation()
    with NativeMirror(mirror_path) as reopened:
        assert reopened.pending() == capture
        reopened.acknowledge(capture.token, 'canonical-receipt-1')
        reopened.acknowledge(capture.token, 'canonical-receipt-1')
        assert reopened.continuation() == capture and reopened.pending() is None
        with pytest.raises(MirrorUnavailable, match='conflicts'):
            reopened.acknowledge(capture.token, 'different-receipt')


def test_prepare_old_edit_delete_backfill_rowid_reuse_and_metadata(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline-receipt')
        before = {name: rows(writer, name) for name in ('message', 'metadata', 'audit')}
        writer.execute('UPDATE message SET body=? WHERE id=1', (b'edited' * 10000,))
        writer.execute('DELETE FROM message WHERE id=2')
        writer.execute('DELETE FROM message WHERE id=3')
        writer.execute("INSERT INTO message VALUES(3,?,'reused-rowid',?)", (b'replacement', float('inf')))
        writer.execute("INSERT INTO message VALUES(-10,?,'old-backfill',0.5)", (b'backfill',))
        writer.execute('UPDATE metadata SET value=?', (b'\xff\0',))
        writer.commit()
        capture = prepare(mirror, path, initial)
        assert not capture.baseline and capture.parent == initial.token and capture.rows_changed == 7
        changed = {(change.table, change.row_id): change for change in mirror.changes(capture.token)}
        assert changed['message', 2].after is None
        assert changed['message', -10].before is None
        assert changed['message', 3].before[1] == b'x' * 60
        assert changed['message', 3].after[-1] == float('inf')
        assert changed['metadata', 1].before == ('context', b'\0\xff')
        for name in before:
            assert rows(mirror.conn, name) == rows(writer, name)
        # Canonical capture may still need the retained old key after deletion.
        assert changed['message', 2].before == before['message'][1][1:]
        with pytest.raises(MirrorUnavailable, match='continuation'):
            prepare(mirror, path, initial)


def test_pending_retry_survives_source_expiry_and_preserves_observation_time(native, tmp_path):
    path, writer = native
    mirror_path = tmp_path / 'mirror.sqlite'
    with NativeMirror(mirror_path) as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline-receipt')
        writer.execute('UPDATE message SET body=? WHERE id=1', (b'prepared',))
        writer.commit()
        capture = prepare(mirror, path, initial)
        expected_changes = list(mirror.changes(capture.token))
    writer.execute('DELETE FROM message')
    writer.commit()
    assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    path.rename(tmp_path / 'expired.sqlite')
    with NativeMirror(mirror_path) as recovered:
        assert recovered.pending() == capture
        assert recovered.pending().observed_at == '2026-10-18T10:15:00Z'
        assert list(recovered.changes(capture.token)) == expected_changes
        assert recovered.conn.execute('SELECT body FROM message WHERE id=1').fetchone()[0] == b'prepared'
        recovered.acknowledge(capture.token, 'committed-canonical-retry')
        assert recovered.continuation().cursor == capture.cursor


def test_mirror_and_page_graph_roll_back_after_late_failure(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline')
        before = rows(mirror.conn, 'message')
        old_graph = [tuple(row) for row in mirror.conn.execute('SELECT * FROM cs_native_page_nodes ORDER BY page')]
        mirror.conn.execute("CREATE TRIGGER fail_prepare BEFORE INSERT ON cs_native_mirror_state BEGIN SELECT RAISE(ABORT,'injected late crash'); END")
        writer.execute("UPDATE message SET body=x'ff' WHERE id=1")
        writer.commit()
        with pytest.raises(sqlite3.IntegrityError, match='injected late crash'):
            prepare(mirror, path, initial)
        assert mirror.continuation() == initial
        assert rows(mirror.conn, 'message') == before
        assert [tuple(row) for row in mirror.conn.execute('SELECT * FROM cs_native_page_nodes ORDER BY page')] == old_graph
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == initial.token
        assert mirror.conn.execute('SELECT count(*) FROM cs_native_mirror_changes').fetchone()[0] == 0
        mirror.conn.execute('DROP TRIGGER fail_prepare')
        assert prepare(mirror, path, initial).rows_changed == 1


def test_mirror_requires_exact_cursor_policy_namespace_and_schema(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline')
        writer.execute("UPDATE message SET body=x'ff' WHERE id=1")
        writer.commit()
        # Supplying a newer starting cursor must never skip the intervening edit.
        with view(path, initial.cursor) as source:
            later_cursor = source.cursor
        with view(path, later_cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages, pytest.raises(MirrorUnavailable, match='continuation'):
            mirror.prepare(source, pages, expected_token=initial.token, namespace='test/account',
                policy_tag='adapter-v1', observed_at='later', context={})
        with view(path, initial.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
            for changes in ({'expected_token': 'wrong'}, {'namespace': 'different'}, {'policy_tag': 'different'}):
                args = {'expected_token': initial.token, 'namespace': 'test/account', 'policy_tag': 'adapter-v1', 'observed_at': 'later', 'context': {}}
                args.update(changes)
                with pytest.raises(MirrorUnavailable, match='continuation'):
                    mirror.prepare(source, pages, **args)
            with pytest.raises(MirrorUnavailable, match='continuation'):
                mirror.prepare(replace(source, schema_version=source.schema_version + 1), pages,
                    expected_token=initial.token, namespace='test/account', policy_tag='adapter-v1', observed_at='later', context={})
        assert mirror.continuation() == initial


def test_two_mirror_writers_cannot_overwrite_prepared_capture(native, tmp_path):
    path, writer = native
    mirror_path = tmp_path / 'mirror.sqlite'
    with NativeMirror(mirror_path) as first, NativeMirror(mirror_path) as second:
        initial = initialize(first, path)
        first.acknowledge(initial.token, 'baseline')
        writer.execute("UPDATE message SET body=x'ff' WHERE id=1")
        writer.commit()
        capture = prepare(first, path, initial)
        with pytest.raises(MirrorUnavailable, match='continuation'):
            prepare(second, path, initial)
        assert second.pending() == capture
        with pytest.raises(MirrorUnavailable, match='conflicts'):
            second.acknowledge(initial.token, 'old-receipt')
        second.acknowledge(capture.token, 'canonical')
        assert first.continuation() == capture


def test_unique_key_swaps_and_no_change_do_not_replay_source_triggers(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline')
        writer.execute("UPDATE message SET name='temporary' WHERE id=1")
        writer.execute("UPDATE message SET name='name-1' WHERE id=2")
        writer.execute("UPDATE message SET name='name-2' WHERE id=1")
        writer.commit()
        capture = prepare(mirror, path, initial)
        assert capture.rows_changed == 2
        assert rows(mirror.conn, 'audit') == rows(writer, 'audit')
        mirror.acknowledge(capture.token, 'swap')
        clean = prepare(mirror, path, capture)
        assert clean.rows_changed == 0 and list(mirror.changes(clean.token)) == []


@pytest.mark.parametrize('committed', [False, True])
@pytest.mark.skipif(sys.platform != 'darwin', reason='source coordinator requires macOS vnode monitoring')
def test_abrupt_process_exit_recovers_whole_prepared_point_or_prior_point(native, tmp_path, committed):
    path, writer = native
    mirror_path = tmp_path / 'mirror.sqlite'
    with NativeMirror(mirror_path) as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline')
    writer.execute("UPDATE message SET body=x'ff' WHERE id=1")
    writer.commit()
    program = """
import os
import sys
from pathlib import Path
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import WalPages
path, target = map(Path, sys.argv[1:3])
mirror = NativeMirror(target)
previous = mirror.continuation()
if sys.argv[3] == 'False':
    mirror._publish = lambda *args, **kwargs: os._exit(23)
with WalSource(path) as source, source.view(previous.cursor) as view, WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
    mirror.prepare(view, pages, expected_token=previous.token, namespace='test/account', policy_tag='adapter-v1', observed_at='original-observation', context={'retained': True})
os._exit(23)
"""
    result = subprocess.run([sys.executable, '-c', program, str(path), str(mirror_path), str(committed)], capture_output=True, text=True, timeout=15, check=False)
    assert result.returncode == 23, result.stderr
    writer.execute('DELETE FROM message')
    writer.commit()
    writer.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    path.rename(tmp_path / 'offline.sqlite')
    with NativeMirror(mirror_path) as recovered:
        if committed:
            pending = recovered.pending()
            assert pending.parent == initial.token and pending.observed_at == 'original-observation'
            assert pending.context == {'retained': True}
            assert pending.rows_changed == 1
            assert next(iter(recovered.changes(pending.token))).after[1] == b'\xff'
            assert recovered.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == pending.token
        else:
            assert recovered.pending() is None and recovered.continuation() == initial
            assert recovered.conn.execute('SELECT body FROM message WHERE id=1').fetchone()[0] == b'x' * 60
            assert recovered.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == initial.token


def test_native_storage_class_change_is_not_lost_to_python_numeric_equality(native, tmp_path):
    path, writer = native
    writer.execute('UPDATE metadata SET value=1')
    writer.commit()
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'baseline')
        writer.execute('UPDATE metadata SET value=1.0')
        writer.commit()
        capture = prepare(mirror, path, initial)
        assert capture.rows_changed == 1
        changed = next(iter(mirror.changes(capture.token)))
        assert type(changed.before[-1]) is int and type(changed.after[-1]) is float


def test_rebase_replaces_schema_tables_and_policy_with_acknowledged_parent(native, tmp_path):
    path, writer = native
    mirror_path = tmp_path / 'mirror.sqlite'
    with NativeMirror(mirror_path) as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'canonical-baseline')
        writer.execute('ALTER TABLE message ADD COLUMN extra TEXT')
        writer.execute("UPDATE message SET body='rebased',extra='new schema' WHERE id=1")
        writer.execute('DELETE FROM message WHERE id=2')
        writer.execute('CREATE TABLE added(id INTEGER PRIMARY KEY,value TEXT)')
        writer.execute("INSERT INTO added VALUES(1,'new table')")
        writer.commit()
        with view(path) as source:
            capture = mirror.rebase(source, ['message', 'added'], expected_token=initial.token,
                namespace=initial.namespace, policy_tag='adapter-v2',
                observed_at='2026-10-18T10:30:00Z', context={'reason': 'schema-policy'})
        assert capture.baseline and capture.parent == initial.token
        assert capture.policy_tag == 'adapter-v2' and capture.schema_version != initial.schema_version
        assert capture.rows_changed == 1000
        assert rows(mirror.conn, 'message') == rows(writer, 'message')
        assert rows(mirror.conn, 'added') == rows(writer, 'added')
        assert not mirror.conn.execute("SELECT 1 FROM sqlite_schema WHERE name='metadata'").fetchone()
        assert list(mirror.changes(capture.token)) == []
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() == capture
        mirror.acknowledge(capture.token, 'canonical-rebased')
        writer.execute("UPDATE message SET body='after rebase' WHERE id=1")
        writer.commit()
        with view(path, capture.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
            child = mirror.prepare(source, pages, expected_token=capture.token,
                namespace=capture.namespace, policy_tag=capture.policy_tag,
                observed_at='2026-10-18T10:45:00Z', context={})
        assert child.parent == capture.token and not child.baseline
        assert child.rows_changed == 1


@pytest.mark.parametrize('conflict', ['pending', 'stale', 'namespace'])
def test_rebase_cannot_discard_pending_or_cross_lineage(native, tmp_path, conflict):
    path, _ = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        if conflict != 'pending':
            mirror.acknowledge(initial.token, 'canonical-baseline')
        before = rows(mirror.conn, 'message')
        with view(path) as source, pytest.raises(MirrorUnavailable, match='exact acknowledged'):
            mirror.rebase(source, ['message'], expected_token='stale' if conflict == 'stale' else initial.token,
                namespace='other/account' if conflict == 'namespace' else initial.namespace,
                policy_tag='v2', observed_at=initial.observed_at, context={})
        assert rows(mirror.conn, 'message') == before
        assert (mirror.pending() if conflict == 'pending' else mirror.continuation()) == initial


def test_late_rebase_failure_restores_old_schema_rows_page_graph_and_receipt(native, tmp_path, monkeypatch):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'canonical-baseline')
        before = rows(mirror.conn, 'message')
        writer.execute('ALTER TABLE message ADD COLUMN extra TEXT')
        writer.execute('DELETE FROM message WHERE id=1')
        writer.commit()
        publish = mirror._publish
        def fail(*args, **kwargs):
            publish(*args, **kwargs)
            raise RuntimeError('late rebase failure')
        monkeypatch.setattr(mirror, '_publish', fail)
        with view(path) as source, pytest.raises(RuntimeError, match='late rebase'):
            mirror.rebase(source, ['message'], expected_token=initial.token, namespace=initial.namespace,
                policy_tag='v2', observed_at=initial.observed_at, context={})
        assert mirror.continuation() == initial
        assert mirror._state()['receipt'] == 'canonical-baseline'
        assert rows(mirror.conn, 'message') == before
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == initial.token
        assert mirror.conn.execute('SELECT count(*) FROM cs_native_mirror_tables').fetchone()[0] == 3


@pytest.mark.skipif(sys.platform != 'darwin', reason='source coordinator requires macOS vnode monitoring')
@pytest.mark.parametrize('committed', [False, True])
def test_abrupt_rebase_recovers_old_or_new_complete_projection(native, tmp_path, committed):
    path, writer = native
    mirror_path = tmp_path / 'mirror.sqlite'
    with NativeMirror(mirror_path) as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'canonical-baseline')
    writer.execute('ALTER TABLE message ADD COLUMN extra TEXT')
    writer.execute("UPDATE message SET body='rebased',extra='retained schema' WHERE id=1")
    writer.commit()
    program = """
import os
import sys
from pathlib import Path
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.source_view import WalSource
path, target = map(Path, sys.argv[1:3])
mirror = NativeMirror(target)
previous = mirror.continuation()
if sys.argv[3] == 'False':
    mirror._publish = lambda *args, **kwargs: os._exit(23)
with WalSource(path) as source, source.view(None) as view:
    mirror.rebase(view, ['message'], expected_token=previous.token, namespace=previous.namespace,
        policy_tag='adapter-v2', observed_at='original-rebase-time', context={'retained': True})
os._exit(23)
"""
    result = subprocess.run([sys.executable, '-c', program, str(path), str(mirror_path), str(committed)],
                            capture_output=True, text=True, timeout=15, check=False)
    assert result.returncode == 23, result.stderr
    writer.execute('DELETE FROM message')
    writer.commit()
    writer.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    path.rename(tmp_path / 'offline.sqlite')
    with NativeMirror(mirror_path) as recovered:
        if committed:
            pending = recovered.pending()
            assert pending.baseline and pending.parent == initial.token
            assert pending.policy_tag == 'adapter-v2' and pending.observed_at == 'original-rebase-time'
            assert pending.context == {'retained': True}
            assert recovered.conn.execute('SELECT body,extra FROM message WHERE id=1').fetchone()[:] == ('rebased', 'retained schema')
            assert recovered.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == pending.token
        else:
            assert recovered.pending() is None and recovered.continuation() == initial
            assert recovered._state()['receipt'] == 'canonical-baseline'
            assert recovered.conn.execute('SELECT body FROM message WHERE id=1').fetchone()[0] == b'x' * 60
            assert recovered.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == initial.token


def test_candidate_validation_failure_rolls_back_rows_and_page_cursor(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        initial = initialize(mirror, path)
        mirror.acknowledge(initial.token, 'canonical-baseline')
        writer.execute("UPDATE message SET body='rejected change' WHERE id=1")
        writer.commit()
        def reject(conn):
            assert conn.in_transaction
            assert conn.execute('SELECT body FROM message WHERE id=1').fetchone()[0] == 'rejected change'
            raise ValueError('candidate rejected')
        with (view(path, initial.cursor) as source,
              WalPages(path.with_name(path.name + '-wal'), source.changes) as pages,
              pytest.raises(ValueError, match='candidate rejected')):
            mirror.prepare(source, pages, expected_token=initial.token, namespace=initial.namespace,
                policy_tag=initial.policy_tag, observed_at='later', context={}, validate_rows=reject)
        assert mirror.pending() is None and mirror.continuation() == initial
        assert mirror.conn.execute('SELECT body FROM message WHERE id=1').fetchone()[0] == b'x' * 60
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == initial.token
        assert not list(mirror.conn.execute('SELECT * FROM cs_native_mirror_changes'))
        retry = prepare(mirror, path, initial)
        assert retry.rows_changed == 1 and retry.parent == initial.token


def test_policy_recovery_retains_original_source_cut_and_requires_rebase(native, tmp_path):
    path, _ = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        original = initialize(mirror, path)
        before = rows(mirror.conn, 'message')
        recovered = mirror.recover_pending_policy(original, policy_tag='adapter-v2', metadata={'binding': 'test'})
        assert recovered.token != original.token and recovered.baseline
        assert recovered.parent == original.parent and recovered.cursor == original.cursor
        assert recovered.observed_at == original.observed_at
        assert recovered.context['policy_recovery']['capture'] == json.loads(json.dumps(asdict(original)))
        assert rows(mirror.conn, 'message') == before
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == recovered.token
        mirror.acknowledge(recovered.token, 'recovered-canonical-receipt')
        with (view(path, recovered.cursor) as source,
              WalPages(path.with_name(path.name + '-wal'), source.changes) as pages,
              pytest.raises(MirrorUnavailable, match='continuation point or policy mismatch')):
            mirror.prepare(source, pages, expected_token=recovered.token, namespace=recovered.namespace,
                policy_tag='adapter-v2', observed_at='2026-10-18T10:30:00Z', context={})
        with view(path) as source:
            rebased = mirror.rebase(source, ['message', 'metadata', 'audit'], expected_token=recovered.token,
                namespace=recovered.namespace, policy_tag='adapter-v2', observed_at='2026-10-18T10:30:00Z', context={})
        assert rebased.baseline and rebased.parent == recovered.token
        assert not rebased.context.get('native_rebase_required')


def test_rejected_policy_rebind_rolls_back_point_and_private_evidence(native, tmp_path):
    path, _ = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        original = initialize(mirror, path)
        mirror.conn.execute('CREATE TABLE retained_evidence(point TEXT)')
        mirror.conn.execute('INSERT INTO retained_evidence VALUES(?)', (original.token,))
        def reject(conn, token):
            conn.execute('UPDATE retained_evidence SET point=?', (token,))
            raise RuntimeError('rebind rejected')
        with pytest.raises(RuntimeError, match='rebind rejected'):
            mirror.recover_pending_policy(original, policy_tag='adapter-v2', metadata={}, rebind=reject)
        assert mirror.pending() == original
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == original.token
        assert mirror.conn.execute('SELECT point FROM retained_evidence').fetchone()[0] == original.token


def reconcile(mirror, path, parent, validator=None):
    with view(path) as source:
        assert mirror.can_reconcile(source, ['message', 'metadata', 'audit'], expected_token=parent.token,
            namespace=parent.namespace, policy_tag=parent.policy_tag)
        return mirror.reconcile(source, ['message', 'metadata', 'audit'], expected_token=parent.token,
            namespace=parent.namespace, policy_tag=parent.policy_tag, observed_at='after-reset', context={}, validate_rows=validator)


def test_reconciliation_after_wal_reset_keeps_exact_changes_indexes_and_parent(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        parent = initialize(mirror, path)
        mirror.acknowledge(parent.token, 'initial-receipt')
        mirror.conn.execute('CREATE INDEX private_body ON message(body)')
        assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        writer.execute("UPDATE message SET body=x'00ff' WHERE id=1")
        writer.execute('DELETE FROM message WHERE id=2')
        writer.execute("INSERT INTO message VALUES(1001,x'ff','new-name',1.25)")
        previous = writer.execute('SELECT * FROM message WHERE id IN (3,4) ORDER BY id').fetchall()
        writer.execute('DELETE FROM message WHERE id IN (3,4)')
        for row, name in zip(previous, (previous[1][2], previous[0][2]), strict=True):
            writer.execute('INSERT INTO message VALUES(?,?,?,?)', (row[0], row[1], name, row[3]))
        writer.commit()
        child = reconcile(mirror, path, parent)
        assert not child.baseline and child.parent == parent.token and child.context['native_reconciled']
        assert child.rows_changed == 8  # Five message rows plus three source audit inserts.
        changes = {change.row_id: change for change in mirror.changes(child.token) if change.table == 'message'}
        assert set(changes) == {1, 2, 3, 4, 1001}
        assert changes[1].before[1] == b'x' * 60 and changes[1].after[1] == b'\x00\xff'
        assert changes[2].after is None and changes[1001].before is None
        assert mirror.conn.execute("SELECT 1 FROM sqlite_schema WHERE name='private_body'").fetchone()
        for table in ('message', 'metadata', 'audit'):
            assert rows(mirror.conn, table) == rows(writer, table)
        mirror.acknowledge(child.token, 'reset-receipt')
        writer.execute("UPDATE message SET body=x'aa' WHERE id=10")
        writer.commit()
        after = prepare(mirror, path, child)
        assert after.rows_changed == 1 and after.parent == child.token


def test_reconciliation_with_no_row_changes_is_not_a_canonical_baseline(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        parent = initialize(mirror, path)
        mirror.acknowledge(parent.token, 'initial-receipt')
        assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        writer.execute('PRAGMA user_version=2')
        writer.commit()
        child = reconcile(mirror, path, parent)
        assert not child.baseline and child.rows_changed == 0
        assert not list(mirror.changes(child.token))
        assert child.cursor.wal.salts != parent.cursor.wal.salts


def test_reconciliation_rejects_policy_schema_identity_scope_or_pending_drift(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        parent = initialize(mirror, path)
        with view(path) as source:
            args = {'expected_token': parent.token, 'namespace': parent.namespace, 'policy_tag': parent.policy_tag}
            assert not mirror.can_reconcile(source, ['message', 'metadata', 'audit'], **args)
            with pytest.raises(MirrorUnavailable, match='compatible acknowledged'):
                mirror.reconcile(source, ['message', 'metadata', 'audit'], observed_at='later', context={}, **args)
        mirror.acknowledge(parent.token, 'initial-receipt')
        with view(path) as source:
            for key, value in [('expected_token', 'wrong'), ('namespace', 'other'), ('policy_tag', 'new-policy')]:
                assert not mirror.can_reconcile(source, ['message', 'metadata', 'audit'], **(args | {key: value}))
            assert not mirror.can_reconcile(source, ['message'], **args)
            assert not mirror.can_reconcile(replace(source, database_identity=(1, 2)), ['message', 'metadata', 'audit'], **args)
        writer.execute('ALTER TABLE metadata ADD COLUMN extra TEXT')
        writer.commit()
        with view(path) as source:
            assert not mirror.can_reconcile(source, ['message', 'metadata', 'audit'], **args)
        assert mirror.pending() is None and mirror.continuation() == parent


def test_rejected_reconciliation_rolls_back_rows_page_graph_and_private_evidence(native, tmp_path):
    path, writer = native
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        parent = initialize(mirror, path)
        mirror.acknowledge(parent.token, 'initial-receipt')
        original = rows(mirror.conn, 'message')
        mirror.conn.execute('CREATE TABLE private_evidence(point TEXT)')
        mirror.conn.execute('INSERT INTO private_evidence VALUES(?)', (parent.token,))
        writer.execute("UPDATE message SET body=x'01' WHERE id=1")
        writer.commit()
        def reject(conn):
            conn.execute("UPDATE private_evidence SET point='rejected'")
            raise RuntimeError('reconcile rejected')
        with pytest.raises(RuntimeError, match='reconcile rejected'):
            reconcile(mirror, path, parent, reject)
        assert rows(mirror.conn, 'message') == original
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == parent.token
        assert mirror.conn.execute('SELECT point FROM private_evidence').fetchone()[0] == parent.token
        assert mirror.pending() is None and mirror.continuation() == parent
        child = reconcile(mirror, path, parent)
        assert child.rows_changed == 1


def test_reconciliation_preserves_sqlite_storage_class_changes(native, tmp_path):
    path, writer = native
    writer.execute('UPDATE metadata SET value=1')
    writer.commit()
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        parent = initialize(mirror, path)
        mirror.acknowledge(parent.token, 'initial-receipt')
        writer.execute('UPDATE metadata SET value=1.0')
        writer.commit()
        child = reconcile(mirror, path, parent)
        changes = list(mirror.changes(child.token))
        assert child.rows_changed == 1 and len(changes) == 1
        assert type(changes[0].before[1]) is int and type(changes[0].after[1]) is float
