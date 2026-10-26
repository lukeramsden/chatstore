import hashlib
import sqlite3
from contextlib import closing
from functools import partial

import pytest

from chatstore.adapters.base import IncompatibleSource
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.wal import WalPages
from chatstore.adapters.whatsapp.media import filename, prepare, read
from chatstore.paths import DataDir, blob_path
from tests.fixtures.synthetic import build_whatsapp
from tests.unit.test_native_mirror import view


@pytest.fixture
def history(tmp_path):
    root = build_whatsapp(tmp_path / 'whatsapp') / 'Message'
    path = root.parent / 'ChatStorage.sqlite'
    dd = DataDir(tmp_path / 'data')
    dd.create()
    with closing(sqlite3.connect(path)) as writer, NativeMirror(tmp_path / 'mirror.sqlite') as mirror:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('PRAGMA user_version=1')
        writer.commit()
        with view(path) as source:
            capture = mirror.initialize(source, ['ZWAMEDIAITEM'], namespace='whatsapp/test', policy_tag='media-v1',
                observed_at='2026-10-22T12:00:00Z', context={},
                validate_rows=partial(prepare, root=root, dd=dd, full=True))
        item = dict(mirror.conn.execute('SELECT * FROM ZWAMEDIAITEM WHERE Z_PK=1').fetchone())
        yield root, path, dd, writer, mirror, capture, item


def advance(history, validator):
    _, path, _, _, mirror, parent, _ = history
    mirror.acknowledge(parent.token, 'test-receipt')
    with view(path, parent.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        return mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at='2026-10-22T12:15:00Z', context={}, validate_rows=validator)


def test_retained_whatsapp_bytes_survive_expiry_and_require_exact_point_and_path(history):
    root, _, dd, _, mirror, capture, item = history
    original = (root / item['ZMEDIALOCALPATH']).read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    (root / item['ZMEDIALOCALPATH']).unlink()
    assert read(mirror.conn, capture.token, item) == ('available', digest, len(original))
    assert blob_path(dd, digest).read_bytes() == original
    with pytest.raises(IncompatibleSource, match='native point'):
        read(mirror.conn, 'wrong', item)
    with pytest.raises(IncompatibleSource, match='path differs'):
        read(mirror.conn, capture.token, dict(item, ZMEDIALOCALPATH='other'))
    with pytest.raises(IncompatibleSource, match='writer transaction'):
        prepare(mirror.conn, root, dd, full=True)


def test_file_only_change_keeps_both_versions_and_explicit_delta_coverage(history):
    root, _, dd, _, mirror, parent, item = history
    original = read(mirror.conn, parent.token, item)
    changed = b'changed bytes without SQL updates'
    (root / item['ZMEDIALOCALPATH']).write_bytes(changed)
    child = advance(history, partial(prepare, root=root, dd=dd, full=False, dirty_ids={1}))
    digest = hashlib.sha256(changed).hexdigest()
    assert child.rows_changed == 0
    assert read(mirror.conn, child.token, item) == ('available', digest, len(changed))
    assert blob_path(dd, original[1]).is_file() and blob_path(dd, digest).read_bytes() == changed
    assert [row[0] for row in mirror.conn.execute('SELECT row_id FROM cs_native_wa_media_changes')] == [1]
    assert tuple(mirror.conn.execute('SELECT point,full FROM cs_native_wa_media_coverage').fetchone()) == (child.token, 0)


def test_failed_candidate_rolls_back_media_binding_and_requires_complete_delta(history):
    root, _, dd, _, mirror, parent, item = history
    original = read(mirror.conn, parent.token, item)
    (root / item['ZMEDIALOCALPATH']).write_bytes(b'rejected capture bytes')
    def reject(conn):
        prepare(conn, root, dd, full=False, dirty_ids={1})
        raise RuntimeError('reject after retained bytes')
    with pytest.raises(RuntimeError, match='reject after'):
        advance(history, reject)
    assert mirror.pending() is None and mirror.continuation() == parent
    assert read(mirror.conn, parent.token, item) == original
    with mirror.conn:
        mirror.conn.execute('BEGIN IMMEDIATE')
        with pytest.raises(IncompatibleSource, match='complete file-change coverage'):
            prepare(mirror.conn, root, dd, full=False)
        mirror.conn.rollback()


@pytest.mark.parametrize(('relative', 'availability'), [
    (None, 'not_downloaded'), ('', 'not_downloaded'), ('../outside', 'unknown'),
    ('/absolute', 'unknown'), ('Media/missing', 'missing'),
])
def test_path_changes_capture_availability_without_escaping_media_root(history, relative, availability):
    root, _, dd, writer, mirror, _, _ = history
    writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=? WHERE Z_PK=1', (relative,))
    writer.commit()
    child = advance(history, partial(prepare, root=root, dd=dd, full=False, dirty_ids=[]))
    item = dict(mirror.conn.execute('SELECT * FROM ZWAMEDIAITEM WHERE Z_PK=1').fetchone())
    assert read(mirror.conn, child.token, item) == (availability, None, 9)
    if availability in ('unknown', 'not_downloaded'):
        assert filename(root, relative) is None


def test_removed_media_retires_evidence_but_keeps_retained_bytes(history):
    root, _, dd, writer, mirror, parent, item = history
    old = read(mirror.conn, parent.token, item)
    writer.execute('DELETE FROM ZWAMEDIAITEM WHERE Z_PK=1')
    writer.commit()
    child = advance(history, partial(prepare, root=root, dd=dd, full=False, dirty_ids=[]))
    with pytest.raises(IncompatibleSource, match='native point'):
        read(mirror.conn, child.token, item)
    assert blob_path(dd, old[1]).is_file()
