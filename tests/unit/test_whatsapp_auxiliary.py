import sqlite3
import sys
from contextlib import closing

import pytest

from chatstore.adapters.monitor import MonitoringUnavailable
from chatstore.adapters.whatsapp.auxiliary import AuxiliaryViews

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS vnode monitoring')


def database(path):
    with closing(sqlite3.connect(path)) as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE IF NOT EXISTS evidence(value TEXT)')
        conn.execute("INSERT INTO evidence VALUES('original')")
        conn.commit()


def test_optional_presence_and_readonly_views_close_after_capture(tmp_path):
    database(tmp_path / 'LID.sqlite')
    views = AuxiliaryViews(tmp_path)
    with views:
        assert views.sources['ContactsV2'] is None
        lid = views.sources['LID']
        assert lid.in_transaction
        assert lid.execute('SELECT value FROM evidence').fetchone()[0] == 'original'
        with pytest.raises(sqlite3.OperationalError, match='readonly'):
            lid.execute("INSERT INTO evidence VALUES('forbidden')")
        assert views.evidence['files']['ContactsV2'] == {'present': False}
        views.validate()
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        lid.execute('SELECT 1')
    assert views._monitor is None and views.sources == {}


@pytest.mark.parametrize('operation', ['wal-write', 'creation', 'replacement'])
def test_auxiliary_change_rejects_candidate_views(tmp_path, operation):
    path = tmp_path / 'LID.sqlite'
    database(path)
    with closing(sqlite3.connect(path)) as writer:
        writer.execute('PRAGMA wal_autocheckpoint=0')
        with AuxiliaryViews(tmp_path) as views:
            if operation == 'wal-write':
                writer.execute("UPDATE evidence SET value='later commit'")
                writer.commit()
                assert views.sources['LID'].execute('SELECT value FROM evidence').fetchone()[0] == 'original'
            elif operation == 'creation':
                database(tmp_path / 'ContactsV2.sqlite')
            else:
                path.rename(tmp_path / 'old.sqlite')
                database(path)
            with pytest.raises(MonitoringUnavailable, match='changed during'):
                views.validate()


def test_symlink_does_not_authorize_optional_source_absence(tmp_path):
    database(tmp_path / 'target.sqlite')
    (tmp_path / 'LID.sqlite').symlink_to(tmp_path / 'target.sqlite')
    views = AuxiliaryViews(tmp_path)
    with pytest.raises(MonitoringUnavailable), views:
        pytest.fail('incomplete source coverage was accepted')
    assert views._monitor is None
