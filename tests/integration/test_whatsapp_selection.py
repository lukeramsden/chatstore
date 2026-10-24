import sqlite3
from contextlib import closing

import pytest

from chatstore.adapters.base import ExtractStats, IncompatibleSource
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.config import Config
from chatstore.identity import urn as U
from tests.fixtures.synthetic import build_whatsapp


class Units:
    def __init__(self):
        self.records = {}

    def extract(self, key, fingerprint, build, stats):
        items = list(build())
        self.records[key] = {item.record['urn']: item.record['revision_digest'] for item in items}
        yield from items


def test_selected_stanza_contains_all_duplicates_and_matches_full_output(tmp_path):
    root = build_whatsapp(tmp_path / 'wa', extra_messages=100)
    adapter, cfg = WhatsAppAdapter(), Config(source_paths={'whatsapp': str(root)})
    snap = adapter.snapshot(cfg, tmp_path / 'staging')
    try:
        baseline = Units()
        all_stats = ExtractStats(memo=baseline)
        list(adapter.extract(snap, 'test-scope', cfg, all_stats, None, full=True))
        selected = Units()
        stats = ExtractStats(memo=selected, selected_message_rowids={3})
        list(adapter.extract(snap, 'test-scope', cfg, stats, {'max_message_pk': 1000000}, full=False))
        assert selected.records == {'1:3EB0AAAA0002': baseline.records['1:3EB0AAAA0002']}
        assert stats.counts['messages'] == 1
        assert not stats.full_scan
        assert not {'ZWAMESSAGE', 'ZWAMEDIAITEM'} & stats.scanned_tables
        assert 'ZWAMESSAGE' in all_stats.scanned_tables
    finally:
        snap.cleanup()


@pytest.mark.parametrize('selected', [set(), {-1}, {999999}])
def test_empty_or_deleted_selection_does_not_extract_unrelated_messages(tmp_path, selected):
    root = build_whatsapp(tmp_path / 'wa')
    adapter, cfg = WhatsAppAdapter(), Config(source_paths={'whatsapp': str(root)})
    snap = adapter.snapshot(cfg, tmp_path / 'staging')
    try:
        units = Units()
        stats = ExtractStats(memo=units, selected_message_rowids=selected)
        list(adapter.extract(snap, 'test-scope', cfg, stats, None, full=False))
        assert units.records == {}
        assert not {'ZWAMESSAGE', 'ZWAMEDIAITEM'} & stats.scanned_tables
    finally:
        snap.cleanup()


def test_explicit_full_reconciliation_ignores_partial_selection(tmp_path):
    root = build_whatsapp(tmp_path / 'wa')
    adapter, cfg = WhatsAppAdapter(), Config(source_paths={'whatsapp': str(root)})
    snap = adapter.snapshot(cfg, tmp_path / 'staging')
    try:
        units = Units()
        stats = ExtractStats(memo=units, selected_message_rowids=set())
        list(adapter.extract(snap, 'test-scope', cfg, stats, None, full=True))
        assert len(units.records) == 7
        assert {'ZWAMESSAGE', 'ZWAMEDIAITEM'} <= stats.scanned_tables
    finally:
        snap.cleanup()


def test_message_group_selection_work_does_not_scale_with_unselected_history(tmp_path):
    steps = []
    for size in (100, 10000):
        root = build_whatsapp(tmp_path / str(size), extra_messages=size)
        with closing(sqlite3.connect(root / 'ChatStorage.sqlite')) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('CREATE INDEX cs_msg_key ON ZWAMESSAGE(ZCHATSESSION,ZSTANZAID)')
            conn.execute('CREATE INDEX cs_media_msg ON ZWAMEDIAITEM(ZMESSAGE)')
            conn.execute('CREATE INDEX cs_data_msg ON ZWAMESSAGEDATAITEM(ZMESSAGE)')
            units = Units()
            stats = ExtractStats(memo=units, selected_message_rowids={3})
            counter = []
            conn.set_progress_handler(lambda counter=counter: counter.append(1) or 0, 100)
            list(WhatsAppAdapter()._messages(conn, 'scope', stats,
                lambda entity, key: U.derive_urn('whatsapp', 'scope', entity, key),
                {1: ('15550001111@s.whatsapp.net', U.derive_urn('whatsapp', 'scope', 'chat', 'test'))},
                U.derive_urn('whatsapp', 'scope', 'identity', 'me'), root / 'Message', None, False))
            assert set(units.records) == {'1:3EB0AAAA0002'}
            steps.append(len(counter))
    assert max(steps) < 20, steps
    assert steps[1] <= steps[0] + 2, steps


def test_invalid_selected_native_id_is_rejected(tmp_path):
    root = build_whatsapp(tmp_path / 'wa')
    adapter, cfg = WhatsAppAdapter(), Config(source_paths={'whatsapp': str(root)})
    snap = adapter.snapshot(cfg, tmp_path / 'staging')
    try:
        with pytest.raises(IncompatibleSource, match='signed integers'):
            list(adapter.extract(snap, 'scope', cfg, ExtractStats(selected_message_rowids={True}), None, full=False))
    finally:
        snap.cleanup()
