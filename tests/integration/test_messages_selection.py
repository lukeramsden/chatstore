import json
import sqlite3
import subprocess
from collections import defaultdict
from contextlib import closing
from pathlib import Path

import pytest

from chatstore.adapters.base import ExtractStats
from chatstore.adapters.messages.adapter import MessagesAdapter, find_helper
from chatstore.adapters.messages.decoder import decode
from chatstore.config import Config
from tests.fixtures.synthetic import build_messages


@pytest.fixture
def source(tmp_path):
    root = build_messages(tmp_path / 'messages', extra_messages=100)
    adapter = MessagesAdapter()
    config = Config(source_paths={'messages': str(root)}, helper_path=str(Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'))
    helper = find_helper(config)
    if helper is None:
        pytest.skip('Rust Messages helper is unavailable')
    snap = adapter.snapshot(config, tmp_path / 'staging')
    yield adapter, config, helper, snap
    snap.cleanup()


def test_selected_helper_rows_equal_full_decoding_including_context_and_missing_ids(source):
    _, _, helper, snap = source
    path = snap.files['chat'].snapshot_path
    with closing(decode(helper, path)) as decoded:
        full = list(decoded)
    selected = [1, 3, 4, 5, 7, 999999]
    with closing(decode(helper, path, selected)) as decoded:
        part = list(decoded)
    assert part[0]['selection'] == {'kind': 'rowids', 'requested': len(selected)}
    assert part[-1]['errors'] == 0
    assert [row for row in part if row['type'] == 'message'] == [row for row in full if row['type'] == 'message' and row['rowid'] in selected]
    with closing(decode(helper, path, [])) as decoded:
        empty = list(decoded)
    assert [row['type'] for row in empty] == ['header', 'footer']
    assert empty[-1]['messages'] == 0


def test_selected_adapter_units_match_full_output_and_do_not_claim_full_message_scan(source):
    adapter, config, _, snap = source
    full_stats = ExtractStats()
    full = list(adapter.extract(snap, 'selection-test', config, full_stats, None, full=False))
    selected_stats = ExtractStats(selected_message_rowids={1, 3, 5, 7})
    selected = list(adapter.extract(snap, 'selection-test', config, selected_stats, None, full=False))
    full_by_native = defaultdict(list)
    selected_by_native = defaultdict(list)
    for output, indexed in ((full, full_by_native), (selected, selected_by_native)):
        for item in output:
            if item.observation and item.observation.native_table in ('message', 'attachment'):
                indexed[item.record['urn']].append(item.record['revision_digest'])
    assert selected_by_native
    assert all(values == full_by_native[key] for key, values in selected_by_native.items())
    selected_keys = {row.record['native_key'] for row in selected if row.record['entity'] == 'messages'}
    expected_keys = {row.record['native_key'] for row in full if row.record['entity'] == 'messages' and row.observation.native_row_ids[0] in selected_stats.selected_message_rowids}
    assert selected_keys == expected_keys
    assert not {'message', 'attachment', 'message.ck_chat_id'} & selected_stats.scanned_tables
    assert {'message', 'attachment', 'message.ck_chat_id'} <= full_stats.scanned_tables


def test_negative_rowid_is_decoded_in_full_and_selected_modes(source):
    _, _, helper, snap = source
    path = snap.files['chat'].snapshot_path
    with sqlite3.connect(path) as writer:
        writer.execute('UPDATE message SET ROWID=-1 WHERE ROWID=1')
    for selected in (None, [-1]):
        with closing(decode(helper, path, selected)) as decoded:
            rows = list(decoded)
        assert any(row.get('rowid') == -1 for row in rows)
        assert rows[-1]['errors'] == 0


@pytest.mark.parametrize('payload', ['null', '{}', '[true]', '[1.5]', '[9223372036854775808]', '[' + ','.join('1' for _ in range(10001)) + ']', ' ' * (256 * 1024 + 1)], ids=['null', 'object', 'boolean', 'float', 'overflow', 'many', 'large'])
def test_helper_rejects_malformed_or_unbounded_selection_before_opening_source(source, tmp_path, payload):
    _, _, helper, _ = source
    ids = tmp_path / 'ids.json'
    ids.write_text(payload)
    result = subprocess.run([str(helper), str(tmp_path / 'missing.sqlite'), '--rowids-file', str(ids)], capture_output=True, text=True, check=False)
    assert result.returncode == 64 and not result.stdout


def test_helper_rejects_unknown_conflicting_and_invalid_since_options(source, tmp_path):
    _, _, helper, _ = source
    ids = tmp_path / 'ids.json'
    ids.write_text(json.dumps([1]))
    for options in (['--unknown', '1'], ['--since-rowid', 'nonsense'], ['--since-rowid'], ['--since-rowid', '1', '--rowids-file', str(ids)]):
        result = subprocess.run([str(helper), str(tmp_path / 'missing.sqlite'), *options], capture_output=True, text=True, check=False)
        assert result.returncode == 64 and not result.stdout


def test_selected_query_uses_indexed_row_lookup_without_history_scan(source):
    adapter, config, helper, snap = source
    # Establish the private dependency indexes used by both full and selective paths.
    for _ in adapter.extract(snap, 'selection-test', config, ExtractStats(), None, full=False):
        pass
    path = snap.files['chat'].snapshot_path
    with closing(decode(helper, path, [1])) as decoded:
        selected = list(decoded)
    assert selected[-1]['messages'] == 1
    assert selected[-1]['query_fullscan_steps'] == 0
    with closing(decode(helper, path)) as decoded:
        full = list(decoded)
    assert full[-1]['query_fullscan_steps'] >= 100


def test_selection_does_not_change_native_unit_fingerprints(source):
    adapter, config, _, snap = source
    class Fingerprints:
        def __init__(self):
            self.values = {}

        def extract(self, key, fingerprint, build, stats):
            self.values[key] = fingerprint
            yield from build()
    baseline, partial = Fingerprints(), Fingerprints()
    for _ in adapter.extract(snap, 'selection-test', config, ExtractStats(memo=baseline), None, full=False):
        pass
    stats = ExtractStats(memo=partial, selected_message_rowids={1, 3, 5, 7})
    for _ in adapter.extract(snap, 'selection-test', config, stats, None, full=False):
        pass
    assert partial.values and all(value == baseline.values[key] for key, value in partial.values.items())


def test_selected_attachment_owner_lookup_does_not_scan_all_shared_links(source, monkeypatch):
    from chatstore.adapters.messages import adapter as messages

    adapter, config, _, snap = source
    for _ in adapter.extract(snap, 'selection-test', config, ExtractStats(), None, full=False):
        pass
    path = snap.files['chat'].snapshot_path
    with sqlite3.connect(path) as writer:
        writer.executemany('INSERT INTO message_attachment_join VALUES(?,1)', ((row,) for row in range(10000, 30000)))
    original = sqlite3.connect
    callbacks = []
    def counted(*args, **kwargs):
        connection = original(*args, **kwargs)
        if Path(args[0]) == path:
            connection.set_progress_handler(lambda: callbacks.append(1) or 0, 100)
        return connection
    monkeypatch.setattr(messages.sqlite3, 'connect', counted)
    stats = ExtractStats(selected_message_rowids={3})
    records = list(adapter.extract(snap, 'selection-test', config, stats, None, full=False))
    attachment = next(item.record for item in records if item.record['entity'] == 'attachments')
    assert attachment['message_urn'] == next(item.record['urn'] for item in records if item.record['entity'] == 'messages')
    assert len(callbacks) * 100 < 20000


def test_large_selection_matches_full_adapter_with_date_order_across_batches(tmp_path):
    root = build_messages(tmp_path / 'large', extra_messages=10020)
    adapter = MessagesAdapter()
    config = Config(source_paths={'messages': str(root)}, helper_path=str(Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'))
    if find_helper(config) is None:
        pytest.skip('Rust Messages helper is unavailable')
    snap = adapter.snapshot(config, tmp_path / 'staging')
    try:
        with sqlite3.connect(snap.files['chat'].snapshot_path) as writer:
            writer.execute('UPDATE message SET date = 1000000000000000000 - ROWID * 1000000000 WHERE ROWID >= 100')
        selected = set(range(100, 10101)) | {999999}
        full = list(adapter.extract(snap, 'large-selection', config, ExtractStats(), None, full=False))
        stats = ExtractStats(selected_message_rowids=selected)
        partial = list(adapter.extract(snap, 'large-selection', config, stats, None, full=False))
        def messages(output):
            return [(item.record['urn'], item.record['revision_digest']) for item in output
                    if item.record['entity'] == 'messages'
                    and item.observation.native_row_ids[0] in selected]
        assert messages(partial) == messages(full)
        assert len(messages(partial)) == 10001
        headers = lambda output: [(item.record['urn'], item.record['revision_digest']) for item in output
                                  if item.observation and item.observation.native_table in ('handle', 'chat', 'chat_handle_join')]
        assert headers(partial) == headers(full)
        assert 'message' not in stats.scanned_tables
    finally:
        snap.cleanup()
