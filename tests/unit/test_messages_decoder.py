import json
import sys
from contextlib import closing

import pytest

from chatstore.adapters.base import IncompatibleSource
from chatstore.adapters.messages.decoder import decode


def helper(tmp_path, code):
    path = tmp_path / 'fake-decoder'
    path.write_text(f'#!{sys.executable}\n' + code)
    path.chmod(0o700)
    return path


def test_selected_mode_refuses_old_helper_instead_of_accepting_full_history(tmp_path):
    binary = helper(tmp_path, "print('{\"type\":\"header\",\"format\":\"chatstore-messages-decoder/1\"}')")
    with closing(decode(binary, tmp_path / 'private.sqlite', [1])) as rows, pytest.raises(IncompatibleSource, match='selection support'):
        list(rows)
    assert [path.name for path in tmp_path.iterdir()] == ['fake-decoder']


@pytest.mark.parametrize('case', ['outside-selection', 'wrong-count', 'after-footer', 'no-footer', 'invalid-json', 'not-object'])
def test_invalid_helper_transport_never_completes_a_selected_capture(tmp_path, case):
    header = {'type': 'header', 'format': 'chatstore-messages-decoder/1', 'capabilities': ['rowids-file-v1'], 'selection': {'kind': 'rowids', 'requested': 1}}
    message = {'type': 'message', 'rowid': 2 if case == 'outside-selection' else 1}
    footer = {'type': 'footer', 'messages': 2 if case == 'wrong-count' else 1, 'errors': 0}
    lines = [json.dumps(header), json.dumps(message)]
    if case != 'no-footer':
        lines.append(json.dumps(footer))
    if case == 'after-footer':
        lines.append(json.dumps(message))
    elif case == 'invalid-json':
        lines[1] = 'invalid json'
    elif case == 'not-object':
        lines[1] = '[]'
    binary = helper(tmp_path, '\n'.join(f'print({line!r})' for line in lines))
    with closing(decode(binary, tmp_path / 'private.sqlite', [1])) as rows, pytest.raises(IncompatibleSource):
        list(rows)
    assert [path.name for path in tmp_path.iterdir()] == ['fake-decoder']


def test_large_stderr_before_header_does_not_deadlock_the_pipe(tmp_path):
    binary = helper(tmp_path, "import sys\nsys.stderr.write('diagnostic' * 200000)\nsys.exit(65)")
    with closing(decode(binary, tmp_path / 'private.sqlite')) as rows, pytest.raises(IncompatibleSource, match='exited 65 without a header') as error:
        list(rows)
    assert len(str(error.value)) < 300


def test_batches_bound_children_and_emit_one_aggregate_stream(monkeypatch, tmp_path):
    from chatstore.adapters.messages import decoder

    calls = []
    closed = []

    def child(helper, database, rowids):
        calls.append(rowids)
        try:
            yield {'type': 'header', 'format': 'test', 'selection': {'requested': len(rowids)}}
            for rowid in rowids:
                yield {'type': 'message', 'rowid': rowid}
            yield {'type': 'footer', 'messages': len(rowids), 'errors': 0, 'query_vm_steps': 7}
        finally:
            closed.append(True)

    monkeypatch.setattr(decoder, 'decode', child)
    rows = list(decoder.decode_batches(tmp_path, tmp_path, range(20003), requested=20003))
    assert [len(batch) for batch in calls] == [10000, 10000, 3]
    assert rows[0]['selection'] == {'kind': 'rowids', 'requested': 20003}
    assert [row['rowid'] for row in rows[1:-1]] == list(range(20003))
    assert rows[-1] == {'type': 'footer', 'messages': 20003, 'errors': 0,
                        'query_fullscan_steps': 0, 'query_vm_steps': 21, 'batches': 3}
    assert len(closed) == 3


@pytest.mark.parametrize('failure', ['header-drift', 'child-failure', 'missing-ids', 'extra-ids'])
def test_later_batch_failure_cannot_emit_completion(monkeypatch, tmp_path, failure):
    from chatstore.adapters.messages import decoder

    calls = 0
    closed = []

    def child(helper, database, rowids):
        nonlocal calls
        calls += 1
        try:
            yield {'type': 'header', 'format': 'changed' if calls == 2 and failure == 'header-drift' else 'test'}
            if calls == 2 and failure == 'child-failure':
                raise IncompatibleSource('interrupted child')
            yield {'type': 'footer', 'messages': 0, 'errors': 0}
        finally:
            closed.append(True)

    monkeypatch.setattr(decoder, 'decode', child)
    requested = 10002 if failure == 'missing-ids' else 10000 if failure == 'extra-ids' else 10001
    output = []
    with closing(decoder.decode_batches(tmp_path, tmp_path, range(10001), requested=requested)) as stream, pytest.raises(IncompatibleSource):
        output.extend(stream)
    assert not any(row['type'] == 'footer' for row in output)
    assert len(closed) == calls
