import json

import pytest

from chatstore.archive import verify
from chatstore.archive.container import Payload
from chatstore.identity.urn import revision_digest
from tests.unit.test_cache import msg


def payload(events):
    return Payload(manifest={'kind': 'delta', 'counts': {'changes': len(events)},
                             'feed': {'after': 0, 'through': len(events)}},
                   members={'records/changes.jsonl': b'\n'.join(json.dumps(event).encode() for event in events)})


def record_event(rec, seq=2):
    return {'seq': seq, 'operation': 'record', 'data': {'record': rec, 'observed_at': 1}}


def revision_event(rec, seq=1):
    return {'seq': seq, 'operation': 'revision', 'data': {
        'entity_urn': rec['urn'], 'entity_kind': rec['entity'], 'revision_digest': rec['revision_digest'],
        'observed_at': 1, 'record': rec}}


def test_identical_revision_and_head_validate_schema_once_per_archive_parse(monkeypatch):
    rec = dict(msg(1, 'retained content', None, None, 1), observed_at=1, sync_run=None)
    archive = payload([revision_event(rec), record_event(dict(reversed(list(rec.items()))))])
    calls = []
    original = verify.validate_record
    def validate(value):
        calls.append(value['urn'])
        return original(value)
    monkeypatch.setattr(verify, 'validate_record', validate)
    for attempt in (1, 2):
        counts, problems, errors = verify.check_records(archive)
        assert not problems and not errors and counts['changes'] == 2
        assert len(calls) == attempt  # A new parse never trusts an earlier archive's memo.


@pytest.mark.parametrize('mutation', ['provenance', 'content', 'wrapper'])
def test_duplicate_claimed_digest_does_not_bypass_row_or_envelope_checks(mutation):
    rec = dict(msg(1, 'retained content', None, None, 1), observed_at=1, sync_run=None)
    bad = dict(rec)
    if mutation == 'provenance':
        bad['observed_at'] = 'invalid integer'
        assert revision_digest(bad) == rec['revision_digest']
        event = record_event(bad)
        expected = 'schema:'
    elif mutation == 'content':
        bad['text'] = 'changed without a new digest'
        event = record_event(bad)
        expected = 'revision_digest'
    else:
        event = revision_event(rec, seq=2)
        event['data']['entity_urn'] = 'wrong entity'
        expected = 'entity_urn'
    counts, problems, errors = verify.check_records(payload([revision_event(rec), event]))
    assert counts['changes'] == 1 and errors == 1
    assert any(expected in problem for problem in problems), problems


@pytest.mark.parametrize('ending', [b'\n', b'\r\n', b'\r'])
@pytest.mark.parametrize('trailing', [False, True])
def test_record_lines_preserve_blank_lines_and_error_locations(ending, trailing):
    rec = dict(msg(1, 'Unicode é🙂 and escaped\nnewline', None, None, 1), observed_at=1, sync_run=None)
    events = [revision_event(rec), record_event(rec)]
    archive = payload(events)
    name = 'records/changes.jsonl'
    lines = [json.dumps(events[0], ensure_ascii=False).encode(), b'  ',
             json.dumps(events[1], ensure_ascii=False).encode()]
    archive.members[name] = ending.join(lines) + (ending if trailing else b'')
    counts, problems, errors, parsed = verify._check_and_parse(archive, True)
    assert counts == {'changes': 2} and not problems and not errors
    assert parsed['changes'] == events
    archive.members[name] = ending.join([lines[0], b'  ', b'{invalid', lines[2]])
    counts, problems, errors = verify.check_records(archive)
    assert counts['changes'] == 1 and errors == 0
    assert f'{name}:3: invalid JSON' in problems
