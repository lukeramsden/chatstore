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
