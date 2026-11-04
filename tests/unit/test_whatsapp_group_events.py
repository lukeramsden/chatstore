import itertools
import json
from pathlib import Path

from chatstore.adapters.whatsapp import events

VECTORS = {v['name']: v['input']['native_key'] for v in
           json.loads((Path(__file__).parents[2] / 'specs/test-vectors/identity-v1.json').read_text())['vectors']}
GROUP = '120363000000000001@g.us'


def row(pk, members, group=GROUP, date=700000090.0, kind=5):
    return {'Z_PK': pk, 'ZGROUPJID': group, 'ZCHANGEDATE': date, 'ZCHANGETYPE': kind, 'ZMEMBERJIDS': members}


def keys(rows):
    return {r['Z_PK']: k for r, k, _ in events.keyed(rows)}


def test_keys_match_published_vectors():
    base = events.key(row(1, 'x'))
    assert base == VECTORS['whatsapp group change event']
    assert events.disambiguated(base, events.members(row(2, '111@LID;222@s.whatsapp.net'))) == \
        VECTORS['whatsapp group change event, colliding row disambiguated by members']


def test_non_colliding_rows_keep_the_base_key():
    rows = [row(1, 'a@lid'), row(2, 'b@lid', date=700000091.0), row(3, 'c@lid', kind=6), row(4, 'd@lid', group='other@g.us')]
    assert keys(rows) == {r['Z_PK']: events.key(r) for r in rows}


def test_collisions_are_distinct_order_independent_and_collapse_identical_rows():
    rows = [row(5, 'b@lid'), row(2, 'A@lid'), row(9, 'a@lid'), row(7, '')]
    base = events.key(rows[0])
    expected = {2: base, 9: base, 5: events.disambiguated(base, ['b@lid']), 7: events.disambiguated(base, [])}
    for order in itertools.permutations(rows):
        assert keys(order) == expected
    assert len(set(expected.values())) == 3
    assert all(k.startswith(base[:-1] + ',') for k in expected.values() if k != base)
