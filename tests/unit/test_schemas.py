from pathlib import Path

import jsonschema

from chatstore.canonical import schemas as S

ROOT = Path(__file__).parents[2]


def test_checked_in_schemas_match_generator(tmp_path):
    S.write_all(tmp_path)
    for f in tmp_path.iterdir():
        assert (ROOT / "specs" / "schemas" / f.name).read_text() == f.read_text(), f.name


def test_schemas_are_valid_draft_2020():
    for schema in S.SCHEMAS.values():
        jsonschema.Draft202012Validator.check_schema(schema)


def test_every_entity_has_schema():
    assert set(S.ENTITY_ORDER) == set(S.SCHEMAS)
