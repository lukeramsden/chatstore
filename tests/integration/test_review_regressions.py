"""Regression tests from the design review: revision identity, import recovery, validation,
empty-bucket retirement, reconciliation coverage, date boundaries, media state."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from unittest import mock

import pytest

from chatstore.archive import container
from chatstore.cache import Cache
from chatstore.cli.app import main
from tests.fixtures.synthetic import build_whatsapp


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *map(str, argv)])
    return code, json.loads(capsys.readouterr().out)


def _view(dd: Path, kind: str = "messages") -> set[tuple[str, str]]:
    c = Cache(dd / "cache.sqlite3", readonly=True)
    try:
        return {(r["urn"], r["revision_digest"]) for r in c.iter_records(kind)}
    finally:
        c.close()


def _set_text(wa: Path, pk: int, text: str) -> None:
    c = sqlite3.connect(wa / "ChatStorage.sqlite")
    c.execute("UPDATE ZWAMESSAGE SET ZTEXT=? WHERE Z_PK=?", (text, pk))
    c.commit()
    c.close()


@pytest.fixture
def world(tmp_path: Path, monkeypatch, capsys):
    wa = build_whatsapp(tmp_path / "wa")
    pw = tmp_path / "pw"
    pw.write_text("review-regression-password\n")
    os.chmod(pw, 0o600)
    monkeypatch.setenv("CHATSTORE_PASSWORD_FILE", str(pw))
    src = tmp_path / "src"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(src))
    assert run(capsys, "init", "--whatsapp-path", wa, "--timezone", "UTC")[0] == 0
    assert run(capsys, "sync", "--source", "whatsapp")[0] == 0
    return {"wa": wa, "src": src, "out": tmp_path / "archives", "dst": tmp_path / "dst", "tmp": tmp_path}


def test_revert_to_earlier_content_restores_correctly(world, capsys):
    """A -> B -> A: the second A must become the head again on restore."""
    for text in ("synthetic changed content", "hello from alice"):
        _set_text(world["wa"], 1, text)
        assert run(capsys, "sync", "--source", "whatsapp", "--mode", "full")[0] == 0
        assert run(capsys, "archive", "export", "-o", world["out"])[0] == 0
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    code, e = run(capsys, "archive", "import", world["out"])
    assert code == 0, e
    assert _view(world["dst"]) == _view(world["src"])
    code, e = run(capsys, "search", "hello from alice")
    assert len(e["data"]) == 1 and e["data"][0]["revision_status"] == "current"


def test_interrupted_blob_restore_recovers_on_retry(world, capsys):
    assert run(capsys, "archive", "export", "-o", world["out"], "--media", "available-media")[0] == 0
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    with mock.patch.object(container, "extract_blob", side_effect=OSError("disk went away")):
        code, e = run(capsys, "archive", "import", world["out"])
    assert code == 1
    code, e = run(capsys, "archive", "import", world["out"])
    assert code == 0
    assert sum(p.is_file() for p in (world["dst"] / "blobs").rglob("*")) == 1
    code, e = run(capsys, "media", "status")
    assert e["data"]["restored_blob_files"] == 1


def test_no_media_import_then_media_import_restores_blobs(world, capsys):
    assert run(capsys, "archive", "export", "-o", world["out"], "--media", "available-media")[0] == 0
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    assert run(capsys, "archive", "import", world["out"], "--no-media")[0] == 0
    assert sum(p.is_file() for p in (world["dst"] / "blobs").rglob("*")) == 0
    code, e = run(capsys, "archive", "import", world["out"])
    assert code == 0 and sum(r["blobs_restored"] for r in e["data"]) == 1


def test_validation_rejects_stale_revision_digest(world, capsys):
    from chatstore.archive.verify import check_records
    c = Cache(world["src"] / "cache.sqlite3", readonly=True)
    msg = next(c.iter_records("messages"))
    c.close()
    bad = dict(msg)
    bad["text"] = "changed without changing digest"
    payload = container.Payload(manifest={"counts": {"messages": 1}}, members={"records/messages.jsonl": json.dumps(bad).encode()},
                                blob_members=[], payload_sha256="", blob_digests={})
    _counts, problems, _n = check_records(payload)
    assert any("digest" in p for p in problems), problems
    # a manifest count for an entity that is absent from the payload is also a problem
    payload = container.Payload(manifest={"counts": {"messages": 1, "events": 3}}, members={"records/messages.jsonl": json.dumps(msg).encode()},
                                blob_members=[], payload_sha256="", blob_digests={})
    _counts, problems, _n = check_records(payload)
    assert any("events" in p for p in problems), problems


def test_emptied_bucket_exports_superseding_revision(world, capsys):
    assert run(capsys, "archive", "export", "-o", world["out"])[0] == 0
    assert run(capsys, "purge", "--source", "whatsapp", "--confirm")[0] == 0
    code, e = run(capsys, "archive", "export", "-o", world["out"])
    assert code == 0
    written = {r["kind"]: r for r in e["data"]["archives"] if r["action"] == "written"}
    assert "bucket" in written and written["bucket"]["revision"] == 2 and written["bucket"]["counts"].get("messages", 0) == 0
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    code, e = run(capsys, "archive", "import", world["out"])
    assert code == 0
    code, e = run(capsys, "search", "hello")
    assert e["data"] == []  # retired records are tombstoned, not current


def test_full_sync_reconciles_emptied_table(world, capsys):
    c = sqlite3.connect(world["wa"] / "ChatStorage.sqlite")
    c.execute("DELETE FROM ZWAMESSAGE")
    c.commit()
    c.close()
    code, _e = run(capsys, "sync", "--source", "whatsapp", "--mode", "full")
    assert code == 0
    cache = Cache(world["src"] / "cache.sqlite3", readonly=True)
    present = cache.conn.execute("SELECT count(*) FROM source_observations WHERE native_table='ZWAMESSAGE' AND present='present'").fetchone()[0]
    cache.close()
    assert present == 0


def test_until_date_is_exclusive(world, capsys):
    from chatstore.cli.app import Ctx, build_parser
    from chatstore.paths import DataDir
    ctx = Ctx(build_parser().parse_args(["status"]), DataDir(world["src"]))
    assert ctx.parse_date("2026-04-01") == ctx.parse_date("2026-04-01T00:00:00Z")
    _code, e = run(capsys, "search", "alice", "--until", "2023-03-08")  # fixture messages are on 2023-03-08 UTC
    assert e["data"] == []
    _code, e = run(capsys, "search", "alice", "--until", "2023-03-09")
    assert len(e["data"]) >= 1
    # 2026-03-29 is spring-forward in Europe/London: the day is 23 hours long
    ctx.cfg.timezone = "Europe/London"
    assert ctx.parse_date("2026-03-30") - ctx.parse_date("2026-03-29") == 23 * 3600 * 1000


def test_media_status_separates_source_and_local_state(world, capsys):
    assert run(capsys, "archive", "export", "-o", world["out"])[0] == 0  # text only
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    assert run(capsys, "archive", "import", world["out"])[0] == 0
    _code, e = run(capsys, "media", "status")
    rows = e["data"]["summary"]
    assert rows and all(r["availability"] == "available" and r["local_state"] == "absent" for r in rows)
    _code, e = run(capsys, "media", "list", "--local-state", "absent")
    assert len(e["data"]) == 1 and e["data"][0]["local_state"] == "absent"
