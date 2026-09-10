"""Incremental vs full equivalence, interrupted export, DST date parsing, concurrent readers."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path
from unittest import mock

import pytest

from chatstore.cache import Cache
from chatstore.cli.app import main
from tests.fixtures.synthetic import build_whatsapp


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *argv])
    env = json.loads(capsys.readouterr().out)
    return code, env


def _view(dd: Path) -> set[tuple[str, str]]:
    c = Cache(dd / "cache.sqlite3", readonly=True)
    try:
        return set(c.conn.execute("SELECT urn, revision_digest FROM records WHERE kind!='sync_runs'").fetchall())
    finally:
        c.close()


def test_incremental_equals_full(tmp_path: Path, monkeypatch, capsys):
    wa = build_whatsapp(tmp_path / "wa")
    a, b = tmp_path / "a", tmp_path / "b"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(a))
    run(capsys, "init", "--whatsapp-path", str(wa), "--timezone", "UTC")
    assert run(capsys, "sync", "--source", "whatsapp")[0] == 0
    # source grows and an old message changes status
    c = sqlite3.connect(wa / "ChatStorage.sqlite")
    c.execute("INSERT INTO ZWAMESSAGE VALUES (60,1,1,0,0,0,0,0,0,60,0,0,1,NULL,NULL,NULL,700000600,NULL,'15550001111@s.whatsapp.net',NULL,'3EB0NEW00060','later message',NULL)")
    c.execute("UPDATE ZWAMESSAGE SET ZMESSAGESTATUS=8 WHERE Z_PK=1")
    c.commit()
    c.close()
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0 and e["data"][0]["mode"] == "incremental"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(b))
    run(capsys, "init", "--whatsapp-path", str(wa), "--timezone", "UTC")
    ident_a = json.loads((a / "identity.json").read_text())
    ident_b = json.loads((b / "identity.json").read_text())
    ident_b["scopes"] = ident_a["scopes"]
    (b / "identity.json").write_text(json.dumps(ident_b))
    assert run(capsys, "sync", "--source", "whatsapp")[0] == 0
    assert _view(a) == _view(b)


def test_interrupted_export_leaves_no_partial_archive(tmp_path: Path, monkeypatch, capsys):
    wa = build_whatsapp(tmp_path / "wa")
    dd = tmp_path / "d"
    pw = tmp_path / "pw"
    pw.write_text("interrupt-test-password-000\n")
    os.chmod(pw, 0o600)
    monkeypatch.setenv("CHATSTORE_PASSWORD_FILE", str(pw))
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(dd))
    run(capsys, "init", "--whatsapp-path", str(wa), "--timezone", "UTC")
    run(capsys, "sync", "--source", "whatsapp")
    out = tmp_path / "out"
    with mock.patch("chatstore.archive.container.write_encrypted_zip", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            from chatstore.archive import export as E
            cache = Cache(dd / "cache.sqlite3")
            from chatstore.config import load_config, load_identity
            from chatstore.paths import DataDir
            plan = E.collect_catalogue(cache)
            E.write_archive(cache, DataDir(dd), load_config(DataDir(dd)), load_identity(DataDir(dd)), plan, output=out,
                            password="x", media="text", force=False, adapter_versions={})
    assert not list(out.glob("*")) if out.exists() else True
    assert not list((dd / "staging").iterdir())
    cache.close()
    # ledger has no entry, so the next export is r0001
    code, e = run(capsys, "archive", "export", "-o", str(out), "--catalogue-only")
    assert code == 0 and e["data"]["archives"][0]["revision"] == 1


def test_dst_boundary_dates(tmp_path: Path, monkeypatch, capsys):
    wa = build_whatsapp(tmp_path / "wa")
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(tmp_path / "d"))
    run(capsys, "init", "--whatsapp-path", str(wa), "--timezone", "Europe/London")
    from chatstore.cli.app import Ctx, build_parser
    from chatstore.paths import DataDir
    ctx = Ctx(build_parser().parse_args(["status"]), DataDir(tmp_path / "d"))
    # 2026-03-29 is the spring-forward day in Europe/London: --until is exclusive and lands on the next local midnight
    start = ctx.parse_date("2026-03-29")
    end = ctx.parse_date("2026-03-29", end=True)
    assert end - start == 23 * 3600 * 1000  # a 23-hour day
    assert ctx.parse_date("2026-03-29T12:00:00+02:00") == ctx.parse_date("2026-03-29T10:00:00Z")
    assert ctx.parse_date("2026-10-25", end=True) - ctx.parse_date("2026-10-25") == 25 * 3600 * 1000


def test_concurrent_readers_during_write(tmp_path: Path, monkeypatch, capsys):
    wa = build_whatsapp(tmp_path / "wa", extra_messages=300)
    dd = tmp_path / "d"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(dd))
    run(capsys, "init", "--whatsapp-path", str(wa), "--timezone", "UTC")
    run(capsys, "sync", "--source", "whatsapp")
    errors: list[str] = []
    stop = threading.Event()

    def reader():
        c = Cache(dd / "cache.sqlite3", readonly=True)
        try:
            while not stop.is_set():
                c.conn.execute("SELECT count(*) FROM records").fetchone()
                c.conn.execute("SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'extra' LIMIT 5").fetchall()
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
        finally:
            c.close()

    threads = [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    try:
        for _ in range(3):
            assert run(capsys, "sync", "--source", "whatsapp", "--mode", "full")[0] == 0
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert errors == []
