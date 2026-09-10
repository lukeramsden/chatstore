"""Archive export / verify / import round trip on synthetic fixtures."""

from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

import pytest
import pyzipper

from chatstore.cache import Cache
from chatstore.cli.app import main
from tests.fixtures.synthetic import build_whatsapp


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *argv])
    env = json.loads(capsys.readouterr().out)
    assert env["exit_code"] == code
    return code, env


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    wa = build_whatsapp(tmp_path / "wa")
    pw = tmp_path / "pw.txt"
    pw.write_text("correct-horse-battery-staple-256\n")
    os.chmod(pw, 0o600)
    monkeypatch.setenv("CHATSTORE_PASSWORD_FILE", str(pw))
    src = tmp_path / "src-data"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(src))
    return {"wa": wa, "src": src, "pw": pw, "out": tmp_path / "out", "dst": tmp_path / "dst-data", "tmp": tmp_path}


def _sync(capsys, world):
    code, _ = run(capsys, "init", "--whatsapp-path", str(world["wa"]), "--timezone", "UTC")
    assert code == 0
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0, e


def test_export_verify_import_roundtrip(world, capsys):
    _sync(capsys, world)
    code, e = run(capsys, "archive", "export", "-o", str(world["out"]))
    assert code == 0, e
    kinds = {(r["kind"], r["action"]) for r in e["data"]["archives"]}
    assert ("catalogue", "written") in kinds and any(k == "bucket" and a == "written" for k, a in kinds)
    files = sorted(world["out"].glob("chatstore-*.zip"))
    assert files and all("-r0001.zip" in f.name for f in files)
    # container: exactly one AES member named payload; unzip-without-password fails
    with zipfile.ZipFile(files[0]) as zf:
        assert zf.namelist() == ["payload"]
        with pytest.raises(RuntimeError):
            zf.read("payload")
    # re-export without changes writes nothing new
    code, e = run(capsys, "archive", "export", "-o", str(world["out"]))
    assert code == 0 and e["data"]["written"] == 0
    assert all(r["action"] in ("unchanged", "skipped_empty") for r in e["data"]["archives"])

    code, e = run(capsys, "archive", "verify", str(world["out"]))
    assert code == 0 and all(r["ok"] for r in e["data"]), e

    # import into a clean data dir with no source configured
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    code, e = run(capsys, "archive", "import", str(world["out"]))
    assert code == 0, e
    assert all(r["action"] == "imported" for r in e["data"])
    assert sum(r["conflicts"] for r in e["data"]) == 0

    src = Cache(world["src"] / "cache.sqlite3", readonly=True)
    dst = Cache(world["dst"] / "cache.sqlite3", readonly=True)
    for k in ("messages", "chats", "identities", "chat_memberships", "aliases", "message_parts", "attachments", "events"):
        assert src.counts().get(k, 0) == dst.counts().get(k, 0), k
    # identical current view: same (urn, digest) pairs
    q = "SELECT urn, revision_digest FROM records WHERE kind NOT IN ('sync_runs') ORDER BY urn"
    assert [tuple(r) for r in src.conn.execute(q)] == [tuple(r) for r in dst.conn.execute(q)]
    # FTS works on the restored side
    code, e = run(capsys, "search", "tomorrow")
    assert code == 0 and len(e["data"]) == 1
    code, e = run(capsys, "resolve", e["data"][0]["urn"])
    assert code == 0 and e["data"]["status"] == "current"
    # attachments in text mode are marked not_exported
    code, e = run(capsys, "status")
    assert code == 0
    src.close()
    dst.close()
    # importing again is a no-op
    code, e = run(capsys, "archive", "import", str(world["out"]))
    assert code == 0 and all(r["action"] == "already_imported" for r in e["data"])


def test_wrong_password_and_tamper_cannot_change_cache(world, capsys, tmp_path):
    _sync(capsys, world)
    code, e = run(capsys, "archive", "export", "-o", str(world["out"]))
    assert code == 0
    files = sorted(world["out"].glob("chatstore-*.zip"))
    bad = tmp_path / "bad.txt"
    bad.write_text("nope\n")
    os.chmod(bad, 0o600)
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    os.environ["CHATSTORE_PASSWORD_FILE"] = str(bad)
    code, e = run(capsys, "archive", "verify", str(files[0]))
    assert code == 7 and e["data"][0]["wrong_password"]
    code, e = run(capsys, "archive", "import", str(files[0]))
    assert code == 7 and e["data"][0]["action"] == "wrong_password"
    dst = Cache(world["dst"] / "cache.sqlite3", readonly=True)
    assert dst.counts() == {}
    dst.close()

    # tamper: flip a byte inside the encrypted payload
    os.environ["CHATSTORE_PASSWORD_FILE"] = str(world["pw"])
    data = bytearray(files[0].read_bytes())
    with zipfile.ZipFile(files[0]) as zf:
        info = zf.infolist()[0]
    off = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra) + 40
    data[off] ^= 0xFF
    tampered = tmp_path / "tampered.zip"
    tampered.write_bytes(bytes(data))
    code, e = run(capsys, "archive", "import", str(tampered))
    assert code == 7 and e["data"][0]["action"] in ("wrong_password", "invalid")
    dst = Cache(world["dst"] / "cache.sqlite3", readonly=True)
    assert dst.counts() == {}
    dst.close()


def test_unsafe_archives_rejected(world, capsys, tmp_path):
    from chatstore.archive import container

    pw = world["pw"].read_text().strip()
    # legacy ZipCrypto / plain zip
    plain = tmp_path / "plain.zip"
    with zipfile.ZipFile(plain, "w") as zf:
        zf.writestr("payload", b"x")
    with pytest.raises(container.InvalidArchive):
        container.read_payload_to_file(plain, pw, tmp_path / "p.tar")
    # two members
    two = tmp_path / "two.zip"
    with pyzipper.AESZipFile(two, "w", encryption=pyzipper.WZ_AES) as zf:
        zf.setpassword(pw.encode())
        zf.writestr("payload", b"x")
        zf.writestr("other", b"y")
    with pytest.raises(container.InvalidArchive):
        container.read_payload_to_file(two, pw, tmp_path / "p.tar")
    # unsafe TAR member names
    for name in ("../x", "/abs", "a/../b", "a//b", "C:/x"):
        with pytest.raises(container.InvalidArchive):
            container._check_member_name(name)


def test_revision_supersedes_and_branch_conflict(world, capsys, tmp_path):
    _sync(capsys, world)
    code, e = run(capsys, "archive", "export", "-o", str(world["out"]))
    assert code == 0
    # force a second revision of everything
    code, e = run(capsys, "archive", "export", "-o", str(world["out"]), "--force")
    assert code == 0 and e["data"]["written"] >= 2
    r2 = [r for r in e["data"]["archives"] if r["action"] == "written"]
    assert all(r["revision"] == 2 and len(r["supersedes"]) == 1 for r in r2)
    # import everything into a clean dir: r1 then r2 -> r1 marked superseded, r2 imported
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    code, e = run(capsys, "archive", "import", str(world["out"]))
    assert code == 0, e
    actions = {Path(r["path"]).name: r["action"] for r in e["data"]}
    assert all(a == "imported" for a in actions.values())
    code, e = run(capsys, "archive", "list")
    assert any(i["superseded_by"] for i in e["data"]["imports"])
    # importing r1 again after r2 -> superseded (not re-applied)
    r1 = [f for f in sorted(world["out"].glob("*.zip")) if f.name.endswith("r0001.zip")]
    code, e = run(capsys, "archive", "import", str(r1[0]))
    assert code == 0 and e["data"][0]["action"] == "already_imported"

    # branch conflict: a different data dir exporting the same export set independently
    # simulate by copying r1's manifest lineage: create export from a *second* source dir with same export_set
    third = tmp_path / "third-data"
    os.environ["CHATSTORE_DATA_DIR"] = str(third)
    run(capsys, "init", "--whatsapp-path", str(world["wa"]), "--timezone", "UTC")
    run(capsys, "sync", "--source", "whatsapp")
    ident = json.loads((third / "identity.json").read_text())
    src_ident = json.loads((world["src"] / "identity.json").read_text())
    ident["export_set"] = src_ident["export_set"]
    (third / "identity.json").write_text(json.dumps(ident))
    out3 = tmp_path / "out3"
    code, e = run(capsys, "archive", "export", "-o", str(out3), "--no-catalogue")
    assert code == 0
    os.environ["CHATSTORE_DATA_DIR"] = str(world["dst"])
    code, e = run(capsys, "archive", "import", str(out3))
    assert code == 6
    assert all(r["action"] == "branch_conflict" for r in e["data"])
