"""Fault injection: interrupted sync and import, decompression bombs, oversized TAR declarations."""

from __future__ import annotations

import io
import json
import os
import tarfile
from pathlib import Path
from unittest import mock

import pytest
import pyzipper

from chatstore import sync as S
from chatstore.archive import container
from chatstore.cache import Cache
from chatstore.cli.app import main
from tests.fixtures.synthetic import build_whatsapp


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *argv])
    return code, json.loads(capsys.readouterr().out)


def _view(dd: Path) -> set[tuple[str, str]]:
    c = Cache(dd / "cache.sqlite3", readonly=True)
    try:
        return set(c.conn.execute("SELECT urn, revision_digest FROM records WHERE kind!='sync_runs'").fetchall())
    finally:
        c.close()


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    wa = build_whatsapp(tmp_path / "wa", extra_messages=400)
    pw = tmp_path / "pw"
    pw.write_text("fault-test-password-0123456789\n")
    os.chmod(pw, 0o600)
    monkeypatch.setenv("CHATSTORE_PASSWORD_FILE", str(pw))
    dd = tmp_path / "d"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(dd))
    return {"wa": wa, "dd": dd, "pw": pw.read_text().strip(), "tmp": tmp_path}


def test_interrupted_sync_resumes_to_same_view(world, capsys, monkeypatch):
    run(capsys, "init", "--whatsapp-path", str(world["wa"]), "--timezone", "UTC")
    monkeypatch.setattr(S, "BATCH", 50)
    real = Cache.upsert_many
    calls = {"n": 0}

    def flaky(self, recs, **kw):
        calls["n"] += 1
        if calls["n"] == 4:
            raise OSError("disk went away")
        return real(self, recs, **kw)

    with mock.patch.object(Cache, "upsert_many", flaky):
        code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 1 and e["errors"][0]["code"] == "internal" and "disk went away" in e["errors"][0]["message"]
    dd = world["dd"]
    partial_view = _view(dd)
    assert 0 < len(partial_view)  # earlier batches committed
    scope = json.loads((dd / "identity.json").read_text())["scopes"]["whatsapp"]
    c = Cache(dd / "cache.sqlite3", readonly=True)
    assert c.get_checkpoint("whatsapp", scope["scope"], "adapter") is None  # no checkpoint until the run finishes
    c.close()
    # re-run completes and matches a clean sync
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0 and e["data"][0]["mode"] == "initial"
    resumed = _view(dd)
    clean = world["tmp"] / "clean"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(clean))
    run(capsys, "init", "--whatsapp-path", str(world["wa"]), "--timezone", "UTC")
    ident_c = json.loads((clean / "identity.json").read_text())
    ident_c["scopes"] = {"whatsapp": scope, **{k: v for k, v in ident_c["scopes"].items() if k != "whatsapp"}}
    (clean / "identity.json").write_text(json.dumps(ident_c))
    run(capsys, "sync", "--source", "whatsapp")
    assert _view(clean) == resumed


def test_interrupted_import_leaves_cache_untouched(world, capsys):
    run(capsys, "init", "--whatsapp-path", str(world["wa"]), "--timezone", "UTC")
    run(capsys, "sync", "--source", "whatsapp")
    out = world["tmp"] / "out"
    code, e = run(capsys, "archive", "export", "-o", str(out))
    assert code == 0
    dst = world["tmp"] / "dst"
    os.environ["CHATSTORE_DATA_DIR"] = str(dst)
    real = Cache._index
    calls = {"n": 0}

    def flaky(self, kind, r, **kw):
        calls["n"] += 1
        if calls["n"] == 200:
            raise OSError("disk went away")
        return real(self, kind, r, **kw)

    with mock.patch.object(Cache, "_index", flaky):
        code, e = run(capsys, "archive", "import", str(out))
    assert code == 1 and "disk went away" in e["errors"][0]["message"]
    c = Cache(dst / "cache.sqlite3", readonly=True)
    n_records = c.conn.execute("SELECT count(*) FROM records").fetchone()[0]
    n_imports = c.conn.execute("SELECT count(*) FROM imports").fetchone()[0]
    c.close()
    # the archive being imported when the fault hit rolled back entirely; earlier archives (if any) are whole
    assert n_records == 0 or n_imports >= 1
    code, e = run(capsys, "archive", "list")
    assert n_imports == len(e["data"]["imports"])
    assert not list((dst / "staging").iterdir())
    # completing the import afterwards yields the source view
    code, e = run(capsys, "archive", "import", str(out))
    assert code == 0
    assert _view(dst) == _view(world["dd"])


def test_decompression_bomb_rejected(world):
    pw = world["pw"]
    bomb = world["tmp"] / "bomb.zip"
    with pyzipper.AESZipFile(bomb, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zf:
        zf.setpassword(pw.encode())
        zf.writestr("payload", b"\0" * (8 * 1024 * 1024))  # ratio ~ 1000:1
    with pytest.raises(container.InvalidArchive, match="ratio"):
        container.read_payload_to_file(bomb, pw, world["tmp"] / "p.tar")
    # declared size over the total limit is refused before inflating anything
    with pytest.raises(container.InvalidArchive, match="declared size"):
        container.read_payload_to_file(bomb, pw, world["tmp"] / "p.tar", limits={"max_ratio": 10**9, "max_total_bytes": 1024})
    # forged: zip headers claim a small size but the stream inflates past the limit
    data = bytearray(bomb.read_bytes())
    with pyzipper.AESZipFile(bomb) as zf:
        local_header = zf.infolist()[0].header_offset
    cd = data.rfind(b"PK\x01\x02")
    assert cd > 0
    small = (1024).to_bytes(4, "little")
    data[cd + 24:cd + 28] = small  # central directory: uncompressed size
    data[local_header + 22:local_header + 26] = small  # local header: uncompressed size
    forged = world["tmp"] / "forged.zip"
    forged.write_bytes(bytes(data))
    # either rejected outright, or inflation is bounded by the declared size (then the TAR is garbage and fails later)
    try:
        container.read_payload_to_file(forged, pw, world["tmp"] / "p.tar", limits={"max_ratio": 10**9, "max_total_bytes": 4096})
    except container.InvalidArchive:
        assert not (world["tmp"] / "p.tar").exists()
    else:
        assert (world["tmp"] / "p.tar").stat().st_size <= 1024
        with pytest.raises(container.InvalidArchive):
            container.open_payload(world["tmp"] / "p.tar", "0" * 64)


def test_tar_member_declaring_huge_size_rejected(world):
    tar_path = world["tmp"] / "big.tar"
    with tarfile.open(tar_path, "w") as tar:
        ti = tarfile.TarInfo("records/messages.jsonl")
        ti.size = 4
        tar.addfile(ti, io.BytesIO(b"abcd"))
    raw = bytearray(tar_path.read_bytes())
    # rewrite the size field (octal, offset 124..136) to 100 GiB and fix the header checksum
    raw[124:136] = b"%011o\0" % (100 * 1024**3)
    hdr = raw[:512]
    hdr[148:156] = b"        "
    raw[148:156] = b"%06o\0 " % sum(hdr)
    tar_path.write_bytes(bytes(raw))
    with pytest.raises(container.InvalidArchive, match="declared size"):
        container.open_payload(tar_path, "0" * 64)
