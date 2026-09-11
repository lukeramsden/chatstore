import hashlib
from pathlib import Path

import pytest

from chatstore import helper_install as H
from chatstore.paths import DataDir


def _release(tmp_path: Path, tag: str, body: bytes, *, corrupt_sum: bool = False) -> str:
    d = tmp_path / "download" / tag
    d.mkdir(parents=True)
    name = H.asset_name("Darwin", "arm64")
    (d / name).write_bytes(body)
    digest = hashlib.sha256(body).hexdigest()
    if corrupt_sum:
        digest = "0" * 64
    (d / H.SUMS_NAME).write_text(f"{digest}  {name}\n{'1' * 64}  other-file\n")
    return tmp_path.as_uri()


def test_asset_naming_and_tags():
    assert H.asset_name("Darwin", "arm64") == "chatstore-messages-decoder-macos-arm64"
    assert H.asset_name("Darwin", "x86_64") == "chatstore-messages-decoder-macos-x86_64"
    with pytest.raises(H.HelperInstallError):
        H.asset_name("Linux", "x86_64")
    assert H.release_tag("0.1.0") == "v0.1.0" and H.release_tag("v0.2.0") == "v0.2.0"
    assert H.parse_sums("abc  x\n" + "f" * 64 + " *helper\n") == {"helper": "f" * 64}


def test_install_verifies_and_places_binary(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(H.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(H.platform, "machine", lambda: "arm64")
    dd = DataDir(tmp_path / "data")
    dd.create()
    base = _release(tmp_path / "rel", "v9.9.9", b"#!/bin/sh\necho helper\n")
    inst = H.install(dd, tag="v9.9.9", base_url=base)
    assert inst.path == dd.root / "bin" / H.HELPER_NAME and inst.path.is_file()
    assert inst.path.stat().st_mode & 0o111
    assert inst.tag == "v9.9.9"


def test_install_fails_closed_on_bad_checksum(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(H.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(H.platform, "machine", lambda: "arm64")
    dd = DataDir(tmp_path / "data")
    dd.create()
    base = _release(tmp_path / "rel", "v9.9.9", b"evil", corrupt_sum=True)
    with pytest.raises(H.HelperInstallError, match="sha256 mismatch"):
        H.install(dd, tag="v9.9.9", base_url=base)
    assert not (dd.root / "bin" / H.HELPER_NAME).exists()
    with pytest.raises(H.HelperInstallError, match="HTTP|No such file|not found|Errno"):
        H.install(dd, tag="v0.0.1", base_url=base)
