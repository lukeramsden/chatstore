"""Encrypted container: WinZip AES-256 ZIP with a single `payload` member holding a TAR.

Reading is fail-closed: the whole payload is decrypted and authenticated (pyzipper raises on
a bad tag only at EOF), size limits are checked before extraction, TAR members are validated,
then checksums.json and the manifest's payload_digest are verified. Only then is anything
returned to the caller.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import posixpath
import tarfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyzipper

ARCHIVE_FORMAT = "chatstore-archive-v1"
PAYLOAD_NAME = "payload"
WZ_AES_ID = 0x9901

DEFAULT_LIMITS = {"max_member_bytes": 64 * 1024 * 1024, "max_total_bytes": 1024 * 1024 * 1024, "max_ratio": 200}
# Blobs may exceed max_member_bytes (documents/videos); they are bounded by max_total_bytes.
MAX_BLOB_BYTES = 512 * 1024 * 1024


class InvalidArchive(Exception):
    """Authentication failure, tampering, checksum mismatch, unsafe member or limit exceeded."""


class WrongPassword(InvalidArchive):
    pass


@dataclass
class Payload:
    manifest: dict[str, Any]
    members: dict[str, bytes] = field(default_factory=dict)  # name -> bytes (excluding blobs when streamed)
    blob_members: list[str] = field(default_factory=list)
    payload_sha256: str = ""
    blob_digests: dict[str, str] = field(default_factory=dict)


# ---- TAR building ---------------------------------------------------------------------------------

class TarBuilder:
    """Builds an uncompressed TAR in a file, recording sha256 of every member."""

    def __init__(self, path: Path):
        self.path = path
        self._fh = open(path, "wb")  # noqa: SIM115 - closed in finish()
        os.chmod(path, 0o600)
        self.tar = tarfile.open(fileobj=self._fh, mode="w", format=tarfile.PAX_FORMAT)  # noqa: SIM115 - closed in finish()
        self.checksums: dict[str, str] = {}
        self.members: list[str] = []

    def add_bytes(self, name: str, data: bytes) -> None:
        _check_member_name(name)
        info = tarfile.TarInfo(name)
        info.size = len(data)
        info.mode = 0o600
        info.mtime = 0
        self.tar.addfile(info, io.BytesIO(data))
        self.checksums[name] = "sha256:" + hashlib.sha256(data).hexdigest()
        self.members.append(name)

    def add_file(self, name: str, src: Path, expected_sha256: str | None = None, expected_size: int | None = None) -> str:
        """Stream a file in from an open descriptor; returns its sha256. Mismatch -> ValueError."""
        _check_member_name(name)
        with open(src, "rb") as f:
            st = os.fstat(f.fileno())
            if expected_size is not None and st.st_size != expected_size:
                raise ValueError(f"size mismatch for {name}")
            info = tarfile.TarInfo(name)
            info.size = st.st_size
            info.mode = 0o600
            info.mtime = 0
            h = hashlib.sha256()

            class HashingReader:
                def read(self_inner, n: int = -1) -> bytes:
                    b = f.read(n)
                    h.update(b)
                    return b

            self.tar.addfile(info, HashingReader())  # type: ignore[arg-type]
        digest = h.hexdigest()
        if expected_sha256 and digest != expected_sha256:
            raise ValueError(f"hash mismatch for {name}")
        self.checksums[name] = "sha256:" + digest
        self.members.append(name)
        return digest

    def finish(self) -> str:
        self.tar.close()
        self._fh.close()
        h = hashlib.sha256()
        with open(self.path, "rb") as f:
            while chunk := f.read(1 << 20):
                h.update(chunk)
        return h.hexdigest()


def _check_member_name(name: str) -> None:
    if not name or name.startswith(("/", "\\")) or "\\" in name:
        raise InvalidArchive(f"unsafe member name {name!r}")
    norm = posixpath.normpath(name)
    if norm != name or any(p in ("..", ".", "") for p in name.split("/")):
        raise InvalidArchive(f"unsafe member name {name!r}")
    if ":" in name.split("/")[0] and len(name.split("/")[0]) == 2:  # Windows drive letter
        raise InvalidArchive(f"unsafe member name {name!r}")


# ---- ZIP write / read -----------------------------------------------------------------------------

def write_encrypted_zip(dst: Path, tar_path: Path, password: str) -> None:
    tmp = dst.with_name(".tmp-" + dst.name)
    if tmp.exists():
        tmp.unlink()
    with pyzipper.AESZipFile(tmp, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zf:
        zf.setpassword(password.encode("utf-8"))
        zf.setencryption(pyzipper.WZ_AES, nbits=256)
        with open(tar_path, "rb") as src, zf.open(PAYLOAD_NAME, "w") as out:
            while chunk := src.read(1 << 20):
                out.write(chunk)
    os.chmod(tmp, 0o600)
    os.replace(tmp, dst)


def _zip_info_checks(zf: pyzipper.AESZipFile, limits: dict[str, int]) -> zipfile.ZipInfo:
    infos = zf.infolist()
    if len(infos) != 1 or infos[0].filename != PAYLOAD_NAME:
        raise InvalidArchive("archive must contain exactly one member named 'payload'")
    info = infos[0]
    if info.is_dir():
        raise InvalidArchive("payload is a directory")
    if not info.flag_bits & 0x1:
        raise InvalidArchive("payload is not encrypted")
    # WinZip AES marks the entry with compress_type 99 (AE-x) — pyzipper maps it back; check the extra field.
    if not _has_aes_extra(info):
        raise InvalidArchive("payload is not WinZip AES encrypted (legacy ZipCrypto is rejected)")
    if info.file_size > limits["max_total_bytes"]:
        raise InvalidArchive(f"payload declared size {info.file_size} exceeds limit {limits['max_total_bytes']}")
    if info.compress_size and info.file_size / max(1, info.compress_size) > limits["max_ratio"]:
        raise InvalidArchive("payload compression ratio exceeds limit")
    return info


def _has_aes_extra(info: zipfile.ZipInfo) -> bool:
    extra = info.extra or b""
    i = 0
    while i + 4 <= len(extra):
        tag = int.from_bytes(extra[i:i + 2], "little")
        ln = int.from_bytes(extra[i + 2:i + 4], "little")
        if tag == WZ_AES_ID:
            # strength byte at offset 4+4: 0x03 == 256-bit
            body = extra[i + 4:i + 4 + ln]
            return len(body) >= 7 and body[4] == 0x03
        i += 4 + ln
    return getattr(info, "compress_type", None) == 99


def read_payload_to_file(zip_path: Path, password: str, dst_tar: Path, limits: dict[str, int] | None = None) -> str:
    """Decrypt + authenticate the payload into dst_tar (0600). Returns plaintext sha256.

    Raises WrongPassword / InvalidArchive. dst_tar is removed on failure.
    """
    limits = {**DEFAULT_LIMITS, **(limits or {})}
    h = hashlib.sha256()
    total = 0
    try:
        with pyzipper.AESZipFile(zip_path) as zf:
            info = _zip_info_checks(zf, limits)
            zf.setpassword(password.encode("utf-8"))
            with zf.open(info) as src, open(dst_tar, "wb") as out:
                os.chmod(dst_tar, 0o600)
                while True:
                    chunk = src.read(1 << 20)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > limits["max_total_bytes"]:
                        raise InvalidArchive("payload exceeds size limit while inflating")
                    h.update(chunk)
                    out.write(chunk)
    except RuntimeError as e:
        _cleanup(dst_tar)
        if "password" in str(e).lower():
            raise WrongPassword("wrong password") from None
        raise InvalidArchive(f"cannot read archive: {type(e).__name__}") from None
    except zipfile.BadZipFile as e:
        _cleanup(dst_tar)
        msg = str(e).lower()
        if "authentication" in msg or "bad" in msg and "password" in msg:
            raise WrongPassword("wrong password or tampered payload (authentication failed)") from None
        raise InvalidArchive(f"invalid archive: {e}") from None
    except InvalidArchive:
        _cleanup(dst_tar)
        raise
    except Exception as e:  # noqa: BLE001 - fail closed: zlib errors, EOF, anything unexpected is "invalid"
        _cleanup(dst_tar)
        raise InvalidArchive(f"cannot read archive (corrupt or tampered payload): {type(e).__name__}") from None
    return h.hexdigest()


def _cleanup(p: Path) -> None:
    try:
        p.unlink()
    except OSError:
        pass


# ---- TAR validation -------------------------------------------------------------------------------

def open_payload(tar_path: Path, payload_sha256: str, *, load_blobs: bool = False,
                 limits: dict[str, int] | None = None) -> Payload:
    """Validate the decrypted TAR: member safety, sizes, checksums, manifest digest."""
    lim = {**DEFAULT_LIMITS, **(limits or {})}
    members: dict[str, bytes] = {}
    blob_names: list[str] = []
    blob_digests: dict[str, str] = {}
    seen: set[str] = set()
    total = 0
    try:
        with tarfile.open(tar_path, mode="r:") as tar:
            for info in tar:
                name = info.name
                _check_member_name(name)
                if not info.isreg():
                    raise InvalidArchive(f"non-regular TAR member {name!r}")
                if name in seen:
                    raise InvalidArchive(f"duplicate TAR member {name!r}")
                seen.add(name)
                is_blob = name.startswith("blobs/")
                cap = MAX_BLOB_BYTES if is_blob else lim["max_member_bytes"]
                if info.size > cap:
                    raise InvalidArchive(f"member {name!r} declared size {info.size} exceeds limit")
                total += info.size
                if total > lim["max_total_bytes"]:
                    raise InvalidArchive("payload total size exceeds limit")
                f = tar.extractfile(info)
                if f is None:
                    raise InvalidArchive(f"cannot read TAR member {name!r}")
                if is_blob and not load_blobs:
                    blob_names.append(name)
                    # still hash to validate checksums
                    h = hashlib.sha256()
                    while chunk := f.read(1 << 20):
                        h.update(chunk)
                    members[name] = b""
                    blob_digests[name] = h.hexdigest()
                    continue
                members[name] = f.read()
                if is_blob:
                    blob_names.append(name)
    except tarfile.TarError as e:
        raise InvalidArchive(f"invalid payload TAR: {e}") from None
    if "manifest.json" not in members or "checksums.json" not in members:
        raise InvalidArchive("payload is missing manifest.json or checksums.json")
    try:
        manifest = json.loads(members["manifest.json"])
        checksums = json.loads(members["checksums.json"])
    except (ValueError, UnicodeDecodeError) as e:
        raise InvalidArchive(f"manifest/checksums not valid JSON: {e}") from None
    if manifest.get("archive_format") != ARCHIVE_FORMAT:
        raise InvalidArchive(f"unsupported archive format {manifest.get('archive_format')!r}")
    if manifest.get("payload_digest") != "sha256:" + members_digest(checksums):
        raise InvalidArchive("payload digest does not match manifest")
    expected = set(checksums)
    actual = set(members) - {"checksums.json", "manifest.json"}
    if expected != actual:
        raise InvalidArchive("checksums.json does not list exactly the payload members")
    for name, want in checksums.items():
        if name in blob_digests:
            got = "sha256:" + blob_digests[name]
        else:
            got = "sha256:" + hashlib.sha256(members[name]).hexdigest()
        if got != want:
            raise InvalidArchive(f"checksum mismatch for {name!r}")
    if set(manifest.get("members", [])) != actual:
        raise InvalidArchive("manifest members list does not match payload")
    return Payload(manifest=manifest, members=members, blob_members=blob_names, payload_sha256=payload_sha256,
                   blob_digests=blob_digests)


def members_digest(checksums: dict[str, str]) -> str:
    """SHA-256 over sorted 'name\\0checksum\\n' lines: binds the manifest to every other member."""
    h = hashlib.sha256()
    for name in sorted(checksums):
        h.update(name.encode())
        h.update(b"\0")
        h.update(checksums[name].encode())
        h.update(b"\n")
    return h.hexdigest()


def extract_blob(tar_path: Path, name: str, dst: Path, expected_sha256: str) -> None:
    """Copy one blob member out of a validated TAR, verifying its hash while streaming."""
    _check_member_name(name)
    with tarfile.open(tar_path, mode="r:") as tar:
        info = tar.getmember(name)
        if not info.isreg():
            raise InvalidArchive("blob member is not a regular file")
        f = tar.extractfile(info)
        if f is None:
            raise InvalidArchive("cannot read blob member")
        h = hashlib.sha256()
        tmp = dst.with_name(dst.name + ".part")
        dst.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "wb") as out:
            os.chmod(tmp, 0o600)
            while chunk := f.read(1 << 20):
                h.update(chunk)
                out.write(chunk)
        if h.hexdigest() != expected_sha256:
            tmp.unlink()
            raise InvalidArchive(f"blob hash mismatch for {name!r}")
        os.replace(tmp, dst)
