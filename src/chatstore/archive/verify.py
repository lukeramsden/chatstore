"""Verify an archive without importing it."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..canonical.schemas import validate_record
from . import container
from .container import InvalidArchive, Payload, WrongPassword


@dataclass
class VerifyResult:
    path: str
    ok: bool
    manifest: dict[str, Any] | None = None
    problems: list[str] = field(default_factory=list)
    wrong_password: bool = False
    counts: dict[str, int] = field(default_factory=dict)
    schema_errors: int = 0


def _tmpdir(staging: Path | None) -> Path:
    if staging is not None:
        d = staging / "verify"
        d.mkdir(parents=True, exist_ok=True)
        os.chmod(d, 0o700)
        return d
    d = Path(tempfile.mkdtemp(prefix="chatstore-verify-"))
    os.chmod(d, 0o700)
    return d


def decrypt_and_validate(path: Path, password: str, work: Path, *, load_blobs: bool = False) -> tuple[Payload, Path]:
    """Decrypt to work/payload.tar, validate structure + checksums. Raises InvalidArchive/WrongPassword."""
    tar_path = work / "payload.tar"
    digest = container.read_payload_to_file(path, password, tar_path)
    payload = container.open_payload(tar_path, digest, load_blobs=load_blobs)
    return payload, tar_path


def check_records(payload: Payload, *, validate_schema: bool = True) -> tuple[dict[str, int], list[str], int]:
    counts: dict[str, int] = {}
    problems: list[str] = []
    schema_errors = 0
    for name, data in payload.members.items():
        if not name.startswith("records/"):
            continue
        entity = name.removeprefix("records/").removesuffix(".jsonl")
        n = 0
        for lineno, line in enumerate(data.splitlines(), 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                problems.append(f"{name}:{lineno}: invalid JSON")
                break
            n += 1
            if validate_schema and entity not in ("revisions", "source_observations", "stubs"):
                if rec.get("entity") != entity:
                    problems.append(f"{name}:{lineno}: entity field {rec.get('entity')!r} != {entity!r}")
                    schema_errors += 1
                    continue
                err = validate_record(rec)
                if err:
                    schema_errors += 1
                    if schema_errors <= 5:
                        problems.append(f"{name}:{lineno}: schema: {err}")
        counts[entity] = n
    m = payload.manifest
    for k, v in (m.get("counts") or {}).items():
        if k in counts and counts[k] != v:
            problems.append(f"manifest count for {k} is {v} but payload has {counts[k]}")
    return counts, problems, schema_errors


def verify_archive(path: Path, password: str, *, staging: Path | None = None, validate_schema: bool = True) -> VerifyResult:
    work = _tmpdir(staging)
    try:
        try:
            payload, _ = decrypt_and_validate(path, password, work)
        except WrongPassword as e:
            return VerifyResult(str(path), False, problems=[str(e)], wrong_password=True)
        except InvalidArchive as e:
            return VerifyResult(str(path), False, problems=[str(e)])
        counts, problems, schema_errors = check_records(payload, validate_schema=validate_schema)
        return VerifyResult(str(path), not problems, manifest=payload.manifest, problems=problems, counts=counts,
                            schema_errors=schema_errors)
    finally:
        shutil.rmtree(work, ignore_errors=True)
