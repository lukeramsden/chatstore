"""The single validation boundary for archives.

`load_archive()` decrypts an archive, checks container integrity, manifest shape, record schemas,
revision digests, counts, lineage and blob naming, and returns a `LoadedArchive` whose records are
parsed and trusted. Import consumes only that; `verify_archive()` reports the same checks.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import Any, BinaryIO

from ..canonical.schemas import validate_record
from ..identity.urn import ID_VERSION, revision_digest
from . import container
from .container import InvalidArchive, Payload, WrongPassword
from .manifest import ARCHIVE_FORMAT

MANIFEST_REQUIRED = ("archive_format", "export_id", "export_set", "created_at", "kind", "revision", "supersedes",
                     "lineage", "counts", "payload_digest", "members")
KINDS = ("catalogue", "bucket", "undated", "range", "delta")
SHA_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
BLOB_RE = re.compile(r"^blobs/sha256/([0-9a-f]{2})/([0-9a-f]{64})$")
NON_ENTITY_MEMBERS = ("revisions", "source_observations", "stubs")
MAX_REPORTED = 5


@dataclass
class VerifyResult:
    path: str
    ok: bool
    manifest: dict[str, Any] | None = None
    problems: list[str] = field(default_factory=list)
    wrong_password: bool = False
    counts: dict[str, int] = field(default_factory=dict)
    schema_errors: int = 0
    receipt: dict[str, Any] | None = None


@dataclass
class LoadedArchive:
    """A fully validated archive. `records` maps member entity name -> parsed rows."""

    manifest: dict[str, Any]
    records: dict[str, list[dict[str, Any]]]
    counts: dict[str, int]
    blob_members: list[str]
    tar_path: Path
    ciphertext_sha256: str = ""
    ciphertext_bytes: int = 0

    @property
    def label(self) -> str:
        return str((self.manifest.get("bucket") or {}).get("label") or self.manifest["kind"])


class SchemaError(InvalidArchive):
    """Archive is well-formed as a container but its content violates the canonical contract."""

    def __init__(self, problems: list[str], counts: dict[str, int], schema_errors: int):
        super().__init__("; ".join(problems))
        self.problems = problems
        self.counts = counts
        self.schema_errors = schema_errors


def _tmpdir(staging: Path | None) -> Path:
    if staging is not None:
        d = staging / "verify"
        d.mkdir(parents=True, exist_ok=True)
        os.chmod(d, 0o700)
        return d
    d = Path(tempfile.mkdtemp(prefix="chatstore-verify-"))
    os.chmod(d, 0o700)
    return d


def decrypt_and_validate(path: Path | BinaryIO, password: str, work: Path, *, load_blobs: bool = False) -> tuple[Payload, Path]:
    """Decrypt to work/payload.tar, validate structure + checksums. Raises InvalidArchive/WrongPassword."""
    tar_path = work / "payload.tar"
    digest = container.read_payload_to_file(path, password, tar_path)
    payload = container.open_payload(tar_path, digest, load_blobs=load_blobs)
    return payload, tar_path


def check_manifest(m: dict[str, Any]) -> list[str]:
    problems = []
    for k in MANIFEST_REQUIRED:
        if k not in m:
            problems.append(f"manifest: missing {k!r}")
    if problems:
        return problems
    for k in ('export_id', 'export_set'):
        if not isinstance(m[k], str) or not m[k].startswith('urn:uuid:'):
            problems.append(f'manifest: {k} must be a UUID URN')
    scopes = m.get('account_scopes', [])
    if not isinstance(scopes, list) or any(not isinstance(sc, dict) or not isinstance(sc.get('scope'), str)
            or sc.get('source') not in ('whatsapp', 'messages') for sc in scopes):
        problems.append('manifest: account_scopes must contain source/scope objects')
    mappings = m.get('scope_mappings', [])
    if not isinstance(mappings, list) or any(not isinstance(mp, dict) or any(not isinstance(mp.get(k), str)
            for k in ('from', 'to')) for mp in mappings):
        problems.append('manifest: scope_mappings must contain from/to objects')
    if m["archive_format"] not in (ARCHIVE_FORMAT, "chatstore-changefeed-v1"):
        problems.append(f"manifest: unsupported archive_format {m['archive_format']!r}")
    if m.get("id_version") and m["id_version"] != ID_VERSION:
        problems.append(f"manifest: unsupported id_version {m['id_version']!r}")
    if m["kind"] not in KINDS:
        problems.append(f"manifest: unknown kind {m['kind']!r}")
    if m["kind"] in ("bucket", "range"):
        b = m.get("bucket") or {}
        if not all(k in b for k in ("label", "start_utc_ms", "end_utc_ms")) or not b["start_utc_ms"] < b["end_utc_ms"]:
            problems.append("manifest: bucket needs label and start_utc_ms < end_utc_ms")
    if not isinstance(m["revision"], int) or m["revision"] < 1:
        problems.append("manifest: revision must be a positive integer")
    lineage, supersedes = m["lineage"], m["supersedes"]
    if not isinstance(lineage, list) or not lineage or lineage[-1] != m["export_id"]:
        problems.append("manifest: lineage must end with this export_id")
    elif len(lineage) != m["revision"]:
        problems.append(f"manifest: lineage has {len(lineage)} entries but revision is {m['revision']}")
    if not isinstance(supersedes, list) or any(s not in (lineage or []) for s in supersedes):
        problems.append("manifest: supersedes must be a subset of lineage")
    if m["revision"] > 1 and not supersedes:
        problems.append("manifest: revision > 1 must supersede an earlier export")
    if not isinstance(m["counts"], dict) or any(not isinstance(v, int) or v < 0 for v in m["counts"].values()):
        problems.append("manifest: counts must map entity -> non-negative integer")
    if not isinstance(m["created_at"], int):
        problems.append("manifest: created_at must be an integer (UTC ms)")
    if m.get("context") not in (None, "full", "minimal"):
        problems.append(f"manifest: unknown context {m['context']!r}")
    if m['kind'] == 'delta' or m['archive_format'] == 'chatstore-changefeed-v1':
        f = m.get('feed')
        if m['kind'] != 'delta' or m['archive_format'] != 'chatstore-changefeed-v1':
            problems.append('manifest: delta requires chatstore-changefeed-v1')
        if not isinstance(f, dict):
            problems.append('manifest: missing feed')
        else:
            if any(not isinstance(f.get(k), str) or not f[k] for k in ('stream', 'producer')):
                problems.append('manifest: feed needs stream and producer identifiers')
            if (type(f.get('after')) is not int or type(f.get('through')) is not int
                    or not 0 <= f['after'] < f['through'] <= 2**63 - 1):
                problems.append('manifest: feed needs 0 <= after < through')
            if f.get('after') == 0:
                if f.get('parent') is not None or f.get('parent_sha256') is not None:
                    problems.append('manifest: baseline must not have a parent')
            elif not isinstance(f.get('parent'), str) or not SHA_RE.fullmatch(str(f.get('parent_sha256', ''))):
                problems.append('manifest: delta needs parent id and ciphertext hash')
            if m.get('requires') != ([f['parent']] if f.get('parent') else []):
                problems.append('manifest: requires must name the exact parent')
        if m.get('supersedes') or m.get('revision') != 1:
            problems.append('manifest: changefeed objects are immutable and never supersede history')
    return problems


def _digest_ok(rec: dict[str, Any]) -> bool:
    body = {k: v for k, v in rec.items() if k != "iso"}
    return SHA_RE.match(str(rec.get("revision_digest", ""))) is not None and revision_digest(body) == rec["revision_digest"]


def _schema_error(rec: dict[str, Any], checked: set[bytes] | None) -> str | None:
    key = hashlib.sha256(json.dumps(rec, sort_keys=True, ensure_ascii=False,
                                   separators=(',', ':')).encode()).digest() if checked is not None else None
    if key is not None and checked is not None and key in checked:
        return None
    error = validate_record(rec)
    if error is None and checked is not None and key is not None:
        checked.add(key)
    return error


def _check_row(entity: str, rec: dict[str, Any], validate_schema: bool, *,
               schema_checked: set[bytes] | None = None) -> str | None:
    """Return a problem string for one parsed row, or None."""
    if not isinstance(rec, dict):
        return "row is not an object"
    if entity == "changes":
        from .incremental import validate_event
        return validate_event(rec, schema_checked=schema_checked)
    if entity == "stubs":
        if not isinstance(rec.get("urn"), str) or not isinstance(rec.get("entity"), str):
            return "stub needs urn and entity"
        return None
    if entity == "revisions":
        inner = rec.get("record")
        if not isinstance(inner, dict) or inner.get("urn") != rec.get("entity_urn") or inner.get("entity") != rec.get("entity_kind"):
            return "revision record does not match entity_urn/entity_kind"
        if not _digest_ok(inner) or inner["revision_digest"] != rec.get("revision_digest"):
            return "revision digest does not match record content"
        if not isinstance(rec.get("observed_at"), int):
            return "revision observed_at must be an integer"
        if validate_schema and (err := _schema_error(inner, schema_checked)):
            return f"schema: {err}"
        return None
    if entity == "source_observations":
        return _check_observation(rec)
    if rec.get("entity") != entity:
        return f"entity field {rec.get('entity')!r} != {entity!r}"
    if not _digest_ok(rec):
        return "revision_digest does not match record content"
    if validate_schema and (err := _schema_error(rec, schema_checked)):
        return f"schema: {err}"
    return None


OBSERVATION_FIELDS = {"entity_urn": str, "source": str, "account_scope": str, "native_table": str, "native_row_ids": list,
                      "fingerprint": str, "adapter_version": str, "observed_at": int, "minted": bool, "present": str}
PRESENT_STATES = ("present", "absent_from_source", "deleted_evidence")


def _check_observation(rec: dict[str, Any]) -> str | None:
    """Observation rows are exported without the record envelope, so they get an explicit shape check."""
    for k, t in OBSERVATION_FIELDS.items():
        if not isinstance(rec.get(k), t) or (t is int and isinstance(rec.get(k), bool)):
            return f"observation field {k!r} missing or not {t.__name__}"
    if rec["present"] not in PRESENT_STATES:
        return f"observation present={rec['present']!r} unknown"
    if rec.get('native_key') is not None and not isinstance(rec['native_key'], str):
        return 'observation native_key must be a string or null'
    if rec.get('sync_run') is not None and not isinstance(rec['sync_run'], str):
        return 'observation sync_run must be a string or null'
    if not rec["entity_urn"].startswith("urn:uuid:"):
        return "observation entity_urn is not a URN"
    return None


def check_records(payload: Payload, *, validate_schema: bool = True) -> tuple[dict[str, int], list[str], int]:
    """Parse and validate every records/ member. Returns (counts, problems, schema_errors)."""
    counts, problems, errors, _ = _check_and_parse(payload, validate_schema)
    return counts, problems, errors


def _check_and_parse(payload: Payload, validate_schema: bool) -> tuple[dict[str, int], list[str], int, dict[str, list[dict[str, Any]]]]:
    counts: dict[str, int] = {}
    problems: list[str] = []
    schema_errors = 0
    parsed: dict[str, list[dict[str, Any]]] = {}
    # Each archive load owns its checks. Exact duplicate bodies may share schema
    # validation; digests, event envelopes and trust boundaries are still checked.
    schema_checked: set[bytes] = set()
    m = payload.manifest
    for name, data in payload.members.items():
        if not name.startswith("records/"):
            continue
        entity = name.removeprefix("records/").removesuffix(".jsonl")
        rows: list[dict[str, Any]] = []
        # Keep only the current line copy while parsed records accumulate.
        for lineno, match in enumerate(re.finditer(rb"[^\r\n]*(?:\r\n?|\n|$)", data), 1):
            line = match.group()
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                problems.append(f"{name}:{lineno}: invalid JSON")
                break
            err = _check_row(entity, rec, validate_schema, schema_checked=schema_checked)
            if err:
                schema_errors += 1
                if schema_errors <= MAX_REPORTED:
                    problems.append(f"{name}:{lineno}: {err}")
                continue
            rows.append(rec)
        counts[entity] = len(rows)
        parsed[entity] = rows
    for k, v in (m.get("counts") or {}).items():
        if k in NON_ENTITY_MEMBERS:
            continue
        have = len(payload.blob_members) if k == "blob_files" else counts.get(k, 0)
        if have != v:
            problems.append(f"manifest count for {k} is {v} but payload has {have}")
    if m.get('kind') == 'delta':
        if set(parsed) != {'changes'}:
            problems.append('changefeed payload must contain only changes and blobs')
        events = parsed.get('changes', [])
        feed = m['feed']
        if (not events or events[0]['seq'] != feed['after'] + 1
                or events[-1]['seq'] != feed['through']
                or any(b['seq'] != a['seq'] + 1 for a, b in pairwise(events))):
            problems.append('changefeed sequences are not contiguous or do not match manifest')
    elif 'changes' in parsed:
        problems.append('changes require a changefeed manifest')
    for name in payload.blob_members:
        mt = BLOB_RE.match(name)
        if not mt or mt.group(2)[:2] != mt.group(1):
            problems.append(f"blob member {name!r} is not content-addressed")
        elif payload.blob_digests and payload.blob_digests.get(name) not in (None, mt.group(2)):
            problems.append(f"blob member {name!r} content does not match its name")
    if schema_errors > MAX_REPORTED:
        problems.append(f"... {schema_errors - MAX_REPORTED} more record problem(s)")
    return counts, problems, schema_errors, parsed


def load_archive(path: Path, password: str, work: Path, *, validate_schema: bool = True) -> LoadedArchive:
    """Decrypt and fully validate. Raises WrongPassword, SchemaError (content) or InvalidArchive (container)."""
    # Hash and decrypt the same open file, even if its pathname is atomically replaced.
    with path.open('rb') as ciphertext:
        before = os.fstat(ciphertext.fileno())
        digest = 'sha256:' + hashlib.file_digest(ciphertext, 'sha256').hexdigest()
        ciphertext.seek(0)
        payload, tar_path = decrypt_and_validate(ciphertext, password, work)
        after = os.fstat(ciphertext.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise InvalidArchive('ciphertext changed during verification')
    problems = check_manifest(payload.manifest)
    if problems:
        raise SchemaError(problems, {}, 0)
    counts, problems, schema_errors, parsed = _check_and_parse(payload, validate_schema)
    if problems:
        raise SchemaError(problems, counts, schema_errors)
    return LoadedArchive(payload.manifest, parsed, counts, list(payload.blob_members), tar_path, digest, before.st_size)


def verify_archive(path: Path, password: str, *, staging: Path | None = None, validate_schema: bool = True) -> VerifyResult:
    work = _tmpdir(staging)
    try:
        try:
            loaded = load_archive(path, password, work, validate_schema=validate_schema)
        except WrongPassword as e:
            return VerifyResult(str(path), False, problems=[str(e)], wrong_password=True)
        except SchemaError as e:
            return VerifyResult(str(path), False, manifest=None, problems=e.problems, counts=e.counts, schema_errors=e.schema_errors)
        except InvalidArchive as e:
            return VerifyResult(str(path), False, problems=[str(e)])
        return VerifyResult(str(path), True, manifest=loaded.manifest, counts=loaded.counts,
                            receipt=verification_receipt(loaded, validate_schema=validate_schema))
    finally:
        shutil.rmtree(work, ignore_errors=True)


def verification_receipt(loaded: LoadedArchive, *, validate_schema: bool = True) -> dict[str, Any]:
    return {'schema': 'chatstore-verified-export-v1', 'export_id': loaded.manifest['export_id'],
            'ciphertext_sha256': loaded.ciphertext_sha256, 'ciphertext_bytes': loaded.ciphertext_bytes,
            'payload_digest': loaded.manifest['payload_digest'], 'feed': loaded.manifest.get('feed'),
            'checks': ['aes_authentication', 'member_checksums', 'revision_digests', 'manifest_shape'] +
                      (['feed_shape'] if loaded.manifest.get('kind') == 'delta' else []) +
                      (['canonical_schema'] if validate_schema else [])}
