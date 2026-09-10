"""Manifest construction and bucket arithmetic for chatstore-archive-v1."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..canonical.schema_version import ARCHIVE_FORMAT, CANONICAL_SCHEMA, ID_VERSION

RECORD_ORDER = [
    "accounts", "identities", "aliases", "chats", "chat_memberships", "messages", "message_parts",
    "events", "attachments", "blobs", "revisions", "source_observations", "sync_runs", "people", "identity_links",
]
CATALOGUE_ONLY = {"people", "identity_links"}


@dataclass(frozen=True)
class Bucket:
    kind: str  # bucket | undated | catalogue | range
    label: str  # "2026-08", "undated", "catalogue", "<startZ>-<endZ>"
    start_utc_ms: int | None = None
    end_utc_ms: int | None = None

    def manifest_bucket(self) -> dict[str, Any] | None:
        if self.kind in ("bucket", "range"):
            return {"start_utc_ms": self.start_utc_ms, "end_utc_ms": self.end_utc_ms, "label": self.label}
        return None


def month_start(utc_ms: int) -> int:
    d = datetime.fromtimestamp(utc_ms / 1000, tz=UTC)
    return int(datetime(d.year, d.month, 1, tzinfo=UTC).timestamp() * 1000)


def next_month(start_ms: int) -> int:
    d = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
    y, m = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
    return int(datetime(y, m, 1, tzinfo=UTC).timestamp() * 1000)


def month_label(start_ms: int) -> str:
    d = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
    return f"{d.year:04d}-{d.month:02d}"


def monthly_buckets(earliest_ms: int, latest_ms: int) -> list[Bucket]:
    out: list[Bucket] = []
    s = month_start(earliest_ms)
    while s <= latest_ms:
        e = next_month(s)
        out.append(Bucket("bucket", month_label(s), s, e))
        s = e
    return out


def range_label(start_ms: int, end_ms: int) -> str:
    f = "%Y%m%dT%H%M%SZ"
    return datetime.fromtimestamp(start_ms / 1000, tz=UTC).strftime(f) + "-" + datetime.fromtimestamp(end_ms / 1000, tz=UTC).strftime(f)


def filename(export_set: str, bucket: Bucket, revision: int) -> str:
    scope8 = export_set.removeprefix("urn:uuid:").replace("-", "")[:8]
    return f"chatstore-{scope8}-{bucket.label}-r{revision:04d}.zip"


def content_digest(pairs: list[tuple[str, str]]) -> str:
    """Digest over sorted (urn, revision_digest) pairs: identifies bucket content, not observation time."""
    h = hashlib.sha256()
    for urn, d in sorted(set(pairs)):
        h.update(urn.encode())
        h.update(b"\0")
        h.update(d.encode())
        h.update(b"\n")
    return "sha256:" + h.hexdigest()


def build_manifest(*, export_id: str, export_set: str, created_at: int, bucket: Bucket, revision: int,
                   supersedes: list[str], lineage: list[str], account_scopes: list[dict[str, Any]],
                   scope_mappings: list[dict[str, Any]], observation: dict[str, Any], coverage: dict[str, Any],
                   adapter_versions: dict[str, str], counts: dict[str, int], attachment_policy: str,
                   media: dict[str, int], payload_digest: str, limits: dict[str, int], members: list[str],
                   content_digest: str) -> dict[str, Any]:
    return {
        "archive_format": ARCHIVE_FORMAT,
        "canonical_schema": CANONICAL_SCHEMA,
        "id_version": ID_VERSION,
        "export_id": export_id,
        "export_set": export_set,
        "created_at": created_at,
        "kind": bucket.kind,
        "bucket": bucket.manifest_bucket(),
        "revision": revision,
        "supersedes": supersedes,
        "lineage": lineage,
        "account_scopes": account_scopes,
        "scope_mappings": scope_mappings,
        "observation": observation,
        "coverage": coverage,
        "adapter_versions": adapter_versions,
        "counts": counts,
        "attachment_policy": attachment_policy,
        "media": media,
        "payload_digest": payload_digest,
        "content_digest": content_digest,
        "limits": limits,
        "requires": [],
        "members": members,
    }


def manifest_bytes(m: dict[str, Any]) -> bytes:
    return json.dumps(m, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")


__all__ = [
    "CATALOGUE_ONLY",
    "RECORD_ORDER",
    "Bucket",
    "build_manifest",
    "content_digest",
    "filename",
    "manifest_bytes",
    "month_label",
    "month_start",
    "monthly_buckets",
    "next_month",
    "range_label",
]
