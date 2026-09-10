"""JSON Schemas for canonical records. Single source of truth; `specs/schemas/*.json` are
generated from here by `python -m chatstore.canonical.schemas <dir>` and checked in tests.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from .schema_version import CANONICAL_SCHEMA

URN = {"type": "string", "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"}
NURN = {"oneOf": [URN, {"type": "null"}]}
SHA = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
HEX64 = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
STR = {"type": "string"}
NSTR = {"type": ["string", "null"]}
INT = {"type": "integer"}
NINT = {"type": ["integer", "null"]}
BOOL = {"type": "boolean"}
NBOOL = {"type": ["boolean", "null"]}
SOURCE = {"enum": ["whatsapp", "messages"]}
NSOURCE = {"enum": ["whatsapp", "messages", None]}
TIMESTAMP = {
    "type": "object",
    "required": ["raw", "unit", "epoch", "utc_ms"],
    "additionalProperties": False,
    "properties": {
        "raw": NSTR,
        "unit": {"enum": ["s", "ms", "us", "ns"]},
        "epoch": STR,
        "utc_ms": NINT,
    },
}
NTIMESTAMP = {"oneOf": [TIMESTAMP, {"type": "null"}]}
EVIDENCE = {
    "type": "array",
    "items": {
        "type": "object",
        "required": ["type"],
        "properties": {"type": STR, "detail": {}},
    },
}
OPEN_OBJ = {"type": "object"}


def _enum_or_unknown(*values: str) -> dict[str, Any]:
    return {"anyOf": [{"enum": list(values)}, {"type": "string", "pattern": "^unknown:"}]}


def _record(kind: str, props: dict[str, Any], required: list[str], *, sourced: bool = True) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema": {"const": CANONICAL_SCHEMA},
        "entity": {"const": kind},
        "urn": URN,
        "revision_digest": SHA,
        "observed_at": INT,
        "sync_run": NURN,
    }
    base_req = ["schema", "entity", "urn", "revision_digest", "observed_at"]
    if sourced:
        base["source"] = SOURCE
        base["account_scope"] = STR
        base_req += ["source", "account_scope"]
    else:
        base["source"] = NSOURCE
        base["account_scope"] = NSTR
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"https://chatstore.local/schemas/{CANONICAL_SCHEMA}/{kind}.json",
        "title": kind,
        "type": "object",
        "additionalProperties": False,
        "required": list(dict.fromkeys(base_req + required)),
        "properties": {**base, **props},
    }


SCHEMAS: dict[str, dict[str, Any]] = {
    "accounts": _record(
        "accounts",
        {"native_key": STR, "label": NSTR, "scope_created_at": NINT},
        ["native_key"],
    ),
    "identities": _record(
        "identities",
        {
            "native_key": STR,
            "kind": _enum_or_unknown(
                "phone", "email", "jid_phone", "jid_lid", "jid_group", "jid_broadcast",
                "jid_status", "jid_bot", "me", "unknown",
            ),
            "address": NSTR,
            "service": NSTR,
            "is_me": BOOL,
            "observed_names": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "origin"],
                    "properties": {"name": STR, "origin": STR},
                },
            },
        },
        ["native_key", "kind", "address", "is_me", "observed_names"],
    ),
    "people": _record(
        "people",
        {"label": NSTR, "notes": NSTR, "created_at": INT},
        ["created_at"],
        sourced=False,
    ),
    "identity_links": _record(
        "identity_links",
        {
            "person_urn": URN,
            "identity_urn": URN,
            "state": {"enum": ["confirmed", "suggested", "rejected"]},
            "evidence": EVIDENCE,
            "created_at": INT,
            "revoked_at": NINT,
        },
        ["person_urn", "identity_urn", "state", "evidence", "created_at"],
        sourced=False,
    ),
    "aliases": _record(
        "aliases",
        {
            "old_urn": URN,
            "new_urn": URN,
            "reason": {"enum": ["scope_map", "lid_phone_pair", "handle_merge", "manual"]},
            "evidence": EVIDENCE,
            "created_at": INT,
        },
        ["old_urn", "new_urn", "reason", "evidence", "created_at"],
        sourced=False,
    ),
    "chats": _record(
        "chats",
        {
            "native_key": STR,
            "kind": _enum_or_unknown("direct", "group", "broadcast", "status", "unknown"),
            "observed_name": NSTR,
            "service": NSTR,
            "assignment": {"enum": ["source", "inferred", "stub"]},
            "is_archived": NBOOL,
            "created_at": NTIMESTAMP,
        },
        ["native_key", "kind", "assignment"],
    ),
    "chat_memberships": _record(
        "chat_memberships",
        {
            "chat_urn": URN,
            "identity_urn": URN,
            "role": {"enum": ["member", "admin", "owner", "unknown"]},
            "state": {"enum": ["active", "left", "removed", "unknown"]},
            "first_seen": NTIMESTAMP,
            "last_seen": NTIMESTAMP,
            "evidence": EVIDENCE,
        },
        ["chat_urn", "identity_urn", "role", "state"],
    ),
    "messages": _record(
        "messages",
        {
            "native_key": STR,
            "chat_urn": NURN,
            "chat_assignment": {"enum": ["join_table", "inferred_ck_chat_id", "source_column", "none"]},
            "sender_urn": NURN,
            "is_from_me": BOOL,
            "transport": {"enum": ["imessage", "sms", "rcs", "whatsapp", "unknown"]},
            "kind": _enum_or_unknown("message", "system", "reaction", "edit", "retraction", "placeholder"),
            "sent_at": NTIMESTAMP,
            "received_at": NTIMESTAMP,
            "edited_at": NTIMESTAMP,
            "retracted_at": NTIMESTAMP,
            "state": {"enum": ["sent", "delivered", "read", "played", "failed", "pending", "unknown"]},
            "reply_to_urn": NURN,
            "reply_to_native": NSTR,
            "text": NSTR,
            "has_attachments": BOOL,
            "part_count": INT,
            "source_flags": OPEN_OBJ,
            "tombstone": {"oneOf": [{"type": "null"}, {"type": "object", "required": ["reason"]}]},
        },
        ["native_key", "chat_urn", "chat_assignment", "sender_urn", "is_from_me", "transport",
         "kind", "sent_at", "state", "text", "has_attachments", "part_count", "source_flags"],
    ),
    "message_parts": _record(
        "message_parts",
        {
            "message_urn": URN,
            "index": INT,
            "type": {"enum": ["text", "attachment", "mention", "link", "location", "contact", "app", "opaque"]},
            "text": NSTR,
            "attachment_urn": NURN,
            "payload": {"type": ["object", "null"]},
            "parser_status": {"enum": ["decoded", "partial", "unsupported", "redacted"]},
        },
        ["message_urn", "index", "type", "parser_status"],
    ),
    "events": _record(
        "events",
        {
            "kind": _enum_or_unknown(
                "reaction", "reaction_removed", "edit", "retraction", "membership_add",
                "membership_remove", "subject_change", "icon_change", "delivery", "read",
            ),
            "target_urn": NURN,
            "target_native": NSTR,
            "actor_urn": NURN,
            "at": NTIMESTAMP,
            "payload": {"type": ["object", "null"]},
            "carrier_message_urn": NURN,
        },
        ["kind", "target_urn", "actor_urn", "at"],
    ),
    "attachments": _record(
        "attachments",
        {
            "native_key": STR,
            "message_urn": NURN,
            "part_index": NINT,
            "filename": NSTR,
            "mime_type": NSTR,
            "uti": NSTR,
            "declared_size": NINT,
            "availability": {"enum": ["available", "not_downloaded", "missing", "not_exported", "unknown"]},
            "blob_sha256": {"oneOf": [HEX64, {"type": "null"}]},
            "source_path_hint": NSTR,
            "is_sticker": NBOOL,
            "kind": {"enum": ["image", "video", "audio", "document", "contact", "location", "other"]},
        },
        ["native_key", "message_urn", "availability", "blob_sha256", "kind"],
    ),
    "blobs": _record(
        "blobs",
        {"sha256": HEX64, "size": INT, "mime_type": NSTR, "first_seen": INT},
        ["sha256", "size", "first_seen"],
    ),
    "revisions": _record(
        "revisions",
        {
            "entity_urn": URN,
            "entity_kind": STR,
            "supersedes": {"oneOf": [SHA, {"type": "null"}]},
            "record": OPEN_OBJ,
        },
        ["entity_urn", "entity_kind", "supersedes", "record"],
        sourced=False,
    ),
    "source_observations": _record(
        "source_observations",
        {
            "entity_urn": URN,
            "native_table": STR,
            "native_row_ids": {"type": "array", "items": {"type": ["integer", "string"]}},
            "native_key": NSTR,
            "minted": BOOL,
            "fingerprint": HEX64,
            "adapter_version": STR,
            "present": {"enum": ["present", "absent_from_source", "deleted_evidence"]},
        },
        ["entity_urn", "native_table", "native_row_ids", "native_key", "minted", "fingerprint",
         "adapter_version", "present"],
    ),
    "sync_runs": _record(
        "sync_runs",
        {
            "started_at": INT,
            "finished_at": NINT,
            "status": {"enum": ["complete", "partial", "failed", "running"]},
            "mode": {"enum": ["initial", "incremental", "full_reconcile", "import"]},
            "snapshot": {"type": ["object", "null"]},
            "checkpoints": {"type": ["object", "null"]},
            "counts": OPEN_OBJ,
            "coverage": {"type": ["object", "null"]},
            "errors": {"type": "array"},
            "adapter_version": NSTR,
            "parser_version": NSTR,
        },
        ["started_at", "finished_at", "status", "mode", "counts", "errors"],
    ),
}

# Fields adapters may copy verbatim into `source_flags` / `payload`. Anything else is dropped.
ALLOWLIST: dict[str, list[str]] = {
    "whatsapp.source_flags": [
        "ZMESSAGETYPE", "ZGROUPEVENTTYPE", "ZMESSAGESTATUS", "ZMESSAGEERRORSTATUS", "ZFLAGS",
        "ZSTARRED", "ZSORT", "ZSPOTLIGHTSTATUS", "zsort_negative_only", "duplicate_rows",
    ],
    "whatsapp.payload": ["ZTITLE", "ZVCARDNAME", "ZLATITUDE", "ZLONGITUDE", "ZMOVIEDURATION",
                         "ZASPECTRATIO", "ZMEDIAORIGIN", "ZCLOUDSTATUS", "ZFILESIZE", "ZMEDIAURL"],
    "messages.source_flags": [
        "item_type", "group_action_type", "associated_message_type", "message_action_type",
        "is_system_message", "is_service_message", "is_audio_message", "is_emote", "is_empty",
        "is_spam", "is_forward", "was_downgraded", "is_delivered", "is_read", "is_sent",
        "error", "service", "part_count", "ck_sync_state", "is_corrupt",
    ],
    "messages.payload": ["balloon_bundle_id", "expressive_send_style_id", "associated_message_emoji",
                         "associated_message_range_location", "associated_message_range_length",
                         "group_title", "app_name", "url", "title", "subtitle", "caption"],
}

ENTITY_ORDER = [
    "accounts", "identities", "people", "identity_links", "aliases", "chats", "chat_memberships",
    "messages", "message_parts", "events", "attachments", "blobs", "revisions",
    "source_observations", "sync_runs",
]


def write_all(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, schema in SCHEMAS.items():
        (directory / f"{name}.json").write_text(json.dumps(schema, indent=2) + "\n")
    (directory / "allowlist.json").write_text(json.dumps(ALLOWLIST, indent=2) + "\n")
    (directory / "timestamp.json").write_text(json.dumps(TIMESTAMP, indent=2) + "\n")


if __name__ == "__main__":
    write_all(Path(sys.argv[1]))
