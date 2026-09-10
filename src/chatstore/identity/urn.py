"""UUIDv5 URN derivation — `chatstore-id-v1`.

Frozen contract. See specs/identity.md and specs/test-vectors/identity-v1.json.
Never change NAMESPACE, ID_VERSION, or the serialisation without a new version.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
import uuid
from typing import Any

NAMESPACE = uuid.UUID("3f14f01c-5cc4-4325-950f-a56812da489b")
ID_VERSION = "chatstore-id-v1"

SOURCES = frozenset({"whatsapp", "messages"})
ENTITY_TYPES = frozenset({"account", "identity", "chat", "message", "attachment", "event"})


def compact_json(value: Any) -> str:
    """Compact JSON, UTF-8 (not ASCII-escaped). Used for native keys and the id tuple."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def canonical_json(value: Any) -> str:
    """Compact JSON with sorted keys. Used for revision digests."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def composite_key(*parts: str) -> str:
    """Serialise a multi-part native key as a compact JSON array of strings."""
    return compact_json([nfc(p) for p in parts])


def derive_uuid(source: str, account_scope: str, entity_type: str, native_key: str) -> uuid.UUID:
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}")
    if entity_type not in ENTITY_TYPES:
        raise ValueError(f"unknown entity_type {entity_type!r}")
    name = compact_json(
        [ID_VERSION, nfc(source), nfc(account_scope), nfc(entity_type), nfc(native_key)]
    )
    return uuid.uuid5(NAMESPACE, name)


def derive_urn(source: str, account_scope: str, entity_type: str, native_key: str) -> str:
    return derive_uuid(source, account_scope, entity_type, native_key).urn


def mint_urn() -> str:
    """Random UUIDv4 URN for entities without a native key."""
    return uuid.uuid4().urn


def is_urn(s: str) -> bool:
    if not s.startswith("urn:uuid:"):
        return False
    try:
        uuid.UUID(s[9:])
    except ValueError:
        return False
    return True


VOLATILE_FIELDS = frozenset({"observed_at", "sync_run", "revision_digest"})


def revision_digest(record: dict[str, Any]) -> str:
    body = {k: v for k, v in record.items() if k not in VOLATILE_FIELDS}
    return "sha256:" + hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


# --- Source-specific native keys -------------------------------------------------------


def messages_handle_key(service: str, handle_id: str) -> str:
    hid = handle_id.lower() if "@" in handle_id else handle_id
    return composite_key("handle", service, hid)


def messages_me_key(account_login: str | None) -> str:
    return composite_key("me", account_login) if account_login else composite_key("me")


def messages_chat_key(chat_guid: str) -> str:
    return composite_key("guid", chat_guid)


def messages_inferred_chat_key(ck_chat_id: str) -> str:
    return composite_key("ck_chat_id", ck_chat_id)


def whatsapp_jid(jid: str) -> str:
    return nfc(jid).lower()


def whatsapp_me_key() -> str:
    return composite_key("me")


def whatsapp_message_key(chat_jid: str, from_jid: str | None, is_from_me: bool, stanza_id: str) -> str:
    sender = "me" if is_from_me else whatsapp_jid(from_jid or "")
    if not is_from_me and not from_jid:
        raise ValueError("incoming WhatsApp message without sender JID")
    return composite_key(whatsapp_jid(chat_jid), sender, stanza_id)


def whatsapp_attachment_key(message_key: str, index: int = 0) -> str:
    return composite_key(message_key, "media", str(index))
