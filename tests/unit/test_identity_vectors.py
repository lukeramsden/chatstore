import json
import unicodedata
import uuid
from pathlib import Path

import pytest

from chatstore.identity import urn as U

VECTORS = Path(__file__).parents[2] / "specs" / "test-vectors" / "identity-v1.json"
DATA = json.loads(VECTORS.read_text())


def test_namespace_frozen():
    assert DATA["namespace"] == "3f14f01c-5cc4-4325-950f-a56812da489b"
    assert str(U.NAMESPACE) == DATA["namespace"]
    assert DATA["version"] == U.ID_VERSION == "chatstore-id-v1"


@pytest.mark.parametrize("v", DATA["vectors"], ids=[v["name"] for v in DATA["vectors"]])
def test_identity_vector(v):
    assert U.derive_urn(**v["input"]) == v["urn"]
    # independent re-derivation from the published serialised name
    assert uuid.uuid5(uuid.UUID(DATA["namespace"]), v["serialised_name"]).urn == v["urn"]


def test_nfd_and_nfc_inputs_agree():
    nfc = U.derive_urn("messages", "s", "chat", "café")
    nfd = U.derive_urn("messages", "s", "chat", unicodedata.normalize("NFD", "café"))
    assert nfc == nfd


def test_invalid_source_and_type_rejected():
    with pytest.raises(ValueError):
        U.derive_urn("telegram", "s", "chat", "x")
    with pytest.raises(ValueError):
        U.derive_urn("whatsapp", "s", "blob", "x")


def test_whatsapp_incoming_requires_sender():
    with pytest.raises(ValueError):
        U.whatsapp_message_key("a@g.us", None, False, "X")


def test_email_handle_lowercased_phone_not():
    assert U.messages_handle_key("iMessage", "A@B.COM") == U.messages_handle_key("iMessage", "a@b.com")
    assert U.messages_handle_key("SMS", "+1555") == '["handle","SMS","+1555"]'


def test_revision_digest_ignores_volatile():
    a = {"urn": "u", "text": "hi", "observed_at": 1}
    b = {"urn": "u", "text": "hi", "observed_at": 2, "sync_run": "x"}
    assert U.revision_digest(a) == U.revision_digest(b)
    assert U.revision_digest(a) != U.revision_digest({"urn": "u", "text": "ho"})


def test_is_urn():
    assert U.is_urn(U.mint_urn())
    assert not U.is_urn("urn:uuid:nope")
    assert not U.is_urn("sha256:abc")
