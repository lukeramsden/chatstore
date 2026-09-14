"""Regression tests for GitHub issues. Each test names the issue it guards."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from chatstore.adapters.messages.adapter import find_helper
from chatstore.cli.app import main
from chatstore.config import Config
from tests.fixtures.synthetic import build_messages, build_whatsapp

needs_helper = pytest.mark.skipif(find_helper(Config()) is None, reason="Rust helper not built (scripts/build-helper.sh)")


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *map(str, argv)])
    env = json.loads(capsys.readouterr().out)
    assert env["exit_code"] == code
    return code, env


@pytest.fixture
def wa_env(tmp_path: Path, monkeypatch, capsys):
    wa = build_whatsapp(tmp_path / "wa")
    data = tmp_path / "data"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(data))
    assert run(capsys, "init", "--whatsapp-path", wa, "--timezone", "UTC")[0] == 0
    assert run(capsys, "sync", "--source", "whatsapp")[0] == 0
    return {"wa": wa, "data": data}


def test_issue_1_whatsapp_direct_chats_have_participants(wa_env, capsys):
    """#1: direct chats must list the counterpart identity and the `me` identity as participants."""
    code, e = run(capsys, "chats", "--source", "whatsapp")
    assert code == 0
    directs = [c for c in e["data"] if c["chat_kind"] == "direct"]
    assert len(directs) == 2
    for chat in directs:
        assert len(chat["participants"]) == 2, chat
        me = [p for p in chat["participants"] if p["label"] == "me"]
        others = [p for p in chat["participants"] if p["label"] != "me"]
        assert len(me) == 1 and len(others) == 1, chat
        # The counterpart identity's address is the chat's own JID.
        _, chat_rec = run(capsys, "resolve", chat["urn"])
        _, ident = run(capsys, "resolve", others[0]["urn"])
        assert ident["data"]["entity"] == "identities"
        assert ident["data"]["record"]["address"] == chat_rec["data"]["record"]["native_key"]


@pytest.fixture
def im_env(tmp_path: Path, monkeypatch, capsys):
    im = build_messages(tmp_path / "im")
    data = tmp_path / "data"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(data))
    assert run(capsys, "init", "--messages-path", im, "--timezone", "UTC")[0] == 0
    assert run(capsys, "sync", "--source", "messages")[0] == 0
    return {"im": im, "data": data}


@needs_helper
def test_issue_2_messages_chat_without_display_name_is_not_labelled_with_raw_key(im_env, capsys):
    """#2: a Messages direct chat with no display_name must be labelled by its counterpart, not the
    JSON composite native_key, in `chats` and in `chat_label` on search/read results."""
    code, e = run(capsys, "chats", "--source", "messages")
    assert code == 0
    for chat in e["data"]:
        assert not (chat["label"] or "").startswith('["'), chat
    direct = next(c for c in e["data"] if c["chat_kind"] == "direct" and c["assignment"] == "source")
    # Label falls back to the single non-me participant's label.
    counterpart = next(p for p in direct["participants"] if p["label"] != "me")
    assert direct["label"] == counterpart["label"] and direct["label"]
    stub = next(c for c in e["data"] if c["assignment"] == "stub")
    assert stub["label"] and not stub["label"].startswith('["')

    code, e = run(capsys, "search", "hello")
    assert code == 0 and e["data"]
    assert e["data"][0]["chat_label"] == direct["label"]
    code, e = run(capsys, "read", direct["urn"])
    assert code == 0 and e["data"] and e["data"][0]["chat_label"] == direct["label"]


def test_issue_3_whatsapp_lid_alias_target_identity_exists(wa_env, capsys):
    """#3: the phone-JID side of a LID->phone alias must resolve even when that JID only appears
    in LID.sqlite / ContactsV2.sqlite and never in ChatStorage.sqlite."""
    code, e = run(capsys, "chats", "--source", "whatsapp")
    bob = next(c for c in e["data"] if c["label"] == "Bob Lid")
    lid_urn = next(p["urn"] for p in bob["participants"] if p["label"] != "me")
    code, e = run(capsys, "resolve", lid_urn)
    assert code == 0 and e["data"]["record"]["kind"] == "jid_lid"
    targets = e["data"]["aliases"]["to"]
    assert len(targets) == 1
    code, e = run(capsys, "resolve", targets[0])
    assert code == 0, e
    assert e["data"]["status"] == "current"
    assert e["data"]["record"]["address"] == "15550002222@s.whatsapp.net"
    assert e["data"]["record"]["kind"] == "jid_phone"


def test_issue_4_read_order_desc_returns_newest_first_with_cursor(wa_env, capsys):
    """#4: `read --order desc --limit N` returns the newest N (newest first) and the cursor walks
    backwards; the default remains oldest-first."""
    code, e = run(capsys, "chats", "--source", "whatsapp")
    chat = next(c for c in e["data"] if c["label"] == "Alice Example")
    code, asc = run(capsys, "read", chat["urn"], "--limit", "50")
    assert code == 0 and asc["data"] and asc["page"]["next_cursor"] is None
    all_urns = [m["urn"] for m in asc["data"]]
    assert len(all_urns) == 3
    assert all_urns[-1] is not None

    code, d1 = run(capsys, "read", chat["urn"], "--order", "desc", "--limit", "2")
    assert code == 0, d1
    assert [m["urn"] for m in d1["data"]] == all_urns[::-1][:2]
    assert d1["page"]["next_cursor"]
    code, d2 = run(capsys, "read", chat["urn"], "--order", "desc", "--limit", "2", "--cursor", d1["page"]["next_cursor"])
    assert code == 0
    assert [m["urn"] for m in d2["data"]] == all_urns[::-1][2:]
    assert d2["page"]["next_cursor"] is None


def test_issue_5_chats_label_and_participant_filters(wa_env, capsys):
    """#5: `chats --label` matches chat and participant labels case-insensitively;
    `chats --participant` accepts an identity URN and follows aliases."""
    code, e = run(capsys, "chats", "--source", "whatsapp")
    assert code == 0 and len(e["data"]) == 3
    group = next(c for c in e["data"] if c["chat_kind"] == "group")
    bob = next(c for c in e["data"] if c["label"] == "Bob Lid")

    code, e = run(capsys, "chats", "--label", "test grOUP")
    assert code == 0 and [c["urn"] for c in e["data"]] == [group["urn"]]
    # participant label match: Bob is the counterpart of his own chat and a member of the group
    code, e = run(capsys, "chats", "--label", "BOB LID")
    assert code == 0 and {c["urn"] for c in e["data"]} == {bob["urn"], group["urn"]}
    code, e = run(capsys, "chats", "--label", "no such chat")
    assert code == 0 and e["data"] == [] and e["page"]["next_cursor"] is None

    bob_lid_urn = next(p["urn"] for p in bob["participants"] if p["label"] != "me")
    code, e = run(capsys, "chats", "--participant", bob_lid_urn)
    assert code == 0 and {c["urn"] for c in e["data"]} == {bob["urn"], group["urn"]}
    # the phone-side alias of Bob's LID finds the same chats
    _, r = run(capsys, "resolve", bob_lid_urn)
    phone_urn = r["data"]["aliases"]["to"][0]
    code, e = run(capsys, "chats", "--participant", phone_urn)
    assert code == 0 and {c["urn"] for c in e["data"]} == {bob["urn"], group["urn"]}


def test_issue_6_message_without_chat_session_error_has_sample(tmp_path: Path, monkeypatch, capsys):
    """#6: adapter errors must carry a non-sensitive pointer in `sample` (table, row id, timestamp,
    type) so a partial sync can be diagnosed. The sample must not include message text or JIDs."""
    wa = build_whatsapp(tmp_path / "wa")
    c = sqlite3.connect(wa / "ChatStorage.sqlite")
    c.execute("INSERT INTO ZWAMESSAGE VALUES (?,1,1,?,?,?,?,?,?,?,0,0,?,NULL,?,?,?,?,?,NULL,?,?,?)",
              (42, 0, 0, 0, 0, 0, 0, 40, None, None, None, 700000300, None, "15550001111@s.whatsapp.net",
               "3EB0ORPHAN01", "secret orphan text", None))
    c.commit()
    c.close()
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(tmp_path / "data"))
    assert run(capsys, "init", "--whatsapp-path", wa, "--timezone", "UTC")[0] == 0
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 5 and e["data"][0]["status"] == "partial"
    err = next(x for x in e["data"][0]["errors"] if x["code"] == "message_without_chat_session")
    assert err["count"] == 1
    sample = err["sample"]
    assert isinstance(sample, str) and sample
    assert "ZWAMESSAGE" in sample and "42" in sample and "700000300" in sample
    assert "secret orphan text" not in sample and "15550001111" not in sample
