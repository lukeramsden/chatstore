"""Regression tests for GitHub issues. Each test names the issue it guards."""

from __future__ import annotations

import json
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
