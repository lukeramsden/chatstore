"""Regression tests for GitHub issues. Each test names the issue it guards."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chatstore.cli.app import main
from tests.fixtures.synthetic import build_whatsapp


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
