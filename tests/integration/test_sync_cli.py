"""End-to-end: synthetic sources -> init/sync -> search/read/resolve/context via the CLI."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from chatstore.adapters.messages.adapter import find_helper
from chatstore.cli.app import main
from chatstore.config import Config
from tests.fixtures.synthetic import build_messages, build_whatsapp

HELPER = find_helper(Config())
needs_helper = pytest.mark.skipif(HELPER is None, reason="Rust helper not built (scripts/build-helper.sh)")


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *argv])
    out = capsys.readouterr().out
    env = json.loads(out)
    assert env["envelope"] == "chatstore-cli-v1"
    assert env["exit_code"] == code
    return code, env


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    wa = build_whatsapp(tmp_path / "wa")
    im = build_messages(tmp_path / "im")
    data = tmp_path / "data"
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(data))
    return {"wa": wa, "im": im, "data": data}


def test_not_initialised(env, capsys):
    code, e = run(capsys, "status")
    assert code == 9 and e["errors"][0]["code"] == "not_initialised"


def test_whatsapp_sync_and_queries(env, capsys):
    code, e = run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--messages-path", str(env["im"]), "--timezone", "UTC")
    assert code == 0 and e["data"]["created"] and len(e["data"]["scopes"]) == 2
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0, e
    r = e["data"][0]
    assert r["status"] == "complete" and r["mode"] == "initial"
    # 8 rows, one history-sync duplicate -> 7 logical messages
    assert r["counts"]["messages"] == 7
    assert r["counts"]["chats"] == 3 and r["counts"]["attachments"] == 1 and r["counts"]["aliases"] == 2  # Bob + owner LID pairs
    assert r["counts"]["unsupported"]["message_type:99"] == 1

    code, e = run(capsys, "search", "tomorrow")
    assert code == 0 and len(e["data"]) == 1
    hit = e["data"][0]
    assert hit["provenance"]["source"] == "whatsapp" and hit["sender_label"] == "me"
    assert hit["chat_label"] == "Alice Example"
    assert next(s for s in e["freshness"]["sources"] if s["source"] == "whatsapp")["last_sync_status"] == "complete"

    # emoji search works with the custom tokenizer categories
    code, e = run(capsys, "search", "🎉")
    assert code == 0 and len(e["data"]) == 1

    code, e = run(capsys, "chats", "--source", "whatsapp")
    assert code == 0 and len(e["data"]) == 3
    direct = next(c for c in e["data"] if c["label"] == "Alice Example")
    assert direct["message_count"] == 3  # excludes system/unknown kinds

    code, e = run(capsys, "read", direct["urn"], "--limit", "2")
    assert code == 0 and e["page"]["returned"] == 2 and e["page"]["next_cursor"]
    code, e2 = run(capsys, "read", direct["urn"], "--limit", "2", "--cursor", e["page"]["next_cursor"])
    assert code == 0 and e2["page"]["returned"] == 1 and e2["page"]["next_cursor"] is None
    att_msg = e2["data"][0]
    assert att_msg["has_attachments"] and att_msg["attachments"][0]["availability"] == "available"
    assert att_msg["attachments"][0]["blob_sha256"]

    code, e = run(capsys, "resolve", hit["urn"])
    assert code == 0 and e["data"]["status"] == "current" and e["data"]["entity"] == "messages"
    assert e["data"]["observations"][0]["native_row_ids"] == [2, 3]  # duplicate rows grouped
    code, e = run(capsys, "resolve", json.dumps({"urn": hit["urn"], "revision_digest": "deadbeef"}))
    assert e["data"]["status"] == "unknown_revision"

    code, e = run(capsys, "context", hit["urn"], "--before", "1", "--after", "1")
    assert code == 0 and len(e["data"]["before"]) == 1 and len(e["data"]["after"]) == 1
    assert e["data"]["target"]["sent_at"]["iso"].endswith("Z")

    # alias LID -> phone resolvable both ways
    code, e = run(capsys, "chats", "--source", "whatsapp")
    lid_chat = next(c for c in e["data"] if c["label"] == "Bob Lid")
    assert lid_chat["chat_kind"] == "direct"

    # second sync is a no-op incremental: no new revisions
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0 and e["data"][0]["mode"] == "incremental"
    conn = sqlite3.connect(env["data"] / "cache.sqlite3")
    assert conn.execute("SELECT count(*) FROM (SELECT entity_urn FROM revisions GROUP BY 1 HAVING count(*) > 1)").fetchone()[0] == 0


def test_whatsapp_incremental_and_revision(env, capsys):
    run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--messages-path", str(env["im"]))
    run(capsys, "sync", "--source", "whatsapp")
    # Edit a message text in the source and add a new one -> new revision + new message
    c = sqlite3.connect(env["wa"] / "ChatStorage.sqlite")
    c.execute("UPDATE ZWAMESSAGE SET ZTEXT='hello from alice (edited)' WHERE Z_PK=1")
    c.execute("INSERT INTO ZWAMESSAGE VALUES (50,1,1,0,0,0,0,0,0,50,0,0,1,NULL,NULL,NULL,700000500,NULL,'15550001111@s.whatsapp.net',NULL,'3EB0NEW00001','brand new message',NULL)")
    c.commit()
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0 and e["data"][0]["mode"] == "incremental"
    code, e = run(capsys, "search", "edited")
    assert len(e["data"]) == 1
    urn = e["data"][0]["urn"]
    code, e = run(capsys, "resolve", urn)
    assert len(e["data"]["revisions"]) == 2
    code, e = run(capsys, "search", "hello", "--history")
    statuses = sorted(h["revision_status"] for h in e["data"])
    assert statuses == ["current", "revision_superseded"]
    code, e = run(capsys, "search", "brand new")
    assert len(e["data"]) == 1


@needs_helper
def test_messages_sync_and_queries(env, capsys):
    code, e = run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--messages-path", str(env["im"]), "--timezone", "UTC")
    code, e = run(capsys, "sync", "--source", "messages")
    assert code == 0, e
    r = e["data"][0]
    assert r["status"] == "complete"
    assert r["counts"]["messages"] == 7 and r["counts"]["identities"] == 5  # 4 handles + me
    assert r["counts"]["chats"] == 3 and r["counts"]["stub_chats"] == 1
    assert r["counts"]["messages_inferred_chat"] == 2
    assert r["counts"]["events"] == 1  # the tapback

    code, e = run(capsys, "search", "tomorrow")
    assert code == 0 and len(e["data"]) == 1 and e["data"][0]["transport"] == "imessage"
    code, e = run(capsys, "search", "Loved")  # reactions are excluded by default
    assert len(e["data"]) == 0
    code, e = run(capsys, "search", "hello")
    hello = e["data"][0]
    code, e = run(capsys, "resolve", hello["urn"])
    rec = e["data"]["record"]
    assert rec["chat_assignment"] == "join_table" and rec["kind"] == "message"
    code, e = run(capsys, "context", hello["urn"], "--before", "0", "--after", "0")
    tgt = e["data"]["target"]
    assert tgt["events"] and tgt["events"][0]["kind"] == "reaction" and tgt["events"][0]["payload"]["emoji"] == "❤️"

    code, e = run(capsys, "search", "orphan via")
    code, e = run(capsys, "resolve", e["data"][0]["urn"])
    assert e["data"]["record"]["chat_assignment"] == "inferred_ck_chat_id"
    code, e = run(capsys, "search", "orphan stub")
    code, e = run(capsys, "resolve", e["data"][0]["chat_urn"])
    assert e["data"]["status"] == "unavailable" and e["data"]["record"]["assignment"] == "stub"

    code, e = run(capsys, "chats", "--source", "messages")
    fam = next(c for c in e["data"] if c["label"] == "Family")
    assert fam["chat_kind"] == "group" and len(fam["participants"]) == 3
    code, e = run(capsys, "read", fam["urn"])
    assert e["data"][0]["reply_to_urn"] is not None

    code, e = run(capsys, "media", "status")
    assert code == 0 and {(r["availability"], r["count"]) for r in e["data"]["summary"]} == {("available", 1)}
    assert e["data"]["restored_blob_files"] == 0 and e["data"]["chats_with_most_unavailable"] == []
    code, e = run(capsys, "media", "list", "--availability", "available")
    assert code == 0 and len(e["data"]) == 1 and e["data"][0]["blob_sha256"] and e["data"][0]["chat_label"]
    code, e = run(capsys, "media", "list", "--availability", "missing")
    assert code == 0 and e["data"] == []
    code, e = run(capsys, "search", "hello", "--has-attachment")
    assert len(e["data"]) == 0


def test_doctor_reports_sources(env, capsys):
    run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--messages-path", str(env["im"]))
    _, e = run(capsys, "doctor")
    d = e["data"]
    assert d["fts5"] is True
    by = {s["source"]: s for s in d["sources"]}
    assert by["whatsapp"]["schema_ok"] and by["whatsapp"]["path_found"]
    assert by["messages"]["path_found"] and by["messages"]["readable"]


def test_schema_mismatch_fails_closed(env, capsys):
    run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--messages-path", str(env["im"]))
    c = sqlite3.connect(env["wa"] / "ChatStorage.sqlite")
    c.execute("ALTER TABLE ZWAMESSAGE RENAME COLUMN ZSTANZAID TO ZSTANZA_RENAMED")
    c.commit()
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 4 and e["data"][0]["status"] == "failed"
    code, e = run(capsys, "doctor")
    assert code == 4


def test_missing_source_is_permission_or_not_found(env, capsys, tmp_path):
    run(capsys, "init", "--whatsapp-path", str(tmp_path / "nowhere"), "--messages-path", str(env["im"]))
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 3 and e["data"][0]["errors"][0]["code"] == "permission"
    assert "not found" in e["data"][0]["errors"][0]["sample"]
