"""Phase 4: identity links, suggestions, scope mapping, conflicts, purge, changed-bucket restore."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from chatstore.cache import Cache
from chatstore.cli.app import main
from tests.fixtures.synthetic import build_messages, build_whatsapp
from tests.integration.test_sync_cli import needs_helper


def run(capsys, *argv: str) -> tuple[int, dict]:
    code = main(["--json", *argv])
    env = json.loads(capsys.readouterr().out)
    assert env["exit_code"] == code
    return code, env


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    wa = build_whatsapp(tmp_path / "wa")
    im = build_messages(tmp_path / "im")
    data = tmp_path / "data"
    pw = tmp_path / "pw"
    pw.write_text("fixture-password-0123456789\n")
    os.chmod(pw, 0o600)
    monkeypatch.setenv("CHATSTORE_PASSWORD_FILE", str(pw))
    monkeypatch.setenv("CHATSTORE_DATA_DIR", str(data))
    return {"wa": wa, "im": im, "data": data, "tmp": tmp_path}


def _init_sync(capsys, env, *sources: str):
    code, _ = run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--messages-path", str(env["im"]), "--timezone", "UTC")
    assert code == 0
    for s in sources:
        code, e = run(capsys, "sync", "--source", s)
        assert code == 0, e


def _identity(capsys, address_fragment: str, source: str) -> str:
    cache = Cache(Path(os.environ["CHATSTORE_DATA_DIR"]) / "cache.sqlite3", readonly=True)
    try:
        for r in cache.iter_records("identities", source=source):
            if address_fragment in (r.get("address") or ""):
                return r["urn"]
    finally:
        cache.close()
    raise AssertionError("identity not found")


@needs_helper
def test_identity_suggest_link_unlink(env, capsys):
    _init_sync(capsys, env, "whatsapp", "messages")
    code, e = run(capsys, "identity", "suggest")
    assert code == 0
    sugg = [s for s in e["data"] if s["evidence"]["type"] == "normalised_phone" and s["evidence"]["detail"].endswith("1111")]
    assert len(sugg) == 1 and {i["source"] for i in sugg[0]["identities"]} == {"whatsapp", "messages"}
    urns = [i["urn"] for i in sugg[0]["identities"]]
    code, e = run(capsys, "identity", "link", *urns, "--label", "Alice")
    assert code == 0 and len(e["data"]["links"]) == 2
    person = e["data"]["person_urn"]
    code, e = run(capsys, "people")
    assert code == 0 and e["data"][0]["urn"] == person and len(e["data"][0]["identities"]) == 2
    # suggestion disappears once all are linked
    code, e = run(capsys, "identity", "suggest")
    assert not [s for s in e["data"] if s["evidence"]["detail"].endswith("1111")]
    # search by person spans sources
    code, e = run(capsys, "search", "hello", "--person", person)
    assert code == 0 and {h["provenance"]["source"] for h in e["data"]} == {"whatsapp"}
    # unlink -> rejected, no longer suggested
    code, e = run(capsys, "identity", "unlink", urns[0])
    assert code == 0 and e["data"]["rejected"][0]["state"] == "rejected"
    code, e = run(capsys, "identity", "suggest")
    assert not [s for s in e["data"] if s["evidence"]["detail"].endswith("1111")]
    # linking a non-identity fails with 8
    code, e = run(capsys, "identity", "link", person, "--label", "x")
    assert code == 8


def test_scope_map_creates_aliases(env, capsys):
    _init_sync(capsys, env, "whatsapp")
    code, e = run(capsys, "scope", "list")
    assert code == 0 and len(e["data"]) == 2
    scope = next(s["scope"] for s in e["data"] if s["source"] == "whatsapp")
    other = "11111111-2222-4333-8444-555555555555"
    code, e = run(capsys, "scope", "map", scope, other)
    assert code == 0 and e["data"]["aliases_created"] > 0
    ident = json.loads((env["data"] / "identity.json").read_text())
    assert ident["scope_mappings"][0]["from"] == scope
    # old URNs resolve; alias points at the derived new URN
    alice = _identity(capsys, "15550001111", "whatsapp")
    code, e = run(capsys, "resolve", alice)
    assert code == 0 and e["data"]["status"] == "current" and e["data"]["aliases"]
    # idempotent
    code, e = run(capsys, "scope", "map", scope, other)
    assert code == 0 and e["data"]["aliases_created"] == 0
    code, e = run(capsys, "scope", "map", scope, scope)
    assert code == 2


def test_purge_entity_and_source(env, capsys):
    _init_sync(capsys, env, "whatsapp")
    code, e = run(capsys, "search", "tomorrow")
    urn = e["data"][0]["urn"]
    code, e = run(capsys, "purge", "--entity", urn)
    assert code == 2 and e["errors"][0]["code"] == "confirm_required"
    code, e = run(capsys, "purge", "--entity", urn, "--confirm")
    assert code == 0 and e["data"]["removed"] >= 1 and e["warnings"]
    code, e = run(capsys, "search", "tomorrow")
    assert code == 0 and e["data"] == []
    code, e = run(capsys, "resolve", urn)
    assert e["data"]["status"] == "unknown"
    code, e = run(capsys, "purge", "--source", "whatsapp", "--confirm")
    assert code == 0
    code, e = run(capsys, "status")
    assert e["data"]["counts"].get("messages", 0) == 0
    # a re-sync brings everything back with the same URNs (deterministic identity)
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0 and e["data"][0]["mode"] == "initial"
    code, e = run(capsys, "search", "tomorrow")
    assert e["data"][0]["urn"] == urn


def test_source_reset_and_deletion_evidence(env, capsys):
    _init_sync(capsys, env, "whatsapp")
    code, e = run(capsys, "search", "direct from lid")
    urn = e["data"][0]["urn"]
    # source loses a row and its max Z_PK goes backwards -> full rescan, absence recorded
    c = sqlite3.connect(env["wa"] / "ChatStorage.sqlite")
    c.execute("DELETE FROM ZWAMESSAGE WHERE Z_PK=8")
    c.commit()
    c.close()
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0
    assert any("reset" in n for n in e["data"][0]["coverage"]["notes"]), e["data"][0]
    assert e["data"][0]["mode"] == "full_reconcile"
    code, e = run(capsys, "resolve", urn)
    obs = e["data"]["observations"]
    assert obs and obs[0]["present"] == "absent_from_source"
    assert e["data"]["status"] == "current"  # record retained; absence is evidence, not deletion


def test_changed_bucket_restore_keeps_shared_data(env, capsys, tmp_path):
    _init_sync(capsys, env, "whatsapp")
    out = tmp_path / "out"
    code, e = run(capsys, "archive", "export", "-o", str(out))
    assert code == 0
    # edit history: message text changes in the source (simulated edit) -> new revision, r0002 bucket
    c = sqlite3.connect(env["wa"] / "ChatStorage.sqlite")
    c.execute("UPDATE ZWAMESSAGE SET ZTEXT='hi alice, see you on friday' WHERE Z_PK=2")
    c.execute("DELETE FROM ZWAMESSAGE WHERE Z_PK=6")  # message vanishes from the source
    c.commit()
    c.close()
    code, e = run(capsys, "sync", "--source", "whatsapp", "--mode", "full")
    assert code == 0
    code, e = run(capsys, "archive", "export", "-o", str(out), "--no-catalogue")
    assert code == 0
    written = [a for a in e["data"]["archives"] if a["action"] == "written"]
    assert written and all(a["revision"] == 2 for a in written)
    # restore r1 first then r2 into a clean dir
    dst = tmp_path / "dst"
    os.environ["CHATSTORE_DATA_DIR"] = str(dst)
    code, e = run(capsys, "archive", "import", str(out))
    assert code == 0, e
    code, e = run(capsys, "search", "friday")
    assert len(e["data"]) == 1
    code, e = run(capsys, "search", "tomorrow", "--history")
    assert len(e["data"]) == 1 and e["data"][0]["revision_status"] == "revision_superseded"
    # deleted message is still present (source absence is evidence, not deletion); chats/identities intact
    code, e = run(capsys, "search", "chatter")
    assert len(e["data"]) == 1
    code, e = run(capsys, "chats")
    assert len(e["data"]) == 3
    code, e = run(capsys, "conflicts", "list")
    assert e["data"] == []


def e_list(capsys):
    return run(capsys, "conflicts", "list")[1]["data"]


def test_content_conflict_recorded_and_resolved(env, capsys, tmp_path):
    """Two independent data dirs observe different content for the same URN -> conflict on import."""
    _init_sync(capsys, env, "whatsapp")
    out_a = tmp_path / "a"
    code, e = run(capsys, "archive", "export", "-o", str(out_a), "--no-catalogue")
    assert code == 0
    # second machine: same export set (copied identity), different observation of message 2
    ident = json.loads((env["data"] / "identity.json").read_text())
    c = sqlite3.connect(env["wa"] / "ChatStorage.sqlite")
    c.execute("UPDATE ZWAMESSAGE SET ZTEXT='hi alice, changed elsewhere' WHERE Z_PK=2")
    c.commit()
    c.close()
    b = tmp_path / "b-data"
    os.environ["CHATSTORE_DATA_DIR"] = str(b)
    run(capsys, "init", "--whatsapp-path", str(env["wa"]), "--timezone", "UTC")
    ident_b = json.loads((b / "identity.json").read_text())
    ident_b["scopes"] = ident["scopes"]  # same account scope, so URNs coincide
    (b / "identity.json").write_text(json.dumps(ident_b))
    code, e = run(capsys, "sync", "--source", "whatsapp")
    assert code == 0
    out_b = tmp_path / "b"
    code, e = run(capsys, "archive", "export", "-o", str(out_b), "--no-catalogue")
    assert code == 0
    # import both into a third dir: different export sets -> not a branch conflict, but content differs
    dst = tmp_path / "dst"
    os.environ["CHATSTORE_DATA_DIR"] = str(dst)
    code, e = run(capsys, "archive", "import", str(out_a))
    assert code == 0
    code, e = run(capsys, "archive", "import", str(out_b))
    assert code == 6 and e["data"][0]["conflicts"] == 2  # the message and its text part
    code, e = run(capsys, "conflicts", "list")
    assert len(e["data"]) == 2 and all(c["kind"] == "content" for c in e["data"])
    code, e = run(capsys, "search", "elsewhere")
    assert e["data"] == []  # current view kept the first import
    for c in e_list(capsys):
        code, e = run(capsys, "conflicts", "resolve", str(c["id"]), "--keep", c["detail"]["incoming_digest"])
        assert code == 0
    code, e = run(capsys, "search", "elsewhere")
    assert len(e["data"]) == 1
    code, e = run(capsys, "conflicts", "list")
    assert e["data"] == []
