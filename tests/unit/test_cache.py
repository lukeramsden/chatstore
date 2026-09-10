import threading

import pytest

from chatstore.cache import Cache
from chatstore.cache import queries as Q
from chatstore.canonical.records import record, timestamp
from chatstore.identity.urn import derive_urn

SCOPE = "11111111-2222-4333-8444-555555555555"


def msg(n: int, text: str, chat: str, sender: str, ms: int, kind: str = "message"):
    urn = derive_urn("whatsapp", SCOPE, "message", f"m{n}")
    return record(
        "messages", urn, "whatsapp", SCOPE, native_key=f"m{n}", chat_urn=chat, chat_assignment="source_column",
        sender_urn=sender, is_from_me=False, transport="whatsapp", kind=kind,
        sent_at=timestamp(ms, "ms", "1970-01-01T00:00:00Z"), received_at=None, edited_at=None, retracted_at=None,
        state="unknown", reply_to_urn=None, reply_to_native=None, text=text, has_attachments=False, part_count=1,
        source_flags={}, tombstone=None,
    )


@pytest.fixture
def cache(tmp_path):
    c = Cache(tmp_path / "cache.sqlite3")
    chat = derive_urn("whatsapp", SCOPE, "chat", "c1")
    ident = derive_urn("whatsapp", SCOPE, "identity", "i1")
    with c.write():
        c.upsert(record("chats", chat, "whatsapp", SCOPE, native_key="c1", kind="direct", observed_name="Test Chat",
                        service=None, assignment="source", is_archived=False, created_at=None))
        c.upsert(record("identities", ident, "whatsapp", SCOPE, native_key="i1", kind="jid_phone", address="i1",
                        service=None, is_me=False, observed_names=[{"name": "Alice", "origin": "push_name"}]))
        for i in range(10):
            c.upsert(msg(i, f"hello number {i} café", chat, ident, 1_000_000 + i * 1000))
        c.upsert(msg(99, "👍", chat, ident, 1_500_000, kind="reaction"))
    yield c
    c.close()


def test_upsert_revision_semantics(cache):
    chat = derive_urn("whatsapp", SCOPE, "chat", "c1")
    ident = derive_urn("whatsapp", SCOPE, "identity", "i1")
    m = msg(0, "hello number 0 café", chat, ident, 1_000_000)
    with cache.write():
        assert cache.upsert(m) == "unchanged"
        m2 = msg(0, "edited text", chat, ident, 1_000_000)
        assert cache.upsert(m2) == "updated"
    revs = cache.revisions_of(m["urn"])
    assert len(revs) == 2 and revs[1]["supersedes"] == revs[0]["revision_digest"]
    r = Q.resolve(cache, m["urn"], revs[0]["revision_digest"])
    assert r["status"] == "revision_superseded"
    assert r["requested_revision"]["text"] == "hello number 0 café"


def test_import_conflict_detection(cache):
    chat = derive_urn("whatsapp", SCOPE, "chat", "c1")
    ident = derive_urn("whatsapp", SCOPE, "identity", "i1")
    m = msg(1, "different content from another machine", chat, ident, 1_001_000)
    with cache.write():
        assert cache.upsert(m, origin="import") == "conflict"
    assert cache.get(m["urn"])["text"] == "hello number 1 café"
    assert len(cache.conflicts()) == 1
    # If the importer knows our current digest as prior history, it may replace it.
    cur = cache.get(m["urn"])["revision_digest"]
    with cache.write():
        assert cache.upsert(m, origin="import", prior_digests={cur}) == "updated"


def test_search_literal_and_paging(cache):
    res, cur = Q.search(cache, "number", Q.Filters(), limit=4)
    assert len(res) == 4 and cur
    res2, cur2 = Q.search(cache, "number", Q.Filters(), limit=4, cursor=cur)
    assert len(res2) == 4 and res2[0]["urn"] != res[0]["urn"]
    res3, cur3 = Q.search(cache, "number", Q.Filters(), limit=4, cursor=cur2)
    assert len(res3) == 2 and cur3 is None
    assert res[0]["chat_label"] == "Test Chat" and res[0]["sender_label"] == "Alice"
    # diacritics-insensitive
    assert len(Q.search(cache, "cafe", Q.Filters(), limit=50)[0]) == 10
    # literal: FTS operators are not interpreted
    assert Q.search(cache, "number OR", Q.Filters(), limit=50)[0] == []
    assert Q.search(cache, 'hello "3"', Q.Filters(), limit=50)[0] != []  # quotes escaped, no error


def test_reactions_excluded_by_default(cache):
    assert Q.search(cache, "👍", Q.Filters(), limit=5)[0] == []
    assert len(Q.search(cache, "👍", Q.Filters(kinds=["reaction"]), limit=5)[0]) == 1


def test_advanced_syntax_error(cache):
    with pytest.raises(ValueError):
        Q.search(cache, "hello AND (", Q.Filters(), limit=5, advanced=True)


def test_context_and_read(cache):
    chat = derive_urn("whatsapp", SCOPE, "chat", "c1")
    target = derive_urn("whatsapp", SCOPE, "message", "m5")
    ctx = Q.context(cache, target, 2, 2, 20000)
    assert ctx["target"]["urn"] == target and len(ctx["before"]) == 2 and len(ctx["after"]) == 2
    msgs, cur, trunc = Q.read_chat(cache, chat, Q.Filters(), limit=3, max_chars=25)
    assert len(msgs) == 3 and cur and trunc


def test_rebuild_fts_equivalence(cache):
    before = Q.search(cache, "number", Q.Filters(), limit=50)[0]
    cache.rebuild_fts()
    after = Q.search(cache, "number", Q.Filters(), limit=50)[0]
    assert [m["urn"] for m in before] == [m["urn"] for m in after]


def test_concurrent_readers_serialised_writer(tmp_path):
    c1 = Cache(tmp_path / "c.sqlite3")
    chat = derive_urn("whatsapp", SCOPE, "chat", "c1")
    ident = derive_urn("whatsapp", SCOPE, "identity", "i1")
    errors = []

    def writer(k):
        c = Cache(tmp_path / "c.sqlite3")
        try:
            for i in range(20):
                with c.write():
                    c.upsert(msg(k * 100 + i, f"w{k} {i}", chat, ident, 1000 + i))
        except Exception as e:  # noqa: BLE001
            errors.append(e)
        finally:
            c.close()

    def reader():
        c = Cache(tmp_path / "c.sqlite3", readonly=True)
        try:
            for _ in range(50):
                c.counts()
        except Exception as e:  # noqa: BLE001
            errors.append(e)
        finally:
            c.close()

    ts = [threading.Thread(target=writer, args=(k,)) for k in range(3)] + [threading.Thread(target=reader) for _ in range(3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors
    assert c1.counts()["messages"] == 60
    c1.close()


def test_resolve_unknown_and_alias(cache):
    assert Q.resolve(cache, "urn:uuid:00000000-0000-0000-0000-000000000000")["status"] == "unknown"
    old = "urn:uuid:00000000-0000-0000-0000-000000000001"
    new = derive_urn("whatsapp", SCOPE, "message", "m1")
    with cache.write():
        cache.upsert(record("aliases", "urn:uuid:00000000-0000-0000-0000-00000000000a", None, None, old_urn=old, new_urn=new,
                            reason="manual", evidence=[], created_at=1))
    r = Q.resolve(cache, old)
    assert r["status"] == "current" and r["resolved_via_alias"] == old and r["urn"] == new
