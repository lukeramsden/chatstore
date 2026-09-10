"""Read queries backing the CLI. All SQL is parameterised; FTS input is literal by default."""

from __future__ import annotations

import base64
import json
import sqlite3
from typing import Any

from . import fts
from .store import Cache


def encode_cursor(utc_ms: int | None, urn: str) -> str:
    return base64.urlsafe_b64encode(json.dumps([utc_ms, urn]).encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> tuple[int | None, str] | None:
    if not cursor:
        return None
    pad = "=" * (-len(cursor) % 4)
    try:
        utc_ms, urn = json.loads(base64.urlsafe_b64decode(cursor + pad))
    except Exception as e:
        raise ValueError("invalid cursor") from e
    return utc_ms, urn


class Filters:
    def __init__(self, *, since_ms: int | None = None, until_ms: int | None = None, source: str | None = None,
                 chat_urn: str | None = None, sender_urns: list[str] | None = None,
                 transport: str | None = None, kinds: list[str] | None = None,
                 has_attachment: bool | None = None, scope: str | None = None):
        self.since_ms, self.until_ms, self.source = since_ms, until_ms, source
        self.chat_urn, self.sender_urns, self.transport = chat_urn, sender_urns, transport
        self.kinds = kinds or ["message"]
        self.has_attachment, self.scope = has_attachment, scope

    def sql(self, alias: str = "m") -> tuple[str, list[Any]]:
        parts: list[str] = []
        args: list[Any] = []
        if self.since_ms is not None:
            parts.append(f"{alias}.utc_ms >= ?")
            args.append(self.since_ms)
        if self.until_ms is not None:
            parts.append(f"{alias}.utc_ms < ?")
            args.append(self.until_ms)
        if self.source:
            parts.append(f"{alias}.source = ?")
            args.append(self.source)
        if self.scope:
            parts.append(f"{alias}.account_scope = ?")
            args.append(self.scope)
        if self.chat_urn:
            parts.append(f"{alias}.chat_urn = ?")
            args.append(self.chat_urn)
        if self.sender_urns:
            parts.append(f"{alias}.sender_urn IN ({','.join('?' * len(self.sender_urns))})")
            args.extend(self.sender_urns)
        if self.transport:
            parts.append(f"{alias}.transport = ?")
            args.append(self.transport)
        if self.kinds and "all" not in self.kinds:
            parts.append(f"{alias}.kind IN ({','.join('?' * len(self.kinds))})")
            args.extend(self.kinds)
        if self.has_attachment is not None:
            parts.append(f"{alias}.has_attachments = ?")
            args.append(1 if self.has_attachment else 0)
        return (" AND ".join(parts) if parts else "1=1"), args


def search(cache: Cache, query: str, flt: Filters, *, limit: int, cursor: str | None = None,
           advanced: bool = False, history: bool = False) -> tuple[list[dict[str, Any]], str | None]:
    conn = cache.conn
    q = query if advanced else fts.literal_query(query)
    where, args = flt.sql("m")
    cur = decode_cursor(cursor)
    cursor_sql = ""
    if cur:
        cursor_sql = " AND (m.utc_ms > ? OR (m.utc_ms = ? AND m.urn > ?))"
        args = args + [cur[0], cur[0], cur[1]]
    if history:
        sql = f"""
            SELECT m.urn, rv.revision_digest AS rd, snippet(revisions_fts, 0, '[', ']', '…', 12) AS snippet,
                   m.utc_ms, m.chat_urn, m.sender_urn, m.transport, m.kind, m.source, m.account_scope
            FROM revisions_fts f JOIN revisions rv ON rv.id = f.rowid JOIN messages_idx m ON m.urn = rv.entity_urn
            WHERE revisions_fts MATCH ? AND {where}{cursor_sql}
            ORDER BY m.utc_ms, m.urn LIMIT ?"""
    else:
        sql = f"""
            SELECT m.urn, NULL AS rd, snippet(messages_fts, 0, '[', ']', '…', 12) AS snippet,
                   m.utc_ms, m.chat_urn, m.sender_urn, m.transport, m.kind, m.source, m.account_scope
            FROM messages_fts f JOIN messages_idx m ON m.id = f.rowid
            WHERE messages_fts MATCH ? AND {where}{cursor_sql}
            ORDER BY m.utc_ms, m.urn LIMIT ?"""
    try:
        rows = conn.execute(sql, [q, *args, limit + 1]).fetchall()
    except sqlite3.OperationalError as e:
        raise ValueError(f"invalid search syntax: {e}") from e
    return _page(cache, rows, limit, history)


def _page(cache: Cache, rows: list[sqlite3.Row], limit: int, history: bool) -> tuple[list[dict[str, Any]], str | None]:
    more = len(rows) > limit
    rows = rows[:limit]
    recs = cache.get_many([r["urn"] for r in rows])
    labels = label_map(cache, [r["chat_urn"] for r in rows] + [r["sender_urn"] for r in rows])
    out = []
    for r in rows:
        rec = recs.get(r["urn"], {})
        out.append({
            "urn": r["urn"],
            "revision_digest": r["rd"] if history else rec.get("revision_digest"),
            "revision_status": ("revision_superseded" if history and r["rd"] != rec.get("revision_digest") else "current"),
            "chat_urn": r["chat_urn"], "chat_label": labels.get(r["chat_urn"]),
            "sender_urn": r["sender_urn"], "sender_label": labels.get(r["sender_urn"]),
            "sent_at": rec.get("sent_at"), "transport": r["transport"], "kind": r["kind"],
            "snippet": r["snippet"],
            "provenance": {"source": r["source"], "account_scope": r["account_scope"]},
        })
    next_cursor = encode_cursor(rows[-1]["utc_ms"], rows[-1]["urn"]) if more and rows else None
    return out, next_cursor


def label_map(cache: Cache, urns: list[str | None]) -> dict[str, str | None]:
    recs = cache.get_many([u for u in urns if u])
    out: dict[str, str | None] = {}
    for u, r in recs.items():
        out[u] = label_of(r)
    return out


def label_of(r: dict[str, Any]) -> str | None:
    if r.get("entity") == "chats":
        return r.get("observed_name") or r.get("native_key")
    if r.get("entity") == "identities":
        if r.get("is_me"):
            return "me"
        names = r.get("observed_names") or []
        return (names[0]["name"] if names else None) or r.get("address")
    if r.get("entity") == "people":
        return r.get("label")
    return None


def read_chat(cache: Cache, chat_urn: str, flt: Filters, *, limit: int, cursor: str | None = None,
              max_chars: int) -> tuple[list[dict[str, Any]], str | None, bool]:
    flt.chat_urn = chat_urn
    where, args = flt.sql("m")
    cur = decode_cursor(cursor)
    cursor_sql = ""
    if cur:
        cursor_sql = " AND (m.utc_ms > ? OR (m.utc_ms = ? AND m.urn > ?))"
        args += [cur[0], cur[0], cur[1]]
    rows = cache.conn.execute(
        f"SELECT m.urn, m.utc_ms FROM messages_idx m WHERE {where}{cursor_sql} ORDER BY m.utc_ms, m.urn LIMIT ?",
        [*args, limit + 1]).fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    msgs = hydrate_messages(cache, [r["urn"] for r in rows])
    truncated = False
    total = 0
    for m in msgs:
        t = m.get("text") or ""
        if total + len(t) > max_chars:
            m["text"] = t[: max(0, max_chars - total)]
            m["truncated"] = True
            truncated = True
        total += len(t)
    next_cursor = encode_cursor(rows[-1]["utc_ms"], rows[-1]["urn"]) if more and rows else None
    return msgs, next_cursor, truncated


def hydrate_messages(cache: Cache, urns: list[str]) -> list[dict[str, Any]]:
    """Attach parts, attachments, events and labels to message records, preserving order."""
    recs = cache.get_many(urns)
    conn = cache.conn
    by_msg_parts: dict[str, list[str]] = {}
    by_msg_att: dict[str, list[str]] = {}
    by_msg_ev: dict[str, list[str]] = {}
    for i in range(0, len(urns), 400):
        chunk = urns[i:i + 400]
        ph = ",".join("?" * len(chunk))
        for r in conn.execute(f"SELECT urn, message_urn FROM parts_idx WHERE message_urn IN ({ph}) ORDER BY idx", chunk):
            by_msg_parts.setdefault(r[1], []).append(r[0])
        for r in conn.execute(f"SELECT urn, message_urn FROM attachments_idx WHERE message_urn IN ({ph})", chunk):
            by_msg_att.setdefault(r[1], []).append(r[0])
        for r in conn.execute(f"SELECT urn, target_urn FROM events_idx WHERE target_urn IN ({ph}) ORDER BY at_ms", chunk):
            by_msg_ev.setdefault(r[1], []).append(r[0])
    sub = cache.get_many([u for d in (by_msg_parts, by_msg_att, by_msg_ev) for lst in d.values() for u in lst])
    labels = label_map(cache, [recs[u].get("sender_urn") for u in urns if u in recs] + [recs[u].get("chat_urn") for u in urns if u in recs])
    out = []
    for u in urns:
        m = recs.get(u)
        if not m:
            continue
        m = dict(m)
        m["sender_label"] = labels.get(m.get("sender_urn") or "")
        m["chat_label"] = labels.get(m.get("chat_urn") or "")
        m["parts"] = [sub[p] for p in by_msg_parts.get(u, []) if p in sub]
        m["attachments"] = [sub[a] for a in by_msg_att.get(u, []) if a in sub]
        m["events"] = [sub[e] for e in by_msg_ev.get(u, []) if e in sub]
        out.append(m)
    return out


def context(cache: Cache, message_urn: str, before: int, after: int, max_chars: int) -> dict[str, Any] | None:
    m = cache.conn.execute("SELECT chat_urn, utc_ms FROM messages_idx WHERE urn=?", (message_urn,)).fetchone()
    if not m:
        return None
    chat_urn, utc_ms = m["chat_urn"], m["utc_ms"]
    b = cache.conn.execute(
        "SELECT urn FROM messages_idx WHERE chat_urn IS ? AND (utc_ms < ? OR (utc_ms = ? AND urn < ?)) ORDER BY utc_ms DESC, urn DESC LIMIT ?",
        (chat_urn, utc_ms, utc_ms, message_urn, before)).fetchall()
    a = cache.conn.execute(
        "SELECT urn FROM messages_idx WHERE chat_urn IS ? AND (utc_ms > ? OR (utc_ms = ? AND urn > ?)) ORDER BY utc_ms, urn LIMIT ?",
        (chat_urn, utc_ms, utc_ms, message_urn, after)).fetchall()
    urns = [r[0] for r in reversed(b)] + [message_urn] + [r[0] for r in a]
    msgs = hydrate_messages(cache, urns)
    budget = max_chars
    for mm in msgs:
        t = mm.get("text") or ""
        if len(t) > budget:
            mm["text"], mm["truncated"] = t[:budget], True
        budget -= min(len(t), budget)
    nb = len(b)
    return {"target": msgs[nb] if len(msgs) > nb else None, "before": msgs[:nb], "after": msgs[nb + 1:]}


def list_chats(cache: Cache, *, source: str | None, since_ms: int | None, limit: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
    args: list[Any] = []
    where = "r.kind='chats'"
    if source:
        where += " AND r.source=?"
        args.append(source)
    having = ""
    if since_ms is not None:
        having = " HAVING max(m.utc_ms) >= ?"
        args.append(since_ms)
    cur = decode_cursor(cursor)
    # Order by last message desc then urn; cursor carries (last_ms, urn).
    sql = f"""
        SELECT r.urn, r.record, count(m.urn) AS n, max(m.utc_ms) AS last_ms
        FROM records r LEFT JOIN messages_idx m ON m.chat_urn = r.urn AND m.kind='message'
        WHERE {where} GROUP BY r.urn{having}
        ORDER BY last_ms DESC NULLS LAST, r.urn"""
    rows = cache.conn.execute(sql, args).fetchall()
    if cur:
        cms, curn = cur
        def after(r: sqlite3.Row) -> bool:
            lm = r["last_ms"]
            if cms is None:
                return lm is None and r["urn"] > curn
            return lm is None or lm < cms or (lm == cms and r["urn"] > curn)
        rows = [r for r in rows if after(r)]
    more = len(rows) > limit
    rows = rows[:limit]
    members: dict[str, list[str]] = {}
    for r in rows:
        members[r["urn"]] = [x[0] for x in cache.conn.execute("SELECT identity_urn FROM memberships_idx WHERE chat_urn=? LIMIT 50", (r["urn"],))]
    labels = label_map(cache, [u for lst in members.values() for u in lst])
    out = []
    for r in rows:
        rec = json.loads(r["record"])
        out.append({
            "urn": rec["urn"], "chat_kind": rec.get("kind"), "label": label_of(rec), "service": rec.get("service"),
            "assignment": rec.get("assignment"), "message_count": r["n"], "last_message_utc_ms": r["last_ms"],
            "participants": [{"urn": u, "label": labels.get(u)} for u in members[rec["urn"]]],
            "provenance": {"source": rec.get("source"), "account_scope": rec.get("account_scope")},
        })
    next_cursor = encode_cursor(rows[-1]["last_ms"], rows[-1]["urn"]) if more and rows else None
    return out, next_cursor


def list_people(cache: Cache, *, limit: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
    cur = decode_cursor(cursor)
    args: list[Any] = []
    where = "kind='people'"
    if cur:
        where += " AND urn > ?"
        args.append(cur[1])
    rows = cache.conn.execute(f"SELECT record FROM records WHERE {where} ORDER BY urn LIMIT ?", [*args, limit + 1]).fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    out = []
    for r in rows:
        p = json.loads(r[0])
        links = cache.conn.execute("SELECT identity_urn, state FROM links_idx WHERE person_urn=?", (p["urn"],)).fetchall()
        idents = cache.get_many([lk[0] for lk in links])
        out.append({
            "urn": p["urn"], "label": p.get("label"),
            "identities": [{"urn": lk[0], "link_state": lk[1], "address": idents.get(lk[0], {}).get("address"),
                            "kind": idents.get(lk[0], {}).get("kind"), "source": idents.get(lk[0], {}).get("source")} for lk in links],
        })
    next_cursor = encode_cursor(None, json.loads(rows[-1][0])["urn"]) if more and rows else None
    return out, next_cursor


def resolve(cache: Cache, urn: str, digest: str | None = None) -> dict[str, Any]:
    rec = cache.get(urn)
    aliases_to = cache.aliases_to(urn)
    aliases_from = cache.aliases_from(urn)
    if rec is None:
        if len(aliases_from) == 1:
            target = resolve(cache, aliases_from[0], digest)
            target["resolved_via_alias"] = urn
            return target
        if len(aliases_from) > 1:
            return {"urn": urn, "status": "ambiguous", "candidates": aliases_from}
        return {"urn": urn, "status": "unknown"}
    status = "current"
    if rec.get("tombstone"):
        status = "tombstoned"
    elif rec.get("entity") == "chats" and rec.get("assignment") == "stub":
        status = "unavailable"
    revs = cache.revisions_of(urn)
    if digest and digest != rec.get("revision_digest"):
        status = "revision_superseded" if any(r["revision_digest"] == digest for r in revs) else "unknown_revision"
    return {
        "urn": urn, "status": status, "entity": rec.get("entity"), "record": rec,
        "requested_revision": (cache.revision_record(urn, digest) if digest and status == "revision_superseded" else None),
        "aliases": {"from": aliases_to, "to": aliases_from},
        "revisions": revs, "observations": cache.observations_of(urn),
    }
