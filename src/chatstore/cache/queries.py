"""Read queries backing the CLI. All SQL is parameterised; FTS input is literal by default."""

from __future__ import annotations

import base64
import json
import sqlite3
from collections.abc import Callable
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
    """Labels for identities, people and chats. Unnamed chats fall back to their counterpart
    identity's label, then to the bare handle in their native key (issue #2)."""
    recs = cache.get_many([u for u in urns if u])
    out: dict[str, str | None] = {}
    unnamed: list[str] = []
    for u, r in recs.items():
        out[u] = label_of(r)
        if r.get("entity") == "chats" and not r.get("observed_name"):
            unnamed.append(u)
    for u, counterpart in _counterpart_labels(cache, unnamed).items():
        out[u] = counterpart
    return out


def _counterpart_labels(cache: Cache, chat_urns: list[str]) -> dict[str, str]:
    """For each chat with exactly one non-me participant, that participant's label."""
    if not chat_urns:
        return {}
    members: dict[str, list[str]] = {}
    for i in range(0, len(chat_urns), 500):
        chunk = chat_urns[i:i + 500]
        q = f"SELECT chat_urn, identity_urn FROM memberships_idx WHERE chat_urn IN ({','.join('?' * len(chunk))})"
        for r in cache.conn.execute(q, chunk):
            members.setdefault(r[0], []).append(r[1])
    idents = cache.get_many([u for lst in members.values() for u in lst])
    out: dict[str, str] = {}
    for chat_urn, lst in members.items():
        others = [idents[u] for u in lst if u in idents and not idents[u].get("is_me")]
        if len(others) == 1:
            label = label_of(others[0])
            if label:
                out[chat_urn] = label
    return out


def _handle_from_native_key(native_key: str | None) -> str | None:
    """Bare handle from a composite chat key such as ["guid","SMS;-;+1555"] -> "+1555"."""
    if not native_key or not native_key.startswith("["):
        return native_key
    try:
        parts = json.loads(native_key)
    except ValueError:
        return native_key
    if not (isinstance(parts, list) and len(parts) == 2 and all(isinstance(p, str) for p in parts)):
        return native_key
    ident = parts[1]
    return ident.rsplit(";", 1)[-1] or native_key


def label_of(r: dict[str, Any]) -> str | None:
    if r.get("entity") == "chats":
        return r.get("observed_name") or _handle_from_native_key(r.get("native_key"))
    if r.get("entity") == "identities":
        if r.get("is_me"):
            return "me"
        names = r.get("observed_names") or []
        return (names[0]["name"] if names else None) or r.get("address")
    if r.get("entity") == "people":
        return r.get("label")
    return None


def read_chat(cache: Cache, chat_urn: str, flt: Filters, *, limit: int, cursor: str | None = None,
              max_chars: int, order: str = "asc") -> tuple[list[dict[str, Any]], str | None, bool]:
    """Page a chat. `order` is `asc` (oldest first, default) or `desc` (newest first); the cursor
    continues in the same direction."""
    if order not in ("asc", "desc"):
        raise ValueError(f"order must be asc or desc, not {order!r}")
    flt.chat_urn = chat_urn
    where, args = flt.sql("m")
    cur = decode_cursor(cursor)
    cursor_sql = ""
    if cur:
        op = ">" if order == "asc" else "<"
        cursor_sql = f" AND (m.utc_ms {op} ? OR (m.utc_ms = ? AND m.urn {op} ?))"
        args += [cur[0], cur[0], cur[1]]
    direction = "" if order == "asc" else " DESC"
    rows = cache.conn.execute(
        f"SELECT m.urn, m.utc_ms FROM messages_idx m WHERE {where}{cursor_sql} "
        f"ORDER BY m.utc_ms{direction}, m.urn{direction} LIMIT ?",
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
    labels = label_map(cache, [u for lst in members.values() for u in lst] + [r["urn"] for r in rows])
    out = []
    for r in rows:
        rec = json.loads(r["record"])
        out.append({
            "urn": rec["urn"], "chat_kind": rec.get("kind"), "label": labels.get(rec["urn"]), "service": rec.get("service"),
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


# ---- media ---------------------------------------------------------------------------------------------

AVAILABILITY_REASONS = {
    "available": "the source app had the file when last synced; sha256 recorded",
    "not_downloaded": "the app never downloaded the media (open the chat in the app and download it, then re-sync)",
    "missing": "the app's database referenced a file that was no longer on disk (deleted, expired or purged)",
    "not_exported": "restored from a text-only archive; the exporting machine did not include the file",
    "unknown": "the source gave no usable path or size for this attachment",
}

_ATT_SQL = """SELECT a.urn AS urn, a.availability AS availability, a.blob_sha256 AS sha, r.record AS record,
                     m.urn AS message_urn, m.chat_urn AS chat_urn, m.sender_urn AS sender_urn, m.utc_ms AS utc_ms, m.source AS source
              FROM attachments_idx a JOIN records r ON r.urn=a.urn LEFT JOIN messages_idx m ON m.urn=a.message_urn"""


def _att_where(source: str | None, chat_urn: str | None, availability: str | None = None) -> tuple[list[str], list[Any]]:
    where, params = ["1=1"], list[Any]()
    if availability:
        where.append("a.availability=?")
        params.append(availability)
    if source:
        where.append("m.source=?")
        params.append(source)
    if chat_urn:
        where.append("m.chat_urn=?")
        params.append(chat_urn)
    return where, params


def media_summary(cache: Cache, local_state: Callable[[dict[str, Any]], str], *, source: str | None = None,
                  chat_urn: str | None = None) -> list[dict[str, Any]]:
    """Attachment counts and declared bytes per (source, availability-as-observed, local_state-now)."""
    where, params = _att_where(source, chat_urn)
    groups: dict[tuple[Any, str, str], dict[str, Any]] = {}
    for r in cache.conn.execute(f"{_ATT_SQL} WHERE {' AND '.join(where)}", params):
        rec = json.loads(r["record"])
        key = (r["source"], r["availability"], local_state(rec))
        g = groups.setdefault(key, {"source": key[0], "availability": key[1], "local_state": key[2], "count": 0,
                                    "declared_bytes": 0, "hashed": 0, "reason": AVAILABILITY_REASONS.get(key[1], "")})
        g["count"] += 1
        g["declared_bytes"] += rec.get("declared_size") or 0
        g["hashed"] += 1 if r["sha"] else 0
    return [groups[k] for k in sorted(groups, key=lambda k: (k[0] or "", k[1], k[2]))]


def media_by_chat(cache: Cache, *, source: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """Chats with the most attachments the source app did not have (as last observed)."""
    where, params = ["a.availability != 'available'"], list[Any]()
    if source:
        where.append("m.source=?")
        params.append(source)
    rows = cache.conn.execute(
        f"""SELECT m.chat_urn AS chat_urn, m.source AS source, count(*) AS n,
                   sum(CASE WHEN a.availability='not_downloaded' THEN 1 ELSE 0 END) AS not_downloaded,
                   sum(CASE WHEN a.availability='missing' THEN 1 ELSE 0 END) AS missing,
                   sum(CASE WHEN a.availability='not_exported' THEN 1 ELSE 0 END) AS not_exported,
                   sum(CASE WHEN a.availability='unknown' THEN 1 ELSE 0 END) AS unknown,
                   sum(coalesce(json_extract(r.record,'$.declared_size'),0)) AS bytes, max(m.utc_ms) AS last_ms
            FROM attachments_idx a JOIN records r ON r.urn=a.urn JOIN messages_idx m ON m.urn=a.message_urn
            WHERE {' AND '.join(where)} GROUP BY m.chat_urn ORDER BY n DESC, m.chat_urn LIMIT ?""", [*params, limit]).fetchall()
    labels = label_map(cache, [r["chat_urn"] for r in rows])
    return [{"chat_urn": r["chat_urn"], "chat_label": labels.get(r["chat_urn"]), "source": r["source"], "unavailable": r["n"],
             "not_downloaded": r["not_downloaded"], "missing": r["missing"], "not_exported": r["not_exported"],
             "unknown": r["unknown"], "declared_bytes": r["bytes"], "last_message_utc_ms": r["last_ms"]} for r in rows]


def media_list(cache: Cache, local_state: Callable[[dict[str, Any]], str], *, availability: str | None = None,
               local: str | None = None, source: str | None = None, chat_urn: str | None = None,
               since_ms: int | None = None, until_ms: int | None = None, limit: int = 50,
               cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
    """Attachments, newest first, paginated by (utc_ms, urn). `local` filters on the derived local_state,
    which is not indexed, so it is applied while scanning forward from the cursor."""
    where, params = _att_where(source, chat_urn, availability)
    if since_ms is not None:
        where.append("m.utc_ms>=?")
        params.append(since_ms)
    if until_ms is not None:
        where.append("m.utc_ms<?")
        params.append(until_ms)
    if cursor:
        try:
            c_ms, c_urn = cursor.split("|", 1)
            params += [int(c_ms), int(c_ms), c_urn]
        except ValueError as e:
            raise ValueError("bad cursor") from e
        where.append("(coalesce(m.utc_ms,0) < ? OR (coalesce(m.utc_ms,0) = ? AND a.urn > ?))")
    out: list[dict[str, Any]] = []
    last: Any = None
    has_next = False
    for r in cache.conn.execute(f"{_ATT_SQL} WHERE {' AND '.join(where)} ORDER BY coalesce(m.utc_ms,0) DESC, a.urn ASC", params):
        rec = json.loads(r["record"])
        state = local_state(rec)
        if local and state != local:
            continue
        if len(out) == limit:
            has_next = True
            break
        last = r
        out.append({
            "urn": r["urn"], "message_urn": r["message_urn"], "chat_urn": r["chat_urn"], "chat_label": None,
            "sender_urn": r["sender_urn"], "sender_label": None, "source": r["source"],
            "sent_at_utc_ms": r["utc_ms"], "availability": r["availability"], "local_state": state, "kind": rec.get("kind"),
            "mime_type": rec.get("mime_type"), "declared_size": rec.get("declared_size"), "blob_sha256": r["sha"],
            "source_path_hint": rec.get("source_path_hint"), "filename": rec.get("filename"),
        })
    labels = label_map(cache, [x["chat_urn"] for x in out] + [x["sender_urn"] for x in out])
    for x in out:
        x["chat_label"] = labels.get(x["chat_urn"])
        x["sender_label"] = labels.get(x["sender_urn"])
    nxt = f"{last['utc_ms'] or 0}|{last['urn']}" if has_next and last is not None else None
    return out, nxt
