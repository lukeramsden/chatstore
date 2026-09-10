"""User curation: people/identity links, suggestions, scope mapping, conflict resolution, purge.

Everything here is explicit user action; nothing runs automatically during sync.
"""

from __future__ import annotations

import json
import re
from typing import Any

from .cache import fts
from .cache.store import Cache
from .canonical.records import now_ms, record
from .config import Identity
from .identity import urn as U

# entity table -> entity_type used in URN derivation (see specs/identity.md)
ENTITY_TYPE = {
    "identities": "identity", "chats": "chat", "messages": "message", "attachments": "attachment",
    "blobs": "attachment", "message_parts": "event", "events": "event", "chat_memberships": "event",
    "aliases": "event", "accounts": "identity",
}
LOCAL_SOURCE = "chatstore"  # pseudo-source for source-independent records (people, links)


def _link_urn(person_urn: str, identity_urn: str) -> str:
    # deterministic so re-linking the same pair is idempotent; no account scope involved
    import uuid
    return uuid.uuid5(U.NAMESPACE, U.compact_json(["identity_link", person_urn, identity_urn])).urn


# ---- people & links -------------------------------------------------------------------------------

def create_person(cache: Cache, label: str | None, notes: str | None = None) -> dict[str, Any]:
    rec = record("people", U.mint_urn(), None, None, label=label, notes=notes, created_at=now_ms())
    cache.upsert(rec)
    return rec


def link(cache: Cache, person_urn: str, identity_urns: list[str], *, state: str = "confirmed",
         evidence: list[dict[str, str]] | None = None) -> list[dict[str, Any]]:
    person = cache.get(person_urn)
    if person is None or person.get("entity") != "people":
        raise ValueError(f"{person_urn} is not a person")
    out = []
    for iu in identity_urns:
        ident = cache.get(iu)
        if ident is None or ident.get("entity") != "identities":
            raise ValueError(f"{iu} is not an identity")
        rec = record("identity_links", _link_urn(person_urn, iu), None, None, person_urn=person_urn, identity_urn=iu,
                     state=state, evidence=evidence or [{"type": "manual", "detail": "chatstore identity link"}],
                     created_at=now_ms(), revoked_at=None)
        cache.upsert(rec)
        out.append(rec)
    return out


def unlink(cache: Cache, identity_urn: str, person_urn: str | None = None) -> list[dict[str, Any]]:
    q = "SELECT urn FROM links_idx WHERE identity_urn=? AND state!='rejected'"
    args: list[Any] = [identity_urn]
    if person_urn:
        q += " AND person_urn=?"
        args.append(person_urn)
    out = []
    for (lu,) in cache.conn.execute(q, args).fetchall():
        rec = cache.get(lu)
        if not rec:
            continue
        rec = {k: v for k, v in rec.items() if k not in ("observed_at", "sync_run", "revision_digest")}
        rec["state"] = "rejected"
        rec["revoked_at"] = now_ms()
        rec["revision_digest"] = U.revision_digest(rec)
        cache.upsert(rec)
        out.append(rec)
    return out


_DIGITS = re.compile(r"\D+")


def normalise_address(kind: str, address: str) -> tuple[str, str] | None:
    """Return (norm_type, key) for cross-source matching, or None."""
    a = address.strip().lower()
    if kind in ("jid_user", "phone") or a.endswith("@s.whatsapp.net") or a.startswith("+"):
        digits = _DIGITS.sub("", a.split("@", 1)[0])
        if len(digits) >= 8:
            return "normalised_phone", digits[-10:]  # last 10 digits: tolerant of country-code formatting
    if "@" in a and not a.endswith(("@s.whatsapp.net", "@lid", "@g.us", "@broadcast")):
        return "normalised_email", a
    return None


def suggest(cache: Cache, *, limit: int = 200) -> list[dict[str, Any]]:
    """Suggested identity links: identities across sources that normalise to the same key."""
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in cache.conn.execute("SELECT record FROM records WHERE kind='identities' AND tombstoned=0"):
        ident = json.loads(r[0])
        if ident.get("is_me"):
            continue
        n = normalise_address(ident.get("kind", ""), ident.get("address") or "")
        if n:
            groups.setdefault(n, []).append(ident)
    linked = {r[0] for r in cache.conn.execute("SELECT identity_urn FROM links_idx WHERE state='confirmed'")}
    rejected = {r[0] for r in cache.conn.execute("SELECT identity_urn FROM links_idx WHERE state='rejected'")}
    out = []
    for (ntype, key), idents in sorted(groups.items()):
        sources = {i["source"] for i in idents}
        if len(idents) < 2 or len(sources) < 2:
            continue
        if all(i["urn"] in linked for i in idents) or any(i["urn"] in rejected for i in idents):
            continue
        out.append({
            "evidence": {"type": ntype, "detail": f"…{key[-4:]}" if ntype == "normalised_phone" else "same e-mail"},
            "identities": [{"urn": i["urn"], "source": i["source"], "kind": i.get("kind"),
                            "names": [n.get("name") for n in (i.get("observed_names") or [])][:3],
                            "already_linked": i["urn"] in linked} for i in idents],
        })
        if len(out) >= limit:
            break
    return out


# ---- scopes -------------------------------------------------------------------------------------

def list_scopes(cache: Cache, ident: Identity) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for src, v in ident.scopes.items():
        seen[v["scope"]] = {"scope": v["scope"], "source": src, "label": v.get("label"), "origin": v.get("origin", "local"),
                            "created_at": v.get("created_at"), "records": 0}
    for r in cache.conn.execute("SELECT account_scope, source, count(*) FROM records WHERE account_scope IS NOT NULL GROUP BY 1,2"):
        e = seen.setdefault(r[0], {"scope": r[0], "source": r[1], "label": None, "origin": "records", "created_at": None, "records": 0})
        e["records"] += r[2]
    for m in ident.scope_mappings:
        if m.get("from") in seen:
            seen[m["from"]]["mapped_to"] = m.get("to")
    return sorted(seen.values(), key=lambda e: (e["source"] or "", e["scope"]))


def map_scope(cache: Cache, ident: Identity, from_scope: str, to_scope: str) -> dict[str, Any]:
    """Declare that account scope `from_scope` is the same account as `to_scope`.

    Emits alias records old→new for every derivable record in `from_scope`. Nothing is rewritten
    or deleted; both URNs stay resolvable.
    """
    if from_scope == to_scope:
        raise ValueError("from and to scopes are identical")
    src_rows = cache.conn.execute("SELECT DISTINCT source FROM records WHERE account_scope=?", (from_scope,)).fetchall()
    if not src_rows:
        raise ValueError(f"no records in scope {from_scope}")
    sources = {r[0] for r in src_rows}
    to_sources = {r[0] for r in cache.conn.execute("SELECT DISTINCT source FROM records WHERE account_scope=?", (to_scope,))}
    to_sources |= {s for s, v in ident.scopes.items() if v["scope"] == to_scope}
    if to_sources and not sources <= to_sources:
        raise ValueError(f"scope {from_scope} ({','.join(sorted(sources))}) and {to_scope} ({','.join(sorted(to_sources))}) belong to different sources")
    created = skipped = 0
    ts = now_ms()
    for r in cache.conn.execute(
            "SELECT o.entity_urn, o.source, o.native_key, rc.kind FROM source_observations o JOIN records rc ON rc.urn=o.entity_urn "
            "WHERE o.account_scope=? AND o.minted=0 AND o.native_key IS NOT NULL", (from_scope,)).fetchall():
        et = ENTITY_TYPE.get(r["kind"])
        if et is None:
            skipped += 1
            continue
        new_urn = U.derive_urn(r["source"], to_scope, et, r["native_key"])
        if new_urn == r["entity_urn"]:
            continue
        a_urn = U.derive_urn(r["source"], from_scope, "event", U.composite_key("scope_map", r["entity_urn"], to_scope))
        rec = record("aliases", a_urn, r["source"], from_scope, old_urn=r["entity_urn"], new_urn=new_urn, reason="scope_map",
                     evidence=[{"type": "manual", "detail": f"chatstore scope map {from_scope} -> {to_scope}"}], created_at=ts)
        if cache.upsert(rec) == "inserted":
            created += 1
    if not any(m.get("from") == from_scope and m.get("to") == to_scope for m in ident.scope_mappings):
        ident.scope_mappings.append({"from": from_scope, "to": to_scope, "created_at": ts})
    return {"from": from_scope, "to": to_scope, "aliases_created": created, "skipped": skipped}


# ---- conflicts ------------------------------------------------------------------------------------

def resolve_conflict(cache: Cache, conflict_id: int, *, keep: str | None = None, accept: bool = False) -> dict[str, Any]:
    row = cache.conn.execute("SELECT id, kind, entity_urn, detail, resolved_at FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
    if row is None:
        raise KeyError(conflict_id)
    if row["resolved_at"]:
        raise ValueError("conflict already resolved")
    detail = json.loads(row["detail"])
    if row["kind"] == "content":
        if not keep:
            raise ValueError("content conflicts need --keep <revision_digest>")
        urn = row["entity_urn"]
        rev = cache.revision_record(urn, keep)
        if rev is None:
            raise ValueError(f"{keep} is not a known revision of {urn}")
        rec = {k: v for k, v in rev.items() if k not in ("observed_at", "sync_run")}
        rec["revision_digest"] = keep
        cur = cache.get(urn)
        if cur and cur.get("revision_digest") != keep:
            # make the chosen revision current by forcing an update (a new head that supersedes the current one)
            cache.conn.execute("UPDATE records SET revision_digest=?, record=?, observed_at=? WHERE urn=?",
                               (keep, json.dumps(rev, ensure_ascii=False, sort_keys=True, separators=(",", ":")), now_ms(), urn))
            cache.conn.execute("UPDATE revisions SET supersedes=? WHERE entity_urn=? AND revision_digest=? AND supersedes IS NULL",
                               (cur["revision_digest"], urn, keep))
            cache._index(rec["entity"], rev)
        resolution: dict[str, Any] = {"kept": keep}
    elif row["kind"] == "archive_branch":
        if not accept:
            raise ValueError("archive branch conflicts need --accept (then re-run archive import)")
        cache.conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, '1')", (f"accepted_branch:{detail['export_id']}",))
        resolution = {"accepted_export_id": detail["export_id"]}
    else:
        resolution = {"acknowledged": True}
    cache.conn.execute("UPDATE conflicts SET resolved_at=?, resolution=? WHERE id=?", (now_ms(), json.dumps(resolution), conflict_id))
    return {"id": conflict_id, "kind": row["kind"], "entity_urn": row["entity_urn"], "resolution": resolution}


def branch_accepted(cache: Cache, export_id: str) -> bool:
    return cache.conn.execute("SELECT 1 FROM meta WHERE key=?", (f"accepted_branch:{export_id}",)).fetchone() is not None


# ---- purge ----------------------------------------------------------------------------------------

def _children(cache: Cache, urn: str, kind: str) -> list[str]:
    out: list[str] = []
    if kind == "messages":
        out += [r[0] for r in cache.conn.execute("SELECT urn FROM parts_idx WHERE message_urn=?", (urn,))]
        out += [r[0] for r in cache.conn.execute("SELECT urn FROM attachments_idx WHERE message_urn=?", (urn,))]
        out += [r[0] for r in cache.conn.execute("SELECT urn FROM events_idx WHERE target_urn=?", (urn,))]
    elif kind == "chats":
        msgs = [r[0] for r in cache.conn.execute("SELECT urn FROM messages_idx WHERE chat_urn=?", (urn,))]
        for m in msgs:
            out += _children(cache, m, "messages")
        out += msgs
        out += [r[0] for r in cache.conn.execute("SELECT urn FROM memberships_idx WHERE chat_urn=?", (urn,))]
        out += [r[0] for r in cache.conn.execute("SELECT urn FROM events_idx WHERE target_urn=?", (urn,))]
    return out


def purge_urns(cache: Cache, urns: list[str]) -> int:
    """Hard-delete records, revisions, observations and index rows. Irreversible locally."""
    n = 0
    for i in range(0, len(urns), 400):
        chunk = urns[i:i + 400]
        ph = ",".join("?" * len(chunk))
        for (rid,) in cache.conn.execute(f"SELECT id FROM messages_idx WHERE urn IN ({ph})", chunk).fetchall():
            fts.set_message_text(cache.conn, rid, None)
        for (rid,) in cache.conn.execute(f"SELECT id FROM revisions WHERE entity_urn IN ({ph}) AND entity_kind='messages'", chunk).fetchall():
            cache.conn.execute("DELETE FROM revisions_fts WHERE rowid=?", (rid,))
        cache.conn.execute(f"DELETE FROM revisions WHERE entity_urn IN ({ph})", chunk)
        cache.conn.execute(f"DELETE FROM source_observations WHERE entity_urn IN ({ph})", chunk)
        cache.conn.execute(f"DELETE FROM bucket_membership WHERE urn IN ({ph})", chunk)
        for t in ("messages_idx", "events_idx", "attachments_idx", "parts_idx", "memberships_idx", "links_idx", "aliases_idx"):
            cache.conn.execute(f"DELETE FROM {t} WHERE urn IN ({ph})", chunk)
        n += cache.conn.execute(f"DELETE FROM records WHERE urn IN ({ph})", chunk).rowcount
    return n


def purge_entity(cache: Cache, urn: str) -> dict[str, Any]:
    rec = cache.get(urn)
    if rec is None:
        raise KeyError(urn)
    urns = _children(cache, urn, rec["entity"]) + [urn]
    urns = list(dict.fromkeys(urns))
    removed = purge_urns(cache, urns)
    return {"target": urn, "entity": rec["entity"], "removed": removed}


def purge_source(cache: Cache, source: str, scope: str | None = None) -> dict[str, Any]:
    q = "SELECT urn FROM records WHERE source=?"
    args: list[Any] = [source]
    if scope:
        q += " AND account_scope=?"
        args.append(scope)
    urns = [r[0] for r in cache.conn.execute(q, args)]
    removed = purge_urns(cache, urns)
    cache.conn.execute("DELETE FROM checkpoints WHERE source=?" + (" AND account_scope=?" if scope else ""), args)
    return {"source": source, "account_scope": scope, "removed": removed}
