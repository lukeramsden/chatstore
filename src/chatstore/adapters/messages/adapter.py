"""Apple Messages adapter (read-only).

Body decoding (typedstream, edits, tapbacks, attachments) is delegated to the pinned Rust
helper `chatstore-messages-decoder`, which runs against the snapshot and emits JSONL.
Everything else (handles, chats, joins, ck_chat_id inference) is plain SQL on the snapshot.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ...canonical.records import EPOCH_2001, allowlisted, record, timestamp
from ...canonical.schemas import ALLOWLIST
from ...config import Config
from ...identity import urn as U
from ..base import (
    Diagnosis,
    Emit,
    ExtractStats,
    IncompatibleSource,
    Observation,
    check_schema,
    fingerprint,
)
from ..snapshot import Snapshot, check_readable, snapshot_databases

SOURCE = "messages"
ADAPTER_VERSION = "messages-adapter/1.0.0"
HELPER_FORMAT = "chatstore-messages-decoder/1"
HELPER_NAME = "chatstore-messages-decoder"

REQUIRED = {
    "message": {"ROWID", "guid", "text", "handle_id", "service", "date", "date_read", "date_delivered", "is_from_me",
                "item_type", "associated_message_guid", "associated_message_type", "attributedBody", "ck_chat_id",
                "group_action_type", "other_handle", "thread_originator_guid", "date_edited"},
    "handle": {"ROWID", "id", "service", "person_centric_id", "uncanonicalized_id"},
    "chat": {"ROWID", "guid", "chat_identifier", "service_name", "display_name", "style", "is_archived"},
    "chat_message_join": {"chat_id", "message_id"},
    "chat_handle_join": {"chat_id", "handle_id"},
    "attachment": {"ROWID", "guid", "filename", "uti", "mime_type", "transfer_name", "total_bytes", "is_sticker"},
    "message_attachment_join": {"message_id", "attachment_id"},
}
TRANSPORT = {"iMessage": "imessage", "SMS": "sms", "RCS": "rcs", "rcs": "rcs", "iMessageLite": "imessage"}
TAPBACK_EMOJI = {"loved": "❤️", "liked": "👍", "disliked": "👎", "laughed": "😂", "emphasized": "‼️", "questioned": "❓"}


class HelperMissing(Exception):
    pass


def find_helper(cfg: Config) -> Path | None:
    candidates: list[Path] = []
    if cfg.helper_path:
        candidates.append(Path(cfg.helper_path).expanduser())
    env = os.environ.get("CHATSTORE_MESSAGES_DECODER")
    if env:
        candidates.append(Path(env).expanduser())
    which = shutil.which(HELPER_NAME)
    if which:
        candidates.append(Path(which))
    # Development checkout: <repo>/helpers/messages-decoder/target/release/<name>
    here = Path(__file__).resolve()
    for parent in here.parents:
        p = parent / "helpers" / "messages-decoder" / "target" / "release" / HELPER_NAME
        if p.exists():
            candidates.append(p)
            break
    for c in candidates:
        if c.is_file() and os.access(c, os.X_OK):
            return c
    return None


def attachment_kind(mime: str | None, uti: str | None) -> str:
    m = (mime or "").lower()
    u = (uti or "").lower()
    if m.startswith("image/") or "image" in u:
        return "image"
    if m.startswith("video/") or "movie" in u or "video" in u:
        return "video"
    if m.startswith("audio/") or "audio" in u:
        return "audio"
    if m == "text/vcard" or "vcard" in u:
        return "contact"
    if m or u:
        return "document"
    return "other"


class MessagesAdapter:
    source = SOURCE
    version = ADAPTER_VERSION

    def discover(self, cfg: Config) -> dict[str, Path]:
        root = cfg.source_root(SOURCE)
        return {"chat": root / "chat.db"}

    def diagnose(self, cfg: Config) -> Diagnosis:
        main = self.discover(cfg)["chat"]
        d = Diagnosis(source=SOURCE, root=str(cfg.source_root(SOURCE)), path_found=main.exists(), readable=False,
                      schema_ok=False, wal_present=main.with_name(main.name + "-wal").exists(), files={"chat.db": main.exists()})
        helper = find_helper(cfg)
        d.files["helper"] = helper is not None
        if helper is None:
            d.problems.append(f"{HELPER_NAME} not found; build it with scripts/build-helper.sh or set config.helper_path")
        ok, hint = check_readable(main)
        d.readable = ok
        d.permission_hint = hint
        if not ok:
            return d
        try:
            conn = sqlite3.connect(f"file:{main}?mode=ro", uri=True, timeout=5)
            try:
                d.problems += check_schema(conn, REQUIRED)
                d.schema_ok = not d.problems
                row = conn.execute("PRAGMA user_version").fetchone()
                d.schema_version = f"user_version-{row[0]}" if row else None
            finally:
                conn.close()
        except sqlite3.Error as e:
            d.problems.append(str(e))
            d.schema_ok = False
        return d

    def snapshot(self, cfg: Config, staging: Path) -> Snapshot:
        return snapshot_databases({"chat": self.discover(cfg)["chat"]}, staging)

    # ---- extraction -----------------------------------------------------------------

    def extract(self, snap: Snapshot, scope: str, cfg: Config, stats: ExtractStats,
                checkpoints: dict[str, Any] | None, *, full: bool) -> Iterator[Emit]:
        db_path = snap.files["chat"].snapshot_path
        helper = find_helper(cfg)
        if helper is None:
            raise HelperMissing(f"{HELPER_NAME} not found")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        problems = check_schema(conn, REQUIRED)
        if problems:
            raise IncompatibleSource("; ".join(problems))
        root = cfg.source_root(SOURCE)

        def urn(entity: str, key: str) -> str:
            return U.derive_urn(SOURCE, scope, entity, key)

        me_key = U.messages_me_key(None)
        me_urn = urn("identity", me_key)

        # --- handles -> identities ------------------------------------------------------
        handle_key: dict[int, str] = {}
        handle_urn: dict[int, str] = {}
        by_key: dict[str, dict[str, Any]] = {}
        for r in conn.execute("SELECT ROWID, id, service, person_centric_id, uncanonicalized_id FROM handle"):
            key = U.messages_handle_key(r["service"] or "", r["id"] or "")
            handle_key[r["ROWID"]] = key
            handle_urn[r["ROWID"]] = urn("identity", key)
            ent = by_key.setdefault(key, {"rows": [], "id": r["id"], "service": r["service"], "pcid": set(), "unc": set()})
            ent["rows"].append(r["ROWID"])
            if r["person_centric_id"]:
                ent["pcid"].add(r["person_centric_id"])
            if r["uncanonicalized_id"]:
                ent["unc"].add(r["uncanonicalized_id"])
        for key, ent in by_key.items():
            names = [{"name": u, "origin": "uncanonicalized_id"} for u in sorted(ent["unc"]) if u != ent["id"]]
            rec = record("identities", urn("identity", key), SOURCE, scope, native_key=key,
                         kind="email" if "@" in (ent["id"] or "") else "phone", address=ent["id"],
                         service=ent["service"], is_me=False, observed_names=names)
            yield Emit(rec, Observation("handle", sorted(ent["rows"]), key, fingerprint(ent["id"], ent["service"], names)))
        yield Emit(record("identities", me_urn, SOURCE, scope, native_key=me_key, kind="me", address=None, service=None,
                          is_me=True, observed_names=[]), Observation("me", [], me_key, fingerprint("me")))
        stats.bump("identities", len(by_key) + 1)
        # person_centric_id groups several handles for the same person: emit as identity_links evidence via aliases? No:
        # aliases are for renames; we emit nothing here but record the hint for Phase 4 link suggestions.
        pc_groups: dict[str, set[str]] = {}
        for key, ent in by_key.items():
            for pc in ent["pcid"]:
                pc_groups.setdefault(pc, set()).add(key)
        stats.checkpoints["person_centric_groups"] = sum(1 for g in pc_groups.values() if len(g) > 1)

        # --- chats -------------------------------------------------------------------
        chat_urn_by_rowid: dict[int, str] = {}
        chat_key_by_rowid: dict[int, str] = {}
        ident_to_chats: dict[str, list[int]] = {}
        for r in conn.execute("SELECT ROWID, guid, chat_identifier, service_name, display_name, style, is_archived FROM chat"):
            key = U.messages_chat_key(r["guid"])
            c_urn = urn("chat", key)
            chat_urn_by_rowid[r["ROWID"]] = c_urn
            chat_key_by_rowid[r["ROWID"]] = key
            ident_to_chats.setdefault(r["chat_identifier"] or "", []).append(r["ROWID"])
            kind = "group" if r["style"] == 43 else "direct" if r["style"] == 45 else f"unknown:{r['style']}"
            rec = record("chats", c_urn, SOURCE, scope, native_key=key, kind=kind, observed_name=r["display_name"] or None,
                         service=r["service_name"], assignment="source", is_archived=bool(r["is_archived"]), created_at=None)
            yield Emit(rec, Observation("chat", [r["ROWID"]], key, fingerprint(r["guid"], r["display_name"], r["style"], r["is_archived"], r["service_name"])))
            stats.bump("chats")

        # --- memberships --------------------------------------------------------------
        for r in conn.execute("SELECT chat_id, handle_id FROM chat_handle_join"):
            if r["chat_id"] not in chat_key_by_rowid or r["handle_id"] not in handle_key:
                stats.error("dangling_chat_handle_join")
                continue
            key = U.composite_key("membership", chat_key_by_rowid[r["chat_id"]], handle_key[r["handle_id"]])
            rec = record("chat_memberships", urn("event", key), SOURCE, scope, chat_urn=chat_urn_by_rowid[r["chat_id"]],
                         identity_urn=handle_urn[r["handle_id"]], role="member", state="unknown", first_seen=None,
                         last_seen=None, evidence=[{"type": "chat_handle_join"}])
            yield Emit(rec, Observation("chat_handle_join", [r["chat_id"], r["handle_id"]], key, fingerprint(key)))
            stats.bump("chat_memberships")

        # --- ck_chat_id inference for messages without a join row ------------------------
        ck_rows = conn.execute(
            """SELECT m.ROWID, m.ck_chat_id, m.service FROM message m
               LEFT JOIN chat_message_join j ON j.message_id = m.ROWID
               WHERE j.message_id IS NULL AND m.ck_chat_id IS NOT NULL""").fetchall()
        inferred: dict[int, tuple[str, str]] = {}  # message rowid -> (chat_urn, assignment)
        stub_chats: dict[str, str] = {}  # ck_chat_id -> chat urn
        for r in ck_rows:
            ck = r["ck_chat_id"]
            tail = ck.split(";", 2)[-1] if ";" in ck else ck
            cands = ident_to_chats.get(tail, [])
            if len(cands) > 1:
                svc_first = ck.split(";", 1)[0]
                same = [c for c in cands if conn.execute("SELECT service_name FROM chat WHERE ROWID=?", (c,)).fetchone()[0] == svc_first]
                cands = same or cands
            if cands:
                inferred[r["ROWID"]] = (chat_urn_by_rowid[min(cands)], "inferred_ck_chat_id")
            else:
                if ck not in stub_chats:
                    key = U.messages_inferred_chat_key(ck)
                    s_urn = urn("chat", key)
                    stub_chats[ck] = s_urn
                    svc = ck.split(";", 1)[0] if ";" in ck else None
                    rec = record("chats", s_urn, SOURCE, scope, native_key=key, kind="direct" if "chat" not in tail else "group",
                                 observed_name=None, service=svc, assignment="stub", is_archived=None, created_at=None)
                    yield Emit(rec, Observation("message.ck_chat_id", [], key, fingerprint(ck)))
                    stats.bump("chats")
                    stats.bump("stub_chats")
                inferred[r["ROWID"]] = (stub_chats[ck], "inferred_ck_chat_id")
        stats.bump("messages_inferred_chat", len(inferred))

        # --- attachments join ----------------------------------------------------------
        # An attachment row can be joined to several messages (observed once). The attachment record
        # is owned by the lowest message ROWID; other messages only reference its URN in their parts.
        att_owner: dict[int, int] = {}
        for r in conn.execute("SELECT attachment_id, min(message_id) AS owner FROM message_attachment_join GROUP BY 1"):
            att_owner[r["attachment_id"]] = r["owner"]

        # --- run helper ------------------------------------------------------------------
        max_rowid = conn.execute("SELECT max(ROWID) FROM message").fetchone()[0] or 0
        prev = int(checkpoints.get("max_message_rowid", 0)) if checkpoints else 0
        if prev > max_rowid:
            stats.notes.append("source reset detected: max message ROWID went backwards; full scan")
            stats.full_scan = True
        stats.checkpoints["max_message_rowid"] = max_rowid

        proc = subprocess.Popen([str(helper), str(db_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                encoding="utf-8", bufsize=1 << 16)
        assert proc.stdout is not None
        header = json.loads(proc.stdout.readline() or "{}")
        if header.get("type") != "header" or header.get("format") != HELPER_FORMAT:
            proc.kill()
            raise IncompatibleSource(f"helper produced unexpected header format {header.get('format')!r}")
        stats.checkpoints["helper"] = {"format": header.get("format"), "library": header.get("library"),
                                       "helper_version": header.get("helper_version")}
        footer: dict[str, Any] | None = None
        guid_by_rowid: dict[int, str] = {}
        for line in proc.stdout:
            o = json.loads(line)
            t = o.get("type")
            if t == "footer":
                footer = o
                break
            if t != "message":
                continue
            guid_by_rowid[o["rowid"]] = o["guid"]
            yield from self._message(o, conn, scope, stats, urn, me_urn, handle_urn, chat_urn_by_rowid, inferred, att_owner, root)
        rc = proc.wait()
        err = proc.stderr.read() if proc.stderr else ""
        if rc != 0:
            raise IncompatibleSource(f"helper exited {rc}: {err.strip()[:200]}")
        if footer is None:
            raise IncompatibleSource("helper output truncated (no footer)")
        if footer.get("errors"):
            stats.error("helper_row_errors", f"{footer['errors']} rows failed to decode")
        conn.close()

    def _message(self, o: dict[str, Any], conn: sqlite3.Connection, scope: str, stats: ExtractStats, urn: Any, me_urn: str,
                 handle_urn: dict[int, str], chat_urn_by_rowid: dict[int, str], inferred: dict[int, tuple[str, str]],
                 att_owner: dict[int, int], root: Path) -> Iterator[Emit]:
        m_key = o["guid"]
        m_urn = urn("message", m_key)
        rowid = o["rowid"]
        if o.get("chat_id") is not None and o["chat_id"] in chat_urn_by_rowid:
            c_urn, assign = chat_urn_by_rowid[o["chat_id"]], "join_table"
        elif rowid in inferred:
            c_urn, assign = inferred[rowid]
        else:
            c_urn, assign = None, "none"
            stats.bump("messages_without_chat")
        is_me = bool(o["is_from_me"])
        if is_me:
            sender = me_urn
        elif o.get("handle_id") in handle_urn:
            sender = handle_urn[o["handle_id"]]
        else:
            sender = None
            if o.get("handle_id"):
                stats.error("unknown_handle_id")
        transport = TRANSPORT.get(o.get("service") or "", "unknown")
        variant = o["variant"]
        kind = "message"
        events: list[dict[str, Any]] = []
        if variant == "tapback" and o.get("tapback"):
            tb = o["tapback"]
            kind = "reaction"
            target = urn("message", tb["target_guid"]) if tb.get("target_guid") else None
            emoji = o.get("associated_message_emoji") or TAPBACK_EMOJI.get(tb["kind"])
            events.append({"kind": "reaction" if tb["action"] == "added" else "reaction_removed", "target_urn": target,
                           "payload": {"tapback": tb["kind"], "emoji": emoji, "part_index": tb["part_index"], "target_native": tb.get("target_guid")}})
        elif o["announcement"] == "fully_unsent":
            kind = "retraction"
            events.append({"kind": "retraction", "target_urn": m_urn, "payload": {}})
        elif o["announcement"] == "group_action" and o.get("group_action"):
            kind = "system"
            ga = o["group_action"]
            ev_kind = {"participant_added": "membership_add", "participant_removed": "membership_remove",
                       "participant_left": "membership_remove", "name_change": "subject_change", "icon_changed": "icon_change",
                       "icon_removed": "icon_change"}.get(ga["kind"], f"unknown:{ga['kind']}")
            payload: dict[str, Any] = {"action": ga["kind"]}
            if "handle_id" in ga and ga["handle_id"] in handle_urn:
                payload["subject_identity_urn"] = handle_urn[ga["handle_id"]]
            if ga["kind"] == "name_change":
                payload["name"] = ga.get("name")
            events.append({"kind": ev_kind, "target_urn": c_urn, "payload": payload})
        elif o["announcement"] and o["announcement"] not in ("audio_message_kept",):
            kind = "system"
        elif o["item_type"] not in (0, 1, 2, 3, 4, 5, 6) and variant == "normal":
            kind = f"unknown:item_type_{o['item_type']}"
            stats.unsupported_bump(f"item_type:{o['item_type']}")
        if variant.startswith("unknown:"):
            stats.unsupported_bump(f"variant:{variant}")
        if o["parser_status"] in ("unsupported", "partial", "empty") and variant == "normal":
            stats.unsupported_bump(f"parser:{o['parser_status']}")

        # parts
        parts: list[dict[str, Any]] = []
        texts: list[str] = []
        att_by_guid: dict[str, dict[str, Any]] = {a["guid"]: a for a in o["attachments"] if a.get("guid")}
        used_att: set[int] = set()
        att_records: list[tuple[dict[str, Any], dict[str, Any]]] = []

        def att_emit(a: dict[str, Any], part_index: int) -> str:
            a_key = a["guid"] or U.composite_key("attachment_rowid", str(a["rowid"]))
            a_urn = urn("attachment", a_key)
            if a["rowid"] is not None:
                used_att.add(a["rowid"])
            fn = a.get("filename")
            availability, sha, size, hint = "unknown", None, a.get("total_bytes"), None
            if a.get("_purged"):
                availability = "missing"
            elif fn:
                p = Path(fn).expanduser()
                try:
                    hint = str(p.relative_to(root))
                except ValueError:
                    hint = os.path.basename(fn)
                if p.is_file() and not p.is_symlink():
                    availability = "available"
                    sha, size = _sha256(p)
                else:
                    availability = "missing" if fn.startswith(("~", "/")) else "unknown"
            else:
                availability = "not_downloaded"
            rec = record("attachments", a_urn, SOURCE, scope, native_key=a_key, message_urn=m_urn, part_index=part_index,
                         filename=os.path.basename(a.get("transfer_name") or fn or "") or None, mime_type=a.get("mime_type"),
                         uti=a.get("uti"), declared_size=size, availability=availability, blob_sha256=sha,
                         source_path_hint=hint, is_sticker=bool(a.get("is_sticker")),
                         kind=attachment_kind(a.get("mime_type"), a.get("uti")))
            if a["rowid"] is None or att_owner.get(a["rowid"], rowid) == rowid:
                att_records.append((rec, a))
            return a_urn

        for p in o["parts"]:
            idx = len(parts)
            if p["kind"] == "text":
                if p.get("text"):
                    texts.append(p["text"])
                payload = {}
                if p.get("links"):
                    payload["links"] = p["links"]
                if p.get("mentions"):
                    payload["mentions"] = p["mentions"]
                if p.get("edit_status") and p["edit_status"] != "original":
                    payload["edit_status"] = p["edit_status"]
                if p.get("edit_history"):
                    payload["edit_history"] = [{"at": timestamp(h["date"], "ns", EPOCH_2001), "text": h["text"]} for h in p["edit_history"]]
                    for h in p["edit_history"][1:]:
                        events.append({"kind": "edit", "target_urn": m_urn, "at_raw": h["date"], "payload": {"part_index": idx}})
                parts.append({"type": "text", "text": p.get("text"), "attachment_urn": None, "payload": payload or None,
                              "parser_status": "decoded" if o["parser_status"] == "decoded" else "partial"})
            elif p["kind"] == "attachment":
                pg = p.get("attachment_guid") or ""
                a = att_by_guid.get(pg)
                if a is None and pg:
                    # The attachment row has been purged from the store; keep a placeholder so the
                    # message still records that an attachment existed (availability=missing).
                    stats.bump("attachments_row_missing")
                    # `pg` is `at_<part>_<message guid>` here, unique per part; keep it verbatim as the key.
                    a = {"guid": U.composite_key("purged", pg), "rowid": None, "filename": None, "transfer_name": p.get("attachment_name"),
                         "uti": None, "mime_type": None, "total_bytes": None, "is_sticker": False, "_purged": True}
                a_urn = att_emit(a, idx) if a else None
                parts.append({"type": "attachment", "text": p.get("text"), "attachment_urn": a_urn,
                              "payload": {"inline": True} if p.get("inline") else None, "parser_status": "decoded" if a else "partial"})
            elif p["kind"] == "app":
                payload = {"balloon_bundle_id": o.get("balloon_bundle_id"), "variant": variant}
                parts.append({"type": "app", "text": None, "attachment_urn": None, "payload": payload, "parser_status": "partial"})
            elif p["kind"] == "retracted":
                parts.append({"type": "opaque", "text": None, "attachment_urn": None, "payload": {"retracted": True}, "parser_status": "decoded"})
        # attachments not referenced inline
        for a in o["attachments"]:
            if a["rowid"] not in used_att:
                a_urn = att_emit(a, len(parts))
                parts.append({"type": "attachment", "text": None, "attachment_urn": a_urn, "payload": None, "parser_status": "decoded"})
        if o.get("subject"):
            parts.insert(0, {"type": "text", "text": o["subject"], "attachment_urn": None, "payload": {"subject": True}, "parser_status": "decoded"})
            texts.insert(0, o["subject"])
        if not parts and o["parser_status"] == "empty" and not o["attachments"] and variant == "normal" and kind == "message":
            kind = "placeholder"

        text = "\n".join(texts) if texts else None
        if kind == "reaction":
            text = None
        flags = allowlisted({"item_type": o["item_type"], "group_action_type": o["group_action_type"],
                             "associated_message_type": o.get("associated_message_type"), "is_read": o.get("is_read"),
                             "service": o.get("service")}, ALLOWLIST["messages.source_flags"])
        flags["variant"] = variant
        flags["parser_status"] = o["parser_status"]
        state = "unknown"
        if is_me:
            state = "read" if o["date_read"] else "delivered" if o["date_delivered"] else "sent"
        reply_native = o.get("thread_originator_guid")
        rec = record(
            "messages", m_urn, SOURCE, scope, native_key=m_key, chat_urn=c_urn, chat_assignment=assign, sender_urn=sender,
            is_from_me=is_me, transport=transport, kind=kind, sent_at=timestamp(o["date"], "ns", EPOCH_2001),
            received_at=timestamp(o["date_delivered"], "ns", EPOCH_2001) if o["date_delivered"] else None,
            edited_at=timestamp(o["date_edited"], "ns", EPOCH_2001) if o["date_edited"] else None,
            retracted_at=timestamp(o["date_edited"], "ns", EPOCH_2001) if o["is_fully_unsent"] and o["date_edited"] else None,
            state=state, reply_to_urn=urn("message", reply_native) if reply_native else None, reply_to_native=reply_native,
            text=text, has_attachments=any(p["type"] == "attachment" for p in parts), part_count=len(parts), source_flags=flags,
            tombstone={"reason": "deleted_from_chat", "evidence": ["chat_recoverable_message_join"]} if o.get("deleted_from") else None,
        )
        fp = fingerprint(o["text"], o["date"], o["date_read"], o["date_delivered"], o["date_edited"], o["item_type"], variant,
                         o["parser_status"], o.get("chat_id"), len(o["attachments"]), o.get("deleted_from"))
        yield Emit(rec, Observation("message", [rowid], m_key, fp))
        stats.bump("messages")
        for i, p in enumerate(parts):
            yield Emit(record("message_parts", urn("event", U.composite_key("part", m_key, str(i))), SOURCE, scope,
                              message_urn=m_urn, index=i, **p))
        for a_rec, a in att_records:
            yield Emit(a_rec, Observation("attachment", [a["rowid"]] if a["rowid"] is not None else [], a_rec["native_key"], fingerprint(a_rec["availability"], a_rec["blob_sha256"], a.get("filename"))))
            stats.bump("attachments")
            if a_rec["availability"] == "available":
                yield Emit(record("blobs", urn("attachment", U.composite_key("blob", a_rec["blob_sha256"])), SOURCE, scope,
                                  sha256=a_rec["blob_sha256"], size=a_rec["declared_size"] or 0, mime_type=a_rec["mime_type"], first_seen=0))
        for n, ev in enumerate(events):
            ev_key = m_key if n == 0 else U.composite_key(m_key, "event", str(n))
            yield Emit(record("events", urn("event", ev_key), SOURCE, scope, kind=ev["kind"], target_urn=ev["target_urn"],
                              target_native=ev["payload"].get("target_native"), actor_urn=sender,
                              at=timestamp(ev.get("at_raw", o["date"]), "ns", EPOCH_2001), payload=ev["payload"], carrier_message_urn=m_urn))
            stats.bump("events")


def _sha256(p: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    n = 0
    with open(p, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


if __name__ == "__main__":  # pragma: no cover
    print(find_helper(Config()), file=sys.stderr)
