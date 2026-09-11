"""WhatsApp for macOS adapter (read-only). See specs/source-findings.md for validated assumptions."""

from __future__ import annotations

import hashlib
import os
import sqlite3
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

SOURCE = "whatsapp"
ADAPTER_VERSION = "whatsapp-adapter/1.0.0"

REQUIRED = {
    "ZWAMESSAGE": {"Z_PK", "ZCHATSESSION", "ZSTANZAID", "ZFROMJID", "ZTOJID", "ZISFROMME", "ZMESSAGEDATE", "ZSENTDATE",
                   "ZTEXT", "ZMESSAGETYPE", "ZGROUPEVENTTYPE", "ZMESSAGESTATUS", "ZFLAGS", "ZSORT", "ZMEDIAITEM",
                   "ZPARENTMESSAGE", "ZPUSHNAME", "ZSTARRED", "ZMESSAGEERRORSTATUS", "ZGROUPMEMBER"},
    "ZWACHATSESSION": {"Z_PK", "ZCONTACTJID", "ZPARTNERNAME", "ZSESSIONTYPE", "ZARCHIVED", "ZGROUPINFO", "ZREMOVED", "ZHIDDEN"},
    "ZWAGROUPMEMBER": {"Z_PK", "ZCHATSESSION", "ZMEMBERJID", "ZISACTIVE", "ZISADMIN", "ZCONTACTNAME"},
    "ZWAMEDIAITEM": {"Z_PK", "ZMESSAGE", "ZMEDIALOCALPATH", "ZFILESIZE", "ZTITLE", "ZVCARDNAME", "ZVCARDSTRING",
                     "ZLATITUDE", "ZLONGITUDE", "ZMOVIEDURATION", "ZMEDIAORIGIN", "ZCLOUDSTATUS", "ZMEDIAURL", "ZMETADATA"},
    "ZWAGROUPINFO": {"Z_PK", "ZCHATSESSION", "ZCREATIONDATE", "ZCREATORJID", "ZOWNERJID"},
    "ZWAPROFILEPUSHNAME": {"ZJID", "ZPUSHNAME"},
    "ZWAMESSAGEDATAITEM": {"Z_PK", "ZMESSAGE", "ZTYPE", "ZTITLE", "ZSUMMARY", "ZMATCHEDTEXT", "ZINDEX"},
    "ZWAGROUPMEMBERSCHANGE": {"Z_PK", "ZCHANGETYPE", "ZCHANGEDATE", "ZGROUPJID", "ZMEMBERJIDS"},
}

# Observed message types. Anything else becomes kind "unknown:<n>" and is counted as unsupported.
CONTENT_TYPES = {0: "text", 1: "image", 2: "video", 3: "audio", 4: "contact", 5: "location", 7: "url",
                 8: "document", 11: "gif", 15: "sticker"}
SYSTEM_TYPE = 6
ATTACHMENT_KIND = {"image": "image", "video": "video", "audio": "audio", "document": "document", "gif": "video",
                   "sticker": "image", "contact": "contact", "location": "location"}


def jid_kind(jid: str | None) -> str:
    if not jid:
        return "unknown"
    suffix = jid.rsplit("@", 1)[-1].lower() if "@" in jid else ""
    return {"s.whatsapp.net": "jid_phone", "lid": "jid_lid", "g.us": "jid_group", "broadcast": "jid_broadcast",
            "status": "jid_status", "lid.status": "jid_status", "bot": "jid_bot"}.get(suffix, "unknown")


def chat_kind(jid: str, session_type: int | None) -> str:
    k = jid_kind(jid)
    if k == "jid_group":
        return "group"
    if k in ("jid_phone", "jid_lid"):
        return "direct"
    if k == "jid_broadcast":
        return "broadcast"
    if k == "jid_status":
        return "status"
    return f"unknown:{session_type}"


class WhatsAppAdapter:
    source = SOURCE
    version = ADAPTER_VERSION

    def discover(self, cfg: Config) -> dict[str, Path]:
        root = cfg.source_root(SOURCE)
        return {"ChatStorage": root / "ChatStorage.sqlite", "LID": root / "LID.sqlite", "ContactsV2": root / "ContactsV2.sqlite"}

    def diagnose(self, cfg: Config) -> Diagnosis:
        files = self.discover(cfg)
        main = files["ChatStorage"]
        d = Diagnosis(source=SOURCE, root=str(cfg.source_root(SOURCE)), path_found=main.exists(), readable=False, schema_ok=False,
                      wal_present=main.with_name(main.name + "-wal").exists(), files={k: v.exists() for k, v in files.items()})
        ok, hint = check_readable(main)
        d.readable = ok
        d.permission_hint = hint
        if not ok:
            return d
        try:
            conn = sqlite3.connect(f"file:{main}?mode=ro", uri=True, timeout=5)
            try:
                d.problems = check_schema(conn, REQUIRED)
                d.schema_ok = not d.problems
                row = conn.execute("SELECT Z_VERSION FROM Z_METADATA").fetchone()
                d.schema_version = f"coredata-z_version-{row[0]}" if row else None
            finally:
                conn.close()
        except sqlite3.Error as e:
            d.problems.append(str(e))
        return d

    def snapshot(self, cfg: Config, staging: Path) -> Snapshot:
        files = self.discover(cfg)
        wanted = {"ChatStorage": files["ChatStorage"]}
        for opt in ("LID", "ContactsV2"):
            if files[opt].exists():
                wanted[opt] = files[opt]
        return snapshot_databases(wanted, staging)

    # ---- extraction --------------------------------------------------------------------

    def extract(self, snap: Snapshot, scope: str, cfg: Config, stats: ExtractStats,
                checkpoints: dict[str, Any] | None, *, full: bool) -> Iterator[Emit]:
        conn = sqlite3.connect(snap.files["ChatStorage"].snapshot_path)
        conn.row_factory = sqlite3.Row
        problems = check_schema(conn, REQUIRED)
        if problems:
            raise IncompatibleSource("; ".join(problems))
        stats.scanned_tables |= {"ZWACHATSESSION", "ZWAGROUPMEMBER", "ZWAGROUPMEMBERSCHANGE"}
        # Indexes on our private copy only.
        conn.execute("CREATE INDEX IF NOT EXISTS cs_msg_key ON ZWAMESSAGE(ZCHATSESSION, ZSTANZAID)")
        conn.execute("CREATE INDEX IF NOT EXISTS cs_media_msg ON ZWAMEDIAITEM(ZMESSAGE)")
        conn.execute("CREATE INDEX IF NOT EXISTS cs_data_msg ON ZWAMESSAGEDATAITEM(ZMESSAGE)")
        conn.commit()
        media_root = cfg.source_root(SOURCE) / "Message"

        def urn(entity: str, key: str) -> str:
            return U.derive_urn(SOURCE, scope, entity, key)

        me_key = U.whatsapp_me_key()
        me_urn = urn("identity", me_key)
        names: dict[str, list[dict[str, str]]] = {}

        def add_name(jid: str | None, name: str | None, origin: str) -> None:
            if not jid or not name:
                return
            lst = names.setdefault(U.whatsapp_jid(jid), [])
            if not any(n["name"] == name for n in lst):
                lst.append({"name": name, "origin": origin})

        for r in conn.execute("SELECT ZJID, ZPUSHNAME FROM ZWAPROFILEPUSHNAME WHERE ZJID IS NOT NULL"):
            add_name(r[0], r[1], "push_name")
        for r in conn.execute("SELECT ZMEMBERJID, ZCONTACTNAME FROM ZWAGROUPMEMBER WHERE ZMEMBERJID IS NOT NULL"):
            add_name(r[0], r[1], "contact_card")

        # --- chats ---------------------------------------------------------------------
        chat_by_pk: dict[int, tuple[str, str]] = {}  # pk -> (jid, urn)
        group_info = {r["ZCHATSESSION"]: r for r in conn.execute(
            "SELECT ZCHATSESSION, ZCREATIONDATE, ZCREATORJID, ZOWNERJID FROM ZWAGROUPINFO WHERE ZCHATSESSION IS NOT NULL")}
        for r in conn.execute("SELECT * FROM ZWACHATSESSION"):
            jid = U.whatsapp_jid(r["ZCONTACTJID"])
            c_urn = urn("chat", jid)
            chat_by_pk[r["Z_PK"]] = (jid, c_urn)
            gi = group_info.get(r["Z_PK"])
            if chat_kind(jid, r["ZSESSIONTYPE"]) == "direct":
                add_name(jid, r["ZPARTNERNAME"], "chat_partner_name")
            rec = record("chats", c_urn, SOURCE, scope, native_key=jid, kind=chat_kind(jid, r["ZSESSIONTYPE"]),
                         observed_name=r["ZPARTNERNAME"], service="whatsapp", assignment="source",
                         is_archived=bool(r["ZARCHIVED"]) if r["ZARCHIVED"] is not None else None,
                         created_at=timestamp(gi["ZCREATIONDATE"], "s", EPOCH_2001) if gi and gi["ZCREATIONDATE"] is not None else None)
            yield Emit(rec, Observation("ZWACHATSESSION", [r["Z_PK"]], jid,
                                        fingerprint(jid, r["ZPARTNERNAME"], r["ZSESSIONTYPE"], r["ZARCHIVED"], r["ZREMOVED"])))
            stats.bump("chats")

        # --- memberships -----------------------------------------------------------------
        member_jids: set[str] = set()
        for r in conn.execute("SELECT Z_PK, ZCHATSESSION, ZMEMBERJID, ZISACTIVE, ZISADMIN FROM ZWAGROUPMEMBER WHERE ZMEMBERJID IS NOT NULL"):
            if r["ZCHATSESSION"] not in chat_by_pk:
                stats.error("membership_without_chat")
                continue
            chat_jid, c_urn = chat_by_pk[r["ZCHATSESSION"]]
            mj = U.whatsapp_jid(r["ZMEMBERJID"])
            member_jids.add(mj)
            key = U.composite_key("membership", chat_jid, mj)
            rec = record("chat_memberships", urn("event", key), SOURCE, scope, chat_urn=c_urn, identity_urn=urn("identity", mj),
                         role="admin" if r["ZISADMIN"] else "member", state="active" if r["ZISACTIVE"] else "left",
                         first_seen=None, last_seen=None, evidence=[{"type": "group_member_table"}])
            yield Emit(rec, Observation("ZWAGROUPMEMBER", [r["Z_PK"]], key, fingerprint(chat_jid, mj, r["ZISACTIVE"], r["ZISADMIN"])))
            stats.bump("chat_memberships")

        # --- identities (all JIDs seen anywhere) -------------------------------------------
        all_jids = set(names) | member_jids | {j for j, _ in chat_by_pk.values()}
        for r in conn.execute("SELECT DISTINCT ZFROMJID FROM ZWAMESSAGE WHERE ZFROMJID IS NOT NULL"):
            all_jids.add(U.whatsapp_jid(r[0]))
        for r in conn.execute("SELECT DISTINCT ZTOJID FROM ZWAMESSAGE WHERE ZTOJID IS NOT NULL"):
            all_jids.add(U.whatsapp_jid(r[0]))
        for jid in sorted(all_jids):
            rec = record("identities", urn("identity", jid), SOURCE, scope, native_key=jid, kind=jid_kind(jid), address=jid,
                         service="whatsapp", is_me=False, observed_names=names.get(jid, []))
            yield Emit(rec, Observation("jid", [jid], jid, fingerprint(jid, names.get(jid, []))))
        yield Emit(record("identities", me_urn, SOURCE, scope, native_key=me_key, kind="me", address=None, service="whatsapp",
                          is_me=True, observed_names=[]), Observation("me", [], me_key, fingerprint("me")))
        stats.bump("identities", len(all_jids) + 1)

        # --- aliases: LID <-> phone evidence ----------------------------------------------
        yield from self._aliases(snap, scope, stats, urn)

        # --- messages ---------------------------------------------------------------------
        yield from self._messages(conn, scope, stats, urn, chat_by_pk, me_urn, media_root, checkpoints, full)

        # --- group membership change events ---------------------------------------------
        for r in conn.execute("SELECT Z_PK, ZCHANGETYPE, ZCHANGEDATE, ZGROUPJID, ZMEMBERJIDS FROM ZWAGROUPMEMBERSCHANGE"):
            gj = U.whatsapp_jid(r["ZGROUPJID"] or "")
            key = U.composite_key("groupchange", gj, str(r["ZCHANGEDATE"]), str(r["ZCHANGETYPE"]))
            members = [U.whatsapp_jid(m) for m in str(r["ZMEMBERJIDS"] or "").replace(";", ",").split(",") if m.strip()]
            rec = record("events", urn("event", key), SOURCE, scope, kind=f"unknown:{r['ZCHANGETYPE']}",
                         target_urn=urn("chat", gj) if gj else None, target_native=gj or None, actor_urn=None,
                         at=timestamp(r["ZCHANGEDATE"], "s", EPOCH_2001),
                         payload={"member_identity_urns": [urn("identity", m) for m in members]}, carrier_message_urn=None)
            yield Emit(rec, Observation("ZWAGROUPMEMBERSCHANGE", [r["Z_PK"]], key, fingerprint(gj, r["ZCHANGEDATE"], r["ZCHANGETYPE"], members)))
            stats.unsupported_bump(f"group_change_type:{r['ZCHANGETYPE']}")
            stats.bump("events")
        conn.close()

    def _aliases(self, snap: Snapshot, scope: str, stats: ExtractStats, urn: Any) -> Iterator[Emit]:
        pairs: dict[tuple[str, str], list[dict[str, Any]]] = {}
        if "LID" in snap.files:
            c = sqlite3.connect(snap.files["LID"].snapshot_path)
            try:
                if {"ZWAZACCOUNT"} <= {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
                    for lid, phone in c.execute("SELECT ZIDENTIFIER, ZPHONENUMBER FROM ZWAZACCOUNT WHERE ZIDENTIFIER IS NOT NULL AND ZPHONENUMBER IS NOT NULL"):
                        digits = "".join(ch for ch in str(phone) if ch.isdigit())
                        if digits and "@lid" in lid:
                            pairs.setdefault((U.whatsapp_jid(lid), f"{digits}@s.whatsapp.net"), []).append({"type": "lid_phone_pair", "detail": "LID.sqlite/ZWAZACCOUNT"})
            finally:
                c.close()
        if "ContactsV2" in snap.files:
            c = sqlite3.connect(snap.files["ContactsV2"].snapshot_path)
            try:
                if {"ZWAADDRESSBOOKCONTACT"} <= {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
                    for lid, wa in c.execute("SELECT ZLID, ZWHATSAPPID FROM ZWAADDRESSBOOKCONTACT WHERE ZLID IS NOT NULL AND ZWHATSAPPID IS NOT NULL"):
                        pairs.setdefault((U.whatsapp_jid(lid), U.whatsapp_jid(wa)), []).append({"type": "address_book", "detail": "ContactsV2.sqlite/ZWAADDRESSBOOKCONTACT"})
            finally:
                c.close()
        # Detect ambiguity: one LID mapping to multiple phones.
        by_lid: dict[str, set[str]] = {}
        for lid, phone in pairs:
            by_lid.setdefault(lid, set()).add(phone)
        for (lid, phone), ev in sorted(pairs.items()):
            if len(by_lid[lid]) > 1:
                stats.error("ambiguous_lid_phone_pair", "one LID maps to multiple phone JIDs")
                continue
            key = U.composite_key("alias", lid, phone)
            rec = record("aliases", urn("event", key), SOURCE, scope, old_urn=urn("identity", lid), new_urn=urn("identity", phone),
                         reason="lid_phone_pair", evidence=ev, created_at=0)
            yield Emit(rec, Observation("lid_phone_pair", [lid], key, fingerprint(lid, phone)))
            stats.bump("aliases")

    def _messages(self, conn: sqlite3.Connection, scope: str, stats: ExtractStats, urn: Any,
                  chat_by_pk: dict[int, tuple[str, str]], me_urn: str, media_root: Path,
                  checkpoints: dict[str, Any] | None, full: bool) -> Iterator[Emit]:
        max_pk = conn.execute("SELECT max(Z_PK) FROM ZWAMESSAGE").fetchone()[0] or 0
        since_pk = 0
        if not full and checkpoints and checkpoints.get("max_message_pk"):
            since_pk = int(checkpoints["max_message_pk"])
            if since_pk > max_pk:
                stats.notes.append("source reset detected: max Z_PK went backwards; falling back to full scan")
                stats.full_scan = True
                since_pk = 0
        stats.checkpoints["max_message_pk"] = max_pk
        if full or since_pk == 0:
            stats.scanned_tables |= {"ZWAMESSAGE", "ZWAMEDIAITEM"}
        # Logical message groups. Incremental: groups containing at least one new row; also a recent
        # window rescan (last 5000 rows) to pick up status/text changes on recently observed groups.
        window_pk = max(0, since_pk - 5000) if since_pk else 0
        groups = conn.execute(
            """SELECT ZCHATSESSION, ZSTANZAID, group_concat(Z_PK) AS pks, count(*) AS n, max(ZSORT) AS maxsort
               FROM ZWAMESSAGE WHERE ZSTANZAID IS NOT NULL AND ZSTANZAID != ''
               GROUP BY ZCHATSESSION, ZSTANZAID HAVING max(Z_PK) > ?""", (window_pk,))
        media_sql = "SELECT * FROM ZWAMEDIAITEM WHERE ZMESSAGE = ?"
        data_sql = "SELECT * FROM ZWAMESSAGEDATAITEM WHERE ZMESSAGE = ? ORDER BY ZINDEX"
        for g in groups:
            pks = [int(x) for x in g["pks"].split(",")]
            # representative: highest ZSORT (prefers non-negative UI rows), then highest PK
            ph = ",".join("?" * len(pks))
            rep = conn.execute(
                f"SELECT * FROM ZWAMESSAGE WHERE Z_PK IN ({ph}) ORDER BY ZSORT DESC, Z_PK DESC LIMIT 1", pks
            ).fetchone()
            if rep["ZCHATSESSION"] is None or rep["ZCHATSESSION"] not in chat_by_pk:
                stats.error("message_without_chat_session")
                stats.unsupported_bump("message_without_chat_session")
                continue
            chat_jid, c_urn = chat_by_pk[rep["ZCHATSESSION"]]
            is_me = bool(rep["ZISFROMME"])
            from_jid = rep["ZFROMJID"]
            if not is_me and not from_jid:
                stats.error("incoming_without_sender")
                continue
            try:
                m_key = U.whatsapp_message_key(chat_jid, from_jid, is_me, rep["ZSTANZAID"])
            except ValueError as e:
                stats.error("bad_message_key", str(e))
                continue
            m_urn = urn("message", m_key)
            sender_urn = me_urn if is_me else urn("identity", U.whatsapp_jid(from_jid))
            mtype = rep["ZMESSAGETYPE"]
            text = rep["ZTEXT"] or None
            if mtype == SYSTEM_TYPE:
                kind = "system"
            elif mtype in CONTENT_TYPES:
                kind = "message"
            else:
                kind = f"unknown:{mtype}"
                stats.unsupported_bump(f"message_type:{mtype}")
            parts: list[dict[str, Any]] = []
            attachments: list[dict[str, Any]] = []
            if text:
                parts.append({"type": "text", "text": text, "parser_status": "decoded", "payload": None, "attachment_urn": None})
            media = conn.execute(media_sql, (rep["Z_PK"],)).fetchone()
            if media is None and len(pks) > 1:
                for pk in pks:
                    media = conn.execute(media_sql, (pk,)).fetchone()
                    if media:
                        break
            ctype = CONTENT_TYPES.get(mtype)
            if media is not None and (ctype in ATTACHMENT_KIND or media["ZMEDIALOCALPATH"]):
                a_key = U.whatsapp_attachment_key(m_key, 0)
                a_urn = urn("attachment", a_key)
                rel = media["ZMEDIALOCALPATH"]
                availability, sha, size = "not_downloaded", None, media["ZFILESIZE"]
                if rel:
                    if rel.startswith("/") or ".." in rel.split("/"):
                        availability = "unknown"
                        stats.error("unsafe_media_path")
                    else:
                        p = media_root / rel
                        if p.is_file() and not p.is_symlink():
                            availability = "available"
                            sha, size = _sha256(p)
                        else:
                            availability = "missing"
                media_payload = allowlisted(dict(media), ALLOWLIST["whatsapp.payload"])
                akind = ATTACHMENT_KIND.get(ctype or "", "other")
                attachments.append({"urn": a_urn, "key": a_key, "rec": record(
                    "attachments", a_urn, SOURCE, scope, native_key=a_key, message_urn=m_urn, part_index=len(parts),
                    filename=os.path.basename(rel) if rel else None, mime_type=None, uti=None, declared_size=size,
                    availability=availability, blob_sha256=sha, source_path_hint=rel, is_sticker=(ctype == "sticker"), kind=akind)})
                part_type = {"contact": "contact", "location": "location"}.get(ctype or "", "attachment")
                ppayload: dict[str, Any] = dict(media_payload)
                if ctype == "location":
                    ppayload = {"latitude": media["ZLATITUDE"], "longitude": media["ZLONGITUDE"]}
                if ctype == "contact":
                    ppayload = {"vcard_name": media["ZVCARDNAME"]}
                if media["ZTITLE"]:
                    ppayload["title"] = media["ZTITLE"]
                parts.append({"type": part_type, "text": media["ZTITLE"] if ctype in ("document", "contact") else None,
                              "attachment_urn": a_urn if part_type == "attachment" else None, "payload": ppayload or None, "parser_status": "decoded"})
                if media["ZMETADATA"] is not None:
                    parts.append({"type": "opaque", "text": None, "attachment_urn": None, "parser_status": "unsupported",
                                  "payload": {"source_field": "ZWAMEDIAITEM.ZMETADATA", "sha256": hashlib.sha256(media["ZMETADATA"]).hexdigest(), "size": len(media["ZMETADATA"])}})
            for d in conn.execute(data_sql, (rep["Z_PK"],)):
                parts.append({"type": "link", "text": None, "attachment_urn": None, "parser_status": "partial",
                              "payload": {k: v for k, v in {"title": d["ZTITLE"], "summary": d["ZSUMMARY"], "url": d["ZMATCHEDTEXT"]}.items() if v}})
            flags = allowlisted(dict(rep), ALLOWLIST["whatsapp.source_flags"])
            flags["zsort_negative_only"] = (g["maxsort"] or 0) < 0
            flags["duplicate_rows"] = len(pks)
            reply_native = None
            if rep["ZPARENTMESSAGE"]:
                pr = conn.execute("SELECT ZCHATSESSION, ZSTANZAID, ZFROMJID, ZISFROMME FROM ZWAMESSAGE WHERE Z_PK=?", (rep["ZPARENTMESSAGE"],)).fetchone()
                if pr and pr["ZCHATSESSION"] in chat_by_pk and pr["ZSTANZAID"]:
                    try:
                        reply_native = U.whatsapp_message_key(chat_by_pk[pr["ZCHATSESSION"]][0], pr["ZFROMJID"], bool(pr["ZISFROMME"]), pr["ZSTANZAID"])
                    except ValueError:
                        reply_native = None
            status = {0: "pending", 1: "sent", 5: "delivered", 6: "sent", 8: "read"}.get(rep["ZMESSAGESTATUS"] or -1, "unknown")
            if not is_me:
                status = "unknown"
            rec = record(
                "messages", m_urn, SOURCE, scope, native_key=m_key, chat_urn=c_urn, chat_assignment="source_column",
                sender_urn=sender_urn, is_from_me=is_me, transport="whatsapp", kind=kind,
                sent_at=timestamp(rep["ZMESSAGEDATE"], "s", EPOCH_2001),
                received_at=timestamp(rep["ZSENTDATE"], "s", EPOCH_2001) if rep["ZSENTDATE"] is not None else None,
                edited_at=None, retracted_at=None, state=status, reply_to_urn=urn("message", reply_native) if reply_native else None,
                reply_to_native=reply_native, text=text, has_attachments=bool(attachments), part_count=len(parts),
                source_flags=flags, tombstone=None,
            )
            fp = fingerprint(text, rep["ZMESSAGEDATE"], mtype, rep["ZMESSAGESTATUS"], rep["ZFLAGS"], len(pks),
                             media["ZMEDIALOCALPATH"] if media else None)
            yield Emit(rec, Observation("ZWAMESSAGE", sorted(pks), m_key, fp))
            stats.bump("messages")
            for i, p in enumerate(parts):
                pk_key = U.composite_key("part", m_key, str(i))
                yield Emit(record("message_parts", urn("event", pk_key), SOURCE, scope, message_urn=m_urn, index=i, **p))
            for a in attachments:
                yield Emit(a["rec"], Observation("ZWAMEDIAITEM", [media["Z_PK"]] if media else [], a["key"], fingerprint(a["rec"]["availability"], a["rec"]["blob_sha256"])))
                stats.bump("attachments")
                if a["rec"]["availability"] == "available":
                    yield Emit(record("blobs", urn("attachment", U.composite_key("blob", a["rec"]["blob_sha256"])), SOURCE, scope,
                                      sha256=a["rec"]["blob_sha256"], size=a["rec"]["declared_size"] or 0, mime_type=None, first_seen=0))
            if kind == "system":
                ev_key = U.composite_key("sysevent", m_key)
                yield Emit(record("events", urn("event", ev_key), SOURCE, scope, kind=f"unknown:{rep['ZGROUPEVENTTYPE']}",
                                  target_urn=c_urn, target_native=chat_jid, actor_urn=sender_urn,
                                  at=timestamp(rep["ZMESSAGEDATE"], "s", EPOCH_2001), payload={"group_event_type": rep["ZGROUPEVENTTYPE"]},
                                  carrier_message_urn=m_urn))
                stats.unsupported_bump(f"group_event_type:{rep['ZGROUPEVENTTYPE']}")
                stats.bump("events")


def _sha256(p: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    n = 0
    with open(p, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n
