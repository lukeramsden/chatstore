"""WhatsApp for macOS adapter (read-only). See specs/source-findings.md for validated assumptions."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing
from functools import partial
from pathlib import Path
from typing import Any

from ...canonical.records import EPOCH_2001, allowlisted, record, timestamp
from ...canonical.schemas import ALLOWLIST
from ...config import Config
from ...identity import urn as U
from ..base import (
    Diagnosis,
    Emission,
    Emit,
    ExtractStats,
    IncompatibleSource,
    Observation,
    check_schema,
    extract_unit,
    fingerprint,
    native_fingerprint,
)
from ..snapshot import Snapshot, check_readable, snapshot_databases
from . import aliases, chats, events, header_ownership, media

POLICY_MODULES = (aliases, chats, events, media)

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


def _message_jids(conn: sqlite3.Connection, field: str, *, incoming: bool = False) -> Iterator[str]:
    """Seek past duplicate JIDs instead of stepping every message's index entry."""
    if field not in ('ZFROMJID', 'ZTOJID'):
        raise ValueError('unsupported message JID field')
    name = 'cs_jid_' + field.lower() + ('_incoming' if incoming else '')
    condition = ' AND ZISFROMME=0' if incoming else ''
    conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON ZWAMESSAGE({field})' +
                 (' WHERE ZISFROMME=0' if incoming else ''))
    last = None
    while True:
        bound = f' AND {field}>?' if last is not None else ''
        row = conn.execute(f'SELECT {field} FROM ZWAMESSAGE INDEXED BY {name} '
                           f'WHERE {field} IS NOT NULL{condition}{bound} ORDER BY {field} LIMIT 1',
                           (last,) if last is not None else ()).fetchone()
        if row is None:
            return
        last = row[0]
        yield last


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
                checkpoints: dict[str, Any] | None, *, full: bool) -> Iterator[Emission]:
        conn = sqlite3.connect(snap.files["ChatStorage"].snapshot_path)
        conn.row_factory = sqlite3.Row
        problems = check_schema(conn, REQUIRED)
        if problems:
            raise IncompatibleSource("; ".join(problems))
        # Indexes on our private copy only.
        conn.execute("CREATE INDEX IF NOT EXISTS cs_msg_key ON ZWAMESSAGE(ZCHATSESSION, ZSTANZAID)")
        conn.execute("CREATE INDEX IF NOT EXISTS cs_media_msg ON ZWAMEDIAITEM(ZMESSAGE)")
        conn.execute("CREATE INDEX IF NOT EXISTS cs_wa_media_file ON ZWAMEDIAITEM(ZMEDIALOCALPATH)")
        conn.execute("CREATE INDEX IF NOT EXISTS cs_wa_group_info_chat ON ZWAGROUPINFO(ZCHATSESSION)")
        conn.execute("CREATE INDEX IF NOT EXISTS cs_data_msg ON ZWAMESSAGEDATAITEM(ZMESSAGE)")
        conn.commit()
        media_root = cfg.source_root(SOURCE) / "Message"

        def urn(entity: str, key: str) -> str:
            return U.derive_urn(SOURCE, scope, entity, key)

        me_key = U.whatsapp_me_key()
        me_urn = urn("identity", me_key)
        if stats.reuse_whatsapp_headers:
            if full or stats.selected_message_rowids is None:
                conn.close()
                raise IncompatibleSource('header reuse requires selected message rows')
            conn.execute('CREATE TEMP TABLE cs_wa_header_selection(pk INTEGER PRIMARY KEY)')
            conn.executemany('INSERT INTO cs_wa_header_selection VALUES(?)',
                             ((pk,) for pk in stats.selected_message_rowids))
            chats = conn.execute("""SELECT c.Z_PK,c.ZCONTACTJID
                FROM temp.cs_wa_header_selection s CROSS JOIN ZWAMESSAGE m CROSS JOIN ZWACHATSESSION c
                WHERE m.Z_PK=s.pk AND c.Z_PK=m.ZCHATSESSION
                UNION SELECT c.Z_PK,c.ZCONTACTJID
                FROM temp.cs_wa_header_selection s CROSS JOIN ZWAMESSAGE m
                CROSS JOIN ZWAMESSAGE p CROSS JOIN ZWACHATSESSION c
                WHERE m.Z_PK=s.pk AND p.Z_PK=m.ZPARENTMESSAGE AND c.Z_PK=p.ZCHATSESSION""")
            selected_chats = {row[0]: (U.whatsapp_jid(row[1]), urn('chat', U.whatsapp_jid(row[1]))) for row in chats}
            stats.bump('prepared_headers_reused')
            try:
                yield from self._messages(conn, scope, stats, urn, selected_chats, me_urn, media_root, checkpoints, full, snap.whatsapp_media_probe)
                if stats.selected_whatsapp_chat_rowids is not None:
                    yield from self._selected_chats(conn, scope, stats, urn, stats.selected_whatsapp_chat_rowids)
                if stats.selected_whatsapp_event_rowids is not None:
                    yield from self._events(conn, scope, stats, urn, stats.selected_whatsapp_event_rowids)
            finally:
                conn.close()
            return
        stats.scanned_tables |= {"ZWACHATSESSION", "ZWAGROUPMEMBER", "ZWAGROUPMEMBERSCHANGE", "jid", "me", "lid_phone_pair"}
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
        chat_kinds: dict[int, str] = {}
        group_info = {r["ZCHATSESSION"]: r for r in conn.execute(
            "SELECT ZCHATSESSION, ZCREATIONDATE, ZCREATORJID, ZOWNERJID FROM ZWAGROUPINFO WHERE ZCHATSESSION IS NOT NULL")}
        for r in conn.execute("SELECT * FROM ZWACHATSESSION"):
            jid = U.whatsapp_jid(r["ZCONTACTJID"])
            c_urn = urn("chat", jid)
            chat_by_pk[r["Z_PK"]] = (jid, c_urn)
            kind = chat_kind(jid, r["ZSESSIONTYPE"])
            chat_kinds[r["Z_PK"]] = kind
            gi = group_info.get(r["Z_PK"])
            if kind == "direct":
                add_name(jid, r["ZPARTNERNAME"], "chat_partner_name")
            yield self._chat(r, gi, scope, urn)
            stats.bump('chats')

        # --- memberships -----------------------------------------------------------------
        # Non-group chats have no ZWAGROUPMEMBER rows. Synthesise the counterpart (the chat's own
        # JID) and `me` so callers can find the other party without reading messages (issue #1).
        member_jids: set[str] = set()
        for pk, (chat_jid, c_urn) in chat_by_pk.items():
            if chat_kinds[pk] == "group":
                continue
            for member_key, identity_urn, evidence in (
                (chat_jid, urn("identity", chat_jid), "chat_session_jid"),
                (me_key, me_urn, "chat_session_me"),
            ):
                key = U.composite_key("membership", chat_jid, member_key)
                rec = record("chat_memberships", urn("event", key), SOURCE, scope, chat_urn=c_urn, identity_urn=identity_urn,
                             role="member", state="active", first_seen=None, last_seen=None, evidence=[{"type": evidence}])
                yield Emit(rec, Observation("ZWACHATSESSION", [pk], key, fingerprint(chat_jid, member_key, evidence)))
                stats.bump("chat_memberships")
        for r in conn.execute("SELECT Z_PK, ZCHATSESSION, ZMEMBERJID, ZISACTIVE, ZISADMIN FROM ZWAGROUPMEMBER WHERE ZMEMBERJID IS NOT NULL"):
            if r["ZCHATSESSION"] not in chat_by_pk:
                stats.error("membership_without_chat", f"ZWAGROUPMEMBER Z_PK={r['Z_PK']} ZCHATSESSION={r['ZCHATSESSION']}")
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
        # Alias pairs come from LID.sqlite / ContactsV2.sqlite; the phone side often never appears
        # in ChatStorage, so include both sides here or the alias target would dangle (issue #3).
        alias_pairs = self._alias_pairs(snap)
        all_jids = set(names) | member_jids | {j for j, _ in chat_by_pk.values()}
        all_jids |= {j for pair in alias_pairs for j in pair}
        for jid in _message_jids(conn, 'ZFROMJID'):
            all_jids.add(U.whatsapp_jid(jid))
        for jid in _message_jids(conn, 'ZTOJID'):
            all_jids.add(U.whatsapp_jid(jid))
        # The owner's real JIDs (phone + LID) appear as group members and as the counterpart of the
        # "message yourself" chat; flag them is_me so consumers do not treat the owner as a third
        # party (issue #7). The synthetic `me` identity below stays the sender of outgoing messages.
        owner_jids = self._owner_jids(conn, alias_pairs, stats)
        all_jids |= owner_jids
        for jid in sorted(all_jids):
            is_me = jid in owner_jids
            rec = record("identities", urn("identity", jid), SOURCE, scope, native_key=jid, kind=jid_kind(jid), address=jid,
                         service="whatsapp", is_me=is_me, observed_names=names.get(jid, []))
            yield Emit(rec, Observation("jid", [jid], jid, fingerprint(jid, names.get(jid, []), is_me)))
        yield Emit(record("identities", me_urn, SOURCE, scope, native_key=me_key, kind="me", address=None, service="whatsapp",
                          is_me=True, observed_names=[]), Observation("me", [], me_key, fingerprint("me")))
        stats.bump("identities", len(all_jids) + 1)

        # --- aliases: LID <-> phone evidence ----------------------------------------------
        yield from self._aliases(alias_pairs, scope, stats, urn)

        # --- messages ---------------------------------------------------------------------
        yield from self._messages(conn, scope, stats, urn, chat_by_pk, me_urn, media_root, checkpoints, full, snap.whatsapp_media_probe)

        yield from self._events(conn, scope, stats, urn)
        conn.close()

    def _chat(self, r, gi, scope, urn):
        jid = U.whatsapp_jid(r['ZCONTACTJID'])
        c_urn = urn('chat', jid)
        kind = chat_kind(jid, r['ZSESSIONTYPE'])
        rec = record("chats", c_urn, SOURCE, scope, native_key=jid, kind=kind,
                     observed_name=r["ZPARTNERNAME"], service="whatsapp", assignment="source",
                     is_archived=bool(r["ZARCHIVED"]) if r["ZARCHIVED"] is not None else None,
                     created_at=timestamp(gi["ZCREATIONDATE"], "s", EPOCH_2001) if gi and gi["ZCREATIONDATE"] is not None else None)
        return Emit(rec, Observation("ZWACHATSESSION", [r["Z_PK"]], jid,
                                    fingerprint(jid, r["ZPARTNERNAME"], r["ZSESSIONTYPE"], r["ZARCHIVED"], r["ZREMOVED"])))

    def _selected_chats(self, conn, scope, stats, urn, selected):
        if not selected:
            return
        identity = header_ownership.rowid_alias(conn, 'ZWACHATSESSION')
        info_identity = header_ownership.rowid_alias(conn, 'ZWAGROUPINFO')
        for pk in sorted(selected):
            row = conn.execute(f'SELECT * FROM ZWACHATSESSION WHERE {identity}=?', (pk,)).fetchone()
            if row is None:
                raise IncompatibleSource('selected chat metadata row is missing')
            info = conn.execute(f'SELECT * FROM ZWAGROUPINFO WHERE ZCHATSESSION=? ORDER BY {info_identity} DESC LIMIT 1', (row['Z_PK'],)).fetchone()
            yield self._chat(row, info, scope, urn)
            stats.bump('chats')

    def _events(self, conn, scope, stats, urn, selected=None):
        fields = 'Z_PK,ZCHANGETYPE,ZCHANGEDATE,ZGROUPJID,ZMEMBERJIDS'
        if selected is None:
            rows = conn.execute(f'SELECT {fields} FROM ZWAGROUPMEMBERSCHANGE')
        else:
            identity = events.rowid_alias(conn)
            rows = (row for pk in sorted(selected) for row in conn.execute(
                f'SELECT {fields} FROM ZWAGROUPMEMBERSCHANGE WHERE {identity}=?', (pk,)))
        for r in rows:
            gj = U.whatsapp_jid(r["ZGROUPJID"] or "")
            key = events.key(r)
            members = [U.whatsapp_jid(m) for m in str(r["ZMEMBERJIDS"] or "").replace(";", ",").split(",") if m.strip()]
            rec = record("events", urn("event", key), SOURCE, scope, kind=f"unknown:{r['ZCHANGETYPE']}",
                         target_urn=urn("chat", gj) if gj else None, target_native=gj or None, actor_urn=None,
                         at=timestamp(r["ZCHANGEDATE"], "s", EPOCH_2001),
                         payload={"member_identity_urns": [urn("identity", m) for m in members]}, carrier_message_urn=None)
            yield Emit(rec, Observation("ZWAGROUPMEMBERSCHANGE", [r["Z_PK"]], key, fingerprint(gj, r["ZCHANGEDATE"], r["ZCHANGETYPE"], members)))
            stats.unsupported_bump(f"group_change_type:{r['ZCHANGETYPE']}")
            stats.bump("events")

    def _owner_jids(self, conn: sqlite3.Connection, alias_pairs: dict[tuple[str, str], list[dict[str, Any]]],
                    stats: ExtractStats) -> set[str]:
        """The account owner's own JIDs.

        On incoming rows (ZISFROMME = 0) ZTOJID, when populated, is the recipient, i.e. the owner
        (validated on real data: exactly one distinct phone JID; it is also the most frequent group
        member and its LID is the self-chat's ZCONTACTJID). Nothing else in ChatStorage names the
        owner. The set is closed over LID<->phone alias pairs so both forms are flagged."""
        owners: set[str] = set()
        for jid in _message_jids(conn, 'ZTOJID', incoming=True):
            j = U.whatsapp_jid(jid)
            if jid_kind(j) in ("jid_phone", "jid_lid"):
                owners.add(j)
        if not owners:
            stats.notes.append("owner JID not detected: no incoming message carries ZTOJID; only the synthetic `me` identity is is_me")
            return owners
        changed = True
        while changed:
            changed = False
            for lid, phone in alias_pairs:
                if (lid in owners) != (phone in owners):
                    owners |= {lid, phone}
                    changed = True
        phones = sorted(j for j in owners if jid_kind(j) == "jid_phone")
        if len(phones) > 1:
            stats.notes.append(f"{len(phones)} distinct owner phone JIDs detected (number change?); all flagged is_me")
        return owners

    def _alias_pairs(self, snap: Snapshot) -> dict[tuple[str, str], list[dict[str, Any]]]:
        """(lid_jid, phone_jid) -> evidence, from LID.sqlite and ContactsV2.sqlite."""
        if snap.whatsapp_alias_pairs is not None:
            return snap.whatsapp_alias_pairs
        with ExitStack() as stack:
            sources = {name: stack.enter_context(closing(sqlite3.connect(snap.files[name].snapshot_path)))
                       if name in snap.files else None for name in aliases.SOURCES}
            return aliases.pairs(aliases.collect(sources))

    def _aliases(self, pairs: dict[tuple[str, str], list[dict[str, Any]]], scope: str, stats: ExtractStats,
                 urn: Any) -> Iterator[Emit]:
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
                  checkpoints: dict[str, Any] | None, full: bool,
                  probe: Callable[[dict[str, Any]], tuple[str, str | None, int | None]] | None = None) -> Iterator[Emission]:
        max_pk = conn.execute("SELECT max(Z_PK) FROM ZWAMESSAGE").fetchone()[0] or 0
        selected = None if full else stats.selected_message_rowids
        since_pk = 0
        if selected is None and not full and checkpoints and checkpoints.get("max_message_pk"):
            since_pk = int(checkpoints["max_message_pk"])
            if since_pk > max_pk:
                stats.notes.append("source reset detected: max Z_PK went backwards; falling back to full scan")
                stats.full_scan = True
                since_pk = 0
        stats.checkpoints["max_message_pk"] = max_pk
        if selected is None:
            stats.scanned_tables |= {"ZWAMESSAGE", "ZWAMEDIAITEM"}
        # WhatsApp exposes no durable edit/delete cursor. A recent-row window misses old edits,
        # backfilled messages and attachment changes, so reconcile every logical group each poll.
        if selected is None:
            groups = conn.execute(
                """SELECT ZCHATSESSION, ZSTANZAID, group_concat(Z_PK) AS pks, count(*) AS n, max(ZSORT) AS maxsort
                   FROM ZWAMESSAGE WHERE ZSTANZAID IS NOT NULL AND ZSTANZAID != ''
                   GROUP BY ZCHATSESSION, ZSTANZAID""")
        else:
            if any(type(pk) is not int or not -(1 << 63) <= pk < (1 << 63) for pk in selected):
                raise IncompatibleSource('selected WhatsApp message IDs must be signed integers')
            conn.execute('CREATE TEMP TABLE cs_selected_wa_messages(pk INTEGER PRIMARY KEY)')
            conn.executemany('INSERT INTO cs_selected_wa_messages VALUES(?)', ((pk,) for pk in selected))
            groups = conn.execute("""WITH chosen AS MATERIALIZED (
                SELECT DISTINCT m.ZCHATSESSION,m.ZSTANZAID
                FROM temp.cs_selected_wa_messages s CROSS JOIN ZWAMESSAGE m
                WHERE m.Z_PK=s.pk AND m.ZSTANZAID IS NOT NULL AND m.ZSTANZAID!='')
                SELECT m.ZCHATSESSION,m.ZSTANZAID,group_concat(m.Z_PK) AS pks,
                       count(*) AS n,max(m.ZSORT) AS maxsort
                FROM chosen CROSS JOIN ZWAMESSAGE m INDEXED BY cs_msg_key
                WHERE m.ZCHATSESSION IS chosen.ZCHATSESSION AND m.ZSTANZAID=chosen.ZSTANZAID
                GROUP BY m.ZCHATSESSION,m.ZSTANZAID""")
        media_sql = "SELECT * FROM ZWAMEDIAITEM WHERE ZMESSAGE = ?"
        data_sql = "SELECT * FROM ZWAMESSAGEDATAITEM WHERE ZMESSAGE = ? ORDER BY ZINDEX"
        for g in groups:
            pks = [int(x) for x in g['pks'].split(',')]
            ph = ','.join('?' * len(pks))
            native_rows = list(conn.execute(
                f'SELECT * FROM ZWAMESSAGE WHERE Z_PK IN ({ph}) ORDER BY ZSORT DESC,Z_PK DESC', pks))
            rep = native_rows[0]
            media = conn.execute(media_sql, (rep['Z_PK'],)).fetchone()
            if media is None and len(pks) > 1:
                for pk in pks:
                    media = conn.execute(media_sql, (pk,)).fetchone()
                    if media:
                        break
            data_rows = list(conn.execute(data_sql, (rep['Z_PK'],)))
            parent = conn.execute('SELECT ZCHATSESSION,ZSTANZAID,ZFROMJID,ZISFROMME FROM ZWAMESSAGE WHERE Z_PK=?',
                                  (rep['ZPARENTMESSAGE'],)).fetchone() if rep['ZPARENTMESSAGE'] else None
            if stats.watch_path and media is not None and media['ZMEDIALOCALPATH']:
                rel = media['ZMEDIALOCALPATH']
                if not rel.startswith('/') and '..' not in rel.split('/'):
                    stats.watch_path(media_root / rel)
            media_probe = probe(dict(media)) if probe is not None and media is not None else _probe_media(media_root, media)
            key = str(g['ZCHATSESSION']) + ':' + str(g['ZSTANZAID'])
            digest = native_fingerprint(
                [dict(r) for r in native_rows], dict(media) if media else None,
                [dict(r) for r in data_rows], dict(parent) if parent else None, media_probe,
                chat_by_pk.get(rep['ZCHATSESSION']), chat_by_pk.get(parent['ZCHATSESSION']) if parent else None, me_urn)
            yield from extract_unit(stats, key, digest,
                partial(self._message_group, g, rep, pks, media, data_rows, parent, media_probe, scope, stats, urn, chat_by_pk, me_urn))

    def _message_group(self, g: Any, rep: Any, pks: list[int], media: Any, data_rows: list[Any], parent: Any,
                       media_probe: tuple[str, str | None, int | None], scope: str, stats: ExtractStats,
                       urn: Any, chat_by_pk: dict[int, tuple[str, str]], me_urn: str) -> Iterator[Emit]:
        if rep["ZCHATSESSION"] is None or rep["ZCHATSESSION"] not in chat_by_pk:
            # Pointer only: no text, no JIDs (issue #6).
            stats.error("message_without_chat_session",
                        f"ZWAMESSAGE Z_PK={rep['Z_PK']} ZCHATSESSION={rep['ZCHATSESSION']} ZMESSAGEDATE={rep['ZMESSAGEDATE']} "
                        f"ZMESSAGETYPE={rep['ZMESSAGETYPE']} ZISFROMME={rep['ZISFROMME']} rows={len(pks)}")
            stats.unsupported_bump("message_without_chat_session")
            return
        chat_jid, c_urn = chat_by_pk[rep["ZCHATSESSION"]]
        is_me = bool(rep["ZISFROMME"])
        from_jid = rep["ZFROMJID"]
        if not is_me and not from_jid:
            stats.error("incoming_without_sender",
                        f"ZWAMESSAGE Z_PK={rep['Z_PK']} ZMESSAGEDATE={rep['ZMESSAGEDATE']} ZMESSAGETYPE={rep['ZMESSAGETYPE']}")
            return
        try:
            m_key = U.whatsapp_message_key(chat_jid, from_jid, is_me, rep["ZSTANZAID"])
        except ValueError as e:
            stats.error("bad_message_key", str(e))
            return
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
        ctype = CONTENT_TYPES.get(mtype)
        if media is not None and (ctype in ATTACHMENT_KIND or media["ZMEDIALOCALPATH"]):
            a_key = U.whatsapp_attachment_key(m_key, 0)
            a_urn = urn("attachment", a_key)
            rel = media["ZMEDIALOCALPATH"]
            availability, sha, size = media_probe
            if rel and (rel.startswith("/") or ".." in rel.split("/")):
                availability = "unknown"
                stats.error("unsafe_media_path", f"ZWAMEDIAITEM Z_PK={media['Z_PK']} ZMESSAGE={rep['Z_PK']}")
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
        for d in data_rows:
            parts.append({"type": "link", "text": None, "attachment_urn": None, "parser_status": "partial",
                          "payload": {k: v for k, v in {"title": d["ZTITLE"], "summary": d["ZSUMMARY"], "url": d["ZMATCHEDTEXT"]}.items() if v}})
        flags = allowlisted(dict(rep), ALLOWLIST["whatsapp.source_flags"])
        flags["zsort_negative_only"] = (g["maxsort"] or 0) < 0
        flags["duplicate_rows"] = len(pks)
        reply_native = None
        if rep["ZPARENTMESSAGE"]:
            pr = parent
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

def _probe_media(root: Path, media: Any) -> tuple[str, str | None, int | None]:
    if media is None:
        return 'not_downloaded', None, None
    rel, size = media['ZMEDIALOCALPATH'], media['ZFILESIZE']
    if not rel:
        return 'not_downloaded', None, size
    if rel.startswith('/') or '..' in rel.split('/'):
        return 'unknown', None, size
    path = root / rel
    if path.is_file() and not path.is_symlink():
        sha, size = _sha256(path)
        return 'available', sha, size
    return 'missing', None, size


def _sha256(p: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    n = 0
    with open(p, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n
