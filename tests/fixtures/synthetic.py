"""Synthetic source databases with entirely made-up data, for tests.

These mirror only the columns the adapters (and the Rust helper) read. Nothing here is derived
from a real database.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

S2001 = 978307200  # unix seconds at 2001-01-01T00:00:00Z


def build_whatsapp(root: Path, *, extra_messages: int = 0) -> Path:
    """Create <root>/ChatStorage.sqlite (+ LID.sqlite, Message/ media) and return root."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "Message" / "Media" / "x").mkdir(parents=True, exist_ok=True)
    (root / "Message" / "Media" / "x" / "pic.jpg").write_bytes(b"\xff\xd8fakejpeg")
    db = root / "ChatStorage.sqlite"
    c = sqlite3.connect(db)
    c.executescript(
        """
        CREATE TABLE Z_METADATA (Z_VERSION INTEGER, Z_UUID VARCHAR(255), Z_PLIST BLOB);
        INSERT INTO Z_METADATA VALUES (1, 'x', NULL);
        CREATE TABLE ZWACHATSESSION (Z_PK INTEGER PRIMARY KEY, Z_ENT INTEGER, Z_OPT INTEGER, ZARCHIVED INTEGER, ZHIDDEN INTEGER,
            ZREMOVED INTEGER, ZSESSIONTYPE INTEGER, ZGROUPINFO INTEGER, ZCONTACTJID VARCHAR, ZPARTNERNAME VARCHAR, ZLASTMESSAGEDATE TIMESTAMP);
        CREATE TABLE ZWAGROUPMEMBER (Z_PK INTEGER PRIMARY KEY, ZISACTIVE INTEGER, ZISADMIN INTEGER, ZCHATSESSION INTEGER,
            ZMEMBERJID VARCHAR, ZCONTACTNAME VARCHAR);
        CREATE TABLE ZWAGROUPINFO (Z_PK INTEGER PRIMARY KEY, ZCHATSESSION INTEGER, ZCREATIONDATE TIMESTAMP, ZCREATORJID VARCHAR, ZOWNERJID VARCHAR);
        CREATE TABLE ZWAPROFILEPUSHNAME (Z_PK INTEGER PRIMARY KEY, ZJID VARCHAR, ZPUSHNAME VARCHAR);
        CREATE TABLE ZWAMESSAGE (Z_PK INTEGER PRIMARY KEY, Z_ENT INTEGER, Z_OPT INTEGER, ZFLAGS INTEGER, ZGROUPEVENTTYPE INTEGER,
            ZISFROMME INTEGER, ZMESSAGEERRORSTATUS INTEGER, ZMESSAGESTATUS INTEGER, ZMESSAGETYPE INTEGER, ZSORT INTEGER,
            ZSPOTLIGHTSTATUS INTEGER, ZSTARRED INTEGER, ZCHATSESSION INTEGER, ZGROUPMEMBER INTEGER, ZMEDIAITEM INTEGER,
            ZPARENTMESSAGE INTEGER, ZMESSAGEDATE TIMESTAMP, ZSENTDATE TIMESTAMP, ZFROMJID VARCHAR, ZPUSHNAME VARCHAR,
            ZSTANZAID VARCHAR, ZTEXT VARCHAR, ZTOJID VARCHAR);
        CREATE TABLE ZWAMEDIAITEM (Z_PK INTEGER PRIMARY KEY, ZCLOUDSTATUS INTEGER, ZFILESIZE INTEGER, ZMEDIAORIGIN INTEGER,
            ZMESSAGE INTEGER, ZLATITUDE FLOAT, ZLONGITUDE FLOAT, ZMOVIEDURATION FLOAT, ZASPECTRATIO FLOAT, ZMEDIALOCALPATH VARCHAR,
            ZMEDIAURL VARCHAR, ZTITLE VARCHAR, ZVCARDNAME VARCHAR, ZVCARDSTRING VARCHAR, ZMETADATA BLOB, ZMEDIAKEY BLOB);
        CREATE TABLE ZWAMESSAGEDATAITEM (Z_PK INTEGER PRIMARY KEY, ZINDEX INTEGER, ZTYPE INTEGER, ZMESSAGE INTEGER,
            ZTITLE VARCHAR, ZSUMMARY VARCHAR, ZMATCHEDTEXT VARCHAR);
        CREATE TABLE ZWAGROUPMEMBERSCHANGE (Z_PK INTEGER PRIMARY KEY, ZCHANGETYPE INTEGER, ZCHANGEDATE TIMESTAMP, ZGROUPJID VARCHAR, ZMEMBERJIDS VARCHAR);
        """
    )
    alice = "15550001111@s.whatsapp.net"
    bob_lid = "987654321@lid"
    group = "15550001111-1600000000@g.us"
    c.execute("INSERT INTO ZWACHATSESSION VALUES (1,1,1,0,0,0,0,NULL,?, 'Alice Example', 700000000)", (alice,))
    c.execute("INSERT INTO ZWACHATSESSION VALUES (2,1,1,0,0,0,1,1,?, 'Test Group', 700000100)", (group,))
    c.execute("INSERT INTO ZWACHATSESSION VALUES (3,1,1,1,0,0,0,NULL,?, 'Bob Lid', 700000200)", (bob_lid,))
    c.execute("INSERT INTO ZWAGROUPINFO VALUES (1, 2, 690000000, ?, ?)", (alice, alice))
    c.execute("INSERT INTO ZWAGROUPMEMBER VALUES (1,1,1,2,?, 'Alice Example')", (alice,))
    c.execute("INSERT INTO ZWAGROUPMEMBER VALUES (2,1,0,2,?, NULL)", (bob_lid,))
    c.execute("INSERT INTO ZWAPROFILEPUSHNAME VALUES (1, ?, 'alice~')", (alice,))
    rows = [
        # pk, flags, gevt, fromme, err, status, type, sort, chat, media, parent, date, sent, fromjid, stanza, text, tojid
        (1, 0, 0, 0, 0, 0, 0, 1, 1, None, None, 700000000, None, alice, "3EB0AAAA0001", "hello from alice", None),
        (2, 0, 0, 1, 0, 8, 0, 2, 1, None, None, 700000010, 700000011, None, "3EB0AAAA0002", "hi alice, see you tomorrow", alice),
        # history-sync duplicate of pk 2 with negative sort
        (3, 0, 0, 1, 0, 8, 0, -5, 1, None, None, 700000010, 700000011, None, "3EB0AAAA0002", "hi alice, see you tomorrow", alice),
        (4, 0, 0, 0, 0, 0, 1, 3, 1, 1, None, 700000020, None, alice, "3EB0AAAA0003", None, None),
        (5, 0, 15, 0, 0, 0, 6, 4, 2, None, None, 700000100, None, alice, "3EB0AAAA0004", None, None),
        (6, 0, 0, 0, 0, 0, 0, 5, 2, None, None, 700000110, None, bob_lid, "3EB0AAAA0005", "group chatter 🎉", None),
        (7, 0, 0, 0, 0, 0, 99, 6, 2, None, None, 700000120, None, bob_lid, "3EB0AAAA0006", None, None),
        (8, 0, 0, 0, 0, 0, 0, 7, 3, None, None, 700000200, None, bob_lid, "3EB0AAAA0007", "direct from lid", None),
    ]
    for i in range(extra_messages):
        rows.append((100 + i, 0, 0, 0, 0, 0, 0, 100 + i, 1, None, None, 700001000 + i, None, alice, f"3EB0EXTRA{i:04d}", f"extra message {i}", None))
    for r in rows:
        c.execute("INSERT INTO ZWAMESSAGE VALUES (?,1,1,?,?,?,?,?,?,?,0,0,?,NULL,?,?,?,?,?,NULL,?,?,?)",
                  (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14], r[15], r[16]))
    c.execute("INSERT INTO ZWAMEDIAITEM VALUES (1, 0, 9, 1, 4, NULL, NULL, NULL, 1.5, 'Media/x/pic.jpg', NULL, NULL, NULL, NULL, NULL, NULL)")
    c.execute("INSERT INTO ZWAGROUPMEMBERSCHANGE VALUES (1, 5, 700000090, ?, ?)", (group, bob_lid))
    c.commit()
    c.close()
    lid = sqlite3.connect(root / "LID.sqlite")
    lid.executescript("CREATE TABLE ZWAZACCOUNT (Z_PK INTEGER PRIMARY KEY, ZIDENTIFIER VARCHAR, ZPHONENUMBER VARCHAR);")
    lid.execute("INSERT INTO ZWAZACCOUNT VALUES (1, ?, '+1 555 000 2222')", (bob_lid,))
    lid.commit()
    lid.close()
    return root


def build_messages(root: Path, *, extra_messages: int = 0) -> Path:
    """Create <root>/chat.db (Apple Messages shape) and return root."""
    root.mkdir(parents=True, exist_ok=True)
    att_dir = root / "Attachments" / "ab" / "01"
    att_dir.mkdir(parents=True, exist_ok=True)
    (att_dir / "IMG_0001.HEIC").write_bytes(b"fakeheic")
    db = root / "chat.db"
    c = sqlite3.connect(db)
    c.executescript(
        """
        CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT, country TEXT, service TEXT, uncanonicalized_id TEXT, person_centric_id TEXT);
        CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT UNIQUE, style INTEGER, state INTEGER, account_id TEXT, properties BLOB,
            chat_identifier TEXT, service_name TEXT, room_name TEXT, account_login TEXT, is_archived INTEGER DEFAULT 0, display_name TEXT,
            group_id TEXT, is_filtered INTEGER, successful_query INTEGER);
        CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
        CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER, message_date INTEGER);
        CREATE TABLE chat_recoverable_message_join (chat_id INTEGER, message_id INTEGER, delete_date INTEGER, ck_sync_state INTEGER);
        CREATE TABLE attachment (ROWID INTEGER PRIMARY KEY, guid TEXT UNIQUE, created_date INTEGER, start_date INTEGER, filename TEXT,
            uti TEXT, mime_type TEXT, transfer_state INTEGER, is_outgoing INTEGER, user_info BLOB, transfer_name TEXT, total_bytes INTEGER,
            is_sticker INTEGER DEFAULT 0, sticker_user_info BLOB, attribution_info BLOB, hide_attachment INTEGER DEFAULT 0,
            ck_sync_state INTEGER, original_guid TEXT, is_commsafety_sensitive INTEGER, emoji_image_short_description TEXT);
        CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
        CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT UNIQUE, text TEXT, replace INTEGER, service_center TEXT, handle_id INTEGER,
            subject TEXT, country TEXT, attributedBody BLOB, version INTEGER, type INTEGER, service TEXT, account TEXT, account_guid TEXT,
            error INTEGER, date INTEGER, date_read INTEGER, date_delivered INTEGER, is_delivered INTEGER, is_finished INTEGER, is_emote INTEGER,
            is_from_me INTEGER, is_empty INTEGER, is_delayed INTEGER, is_auto_reply INTEGER, is_prepared INTEGER, is_read INTEGER,
            is_system_message INTEGER, is_sent INTEGER, has_dd_results INTEGER, is_service_message INTEGER, is_forward INTEGER,
            was_downgraded INTEGER, is_archive INTEGER, cache_has_attachments INTEGER, cache_roomnames TEXT, was_data_detected INTEGER,
            was_deduplicated INTEGER, is_audio_message INTEGER, is_played INTEGER, date_played INTEGER, item_type INTEGER,
            other_handle INTEGER, group_title TEXT, group_action_type INTEGER, share_status INTEGER, share_direction INTEGER,
            is_expirable INTEGER, expire_state INTEGER, message_action_type INTEGER, message_source INTEGER, associated_message_guid TEXT,
            associated_message_type INTEGER, balloon_bundle_id TEXT, payload_data BLOB, expressive_send_style_id TEXT,
            associated_message_range_location INTEGER, associated_message_range_length INTEGER, time_expressive_send_played INTEGER,
            message_summary_info BLOB, ck_sync_state INTEGER, ck_record_id TEXT, ck_record_change_tag TEXT, destination_caller_id TEXT,
            is_corrupt INTEGER, reply_to_guid TEXT, sort_id INTEGER, is_spam INTEGER, has_unseen_mention INTEGER, thread_originator_guid TEXT,
            thread_originator_part TEXT, syndication_ranges TEXT, synced_syndication_ranges TEXT, was_delivered_quietly INTEGER,
            did_notify_recipient INTEGER, date_retracted INTEGER, date_edited INTEGER, was_detonated INTEGER, part_count INTEGER,
            is_stewie INTEGER, is_kt_verified INTEGER, is_sos INTEGER, is_critical INTEGER, bia_reference_id TEXT, fallback_hash TEXT,
            associated_message_emoji TEXT, is_pending_satellite_send INTEGER, needs_relay INTEGER, schedule_type INTEGER,
            schedule_state INTEGER, sent_or_received_off_grid INTEGER, ck_chat_id TEXT);
        PRAGMA user_version = 1;
        """
    )
    c.execute("INSERT INTO handle VALUES (1, '+15550003333', 'us', 'iMessage', '(555) 000-3333', NULL)")
    c.execute("INSERT INTO handle VALUES (2, 'Carol@Example.com', 'us', 'iMessage', NULL, NULL)")
    c.execute("INSERT INTO handle VALUES (3, '+15550003333', 'us', 'SMS', NULL, NULL)")
    # same phone as the WhatsApp fixture's Alice -> `identity suggest` candidate
    c.execute("INSERT INTO handle VALUES (4, '+1 (555) 000-1111', 'us', 'SMS', NULL, NULL)")
    c.execute("INSERT INTO chat (ROWID, guid, style, chat_identifier, service_name, display_name, is_archived) VALUES (1, 'iMessage;-;+15550003333', 45, '+15550003333', 'iMessage', NULL, 0)")
    c.execute("INSERT INTO chat (ROWID, guid, style, chat_identifier, service_name, display_name, is_archived) VALUES (2, 'iMessage;+;chat100200300', 43, 'chat100200300', 'iMessage', 'Family', 0)")
    c.executemany("INSERT INTO chat_handle_join VALUES (?,?)", [(1, 1), (2, 1), (2, 2), (2, 4)])

    def ns(sec: int) -> int:
        return sec * 1_000_000_000

    msgs = [
        # rowid, guid, text, handle, service, date, is_from_me, item_type, assoc_guid, assoc_type, ck_chat_id, thread_orig
        (1, "AAAAAAAA-0000-4000-8000-000000000001", "hello dave", 1, "iMessage", ns(700000000), 0, 0, None, 0, None, None),
        (2, "AAAAAAAA-0000-4000-8000-000000000002", "hi carol, see you tomorrow", 0, "iMessage", ns(700000010), 1, 0, None, 0, None, None),
        (3, "AAAAAAAA-0000-4000-8000-000000000003", None, 1, "iMessage", ns(700000020), 0, 0, None, 0, None, None),  # attachment only
        (4, "AAAAAAAA-0000-4000-8000-000000000004", "Loved “hello dave”", 2, "iMessage", ns(700000030), 0, 0,
         "p:0/AAAAAAAA-0000-4000-8000-000000000001", 2000, None, None),
        (5, "AAAAAAAA-0000-4000-8000-000000000005", "orphan via ck", 1, "iMessage", ns(700000040), 0, 0, None, 0, "iMessage;-;+15550003333", None),
        (6, "AAAAAAAA-0000-4000-8000-000000000006", "orphan stub", 3, "SMS", ns(700000050), 0, 0, None, 0, "SMS;-;+15550009999", None),
        (7, "AAAAAAAA-0000-4000-8000-000000000007", "reply in thread", 1, "iMessage", ns(700000060), 0, 0, None, 0, None,
         "AAAAAAAA-0000-4000-8000-000000000001"),
    ]
    for i in range(extra_messages):
        msgs.append((100 + i, f"AAAAAAAA-0000-4000-8000-0000000{i:05d}", f"extra message {i}", 1, "iMessage", ns(700001000 + i), 0, 0, None, 0, None, None))
    for m in msgs:
        c.execute(
            """INSERT INTO message (ROWID, guid, text, handle_id, service, date, date_read, date_delivered, is_from_me, is_read, item_type,
                   group_action_type, associated_message_guid, associated_message_type, ck_chat_id, thread_originator_guid, date_edited, error)
               VALUES (?,?,?,?,?,?,0,0,?,0,?,0,?,?,?,?,0,0)""",
            (m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7], m[8], m[9], m[10], m[11]))
    joins = [(1, 1), (1, 2), (1, 3), (1, 4), (2, 7)] + [(1, 100 + i) for i in range(extra_messages)]
    c.executemany("INSERT INTO chat_message_join VALUES (?,?,0)", joins)
    c.execute("INSERT INTO attachment (ROWID, guid, filename, uti, mime_type, transfer_name, total_bytes, is_sticker) VALUES "
              "(1, 'ATT00000-0000-4000-8000-000000000001', ?, 'public.heic', 'image/heic', 'IMG_0001.HEIC', 8, 0)",
              (str(att_dir / "IMG_0001.HEIC"),))
    c.execute("INSERT INTO message_attachment_join VALUES (3, 1)")
    c.commit()
    c.close()
    return root
