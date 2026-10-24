//! Decodes an Apple Messages `chat.db` snapshot to versioned JSONL on stdout.
//!
//! Read-only: opens the given path with `imessage_database::tables::table::get_connection`
//! (SQLITE_OPEN_READ_ONLY). Only ever point it at a snapshot, never at the live database.
//!
//! Usage: chatstore-messages-decoder <path-to-chat.db> [--since-rowid N | --rowids-file rowids.json]
//!
//! Output: one JSON object per line.
//!   first line : {"type":"header","format":"chatstore-messages-decoder/1","library":"imessage-database 4.2.0",...}
//!   per message: {"type":"message", ...}
//!   last line  : {"type":"footer","messages":N,"errors":N}
//! Body text and edit history are emitted verbatim; the Python side never re-parses typedstream.

use std::collections::BTreeSet;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use imessage_database::{
    message_types::{
        edited::EditStatus,
        text_effects::text_effect::TextEffect,
        variants::{Announcement, CustomBalloon, TapbackAction, Variant},
    },
    tables::{
        attachment::Attachment,
        messages::{
            models::{BubbleComponent, GroupAction},
            Message,
        },
        table::{get_connection, Table},
    },
    util::query_context::QueryContext,
};
use serde::Serialize;

const FORMAT: &str = "chatstore-messages-decoder/1";

#[derive(Serialize)]
struct Header<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    format: &'a str,
    library: &'a str,
    helper_version: &'a str,
    capabilities: [&'a str; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    selection: Option<Selection>,
}

#[derive(Serialize)]
struct Selection {
    kind: &'static str,
    requested: usize,
}

struct Options {
    since: Option<i64>,
    rowids: Option<BTreeSet<i64>>,
}

fn options(args: &[String]) -> Result<Options, String> {
    let mut result = Options { since: None, rowids: None };
    let mut position = 2;
    while position < args.len() {
        let value = args.get(position + 1).ok_or("missing selection argument")?;
        match args[position].as_str() {
            "--since-rowid" if result.since.is_none() && result.rowids.is_none() => {
                result.since = Some(value.parse().map_err(|_| "invalid --since-rowid integer")?);
            }
            "--rowids-file" if result.since.is_none() && result.rowids.is_none() => {
                let mut file = std::fs::File::open(value).map_err(|e| format!("cannot open rowids file: {e}"))?;
                let mut data = Vec::new();
                Read::by_ref(&mut file).take(256 * 1024 + 1).read_to_end(&mut data).map_err(|e| e.to_string())?;
                if data.len() > 256 * 1024 {
                    return Err("rowids file exceeds byte limit".into());
                }
                let values: Vec<i64> = serde_json::from_slice(&data).map_err(|e| format!("invalid rowids array: {e}"))?;
                if values.len() > 10000 {
                    return Err("rowids selection exceeds row limit".into());
                }
                result.rowids = Some(values.into_iter().collect());
            }
            _ => return Err("unknown, repeated or conflicting selection option".into()),
        }
        position += 2;
    }
    Ok(result)
}

fn selected_query(query: &str, options: &Options) -> Result<String, &'static str> {
    // Keep the pinned library's schema-specific columns and correlated joins.
    // Refuse a changed template rather than risk filtering a full materialization.
    let head = query.trim_end().strip_suffix("\nORDER BY\n    m.date;").ok_or("unsupported library query template")?;
    let filter = if let Some(rowids) = &options.rowids {
        if rowids.is_empty() { "0".to_string() } else {
            format!("m.ROWID IN ({})", rowids.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
        }
    } else if let Some(since) = options.since {
        format!("m.ROWID > {since}")
    } else {
        return Ok(format!("{head}ORDER BY m.date,m.ROWID,c.chat_id,deleted_from;"));
    };
    Ok(format!("{head}WHERE {filter}\nORDER BY m.date,m.ROWID,c.chat_id,deleted_from;"))
}

#[derive(Serialize)]
struct Footer<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    messages: u64,
    errors: u64,
    query_fullscan_steps: i32,
    query_vm_steps: i32,
}

#[derive(Serialize)]
struct Part {
    index: usize,
    kind: &'static str, // text | attachment | app | retracted
    text: Option<String>,
    attachment_guid: Option<String>,
    attachment_name: Option<String>,
    inline: bool,
    links: Vec<String>,
    mentions: Vec<String>,
    edit_status: Option<&'static str>,
    edit_history: Vec<EditEvent>,
}

#[derive(Serialize)]
struct EditEvent {
    date: i64,
    text: String,
}

#[derive(Serialize)]
struct Att {
    rowid: i32,
    guid: Option<String>,
    filename: Option<String>,
    transfer_name: Option<String>,
    uti: Option<String>,
    mime_type: Option<String>,
    total_bytes: i64,
    is_sticker: bool,
    hide_attachment: i32,
}

#[derive(Serialize)]
struct Tapback {
    part_index: usize,
    action: &'static str,
    kind: String,
    target_guid: Option<String>,
}

#[derive(Serialize)]
struct Out {
    #[serde(rename = "type")]
    ty: &'static str,
    rowid: i32,
    guid: String,
    chat_id: Option<i32>,
    handle_id: Option<i32>,
    other_handle: Option<i32>,
    destination_caller_id: Option<String>,
    service: Option<String>,
    date: i64,
    date_read: i64,
    date_delivered: i64,
    date_edited: i64,
    is_from_me: bool,
    is_read: bool,
    item_type: i32,
    group_action_type: i32,
    group_title: Option<String>,
    group_action: Option<serde_json::Value>,
    announcement: Option<String>,
    variant: String,
    balloon_bundle_id: Option<String>,
    expressive_send_style_id: Option<String>,
    thread_originator_guid: Option<String>,
    thread_originator_part: Option<String>,
    associated_message_guid: Option<String>,
    associated_message_type: Option<i32>,
    tapback: Option<Tapback>,
    subject: Option<String>,
    text: Option<String>,
    parser_status: &'static str, // decoded | partial | unsupported | empty
    parts: Vec<Part>,
    attachments: Vec<Att>,
    num_attachments: i32,
    num_replies: i32,
    deleted_from: Option<i32>,
    is_edited: bool,
    is_fully_unsent: bool,
}

fn describe_variant(v: &Variant) -> String {
    match v {
        Variant::Normal => "normal".into(),
        Variant::Edited => "edited".into(),
        Variant::Tapback(_, _, _) => "tapback".into(),
        Variant::App(b) => match b {
            CustomBalloon::Application(id) => format!("app:{id}"),
            CustomBalloon::URL => "app:url".into(),
            CustomBalloon::Handwriting => "app:handwriting".into(),
            CustomBalloon::DigitalTouch => "app:digital_touch".into(),
            CustomBalloon::ApplePay => "app:apple_pay".into(),
            CustomBalloon::Fitness => "app:fitness".into(),
            CustomBalloon::Slideshow => "app:slideshow".into(),
            CustomBalloon::CheckIn => "app:check_in".into(),
            CustomBalloon::FindMy => "app:find_my".into(),
            CustomBalloon::Polls => "app:polls".into(),
            CustomBalloon::Business => "app:business".into(),
        },
        Variant::SharePlay => "shareplay".into(),
        Variant::Vote => "vote".into(),
        Variant::PollUpdate => "poll_update".into(),
        Variant::Unknown(n) => format!("unknown:{n}"),
    }
}

fn tapback_of(m: &Message) -> Option<Tapback> {
    if let Variant::Tapback(idx, action, kind) = m.variant() {
        let action = match action {
            TapbackAction::Added => "added",
            TapbackAction::Removed => "removed",
        };
        let kind = format!("{:?}", kind).to_lowercase();
        let target = m.clean_associated_guid().map(|(_, g)| g.to_string());
        return Some(Tapback { part_index: idx, action, kind, target_guid: target });
    }
    None
}

fn group_action_json(m: &Message) -> Option<serde_json::Value> {
    m.group_action().map(|a| match a {
        GroupAction::ParticipantAdded(h) => serde_json::json!({"kind":"participant_added","handle_id":h}),
        GroupAction::ParticipantRemoved(h) => serde_json::json!({"kind":"participant_removed","handle_id":h}),
        GroupAction::NameChange(n) => serde_json::json!({"kind":"name_change","name":n}),
        GroupAction::ParticipantLeft => serde_json::json!({"kind":"participant_left"}),
        GroupAction::GroupIconChanged => serde_json::json!({"kind":"icon_changed"}),
        GroupAction::GroupIconRemoved => serde_json::json!({"kind":"icon_removed"}),
        GroupAction::ChatBackgroundChanged => serde_json::json!({"kind":"background_changed"}),
        GroupAction::ChatBackgroundRemoved => serde_json::json!({"kind":"background_removed"}),
        GroupAction::PhoneNumberChanged(h) => serde_json::json!({"kind":"phone_number_changed","handle_id":h}),
    })
}

fn slice_text(text: &Option<String>, start: usize, end: usize) -> Option<String> {
    let t = text.as_ref()?;
    // Ranges are in UTF-16 code units per NSAttributedString; the library normalises to char
    // indices. Be defensive: clamp and fall back to whole text.
    let chars: Vec<char> = t.chars().collect();
    if start <= end && end <= chars.len() {
        Some(chars[start..end].iter().collect())
    } else {
        None
    }
}

fn build_parts(m: &Message) -> Vec<Part> {
    let mut out = Vec::new();
    for (i, c) in m.components.iter().enumerate() {
        let (status, history) = match &m.edited_parts {
            Some(e) => match e.parts.get(i) {
                Some(p) => (
                    Some(match p.status {
                        EditStatus::Edited => "edited",
                        EditStatus::Unsent => "unsent",
                        EditStatus::Original => "original",
                    }),
                    p.edit_history
                        .iter()
                        .map(|h| EditEvent { date: h.date, text: h.text.clone() })
                        .collect(),
                ),
                None => (None, vec![]),
            },
            None => (None, vec![]),
        };
        match c {
            BubbleComponent::Run(ranges) => {
                let mut text_buf = String::new();
                let mut links = vec![];
                let mut mentions = vec![];
                let mut att: Option<(Option<String>, Option<String>, bool)> = None;
                for r in ranges {
                    if let Some(meta) = &r.attachment {
                        att = Some((meta.guid.clone(), meta.name.clone(), r.emoji_image));
                    } else if let Some(s) = slice_text(&m.text, r.start, r.end) {
                        text_buf.push_str(&s);
                    }
                    for e in &r.effects {
                        match e {
                            TextEffect::Link(u) => links.push(u.to_string()),
                            TextEffect::Mention(h) => mentions.push(h.to_string()),
                            _ => {}
                        }
                    }
                }
                if let Some((guid, name, inline)) = att {
                    out.push(Part {
                        index: i,
                        kind: "attachment",
                        text: if text_buf.trim().is_empty() { None } else { Some(text_buf.clone()) },
                        attachment_guid: guid,
                        attachment_name: name,
                        inline,
                        links: vec![],
                        mentions: vec![],
                        edit_status: status,
                        edit_history: vec![],
                    });
                } else {
                    out.push(Part {
                        index: i,
                        kind: "text",
                        text: if text_buf.is_empty() { None } else { Some(text_buf) },
                        attachment_guid: None,
                        attachment_name: None,
                        inline: false,
                        links,
                        mentions,
                        edit_status: status,
                        edit_history: history,
                    });
                }
            }
            BubbleComponent::App => out.push(Part {
                index: i,
                kind: "app",
                text: None,
                attachment_guid: None,
                attachment_name: None,
                inline: false,
                links: vec![],
                mentions: vec![],
                edit_status: status,
                edit_history: history,
            }),
            BubbleComponent::Retracted => out.push(Part {
                index: i,
                kind: "retracted",
                text: None,
                attachment_guid: None,
                attachment_name: None,
                inline: false,
                links: vec![],
                mentions: vec![],
                edit_status: status,
                edit_history: history,
            }),
        }
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args[1] == "--help" {
        eprintln!("usage: chatstore-messages-decoder <chat.db snapshot> [--since-rowid N | --rowids-file rowids.json]");
        std::process::exit(64);
    }
    let path = Path::new(&args[1]);
    let options = options(&args).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(64);
    });
    let db = match get_connection(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot open database: {e}");
            std::process::exit(66);
        }
    };
    let stdout = std::io::stdout();
    let mut w = BufWriter::with_capacity(1 << 20, stdout.lock());
    let header = Header { ty: "header", format: FORMAT, library: "imessage-database 4.2.0", helper_version: env!("CARGO_PKG_VERSION"),
        capabilities: ["rowids-file-v1"],
        selection: options.rowids.as_ref().map(|ids| Selection { kind: "rowids", requested: ids.len() }) };
    writeln!(w, "{}", serde_json::to_string(&header).unwrap()).unwrap();

    let ctx = QueryContext::default();
    let base = match Message::stream_rows(&db, &ctx) {
        Ok(statement) => statement.expanded_sql().expect("library query has no unresolved bindings"),
        Err(error) => {
            eprintln!("query failed (schema mismatch?): {error}");
            std::process::exit(65);
        }
    };
    let query = selected_query(&base, &options).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(65);
    });
    let mut stmt = db.prepare_cached(&query).unwrap_or_else(|error| {
        eprintln!("selected query failed: {error}");
        std::process::exit(65);
    });
    let rows = match Message::rows(&mut stmt, []) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("query failed: {e}");
            std::process::exit(65);
        }
    };
    let (mut n, mut errs) = (0u64, 0u64);
    for row in rows {
        let mut m = match row {
            Ok(m) => m,
            Err(e) => {
                errs += 1;
                eprintln!("row error: {e}");
                continue;
            }
        };
        let mut parser_status = "decoded";
        match m.parse_body(&db) {
            Ok(b) => m.apply_body(b),
            Err(_) => {
                // Fallback to legacy parsing; if still nothing, mark by whether raw fields exist.
                if m.generate_text_legacy(&db).is_ok() {
                    parser_status = "partial";
                } else if m.attributed_body(&db).is_some() {
                    parser_status = "unsupported";
                } else {
                    parser_status = "empty";
                }
            }
        }
        let attachments = match Attachment::from_message(&db, &m) {
            Ok(v) => v
                .into_iter()
                .map(|a| Att {
                    rowid: a.rowid,
                    guid: a.guid,
                    filename: a.filename,
                    transfer_name: a.transfer_name,
                    uti: a.uti,
                    mime_type: a.mime_type,
                    total_bytes: a.total_bytes,
                    is_sticker: a.is_sticker,
                    hide_attachment: a.hide_attachment,
                })
                .collect(),
            Err(_) => {
                errs += 1;
                vec![]
            }
        };
        let variant = describe_variant(&m.variant());
        if variant.starts_with("unknown:") && parser_status == "decoded" {
            parser_status = "partial";
        }
        let announcement = m.get_announcement().map(|a| match a {
            Announcement::FullyUnsent => "fully_unsent".to_string(),
            Announcement::GroupAction(_) => "group_action".to_string(),
            Announcement::AudioMessageKept => "audio_message_kept".to_string(),
            Announcement::Unknown(n) => format!("unknown:{n}"),
        });
        let out = Out {
            ty: "message",
            rowid: m.rowid,
            guid: m.guid.clone(),
            chat_id: m.chat_id,
            handle_id: m.handle_id,
            other_handle: m.other_handle,
            destination_caller_id: m.destination_caller_id.clone(),
            service: m.service.clone(),
            date: m.date,
            date_read: m.date_read,
            date_delivered: m.date_delivered,
            date_edited: m.date_edited,
            is_from_me: m.is_from_me,
            is_read: m.is_read,
            item_type: m.item_type,
            group_action_type: m.group_action_type,
            group_title: m.group_title.clone(),
            group_action: group_action_json(&m),
            announcement,
            variant,
            balloon_bundle_id: m.balloon_bundle_id.clone(),
            expressive_send_style_id: m.expressive_send_style_id.clone(),
            thread_originator_guid: m.thread_originator_guid.clone(),
            thread_originator_part: m.thread_originator_part.clone(),
            associated_message_guid: m.associated_message_guid.clone(),
            associated_message_type: m.associated_message_type,
            tapback: tapback_of(&m),
            subject: m.subject.clone(),
            text: m.text.clone(),
            parser_status,
            parts: build_parts(&m),
            attachments,
            num_attachments: m.num_attachments,
            num_replies: m.num_replies,
            deleted_from: m.deleted_from,
            is_edited: m.is_edited(),
            is_fully_unsent: m.is_fully_unsent(),
        };
        writeln!(w, "{}", serde_json::to_string(&out).unwrap()).unwrap();
        n += 1;
    }
    let footer = Footer { ty: "footer", messages: n, errors: errs,
        query_fullscan_steps: stmt.get_status(rusqlite::StatementStatus::FullscanStep),
        query_vm_steps: stmt.get_status(rusqlite::StatementStatus::VmStep) };
    writeln!(w, "{}", serde_json::to_string(&footer).unwrap()).unwrap();
    w.flush().unwrap();
}
