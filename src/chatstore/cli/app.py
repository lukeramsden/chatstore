"""CLI surface. Contract: specs/cli-json.md (`chatstore-cli-v1`)."""

from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..canonical.records import iso_utc
from ..config import (
    Config,
    Identity,
    NotInitialised,
    load_config,
    load_identity,
    save_config,
    save_identity,
)
from ..paths import DataDir, resolve_data_dir

ENVELOPE = "chatstore-cli-v1"

EXIT_OK, EXIT_FAIL, EXIT_USAGE, EXIT_PERMISSION, EXIT_INCOMPATIBLE = 0, 1, 2, 3, 4
EXIT_PARTIAL, EXIT_CONFLICT, EXIT_INVALID_ARCHIVE, EXIT_NOT_FOUND, EXIT_NOT_INIT = 5, 6, 7, 8, 9

SOURCES = ["whatsapp", "messages"]


class CliError(Exception):
    def __init__(self, code: int, err_code: str, message: str):
        super().__init__(message)
        self.code, self.err_code, self.message = code, err_code, message


@dataclass
class Result:
    data: Any
    exit_code: int = EXIT_OK
    page: dict[str, Any] | None = None
    warnings: list[dict[str, str]] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    human: Callable[[Any], str] | None = None


@dataclass
class Ctx:
    args: argparse.Namespace
    data_dir: DataDir
    _cfg: Config | None = None
    _ident: Identity | None = None
    _cache: Any = None

    def require_init(self) -> None:
        if not self.data_dir.exists():
            raise CliError(EXIT_NOT_INIT, "not_initialised", f"data directory {self.data_dir.root} is not initialised; run `chatstore init`")

    @property
    def cfg(self) -> Config:
        if self._cfg is None:
            self.require_init()
            self._cfg = load_config(self.data_dir)
        return self._cfg

    @property
    def ident(self) -> Identity:
        if self._ident is None:
            self.require_init()
            self._ident = load_identity(self.data_dir)
        return self._ident

    def cache(self, readonly: bool = False) -> Any:
        from ..cache import Cache
        if self._cache is None:
            self.require_init()
            self._cache = Cache(self.data_dir.cache, readonly=readonly)
        return self._cache

    def tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.cfg.timezone)
        except (KeyError, ValueError, OSError):
            return ZoneInfo("UTC")

    def parse_date(self, s: str | None, *, end: bool = False) -> int | None:
        if not s:
            return None
        try:
            if len(s) == 10:
                d = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=self.tz())
                if end:
                    d = d + timedelta(days=1)
                return int(d.timestamp() * 1000)
            d = datetime.fromisoformat(s)
            if d.tzinfo is None:
                d = d.replace(tzinfo=self.tz())
            return int(d.timestamp() * 1000)
        except ValueError as e:
            raise CliError(EXIT_USAGE, "bad_date", f"cannot parse date {s!r}: use YYYY-MM-DD or RFC 3339") from e

    def limit(self, default: int | None = None) -> int:
        lim = getattr(self.args, "limit", None) or default or self.cfg.search_limit
        return max(1, min(int(lim), self.cfg.max_limit))


# ---- helpers ----------------------------------------------------------------------------------

def _add_ts_iso(obj: Any) -> Any:
    """Add `iso` to canonical timestamp objects for convenience (recursively)."""
    if isinstance(obj, dict):
        if set(obj) >= {"raw", "unit", "epoch", "utc_ms"} and "iso" not in obj:
            obj = dict(obj)
            obj["iso"] = iso_utc(obj["utc_ms"])
            return obj
        return {k: _add_ts_iso(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_add_ts_iso(x) for x in obj]
    return obj


def freshness(ctx: Ctx) -> dict[str, Any] | None:
    try:
        cache = ctx.cache()
    except (CliError, sqlite3.Error):
        return None
    runs = cache.latest_sync_runs()
    out = []
    for source, info in ctx.ident.scopes.items():
        r = next((x for x in runs if x.get("source") == source), None)
        out.append({"source": source, "scope": info["scope"], "last_sync_finished_at": r.get("finished_at") if r else None,
                    "last_sync_status": r.get("status") if r else None, "coverage": r.get("coverage") if r else None})
    return {"sources": out, "timezone": ctx.cfg.timezone}


def hms(ms: int | None, tz: ZoneInfo) -> str:
    if ms is None:
        return "?"
    return datetime.fromtimestamp(ms / 1000, tz).strftime("%Y-%m-%d %H:%M")


def adapter_for(source: str) -> Any:
    if source == "whatsapp":
        from ..adapters.whatsapp import WhatsAppAdapter
        return WhatsAppAdapter()
    if source == "messages":
        from ..adapters.messages import MessagesAdapter
        return MessagesAdapter()
    raise CliError(EXIT_USAGE, "bad_source", f"unknown source {source!r}")


# ---- commands ---------------------------------------------------------------------------------

def cmd_doctor(ctx: Ctx) -> Result:
    from ..adapters.messages.adapter import find_helper
    from ..cache.fts import fts5_available
    dd = ctx.data_dir
    cfg = load_config(dd) if dd.exists() else Config()
    sources = []
    for s in SOURCES:
        d = adapter_for(s).diagnose(cfg).to_json()
        d.pop("root", None)
        sources.append(d)
    helper = find_helper(cfg)
    data = {
        "data_dir": str(dd.root), "initialised": dd.exists(), "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version, "fts5": fts5_available(), "sources": sources,
        "helper": {"found": helper is not None, "path": str(helper) if helper else None},
        "keychain": platform.system() == "Darwin",
    }
    problems = [p for s in sources for p in s.get("problems", [])]
    code = EXIT_OK
    if any(not s["readable"] and s["path_found"] for s in sources):
        code = EXIT_PERMISSION
    elif not data["fts5"] or any(s["path_found"] and s["readable"] and not s["schema_ok"] for s in sources):
        code = EXIT_INCOMPATIBLE

    def human(d: dict[str, Any]) -> str:
        lines = [f"data dir: {d['data_dir']} ({'initialised' if d['initialised'] else 'not initialised'})",
                 f"python {d['python']}, sqlite {d['sqlite']}, fts5 {'ok' if d['fts5'] else 'MISSING'}",
                 f"messages decoder helper: {d['helper']['path'] or 'NOT FOUND (run scripts/build-helper.sh)'}"]
        for s in d["sources"]:
            st = "ok" if s["schema_ok"] else "unreadable" if not s["readable"] else "not found" if not s["path_found"] else "schema problem"
            lines.append(f"{s['source']:9} {st}  schema={s.get('schema_version')}")
            if s.get("permission_hint"):
                lines.append(f"          hint: {s['permission_hint']}")
            for p in s.get("problems", []):
                lines.append(f"          problem: {p}")
        return "\n".join(lines)

    return Result(data, code, human=human, warnings=[{"code": "source_problem", "message": p} for p in problems])


def cmd_init(ctx: Ctx) -> Result:
    dd = ctx.data_dir
    created = not dd.exists()
    dd.create()
    cfg = load_config(dd) if not created else Config()
    if ctx.args.timezone:
        cfg.timezone = ctx.args.timezone
    elif created:
        cfg.timezone = _local_tz_name()
    for s in SOURCES:
        override = getattr(ctx.args, f"{s}_path", None)
        if override:
            cfg.source_paths[s] = override
    if ctx.args.helper_path:
        cfg.helper_path = ctx.args.helper_path
    ident = load_identity(dd) if not created else Identity()
    for s in SOURCES:
        ident.ensure_scope(s)
    save_config(dd, cfg)
    save_identity(dd, ident)
    from ..cache import Cache
    Cache(dd.cache).close()
    data = {"data_dir": str(dd.root), "created": created, "timezone": cfg.timezone,
            "scopes": [{"source": s, "scope": v["scope"]} for s, v in ident.scopes.items()]}
    return Result(data, human=lambda d: f"{'created' if d['created'] else 'updated'} {d['data_dir']} (timezone {d['timezone']})")


def _local_tz_name() -> str:
    try:
        p = Path("/etc/localtime").resolve()
        parts = p.parts
        if "zoneinfo" in parts:
            return "/".join(parts[parts.index("zoneinfo") + 1:])
    except OSError:
        pass
    return "UTC"


def cmd_sync(ctx: Ctx) -> Result:
    from ..sync import run_sync
    sources = SOURCES if ctx.args.source == "all" else [ctx.args.source]
    runs, code = [], EXIT_OK
    quiet = ctx.args.json or ctx.args.quiet
    for s in sources:
        ad = adapter_for(s)
        ident = ctx.ident
        if s not in ident.scopes:
            ident.ensure_scope(s)
            save_identity(ctx.data_dir, ident)
        def progress(msg: str, _s: str = s) -> None:
            print(f"[{_s}] {msg}", file=sys.stderr)

        out = run_sync(ad, ctx.data_dir, ctx.cfg, ident, ctx.cache(), full=(ctx.args.mode == "full"),
                       progress=None if quiet else progress)
        runs.append({"source": s, **out.run})
        code = max(code, out.exit_code) if out.exit_code != EXIT_OK else code
    warnings = [{"code": f"sync_{r['status']}", "message": f"{r['source']}: {r['status']} ({', '.join(e['code'] for e in r['errors'])})"}
                for r in runs if r["status"] != "complete"]

    def human(rs: list[dict[str, Any]]) -> str:
        lines = []
        for r in rs:
            c = {k: v for k, v in r["counts"].items() if k != "unsupported"}
            lines.append(f"{r['source']}: {r['status']} ({r['mode']}) counts={c}")
            if r["counts"].get("unsupported"):
                lines.append(f"  unsupported: {r['counts']['unsupported']}")
            for e in r["errors"]:
                lines.append(f"  error {e['code']} x{e['count']}: {e.get('sample') or ''}")
        return "\n".join(lines)

    return Result(runs, code, human=human, warnings=warnings)


def cmd_status(ctx: Ctx) -> Result:
    cache = ctx.cache(readonly=True)
    data = {"data_dir": str(ctx.data_dir.root), "timezone": ctx.cfg.timezone,
            "scopes": [{"source": s, "scope": v["scope"], "label": v.get("label")} for s, v in ctx.ident.scopes.items()],
            "sync_runs_latest": cache.latest_sync_runs(), "counts": cache.counts(), "coverage": cache.coverage(),
            "conflicts_open": cache.open_conflict_count()}

    def human(d: dict[str, Any]) -> str:
        tz = ctx.tz()
        lines = [f"data dir: {d['data_dir']}  timezone: {d['timezone']}"]
        for r in d["sync_runs_latest"]:
            cov = r.get("coverage") or {}
            lines.append(f"{r['source']:9} last sync {hms(r.get('finished_at'), tz)} {r['status']} ({r['mode']}); "
                         f"messages {cov.get('messages')} from {hms(cov.get('earliest_utc_ms'), tz)} to {hms(cov.get('latest_utc_ms'), tz)}")
        lines.append("records: " + ", ".join(f"{k}={v}" for k, v in sorted(d["counts"].items())))
        if d["conflicts_open"]:
            lines.append(f"open conflicts: {d['conflicts_open']} (see `chatstore conflicts list`)")
        return "\n".join(lines)

    return Result(data, human=human)


def _filters(ctx: Ctx) -> Any:
    from ..cache.queries import Filters
    a = ctx.args
    kinds = None
    if getattr(a, "kind", None):
        kinds = [k.strip() for k in a.kind.split(",") if k.strip()]
    senders = None
    if getattr(a, "sender", None):
        senders = [a.sender]
    if getattr(a, "person", None):
        senders = (senders or []) + ctx.cache().identities_of_person(a.person)
        if not senders:
            raise CliError(EXIT_NOT_FOUND, "not_found", f"person {a.person} has no linked identities")
    return Filters(since_ms=ctx.parse_date(getattr(a, "since", None)), until_ms=ctx.parse_date(getattr(a, "until", None), end=True),
                   source=getattr(a, "source", None), chat_urn=getattr(a, "chat", None), sender_urns=senders,
                   transport=getattr(a, "transport", None), kinds=kinds,
                   has_attachment=True if getattr(a, "has_attachment", False) else None)


def _page(items: list[Any], limit: int, cursor: str | None, truncated: bool = False) -> dict[str, Any]:
    return {"limit": limit, "returned": len(items), "next_cursor": cursor, "truncated": truncated or cursor is not None}


def cmd_search(ctx: Ctx) -> Result:
    from ..cache.queries import search
    cache = ctx.cache(readonly=True)
    limit = ctx.limit()
    try:
        items, cur = search(cache, ctx.args.query, _filters(ctx), limit=limit, cursor=ctx.args.cursor,
                            advanced=ctx.args.advanced, history=ctx.args.history)
    except sqlite3.OperationalError as e:
        raise CliError(EXIT_USAGE, "bad_query", f"FTS query error: {e}") from e
    except ValueError as e:
        raise CliError(EXIT_USAGE, "bad_query" if "syntax" in str(e) else "bad_cursor", str(e)) from e
    tz = ctx.tz()

    def human(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "no matches"
        lines = []
        for r in rows:
            sent = r.get("sent_at") or {}
            lines.append(f"{hms(sent.get('utc_ms'), tz)}  [{r['provenance']['source']}] {r.get('chat_label') or '?'} / {r.get('sender_label') or '?'}"
                         f"\n    {r['snippet']}\n    {r['urn']}")
        if cur:
            lines.append(f"-- more: --cursor {cur}")
        return "\n".join(lines)

    return Result(items, page=_page(items, limit, cur), human=human)


def cmd_chats(ctx: Ctx) -> Result:
    from ..cache.queries import list_chats
    cache = ctx.cache(readonly=True)
    limit = ctx.limit()
    try:
        items, cur = list_chats(cache, source=ctx.args.source, since_ms=ctx.parse_date(ctx.args.since), limit=limit, cursor=ctx.args.cursor)
    except ValueError as e:
        raise CliError(EXIT_USAGE, "bad_cursor", str(e)) from e
    tz = ctx.tz()

    def human(rows: list[dict[str, Any]]) -> str:
        lines = [f"{hms(r['last_message_utc_ms'], tz)}  [{r['provenance']['source']}] {r['chat_kind']:7} {r['message_count']:6}  {r['label'] or '?'}\n    {r['urn']}" for r in rows]
        if cur:
            lines.append(f"-- more: --cursor {cur}")
        return "\n".join(lines) or "no chats"

    return Result(items, page=_page(items, limit, cur), human=human)


def cmd_people(ctx: Ctx) -> Result:
    from ..cache.queries import list_people
    cache = ctx.cache(readonly=True)
    limit = ctx.limit()
    items, cur = list_people(cache, limit=limit, cursor=ctx.args.cursor)

    def human(rows: list[dict[str, Any]]) -> str:
        out = []
        for p in rows:
            out.append(f"{p['label'] or '?'}  {p['urn']}")
            for i in p["identities"]:
                out.append(f"    {i['link_state']:9} [{i.get('source')}] {i.get('address')}  {i['urn']}")
        return "\n".join(out) or "no people yet (use `chatstore identity link`)"

    return Result(items, page=_page(items, limit, cur), human=human)


def _render_message(m: dict[str, Any], tz: ZoneInfo) -> str:
    sent = (m.get("sent_at") or {}).get("utc_ms")
    who = "me" if m.get("is_from_me") else (m.get("sender_label") or "?")
    text = m.get("text") or ""
    extras = []
    for a in m.get("attachments", []):
        extras.append(f"[attachment {a.get('kind')} {a.get('filename') or ''} {a.get('availability')}]")
    for e in m.get("events", []):
        if e.get("kind", "").startswith("reaction"):
            extras.append(f"[{e['kind']} {e.get('payload', {}).get('emoji') or ''}]")
    if m.get("kind") not in ("message", None):
        extras.append(f"[{m['kind']}]")
    line = f"{hms(sent, tz)}  {who}: {text}"
    if extras:
        line += "  " + " ".join(extras)
    if m.get("truncated"):
        line += " …[truncated]"
    return line + f"\n    {m['urn']}"


def cmd_read(ctx: Ctx) -> Result:
    from ..cache.queries import read_chat
    cache = ctx.cache(readonly=True)
    limit = ctx.limit()
    chat = cache.get(ctx.args.chat_urn)
    if not chat or chat.get("entity") != "chats":
        raise CliError(EXIT_NOT_FOUND, "not_found", f"chat {ctx.args.chat_urn} not found (try `chatstore resolve`)")
    try:
        msgs, cur, truncated = read_chat(cache, ctx.args.chat_urn, _filters(ctx), limit=limit, cursor=ctx.args.cursor,
                                         max_chars=ctx.args.max_chars or ctx.cfg.max_chars)
    except ValueError as e:
        raise CliError(EXIT_USAGE, "bad_cursor", str(e)) from e
    tz = ctx.tz()

    def human(rows: list[dict[str, Any]]) -> str:
        lines = [_render_message(m, tz) for m in rows]
        if cur:
            lines.append(f"-- more: --cursor {cur}")
        return "\n".join(lines) or "no messages"

    return Result(msgs, page=_page(msgs, limit, cur, truncated), human=human)


def cmd_resolve(ctx: Ctx) -> Result:
    from ..cache.queries import resolve
    cache = ctx.cache(readonly=True)
    ref = ctx.args.ref
    digest = ctx.args.revision
    if ref.strip().startswith("{"):
        try:
            cit = json.loads(ref)
        except json.JSONDecodeError as e:
            raise CliError(EXIT_USAGE, "bad_citation", "citation must be JSON with a `urn` field") from e
        ref = cit.get("urn") or ""
        digest = digest or cit.get("revision_digest")
    if not ref.startswith("urn:uuid:"):
        raise CliError(EXIT_USAGE, "bad_urn", "expected a urn:uuid:... value")
    data = _add_ts_iso(resolve(cache, ref, digest))
    code = EXIT_NOT_FOUND if data["status"] in ("unknown", "ambiguous") else EXIT_OK

    def human(d: dict[str, Any]) -> str:
        if d["status"] in ("unknown", "ambiguous"):
            return f"{d['urn']}: {d['status']}" + (f" candidates={d.get('candidates')}" if d.get("candidates") else "")
        rec = d["record"]
        lines = [f"{d['urn']}: {d['status']} ({d['entity']}, source {rec.get('source')})",
                 f"  revisions: {len(d['revisions'])}, current digest {rec.get('revision_digest')}"]
        if d.get("resolved_via_alias"):
            lines.append(f"  resolved via alias from {d['resolved_via_alias']}")
        if d["entity"] == "messages":
            lines.append("  " + _render_message(rec | {"sender_label": None}, ctx.tz()))
        elif d["entity"] in ("chats", "identities"):
            lines.append(f"  {rec.get('observed_name') or rec.get('address') or rec.get('native_key')}")
        return "\n".join(lines)

    return Result(data, code, human=human)


def cmd_context(ctx: Ctx) -> Result:
    from ..cache.queries import context
    cache = ctx.cache(readonly=True)
    data = context(cache, ctx.args.message_urn, ctx.args.before, ctx.args.after, ctx.args.max_chars or ctx.cfg.max_chars)
    if data is None:
        raise CliError(EXIT_NOT_FOUND, "not_found", f"message {ctx.args.message_urn} not found")
    tz = ctx.tz()

    def human(d: dict[str, Any]) -> str:
        lines = [_render_message(m, tz) for m in d["before"]]
        if d["target"]:
            lines.append(">>> " + _render_message(d["target"], tz))
        lines += [_render_message(m, tz) for m in d["after"]]
        return "\n".join(lines)

    return Result(_add_ts_iso(data), human=human)


def cmd_rebuild_index(ctx: Ctx) -> Result:
    cache = ctx.cache()
    with cache.write():
        n = cache.rebuild_fts()
    return Result({"rows": n}, human=lambda d: f"rebuilt FTS index: {d['rows']} rows")


# ---- parser -----------------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="chatstore", description="Local-first search and encrypted archiving of your WhatsApp and Messages history.")
    p.add_argument("--data-dir", help="Override the data directory (also CHATSTORE_DATA_DIR).")
    p.add_argument("--json", action="store_true", help="Emit the chatstore-cli-v1 JSON envelope.")
    p.add_argument("--quiet", action="store_true", help="Suppress progress output on stderr.")
    sub = p.add_subparsers(dest="command", metavar="command")

    sub.add_parser("doctor", help="Check sources, permissions, helper and SQLite features.").set_defaults(fn=cmd_doctor)
    s = sub.add_parser("init", help="Create the data directory, config and account scopes.")
    s.add_argument("--timezone")
    s.add_argument("--whatsapp-path", help="Override the WhatsApp container directory.")
    s.add_argument("--messages-path", help="Override the Messages directory (contains chat.db).")
    s.add_argument("--helper-path", help="Path to chatstore-messages-decoder.")
    s.set_defaults(fn=cmd_init)
    s = sub.add_parser("sync", help="Ingest from sources into the cache.")
    s.add_argument("--source", choices=["all", *SOURCES], default="all")
    s.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    s.set_defaults(fn=cmd_sync)
    sub.add_parser("status", help="Show sync state, coverage and counts.").set_defaults(fn=cmd_status)

    def common_filters(sp: argparse.ArgumentParser, *, chat: bool = True) -> None:
        sp.add_argument("--since")
        sp.add_argument("--until")
        sp.add_argument("--source", choices=SOURCES)
        if chat:
            sp.add_argument("--chat", help="Chat URN")
        sp.add_argument("--sender", help="Identity URN")
        sp.add_argument("--person", help="Person URN (searches all linked identities)")
        sp.add_argument("--transport", choices=["imessage", "sms", "rcs", "whatsapp", "unknown"])
        sp.add_argument("--kind", help="Comma-separated message kinds (default message; `all`)")
        sp.add_argument("--has-attachment", action="store_true")
        sp.add_argument("--limit", type=int)
        sp.add_argument("--cursor")

    s = sub.add_parser("search", help="Full-text search.")
    s.add_argument("query")
    s.add_argument("--advanced", action="store_true", help="Treat query as raw FTS5 syntax.")
    s.add_argument("--history", action="store_true", help="Also search superseded revisions.")
    common_filters(s)
    s.set_defaults(fn=cmd_search)
    s = sub.add_parser("chats", help="List chats.")
    s.add_argument("--source", choices=SOURCES)
    s.add_argument("--since")
    s.add_argument("--limit", type=int)
    s.add_argument("--cursor")
    s.set_defaults(fn=cmd_chats)
    s = sub.add_parser("people", help="List people (linked identities).")
    s.add_argument("--limit", type=int)
    s.add_argument("--cursor")
    s.set_defaults(fn=cmd_people)
    s = sub.add_parser("read", help="Read a chat chronologically.")
    s.add_argument("chat_urn")
    common_filters(s, chat=False)
    s.add_argument("--max-chars", type=int)
    s.set_defaults(fn=cmd_read)
    s = sub.add_parser("resolve", help="Resolve a URN or citation JSON.")
    s.add_argument("ref")
    s.add_argument("--revision", help="Revision digest to check.")
    s.set_defaults(fn=cmd_resolve)
    s = sub.add_parser("context", help="Messages around a message.")
    s.add_argument("message_urn")
    s.add_argument("--before", type=int, default=5)
    s.add_argument("--after", type=int, default=5)
    s.add_argument("--max-chars", type=int)
    s.set_defaults(fn=cmd_context)
    sub.add_parser("rebuild-index", help="Rebuild the FTS index from records.").set_defaults(fn=cmd_rebuild_index)

    from .extra import register_extra
    register_extra(sub)
    return p


def emit(args: argparse.Namespace, command: str, res: Result, fresh: dict[str, Any] | None) -> int:
    if args.json:
        env = {"envelope": ENVELOPE, "command": command, "ok": res.exit_code in (EXIT_OK, EXIT_PARTIAL) and not res.errors,
               "exit_code": res.exit_code, "data": _add_ts_iso(res.data), "page": res.page, "freshness": fresh,
               "warnings": res.warnings, "errors": res.errors}
        sys.stdout.write(json.dumps(env, ensure_ascii=False) + "\n")
    else:
        if res.data is not None:
            text = res.human(res.data) if res.human else json.dumps(res.data, ensure_ascii=False, indent=2)
            sys.stdout.write(text + "\n")
        for w in res.warnings:
            print(f"warning: {w['message']}", file=sys.stderr)
        for e in res.errors:
            print(f"error: {e['message']}", file=sys.stderr)
    return res.exit_code


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return int(e.code) if isinstance(e.code, int) else EXIT_USAGE
    if not getattr(args, "command", None) or not hasattr(args, "fn"):
        parser.print_help()
        return EXIT_USAGE
    ctx = Ctx(args, DataDir(resolve_data_dir(args.data_dir)))
    command = " ".join(str(v) for k, v in vars(args).items() if k == "command" or k.endswith("_command") and v)
    try:
        res = args.fn(ctx)
        fresh = freshness(ctx) if args.json and args.command not in ("doctor", "init") and ctx.data_dir.exists() else None
        return emit(args, command, res, fresh)
    except CliError as e:
        return emit(args, command, Result(None, e.code, errors=[{"code": e.err_code, "message": e.message}]), None)
    except NotInitialised as e:
        return emit(args, command, Result(None, EXIT_NOT_INIT, errors=[{"code": "not_initialised", "message": str(e)}]), None)
    except KeyboardInterrupt:
        return 130
    except Exception as e:  # noqa: BLE001 - last resort: keep the JSON contract, never leak bodies
        return emit(args, command, Result(None, EXIT_FAIL, errors=[{"code": "internal", "message": f"{type(e).__name__}: {e}"}]), None)
    finally:
        if ctx._cache is not None:
            ctx._cache.close()
