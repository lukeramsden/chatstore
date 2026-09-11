"""Archive / identity / scope / conflicts / purge commands (Phases 3 and 4)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ..config import save_identity


def _app() -> Any:
    from . import app
    return app


# ---- archive ----------------------------------------------------------------------------------------

def _password(ctx: Any, *, confirm: bool = False) -> str:
    from ..archive import password as P
    A = _app()
    try:
        pw, _origin = P.obtain(ctx.data_dir.root, confirm=confirm, allow_prompt=not ctx.args.json or sys.stdin.isatty())
    except P.PasswordUnavailable as e:
        raise A.CliError(A.EXIT_USAGE, "no_password", str(e)) from None
    except OSError as e:
        raise A.CliError(A.EXIT_USAGE, "no_password", f"cannot read password source: {type(e).__name__}") from None
    return pw


def _progress(ctx: Any) -> Any:
    if ctx.args.json or ctx.args.quiet:
        return None
    return lambda msg: print(msg, file=sys.stderr)


def cmd_archive_export(ctx: Any) -> Any:
    from ..archive import export as E
    A = _app()
    ctx.require_init()
    a = ctx.args
    since = ctx.parse_date(a.since)
    until = ctx.parse_date(a.until)
    output = Path(a.output).expanduser() if a.output else ctx.data_dir.exports
    pw = _password(ctx, confirm=True)
    cache = ctx.cache()
    adapter_versions = {s: A.adapter_for(s).version for s in A.SOURCES}
    results = []
    plans = []
    if not a.no_catalogue:
        plans.append(E.collect_catalogue(cache))
    if not a.catalogue_only:
        for b in E.plan_buckets(cache, since, until):
            plans.append(E.collect_bucket(cache, b, context=a.context))
    prog = _progress(ctx)
    for plan in plans:
        r = E.write_archive(cache, ctx.data_dir, ctx.cfg, ctx.ident, plan, output=output, password=pw, media=a.media,
                            force=a.force, adapter_versions=adapter_versions, progress=prog)
        results.append(r.__dict__)
    save_identity(ctx.data_dir, ctx.ident)
    written = sum(1 for r in results if r["action"] == "written")
    partial = any(r.get("coverage", {}).get("status") == "partial" for r in results)
    warnings = []
    if partial:
        warnings.append({"code": "partial_coverage", "message": "some archives were built from incomplete sync runs"})
    if a.media == "text":
        warnings.append({"code": "media_excluded", "message": "attachment bytes were not exported (--media text)"})

    def human(d: Any) -> str:
        lines = [f"{r['kind']:<9} {r['label']:<20} {r['action']:<14} {r.get('path') or ''}" for r in d["archives"]]
        return "\n".join(lines + [f"{written} archive(s) written to {output}"])
    return A.Result({"output": str(output), "written": written, "archives": results}, warnings=warnings, human=human)


def _archive_paths(arg: list[str]) -> list[Path]:
    from ..archive.import_ import order_paths
    out: list[Path] = []
    for p in arg:
        path = Path(p).expanduser()
        if path.is_dir():
            out.extend(sorted(path.glob("chatstore-*.zip")))
        else:
            out.append(path)
    return order_paths(out)


def cmd_archive_verify(ctx: Any) -> Any:
    from ..archive.verify import verify_archive
    A = _app()
    paths = _archive_paths(ctx.args.paths)
    if not paths:
        raise A.CliError(A.EXIT_NOT_FOUND, "not_found", "no archives found")
    pw = _password(ctx)
    results = []
    code = A.EXIT_OK
    for p in paths:
        if not p.is_file():
            results.append({"path": str(p), "ok": False, "problems": ["file not found"]})
            code = A.EXIT_INVALID_ARCHIVE
            continue
        v = verify_archive(p, pw, validate_schema=not ctx.args.no_schema)
        results.append({"path": v.path, "ok": v.ok, "wrong_password": v.wrong_password, "problems": v.problems,
                        "counts": v.counts, "schema_errors": v.schema_errors,
                        "manifest": {k: v.manifest.get(k) for k in ("export_id", "export_set", "kind", "bucket", "revision",
                                                                    "supersedes", "created_at", "coverage", "attachment_policy",
                                                                    "media", "counts")} if v.manifest else None})
        if not v.ok:
            code = A.EXIT_INVALID_ARCHIVE

    def human(d: Any) -> str:
        return "\n".join(f"{'OK ' if r['ok'] else 'BAD'} {Path(r['path']).name}" + ("" if r["ok"] else "  " + "; ".join(r["problems"])) for r in d)
    return A.Result(results, code, human=human)


def cmd_archive_import(ctx: Any) -> Any:
    from ..archive.import_ import import_archive
    A = _app()
    a = ctx.args
    if not ctx.data_dir.exists():
        ctx.data_dir.create()
        from ..config import Config, Identity, save_config
        save_config(ctx.data_dir, Config())
        save_identity(ctx.data_dir, Identity())
    paths = _archive_paths(a.paths)
    if not paths:
        raise A.CliError(A.EXIT_NOT_FOUND, "not_found", "no archives found")
    pw = _password(ctx)
    cache = ctx.cache()
    results = []
    code = A.EXIT_OK
    prog = _progress(ctx)
    for p in paths:
        if prog:
            prog(f"importing {p.name}")
        r = import_archive(cache, ctx.data_dir, ctx.ident, p, pw, staging=ctx.data_dir.staging, restore_media=not a.no_media)
        d = dict(r.__dict__)
        d["exit_code"] = r.exit_code
        results.append(d)
        if r.exit_code and (code == A.EXIT_OK or r.exit_code < code):
            code = r.exit_code
        if r.exit_code == A.EXIT_CONFLICT and a.stop_on_conflict:
            break
    conflicts = sum(r["conflicts"] for r in results)
    if conflicts and code == A.EXIT_OK:
        code = A.EXIT_CONFLICT
    warnings = []
    unresolved = sum(r["unresolved_refs"] for r in results)
    if unresolved:
        warnings.append({"code": "unresolved_refs", "message": f"{unresolved} cross-archive reference(s) not yet importable; import the other buckets"})

    def human(d: Any) -> str:
        lines = []
        for r in d:
            o = r["outcomes"]
            lines.append(f"{r['action']:<17} {Path(r['path']).name}  +{o.get('inserted', 0)} ~{o.get('updated', 0)} ={o.get('unchanged', 0)} "
                         f"conflicts={r['conflicts']} retired={r['retired']} blobs={r['blobs_restored']}"
                         + ("  " + "; ".join(r["problems"]) if r["problems"] else ""))
        return "\n".join(lines)
    return A.Result(results, code, warnings=warnings, human=human)


def cmd_archive_password(ctx: Any) -> Any:
    from ..archive import password as P
    A = _app()
    a = ctx.args
    root = ctx.data_dir.root
    if a.action == "suggest":
        pw = P.suggest()
        # deliberately outside the JSON envelope contract: a suggestion is not a secret yet, but keep it off JSON
        if a.json:
            raise A.CliError(A.EXIT_USAGE, "no_json", "password suggestions are never emitted in JSON; run without --json")
        print(pw)
        return A.Result(None)
    if a.action == "set":
        if not P.keychain_available():
            raise A.CliError(A.EXIT_USAGE, "no_keychain", "no OS keychain integration on this platform; use CHATSTORE_PASSWORD_FILE")
        if not sys.stdin.isatty():
            raise A.CliError(A.EXIT_USAGE, "no_tty", "password set needs an interactive terminal")
        import getpass
        pw = getpass.getpass("archive password: ")
        if pw != getpass.getpass("confirm password: ") or not pw:
            raise A.CliError(A.EXIT_USAGE, "mismatch", "passwords did not match or were empty")
        ok = P.keychain_set(root, pw)
        return A.Result({"stored": ok, "service": P.KEYCHAIN_SERVICE, "account": P.keychain_account(root)},
                        A.EXIT_OK if ok else A.EXIT_FAIL, human=lambda d: "stored in keychain" if d["stored"] else "keychain store failed")
    if a.action == "status":
        has_kc = bool(P.keychain_get(root)) if P.keychain_available() else False
        import os
        return A.Result({"keychain": has_kc, "password_file": bool(os.environ.get("CHATSTORE_PASSWORD_FILE")),
                         "keychain_available": P.keychain_available()},
                        human=lambda d: f"keychain: {d['keychain']}  password file: {d['password_file']}")
    if a.action == "clear":
        return A.Result({"cleared": P.keychain_delete(root)}, human=lambda d: "cleared" if d["cleared"] else "nothing to clear")
    raise A.CliError(A.EXIT_USAGE, "usage", "unknown password action")


def cmd_archive_list(ctx: Any) -> Any:
    A = _app()
    ctx.require_init()
    cache = ctx.cache(readonly=True)
    exp = [dict(r) for r in cache.conn.execute("SELECT export_id, export_set, kind, bucket_label, revision, path, created_at FROM exports ORDER BY kind, bucket_label, revision")]
    imp = [dict(r) for r in cache.conn.execute("SELECT export_id, export_set, kind, bucket_label, revision, imported_at, superseded_by FROM imports ORDER BY kind, bucket_label, revision")]

    def human(d: Any) -> str:
        lines = [f"exported  {e['kind']:<9} {e['bucket_label']:<20} r{e['revision']:04d} {e['path']}" for e in d["exports"]]
        lines += [f"imported  {e['kind']:<9} {e['bucket_label']:<20} r{e['revision']:04d} {e['export_id']}" + (" (superseded)" if e["superseded_by"] else "") for e in d["imports"]]
        return "\n".join(lines) or "no archives"
    return A.Result({"exports": exp, "imports": imp}, human=human)


# ---- identity / scope / conflicts / purge -------------------------------------------------------------

def cmd_identity(ctx: Any) -> Any:
    from .. import curation as C
    A = _app()
    ctx.require_init()
    a = ctx.args
    cache = ctx.cache()
    if a.identity_command == "suggest":
        items = C.suggest(cache, limit=ctx.limit(200))

        def human(d: Any) -> str:
            lines = []
            for s in d:
                lines.append(f"{s['evidence']['type']} {s['evidence']['detail']}")
                for i in s["identities"]:
                    lines.append(f"    [{i['source']}] {i['urn']}  {', '.join(n for n in i['names'] if n)}" + ("  (linked)" if i["already_linked"] else ""))
            return "\n".join(lines) or "no suggestions"
        return A.Result(items, human=human)
    if a.identity_command == "link":
        with cache.write():
            try:
                if a.person:
                    person_urn = a.person
                else:
                    person_urn = C.create_person(cache, a.label)["urn"]
                links = C.link(cache, person_urn, a.identities)
            except ValueError as e:
                raise A.CliError(A.EXIT_NOT_FOUND, "not_found", str(e)) from None
        return A.Result({"person_urn": person_urn, "links": links},
                        human=lambda d: f"{d['person_urn']}: linked {len(d['links'])} identit{'y' if len(d['links']) == 1 else 'ies'}")
    if a.identity_command == "unlink":
        with cache.write():
            links = C.unlink(cache, a.identity, a.person)
        if not links:
            raise A.CliError(A.EXIT_NOT_FOUND, "not_found", "no active link for that identity")
        return A.Result({"rejected": links}, human=lambda d: f"rejected {len(d['rejected'])} link(s)")
    raise A.CliError(A.EXIT_USAGE, "usage", "identity link|unlink|suggest")


def cmd_scope(ctx: Any) -> Any:
    from .. import curation as C
    A = _app()
    ctx.require_init()
    a = ctx.args
    cache = ctx.cache()
    if a.scope_command == "list":
        rows = C.list_scopes(cache, ctx.ident)
        return A.Result(rows, human=lambda d: "\n".join(
            f"{r['source'] or '?':<9} {r['scope']}  {r['records']:>8} records  {r['origin']}" + (f"  -> {r['mapped_to']}" if r.get("mapped_to") else "") for r in d) or "no scopes")
    if a.scope_command == "map":
        try:
            with cache.write():
                res = C.map_scope(cache, ctx.ident, a.from_scope, a.to_scope)
        except ValueError as e:
            raise A.CliError(A.EXIT_USAGE, "bad_scope", str(e)) from None
        save_identity(ctx.data_dir, ctx.ident)
        return A.Result(res, human=lambda d: f"mapped {d['from']} -> {d['to']}: {d['aliases_created']} aliases created")
    raise A.CliError(A.EXIT_USAGE, "usage", "scope list|map")


def cmd_conflicts(ctx: Any) -> Any:
    from .. import curation as C
    A = _app()
    ctx.require_init()
    a = ctx.args
    cache = ctx.cache()
    if a.conflicts_command == "list":
        rows = cache.conflicts(unresolved_only=not a.all)
        return A.Result(rows, human=lambda d: "\n".join(
            f"#{r['id']} {r['kind']:<14} {r['entity_urn'] or ''} {json.dumps(r['detail'])[:100]}" + ("  resolved" if r["resolved_at"] else "") for r in d) or "no conflicts")
    if a.conflicts_command == "resolve":
        try:
            with cache.write():
                res = C.resolve_conflict(cache, a.id, keep=a.keep, accept=a.accept)
        except KeyError:
            raise A.CliError(A.EXIT_NOT_FOUND, "not_found", f"no conflict #{a.id}") from None
        except ValueError as e:
            raise A.CliError(A.EXIT_USAGE, "bad_resolution", str(e)) from None
        return A.Result(res, human=lambda d: f"resolved #{d['id']}: {json.dumps(d['resolution'])}")
    raise A.CliError(A.EXIT_USAGE, "usage", "conflicts list|resolve")


def cmd_purge(ctx: Any) -> Any:
    from .. import curation as C
    A = _app()
    ctx.require_init()
    a = ctx.args
    if not a.confirm:
        raise A.CliError(A.EXIT_USAGE, "confirm_required", "purge is irreversible locally; add --confirm")
    if bool(a.entity) == bool(a.source):
        raise A.CliError(A.EXIT_USAGE, "usage", "give exactly one of --entity <urn> or --source <source>")
    cache = ctx.cache()
    try:
        with cache.write():
            res = C.purge_entity(cache, a.entity) if a.entity else C.purge_source(cache, a.source, a.scope)
    except KeyError:
        raise A.CliError(A.EXIT_NOT_FOUND, "not_found", f"unknown entity {a.entity}") from None
    warning = "already exported archives still contain the purged records; re-export produces a new revision"
    return A.Result(res | {"warning": warning}, warnings=[{"code": "archives_unchanged", "message": warning}],
                    human=lambda d: f"removed {d['removed']} record(s). {warning}")


def cmd_media(ctx: Any) -> Any:
    from ..cache import queries as Q
    A = _app()
    ctx.require_init()
    a = ctx.args
    cache = ctx.cache(readonly=True)
    tz = ctx.tz()
    from ..media import LOCAL_STATES, local_state

    def state(att: dict[str, Any]) -> str:
        return local_state(ctx.data_dir, ctx.cfg, att)

    if a.media_command == "status":
        summary = Q.media_summary(cache, state, source=a.source, chat_urn=a.chat)
        by_chat = Q.media_by_chat(cache, source=a.source, limit=a.top) if not a.chat else []
        blob_files = sum(1 for p in ctx.data_dir.blobs.rglob("*") if p.is_file()) if ctx.data_dir.blobs.exists() else 0
        data = {"summary": summary, "restored_blob_files": blob_files, "chats_with_most_unavailable": by_chat,
                "reasons": Q.AVAILABILITY_REASONS, "local_states": LOCAL_STATES}

        def human(d: dict[str, Any]) -> str:
            lines = ["source     availability (as observed)  local state   count      declared MB  hashed"]
            for r in d["summary"]:
                lines.append(f"{r['source'] or '-':10} {r['availability']:26} {r['local_state']:12} {r['count']:6}  "
                             f"{r['declared_bytes'] / 1e6:12.1f}  {r['hashed']:6}")
            lines.append(f"blob files in this data dir: {d['restored_blob_files']}")
            if d["chats_with_most_unavailable"]:
                lines.append("\nchats with most unavailable media:")
                for c in d["chats_with_most_unavailable"]:
                    lines.append(f"  {c['unavailable']:5}  (nd {c['not_downloaded']}, missing {c['missing']}, unknown {c['unknown']})  "
                                 f"[{c['source']}] {c['chat_label'] or '?'}\n         {c['chat_urn']}")
            lines.append("\navailability (what the source app had when last synced):")
            lines += [f"  {k:15} {v}" for k, v in d["reasons"].items()]
            lines.append("local state (where the bytes are on this machine now):")
            lines += [f"  {k:15} {v}" for k, v in d["local_states"].items()]
            return "\n".join(lines)

        return A.Result(data, human=human)
    limit = ctx.limit()
    try:
        items, cur = Q.media_list(cache, state, availability=a.availability, local=a.local_state, source=a.source, chat_urn=a.chat,
                                  since_ms=ctx.parse_date(a.since), until_ms=ctx.parse_date(a.until),
                                  limit=limit, cursor=a.cursor)
    except ValueError as e:
        raise A.CliError(A.EXIT_USAGE, "bad_cursor", str(e)) from e

    def human_list(rows: list[dict[str, Any]]) -> str:
        out = [f"{A.hms(r['sent_at_utc_ms'], tz)}  {r['availability']:14} {r['local_state']:11} {r['kind'] or '?':9} {(r['declared_size'] or 0) / 1e6:7.1f}MB  "
               f"{r['chat_label'] or '?'}\n    {r['urn']}" for r in rows]
        if cur:
            out.append(f"-- continue: --cursor {cur}")
        return "\n".join(out) or "no attachments match"

    return A.Result(items, page=A._page(items, limit, cur), human=human_list)


# ---- registration -----------------------------------------------------------------------------------

def register_extra(sub: Any) -> None:
    ar = sub.add_parser("archive", help="Encrypted archive export / verify / import.")
    ars = ar.add_subparsers(dest="archive_command", metavar="action")

    s = ars.add_parser("export", help="Write encrypted monthly archives + catalogue.")
    s.add_argument("--output", "-o", help="Destination directory (default: <data-dir>/exports).")
    s.add_argument("--since")
    s.add_argument("--until")
    s.add_argument("--media", choices=["text", "available-media"], default="text")
    s.add_argument("--context", choices=["full", "minimal"], default="full",
                   help="full: each month carries the chats/identities it needs; minimal: stubs only (smaller, needs the catalogue).")
    s.add_argument("--force", action="store_true", help="Write a new revision even if content is unchanged.")
    s.add_argument("--catalogue-only", action="store_true")
    s.add_argument("--no-catalogue", action="store_true")
    s.set_defaults(fn=cmd_archive_export)

    s = ars.add_parser("verify", help="Decrypt, authenticate and validate archives without importing.")
    s.add_argument("paths", nargs="+", help="Archive files or directories.")
    s.add_argument("--no-schema", action="store_true", help="Skip per-record JSON schema validation.")
    s.set_defaults(fn=cmd_archive_verify)

    s = ars.add_parser("import", help="Import archives into the data directory (creates it if needed).")
    s.add_argument("paths", nargs="+")
    s.add_argument("--no-media", action="store_true", help="Do not restore blobs.")
    s.add_argument("--stop-on-conflict", action="store_true")
    s.set_defaults(fn=cmd_archive_import)

    s = ars.add_parser("password", help="Manage the archive password (keychain).")
    s.add_argument("action", choices=["suggest", "set", "status", "clear"])
    s.set_defaults(fn=cmd_archive_password)

    ars.add_parser("list", help="Show export and import ledgers.").set_defaults(fn=cmd_archive_list)

    idp = sub.add_parser("identity", help="Curate people and identity links.")
    ids = idp.add_subparsers(dest="identity_command", metavar="action")
    s = ids.add_parser("link", help="Link identities to a person (creates the person unless --person).")
    s.add_argument("identities", nargs="+", metavar="identity-urn")
    s.add_argument("--person", help="Existing person URN.")
    s.add_argument("--label", help="Label for a new person.")
    s.set_defaults(fn=cmd_identity)
    s = ids.add_parser("unlink", help="Reject a link (kept as evidence, state=rejected).")
    s.add_argument("identity", metavar="identity-urn")
    s.add_argument("--person")
    s.set_defaults(fn=cmd_identity)
    s = ids.add_parser("suggest", help="Suggest cross-source links by normalised phone/e-mail.")
    s.add_argument("--limit", type=int)
    s.set_defaults(fn=cmd_identity)

    scp = sub.add_parser("scope", help="Account scopes and explicit scope mapping.")
    scs = scp.add_subparsers(dest="scope_command", metavar="action")
    scs.add_parser("list", help="List account scopes.").set_defaults(fn=cmd_scope)
    s = scs.add_parser("map", help="Declare two scopes the same account; emits aliases.")
    s.add_argument("from_scope", metavar="from")
    s.add_argument("to_scope", metavar="to")
    s.set_defaults(fn=cmd_scope)

    cfp = sub.add_parser("conflicts", help="Inspect and resolve import conflicts.")
    cfs = cfp.add_subparsers(dest="conflicts_command", metavar="action")
    s = cfs.add_parser("list")
    s.add_argument("--all", action="store_true", help="Include resolved conflicts.")
    s.set_defaults(fn=cmd_conflicts)
    s = cfs.add_parser("resolve")
    s.add_argument("id", type=int)
    s.add_argument("--keep", metavar="revision_digest", help="Content conflicts: revision to make current.")
    s.add_argument("--accept", action="store_true", help="Archive branch conflicts: allow the branch to import.")
    s.set_defaults(fn=cmd_conflicts)

    mp = sub.add_parser("media", help="Attachment availability: what is on disk, what is missing and why.")
    ms = mp.add_subparsers(dest="media_command", metavar="action")
    s = ms.add_parser("status", help="Counts per source and availability; chats with the most unavailable media.")
    s.add_argument("--source", choices=["whatsapp", "messages"])
    s.add_argument("--chat", metavar="chat-urn")
    s.add_argument("--top", type=int, default=15, help="How many chats to list.")
    s.set_defaults(fn=cmd_media)
    s = ms.add_parser("list", help="List attachments (newest first).")
    s.add_argument("--availability", choices=["available", "not_downloaded", "missing", "not_exported", "unknown"],
                   help="What the source app had when last synced.")
    s.add_argument("--local-state", choices=["restored", "source_file", "absent"], help="Where the bytes are on this machine now.")
    s.add_argument("--source", choices=["whatsapp", "messages"])
    s.add_argument("--chat", metavar="chat-urn")
    s.add_argument("--since")
    s.add_argument("--until")
    s.add_argument("--limit", type=int)
    s.add_argument("--cursor")
    s.set_defaults(fn=cmd_media)

    s = sub.add_parser("purge", help="Irreversibly delete records from the local cache.")
    s.add_argument("--entity", metavar="urn")
    s.add_argument("--source", choices=["whatsapp", "messages"])
    s.add_argument("--scope")
    s.add_argument("--confirm", action="store_true")
    s.set_defaults(fn=cmd_purge)
    _ = argparse
