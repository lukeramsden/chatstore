"""Archive / identity / scope / conflicts / purge commands (Phases 3 and 4)."""

from __future__ import annotations

import argparse
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
    until = ctx.parse_date(a.until, end=True)
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
            plans.append(E.collect_bucket(cache, b))
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
    save_identity(ctx.data_dir, ctx.ident)
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


# ---- registration -----------------------------------------------------------------------------------

def register_extra(sub: Any) -> None:
    ar = sub.add_parser("archive", help="Encrypted archive export / verify / import.")
    ars = ar.add_subparsers(dest="archive_command", metavar="action")

    s = ars.add_parser("export", help="Write encrypted monthly archives + catalogue.")
    s.add_argument("--output", "-o", help="Destination directory (default: <data-dir>/exports).")
    s.add_argument("--since")
    s.add_argument("--until")
    s.add_argument("--media", choices=["text", "available-media"], default="text")
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
    _ = argparse
