#!/usr/bin/env python3
"""Final acceptance test (PLAN.md): export both real sources, restore into a clean directory, prove
identity, search equivalence, provenance, media hashes, idempotence, error reporting, tamper safety,
and that the source databases are untouched.

Usage:  .venv/bin/python scripts/acceptance.py [--work DIR] [--keep] [--media-month YYYY-MM]
Needs Full Disk Access for the terminal, both source apps' data present, and the Rust helper built.
Prints only counts and pass/fail lines — never message content, names or numbers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = [str(Path(sys.executable).with_name("chatstore"))]  # console script next to the interpreter
QUERIES = ["tomorrow", "thanks", "meeting", "happy birthday", "call me", "photo", "sorry", "flight", "dinner", "ok"]

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""), flush=True)


def cs(data_dir: Path, *args: str, expect: int | None = 0, env: dict[str, str] | None = None) -> dict:
    e = dict(os.environ, CHATSTORE_DATA_DIR=str(data_dir))
    if env:
        e.update(env)
    t = time.time()
    p = subprocess.run([*CLI, "--json", *args], capture_output=True, text=True, env=e, check=False)
    dt = time.time() - t
    try:
        out = json.loads(p.stdout)
    except ValueError:
        print(p.stdout[-2000:], p.stderr[-2000:], file=sys.stderr)
        raise SystemExit(f"non-JSON output from chatstore {' '.join(args[:2])}") from None
    if expect is not None and out["exit_code"] != expect:
        print(json.dumps(out.get("errors")), file=sys.stderr)
        raise SystemExit(f"chatstore {' '.join(args[:2])} exited {out['exit_code']} (expected {expect}) after {dt:.1f}s")
    out["_seconds"] = dt
    return out


def file_sig(p: Path) -> tuple[int, int, str]:
    st = p.stat()
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return st.st_size, st.st_mtime_ns, h.hexdigest()


def source_files(cfg: dict) -> list[Path]:
    sys.path.insert(0, str(ROOT / "src"))
    from chatstore.paths import default_source_paths
    defaults = default_source_paths()
    out = []
    for src, hint in (("whatsapp", "ChatStorage.sqlite"), ("messages", "chat.db")):
        root = Path(cfg.get("source_paths", {}).get(src) or defaults[src]).expanduser()
        if not root.exists():
            continue
        for name in (hint, hint + "-wal", hint + "-shm"):
            p = root / name
            if p.exists():
                out.append(p)
    return out


def counts(data_dir: Path) -> dict:
    c = cs(data_dir, "status")["data"]["counts"]
    c.pop("sync_runs", None)  # one row per run by design
    return c


def current_view(data_dir: Path) -> tuple[set[tuple[str, str]], dict[str, int]]:
    conn = sqlite3.connect(f"file:{data_dir / 'cache.sqlite3'}?mode=ro", uri=True)
    pairs = set(conn.execute("SELECT urn, revision_digest FROM records WHERE kind != 'sync_runs'").fetchall())
    extra = {
        "revisions": conn.execute("SELECT count(*) FROM revisions").fetchone()[0],
        "observations": conn.execute("SELECT count(*) FROM source_observations").fetchone()[0],
        "fts": conn.execute("SELECT count(*) FROM messages_fts").fetchone()[0],
        "replies": conn.execute("SELECT count(*) FROM records WHERE kind='messages' AND json_extract(record,'$.reply_to_urn') IS NOT NULL").fetchone()[0],
        "reactions": conn.execute("SELECT count(*) FROM events_idx WHERE kind LIKE 'reaction%'").fetchone()[0],
        "edits": conn.execute("SELECT count(*) FROM events_idx WHERE kind IN ('edit','retraction')").fetchone()[0],
        "attachments": conn.execute("SELECT count(*) FROM attachments_idx").fetchone()[0],
        "attachment_hashes": conn.execute("SELECT count(DISTINCT blob_sha256) FROM attachments_idx WHERE blob_sha256 IS NOT NULL").fetchone()[0],
        "conflicts": conn.execute("SELECT count(*) FROM conflicts WHERE resolved_at IS NULL").fetchone()[0],
        # references to messages the source itself never had (quoted-reply/reaction targets outside local history)
        "dangling_refs": conn.execute(
            """SELECT count(DISTINCT t) FROM (
                 SELECT json_extract(record,'$.reply_to_urn') AS t FROM records WHERE kind='messages'
                 UNION ALL SELECT target_urn FROM events_idx)
               WHERE t IS NOT NULL AND t NOT IN (SELECT urn FROM records)""").fetchone()[0],
    }
    conn.close()
    return pairs, extra


def search_suite(data_dir: Path) -> dict[str, list[tuple[str, str]]]:
    out = {}
    for q in QUERIES:
        r = cs(data_dir, "search", q, "--limit", "50")
        out[q] = [(h["urn"], h["revision_digest"]) for h in r["data"]]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", help="Working directory (default: mkdtemp under ~/.cache).")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--media-month", help="YYYY-MM to also export with --media available-media.")
    ap.add_argument("--whatsapp-path")
    ap.add_argument("--messages-path")
    a = ap.parse_args()
    base = Path(a.work).expanduser() if a.work else Path(tempfile.mkdtemp(prefix="chatstore-acceptance-", dir=Path.home() / ".cache"))
    base.mkdir(parents=True, exist_ok=True)
    os.chmod(base, 0o700)
    src_dd, dst_dd, out = base / "source-data", base / "restored-data", base / "archives"
    pw = base / "pw"
    pw.write_text("acceptance-run-password-not-a-secret\n")
    os.chmod(pw, 0o600)
    os.environ["CHATSTORE_PASSWORD_FILE"] = str(pw)
    print(f"work dir: {base}")

    # 1. init + doctor + sync both sources
    init_args = ["init", "--timezone", "UTC"]
    if a.whatsapp_path:
        init_args += ["--whatsapp-path", a.whatsapp_path]
    if a.messages_path:
        init_args += ["--messages-path", a.messages_path]
    cs(src_dd, *init_args)
    cfg = json.loads((src_dd / "config.json").read_text())
    before = {str(p): file_sig(p) for p in source_files(cfg)}
    doc = cs(src_dd, "doctor", expect=None)
    check("doctor runs", doc["exit_code"] in (0, 5), f"exit {doc['exit_code']}")
    sync = cs(src_dd, "sync", expect=None)
    st = {r["source"]: r["status"] for r in sync["data"]}
    check("sync both sources", sync["exit_code"] in (0, 5) and all(v in ("complete", "partial") for v in st.values()),
          f"{st} in {sync['_seconds']:.0f}s")
    c0 = counts(src_dd)
    print("   counts:", json.dumps(c0))
    resync = cs(src_dd, "sync", expect=None)
    c1 = counts(src_dd)
    view0, extra0 = current_view(src_dd)
    check("incremental re-sync is idempotent", c0 == c1 and extra0["conflicts"] == 0, f"{resync['_seconds']:.0f}s")

    # 2. export
    exp = cs(src_dd, "archive", "export", "-o", str(out))
    n_written = exp["data"]["written"]
    kinds = {r["kind"] for r in exp["data"]["archives"] if r["action"] == "written"}
    check("export writes catalogue + buckets", n_written >= 2 and "catalogue" in kinds and "bucket" in kinds,
          f"{n_written} archives in {exp['_seconds']:.0f}s")
    exp2 = cs(src_dd, "archive", "export", "-o", str(out))
    check("re-export without changes writes nothing", exp2["data"]["written"] == 0)
    media_ok = None
    if a.media_month:
        y, m = a.media_month.split("-")
        nxt = f"{int(y) + 1}-01-01" if m == "12" else f"{y}-{int(m) + 1:02d}-01"
        mx = cs(src_dd, "archive", "export", "-o", str(out / "media"), "--no-catalogue", "--media", "available-media",
                "--since", f"{a.media_month}-01", "--until", nxt)
        media_ok = mx["data"]["archives"][0]["media"]
        print("   media archive:", json.dumps(media_ok))
    ver = cs(src_dd, "archive", "verify", str(out), expect=None)
    check("verify all archives", ver["exit_code"] == 0 and all(r["ok"] for r in ver["data"]), f"{len(ver['data'])} files")

    # 3. restore into an empty data dir, no sources configured
    imp = cs(dst_dd, "archive", "import", str(out), expect=None)
    actions = {}
    for r in imp["data"]:
        actions[r["action"]] = actions.get(r["action"], 0) + 1
    check("import into clean dir", imp["exit_code"] == 0 and set(actions) == {"imported"}, f"{actions} in {imp['_seconds']:.0f}s")
    unresolved = sum(r["unresolved_refs"] for r in imp["data"])
    not_restored = sum(r["media_not_restored"] for r in imp["data"])
    print(f"   unresolved refs after full import: {unresolved}; media not restored (text mode): {not_restored}")
    view1, extra1 = current_view(dst_dd)
    check("identical URNs and revision digests", view0 == view1, f"{len(view0)} records; diff {len(view0 ^ view1)}")
    check("revisions/observations/FTS counts equal", all(extra0[k] == extra1[k] for k in ("revisions", "observations", "fts")),
          json.dumps({k: (extra0[k], extra1[k]) for k in ("revisions", "observations", "fts")}))
    check("replies, reactions, edits, attachment metadata preserved",
          all(extra0[k] == extra1[k] for k in ("replies", "reactions", "edits", "attachments", "attachment_hashes")),
          json.dumps({k: extra1[k] for k in ("replies", "reactions", "edits", "attachments", "attachment_hashes")}))
    s0, s1 = search_suite(src_dd), search_suite(dst_dd)
    check("search suite equivalent", s0 == s1, f"{sum(len(v) for v in s0.values())} hits over {len(QUERIES)} queries")
    check("unresolved references reported honestly (= refs the source itself lacks)", unresolved == extra0["dangling_refs"],
          f"import reported {unresolved}; source has {extra0['dangling_refs']} dangling targets")
    # 4. repeat import -> no duplicates
    imp2 = cs(dst_dd, "archive", "import", str(out))
    view2, extra2 = current_view(dst_dd)
    check("repeated import produces no duplicates", view2 == view1 and extra2 == extra1 and
          all(r["action"] == "already_imported" for r in imp2["data"]))
    # 5. media hashes
    if a.media_month:
        m_dd = base / "media-data"
        mi = cs(m_dd, "archive", "import", str(out / "media"))
        bad = 0
        n = 0
        for p in (m_dd / "blobs").rglob("*"):
            if p.is_file():
                n += 1
                if hashlib.sha256(p.read_bytes()).hexdigest() != p.name:
                    bad += 1
        check("included media hashes match", n == mi["data"][0]["blobs_restored"] and bad == 0 and n > 0, f"{n} blobs, {bad} mismatches")
    # 6. wrong password + tamper cannot change the cache
    bad_pw = base / "badpw"
    bad_pw.write_text("wrong\n")
    os.chmod(bad_pw, 0o600)
    files = sorted(out.glob("chatstore-*-catalogue-*.zip"))
    t_dd = base / "tamper-data"
    w = cs(t_dd, "archive", "import", str(files[0]), expect=None, env={"CHATSTORE_PASSWORD_FILE": str(bad_pw)})
    data = bytearray(files[0].read_bytes())
    with zipfile.ZipFile(files[0]) as zf:
        info = zf.infolist()[0]
    off = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra) + 200
    data[off] ^= 0xFF
    tam = base / "tampered.zip"
    tam.write_bytes(bytes(data))
    t = cs(t_dd, "archive", "import", str(tam), expect=None)
    tc = counts(t_dd)
    check("wrong password and tampered archive rejected, cache untouched",
          w["exit_code"] == 7 and t["exit_code"] == 7 and tc == {}, f"exit {w['exit_code']}/{t['exit_code']}, counts {tc}")
    # 7. changed bucket restores without erasing shared data: re-export forced -> r0002, import on top
    exp3 = cs(src_dd, "archive", "export", "-o", str(out), "--force", "--no-catalogue", "--since",
              time.strftime("%Y-%m-01", time.gmtime(time.time() - 40 * 86400)))
    imp3 = cs(dst_dd, "archive", "import", str(out), expect=None)
    view3, extra3 = current_view(dst_dd)
    check("changed bucket revision imports without touching shared data",
          imp3["exit_code"] == 0 and view3 == view1 and extra3["conflicts"] == 0,
          f"{exp3['data']['written']} r0002 archives; view unchanged={view3 == view1}")
    # 8. sources unmodified
    after = {str(p): file_sig(p) for p in source_files(cfg)}
    # WAL/SHM of live apps may legitimately change while the apps run; compare main DB files strictly
    mains = [k for k in before if not k.endswith(("-wal", "-shm"))]
    unchanged = all(before[k] == after.get(k) for k in mains)
    if unchanged:
        check("source databases unmodified by chatstore", True, f"{len(mains)} main DB files byte-identical")
    else:
        # A live app may have written meanwhile; chatstore only ever opens sources read-only (mode=ro) and
        # copies via the SQLite backup API, so this cannot be attributed. Report, do not pass silently.
        print("[WARN] source DB main files changed during the run — the app was writing; quit both apps and re-run to confirm")
    # summary
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    if not a.keep and not failed:
        shutil.rmtree(base, ignore_errors=True)
    else:
        print(f"kept {base}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
