"""Where attachment bytes are *right now* on this machine.

An attachment record's `availability` is a source observation ("the app had the file when we last
synced"); it is archived verbatim and never changes on restore. `local_state` is derived here from the
filesystem at query time and is never stored.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import Config
from .paths import DataDir, blob_path

LOCAL_STATES = {
    "restored": "the bytes are in this data dir's blob store (from an archive or export)",
    "source_file": "the source app's file is present on this machine at the recorded path",
    "absent": "no copy of the bytes on this machine (never downloaded, deleted, or restored text-only)",
}


def restored_blob(dd: DataDir, att: dict[str, Any]) -> Path | None:
    sha = att.get("blob_sha256")
    if sha:
        p = blob_path(dd, sha)
        if p.is_file():
            return p
    return None


def source_file(cfg: Config, att: dict[str, Any]) -> Path | None:
    hint = att.get("source_path_hint")
    if not hint or hint.startswith("/") or ".." in hint.split("/"):
        return None
    root = cfg.source_root(att["source"])
    if att["source"] == "whatsapp":
        root = root / "Message"
    p = root / hint
    return p if p.is_file() and not p.is_symlink() else None


def locate_blob(dd: DataDir, cfg: Config, att: dict[str, Any]) -> Path | None:
    """Best local copy of an attachment's bytes: restored blob first, then the source app's file."""
    return restored_blob(dd, att) or source_file(cfg, att)


def local_state(dd: DataDir, cfg: Config, att: dict[str, Any]) -> str:
    if restored_blob(dd, att):
        return "restored"
    if source_file(cfg, att):
        return "source_file"
    return "absent"
