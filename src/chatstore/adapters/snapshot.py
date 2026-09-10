"""Consistent read-only snapshots of source SQLite databases via the online backup API.

Never opens the source with write intent. Never checkpoints the source WAL.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class SnapshotError(Exception):
    pass


class PermissionProblem(SnapshotError):
    pass


@dataclass
class SnapshotFile:
    name: str
    source_path: str
    snapshot_path: Path
    size: int
    sha256: str
    source_mtime_ns: int
    wal_present: bool
    started_at: int
    finished_at: int

    def manifest(self) -> dict[str, Any]:
        return {"name": self.name, "size": self.size, "sha256": self.sha256, "source_mtime_ns": self.source_mtime_ns,
                "wal_present": self.wal_present, "started_at": self.started_at, "finished_at": self.finished_at}


@dataclass
class Snapshot:
    directory: Path
    files: dict[str, SnapshotFile] = field(default_factory=dict)

    def manifest(self) -> dict[str, Any]:
        return {"files": [f.manifest() for f in self.files.values()]}

    def cleanup(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def check_readable(path: Path) -> tuple[bool, str | None]:
    """Return (readable, hint). Distinguishes missing, permission (Full Disk Access) and other."""
    if not path.exists():
        # On macOS a TCC-protected directory can appear empty/missing; check parent access.
        try:
            list(path.parent.iterdir())
        except PermissionError:
            return False, "permission denied listing parent directory; grant Full Disk Access to the terminal/process"
        except FileNotFoundError:
            return False, f"directory not found: {path.parent}"
        except OSError as e:
            return False, f"cannot list {path.parent}: {e.strerror}"
        return False, "file not found"
    try:
        with open(path, "rb") as f:
            f.read(16)
    except PermissionError:
        return False, "permission denied (macOS: grant Full Disk Access to the launching terminal or process)"
    except OSError as e:
        return False, f"cannot read: {e.strerror}"
    return True, None


def snapshot_databases(sources: dict[str, Path], staging_root: Path, *, retries: int = 3,
                       busy_timeout_s: float = 10.0, pages_per_step: int = 4096) -> Snapshot:
    """Copy each source database into a fresh 0700 staging directory using the backup API."""
    staging_root.mkdir(parents=True, exist_ok=True)
    os.chmod(staging_root, 0o700)
    snap_dir = staging_root / f"snap-{int(time.time() * 1000)}-{os.getpid()}"
    snap_dir.mkdir(mode=0o700)
    snap = Snapshot(snap_dir)
    try:
        for name, src in sources.items():
            ok, hint = check_readable(src)
            if not ok:
                raise PermissionProblem(f"{name}: {src.name}: {hint}")
            snap.files[name] = _backup_one(name, src, snap_dir / f"{name}.sqlite", retries, busy_timeout_s, pages_per_step)
    except Exception:
        snap.cleanup()
        raise
    return snap


def _backup_one(name: str, src: Path, dst: Path, retries: int, busy_timeout_s: float, pages: int) -> SnapshotFile:
    started = int(time.time() * 1000)
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            source = sqlite3.connect(f"file:{src}?mode=ro", uri=True, timeout=busy_timeout_s)
            try:
                source.execute("PRAGMA query_only=1")
                fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                os.close(fd)
                target = sqlite3.connect(dst)
                try:
                    source.backup(target, pages=pages, sleep=0.05)
                finally:
                    target.close()
            finally:
                source.close()
            st = src.stat()
            return SnapshotFile(
                name=name, source_path=str(src), snapshot_path=dst, size=dst.stat().st_size, sha256=_sha256_file(dst),
                source_mtime_ns=st.st_mtime_ns, wal_present=(src.with_name(src.name + "-wal")).exists(),
                started_at=started, finished_at=int(time.time() * 1000),
            )
        except sqlite3.OperationalError as e:
            last_err = e
            if "locked" in str(e) or "busy" in str(e):
                time.sleep(0.5 * (attempt + 1))
                continue
            if "unable to open" in str(e) or "authorization" in str(e):
                raise PermissionProblem(f"{name}: {e}") from e
            raise SnapshotError(f"{name}: {e}") from e
    raise SnapshotError(f"{name}: source busy after {retries} attempts: {last_err}")
