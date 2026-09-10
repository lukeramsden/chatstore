"""Data directory resolution. Never read from an untrusted repository configuration."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

ENV_DATA_DIR = "CHATSTORE_DATA_DIR"
ENV_PASSWORD_FILE = "CHATSTORE_PASSWORD_FILE"


def default_data_dir() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "chatstore"
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(base) / "chatstore"
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(base) / "chatstore"


def resolve_data_dir(override: str | None) -> Path:
    if override:
        return Path(override).expanduser()
    env = os.environ.get(ENV_DATA_DIR)
    if env:
        return Path(env).expanduser()
    return default_data_dir()


@dataclass(frozen=True)
class DataDir:
    root: Path

    @property
    def config(self) -> Path:
        return self.root / "config.json"

    @property
    def identity(self) -> Path:
        return self.root / "identity.json"

    @property
    def cache(self) -> Path:
        return self.root / "cache.sqlite3"

    @property
    def blobs(self) -> Path:
        return self.root / "blobs" / "sha256"

    @property
    def snapshots(self) -> Path:
        return self.root / "snapshots"

    @property
    def exports(self) -> Path:
        return self.root / "exports"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    def exists(self) -> bool:
        return self.identity.exists() and self.config.exists()

    def create(self) -> None:
        for p in (self.root, self.blobs, self.snapshots, self.exports, self.staging):
            p.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(p, 0o700)
            except OSError:
                pass


def blob_path(data_dir: DataDir, sha256: str) -> Path:
    return data_dir.blobs / sha256[:2] / sha256


def default_source_paths() -> dict[str, Path]:
    home = Path.home()
    return {
        "whatsapp": home / "Library" / "Group Containers" / "group.net.whatsapp.WhatsApp.shared",
        "messages": home / "Library" / "Messages",
    }
