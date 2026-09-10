"""config.json and identity.json handling."""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .paths import DataDir, default_source_paths

CONFIG_VERSION = 1


class NotInitialised(Exception):
    pass


@dataclass
class Config:
    timezone: str = "UTC"
    source_paths: dict[str, str] = field(default_factory=dict)
    search_limit: int = 50
    max_limit: int = 500
    max_chars: int = 20000
    helper_path: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {"version": CONFIG_VERSION, **self.__dict__}

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> Config:
        d = dict(d)
        d.pop("version", None)
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def source_root(self, source: str) -> Path:
        p = self.source_paths.get(source)
        return Path(p).expanduser() if p else default_source_paths()[source]


@dataclass
class Identity:
    scopes: dict[str, dict[str, Any]] = field(default_factory=dict)  # source -> {scope, created_at, label}
    scope_mappings: list[dict[str, Any]] = field(default_factory=list)

    def scope_for(self, source: str) -> str:
        if source not in self.scopes:
            raise NotInitialised(f"no account scope for source {source!r}; run `chatstore init`")
        return str(self.scopes[source]["scope"])

    def ensure_scope(self, source: str) -> tuple[str, bool]:
        if source in self.scopes:
            return str(self.scopes[source]["scope"]), False
        scope = str(uuid.uuid4())
        self.scopes[source] = {"scope": scope, "created_at": int(time.time() * 1000), "label": None}
        return scope, True

    def to_json(self) -> dict[str, Any]:
        return {"version": CONFIG_VERSION, "scopes": self.scopes, "scope_mappings": self.scope_mappings}

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> Identity:
        return cls(scopes=dict(d.get("scopes", {})), scope_mappings=list(d.get("scope_mappings", [])))


def _write_private(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def load_config(dd: DataDir) -> Config:
    if not dd.config.exists():
        raise NotInitialised(f"{dd.root} is not initialised; run `chatstore init`")
    return Config.from_json(json.loads(dd.config.read_text()))


def save_config(dd: DataDir, cfg: Config) -> None:
    _write_private(dd.config, cfg.to_json())


def load_identity(dd: DataDir) -> Identity:
    if not dd.identity.exists():
        raise NotInitialised(f"{dd.root} is not initialised; run `chatstore init`")
    return Identity.from_json(json.loads(dd.identity.read_text()))


def save_identity(dd: DataDir, ident: Identity) -> None:
    _write_private(dd.identity, ident.to_json())
