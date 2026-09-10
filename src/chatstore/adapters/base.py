"""Adapter contract. Adapters open sources read-only and emit canonical records with provenance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from ..config import Config
from .snapshot import Snapshot


@dataclass
class Observation:
    native_table: str
    native_row_ids: list[Any]
    native_key: str | None
    fingerprint: str
    minted: bool = False


@dataclass
class Emit:
    record: dict[str, Any]
    observation: Observation | None = None


@dataclass
class Diagnosis:
    source: str
    root: str
    path_found: bool
    readable: bool
    schema_ok: bool
    schema_version: str | None = None
    permission_hint: str | None = None
    wal_present: bool = False
    problems: list[str] = field(default_factory=list)
    files: dict[str, bool] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return self.__dict__


@dataclass
class ExtractStats:
    counts: dict[str, int] = field(default_factory=dict)
    unsupported: dict[str, int] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    checkpoints: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def bump(self, key: str, n: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + n

    def unsupported_bump(self, key: str) -> None:
        self.unsupported[key] = self.unsupported.get(key, 0) + 1

    def error(self, code: str, detail: str | None = None) -> None:
        for e in self.errors:
            if e["code"] == code:
                e["count"] += 1
                return
        self.errors.append({"code": code, "count": 1, "sample": detail})


class IncompatibleSource(Exception):
    """Source schema does not match what the adapter supports. Fail closed."""


class SourceAdapter(Protocol):
    source: str
    version: str

    def discover(self, cfg: Config) -> dict[str, Path]: ...
    def diagnose(self, cfg: Config) -> Diagnosis: ...
    def snapshot(self, cfg: Config, staging: Path) -> Snapshot: ...
    def extract(self, snap: Snapshot, scope: str, cfg: Config, stats: ExtractStats,
                checkpoints: dict[str, Any] | None, *, full: bool) -> Iterator[Emit]: ...


def fingerprint(*parts: Any) -> str:
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":"), default=str).encode()).hexdigest()


def table_columns(conn: Any, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}


def check_schema(conn: Any, required: dict[str, set[str]]) -> list[str]:
    problems = []
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for t, cols in required.items():
        if t not in tables:
            problems.append(f"missing table {t}")
            continue
        missing = cols - table_columns(conn, t)
        if missing:
            problems.append(f"table {t} missing columns {sorted(missing)}")
    return problems
