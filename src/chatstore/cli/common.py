"""Shared CLI contract: exit codes, envelope, errors, results and the command context.

Both `app` (core commands) and `extra` (archive / curation / media commands) import from here, so
neither needs to import the other.
"""

from __future__ import annotations

import argparse
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..adapters.base import SourceAdapter
from ..cache import Cache
from ..canonical.records import iso_utc
from ..config import Config, Identity, load_config, load_identity
from ..paths import DataDir

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
    _cache: Cache | None = None

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

    def cache(self, readonly: bool = False) -> Cache:
        if self._cache is None:
            self.require_init()
            self._cache = Cache(self.data_dir.cache, readonly=readonly)
        return self._cache

    def tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.cfg.timezone)
        except (KeyError, ValueError, OSError):
            return ZoneInfo("UTC")

    def parse_date(self, s: str | None) -> int | None:
        """YYYY-MM-DD (local midnight in config.timezone) or RFC 3339 -> UTC ms. Both --since and --until
        name an instant; --until is exclusive, so `--until 2026-04-01` stops before April 1."""
        if not s:
            return None
        try:
            if len(s) == 10:
                d = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=self.tz())
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

def add_ts_iso(obj: Any) -> Any:
    """Add `iso` to canonical timestamp objects for convenience (recursively)."""
    if isinstance(obj, dict):
        if set(obj) >= {"raw", "unit", "epoch", "utc_ms"} and "iso" not in obj:
            obj = dict(obj)
            obj["iso"] = iso_utc(obj["utc_ms"])
            return obj
        return {k: add_ts_iso(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [add_ts_iso(x) for x in obj]
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


def adapter_for(source: str) -> SourceAdapter:
    if source == "whatsapp":
        from ..adapters.whatsapp import WhatsAppAdapter
        return WhatsAppAdapter()
    if source == "messages":
        from ..adapters.messages import MessagesAdapter
        return MessagesAdapter()
    raise CliError(EXIT_USAGE, "bad_source", f"unknown source {source!r}")


def page(items: list[Any], limit: int, cursor: str | None, truncated: bool = False) -> dict[str, Any]:
    return {"limit": limit, "returned": len(items), "next_cursor": cursor, "truncated": truncated or cursor is not None}
