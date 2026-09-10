"""Helpers to construct canonical records (specs/canonical-model.md)."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from ..identity.urn import revision_digest
from .schema_version import CANONICAL_SCHEMA

EPOCH_2001 = "2001-01-01T00:00:00Z"
EPOCH_1970 = "1970-01-01T00:00:00Z"
_EPOCH_OFFSET_MS = {EPOCH_2001: 978307200000, EPOCH_1970: 0}
_UNIT_DIV = {"s": Decimal(1) / 1000, "ms": Decimal(1), "us": Decimal(1000), "ns": Decimal(1_000_000)}


def now_ms() -> int:
    return int(time.time() * 1000)


def timestamp(raw: float | str | None, unit: str, epoch: str) -> dict[str, Any] | None:
    """Build a timestamp object. Returns None when raw is None."""
    if raw is None:
        return None
    if unit not in _UNIT_DIV or epoch not in _EPOCH_OFFSET_MS:
        raise ValueError(f"unsupported unit/epoch {unit}/{epoch}")
    raw_str = str(raw) if not isinstance(raw, float) else repr(raw)
    d = Decimal(raw_str)
    utc_ms = int(d / _UNIT_DIV[unit]) + _EPOCH_OFFSET_MS[epoch] if unit != "s" else int(d * 1000) + _EPOCH_OFFSET_MS[epoch]
    return {"raw": raw_str, "unit": unit, "epoch": epoch, "utc_ms": utc_ms}


def ts_utc_ms(ts: dict[str, Any] | None) -> int | None:
    return None if ts is None else ts.get("utc_ms")


def iso_utc(utc_ms: int | None) -> str | None:
    if utc_ms is None:
        return None
    s, ms = divmod(utc_ms, 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(s)) + f".{ms:03d}Z"


def record(entity: str, urn: str, source: str | None, account_scope: str | None, **fields: Any) -> dict[str, Any]:
    """Assemble a record and stamp its revision digest. observed_at/sync_run are added by the cache."""
    rec: dict[str, Any] = {
        "schema": CANONICAL_SCHEMA,
        "entity": entity,
        "urn": urn,
        "source": source,
        "account_scope": account_scope,
        **fields,
    }
    rec["revision_digest"] = revision_digest(rec)
    return rec


def restamp(rec: dict[str, Any]) -> dict[str, Any]:
    rec["revision_digest"] = revision_digest(rec)
    return rec


def allowlisted(obj: dict[str, Any], allowed: list[str]) -> dict[str, Any]:
    return {k: obj[k] for k in allowed if k in obj and obj[k] is not None}
