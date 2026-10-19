"""Process-local, monitored reuse of a successful source capture."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Self

from .adapters.base import ExtractStats, SourceAdapter
from .adapters.messages.adapter import MessagesAdapter, find_helper
from .adapters.monitor import CaptureToken, MonitoringUnavailable, SourceMonitor
from .adapters.whatsapp.adapter import WhatsAppAdapter
from .cache import Cache
from .cache.changefeed import append, id_ranges
from .cache.extraction import code_signature
from .cache.freshness import stamp
from .cache.projection_epoch import read_source
from .config import Config


class SyncSession:
    """Retain one source monitor across calls to run_sync(session=...).

    A restart, incomplete capture, changed dependency, or local semantic cache edit
    requires ordinary reconciliation. Nothing here advances a durable source cursor.
    """

    def __init__(self) -> None:
        self._monitor: SourceMonitor | None = None
        self._key: tuple[str, ...] | None = None
        self._summary: dict[str, Any] | None = None
        self._before: CaptureToken | None = None
        self._epoch = 0
        self._eligible = False
        self._source_paths: list[Path] = []

    def prepare(self, adapter: SourceAdapter, cfg: Config, cache: Cache, scope: str) -> None:
        helper_digest = ''
        if type(adapter) is MessagesAdapter:
            helper = find_helper(cfg)
            if helper:
                try:
                    with helper.open('rb') as binary:
                        helper_digest = hashlib.file_digest(binary, 'sha256').hexdigest()
                except OSError:
                    self.close()
                    return
        key = (adapter.source, scope, str(cache.path.resolve()), code_signature(adapter, cfg), helper_digest, str(Path.cwd()))
        if key != self._key or (self._monitor and self._monitor.failure):
            self.close()
            self._key = key
        if type(adapter) not in (WhatsAppAdapter, MessagesAdapter):
            return  # Unknown adapters have not declared complete media dependency coverage.
        if self._monitor is None:
            try:
                self._monitor = SourceMonitor()
            except MonitoringUnavailable:
                return
        self._source_paths = list(adapter.discover(cfg).values())
        for path in self._source_paths:
            self._monitor.watch(path)
            if path.exists():
                self._monitor.database(path)

    def _read_epoch(self, cache: Cache) -> int:
        if self._key is None:
            raise RuntimeError('sync session is not prepared')
        return read_source(cache.conn, self._key[0], self._key[1])

    def coverage(self, cache: Cache, source: str, scope: str) -> dict[str, Any] | None:
        if (self._summary is not None and self._key is not None
                and self._key[:3] == (source, scope, str(cache.path.resolve()))
                and self._read_epoch(cache) == self._epoch):
            return dict(self._summary['coverage'])
        return None

    def reuse(self, cache: Cache, stats: ExtractStats, at: int, run: str) -> int | None:
        summary = self._summary
        if self._monitor is None or summary is None:
            return None
        with cache.write():
            if self._read_epoch(cache) != self._epoch or not self._monitor.unchanged():
                return None
            for kind, op in (('record', 'record_refresh'), ('observation', 'observation_refresh')):
                spans = summary[kind]
                if spans:
                    stamp(cache.conn, kind, spans, at, run)
                    append(cache.conn, op, {'ranges': spans, 'observed_at': at, 'sync_run': run})
            stats.counts.update(summary['counts'])
            stats.unsupported.update(summary['unsupported'])
            stats.checkpoints.update(summary['checkpoints'])
            stats.counts['monitored_coverage_reused'] = 1
            stats.notes.append('unchanged source verified by retained SQLite/vnode monitor; exact prior coverage re-sighted')
            return sum(hi - lo + 1 for lo, hi in summary['record'])

    def begin(self, cache: Cache, stats: ExtractStats) -> None:
        self._summary = None
        if self._monitor:
            self._monitor.dirty_paths()
            # A previously missing auxiliary database may now exist. Register
            # its SQLite data_version before accepting coverage that includes it.
            for path in self._source_paths:
                if path.exists():
                    self._monitor.database(path)
        self._before = self._monitor.token() if self._monitor else None
        self._eligible = self._before is not None
        if self._before is None:
            reason = self._monitor.failure if self._monitor else 'monitor unavailable for this source/platform'
            stats.notes.append(f'ordinary capture required: {reason}')
        self._epoch = self._read_epoch(cache)
        if self._monitor:
            stats.watch_path = self._monitor.watch

    @contextmanager
    def writing(self, cache: Cache):
        # Called inside the actual writer transaction: intervening writers invalidate
        # aggregation, while this capture's own semantic writes advance its generation.
        if self._read_epoch(cache) != self._epoch:
            self._eligible = False
        yield
        self._epoch = self._read_epoch(cache)

    def finish(self, cache: Cache, stats: ExtractStats, run: str, coverage: dict[str, Any]) -> None:
        if not self._eligible or self._monitor is None or not self._monitor.accept_capture(self._before):
            return
        with cache.write():
            if self._read_epoch(cache) != self._epoch:
                return
            summary: dict[str, Any] = {}
            for kind in ('record', 'observation'):
                if kind == 'record':
                    sql = """SELECT k.id FROM records r INDEXED BY records_capture_run
                        JOIN entity_keys k ON k.urn=r.urn WHERE r.sync_run=? AND r.kind!='sync_runs'"""
                else:
                    sql = 'SELECT observation_id FROM source_observations INDEXED BY observations_capture_run WHERE sync_run=?'
                spans = id_ranges([r[0] for r in cache.conn.execute(sql, (run,))])
                spans.extend([r[0], r[1]] for r in cache.conn.execute("""SELECT first_id,last_id
                    FROM freshness_ranges WHERE sync_run=? AND kind=?""", (run, kind)))
                merged: list[list[int]] = []
                for lo, hi in sorted(spans):
                    if merged and lo <= merged[-1][1] + 1:
                        merged[-1][1] = max(merged[-1][1], hi)
                    else:
                        merged.append([lo, hi])
                summary[kind] = merged
            summary['counts'] = {k: v for k, v in stats.counts.items()
                                 if k not in ('canonical_units_rebuilt', 'canonical_units_reused')}
            summary['unsupported'] = dict(stats.unsupported)
            summary['checkpoints'] = dict(stats.checkpoints)
            summary['coverage'] = {k: v for k, v in coverage.items() if k != 'notes'}
            self._summary = summary

    def close(self) -> None:
        if self._monitor:
            self._monitor.close()
        self._monitor = None
        self._summary = None
        self._before = None
        self._eligible = False
        self._source_paths = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
