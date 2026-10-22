"""Guard optional WhatsApp databases while pinning their read-only source views."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Self

from ..monitor import CaptureToken, MonitoringUnavailable, SourceMonitor
from . import aliases
from .aliases import SOURCES


class AuxiliaryViews:
    def __init__(self, root: Path, *, monitor: SourceMonitor | None = None) -> None:
        self.root = root.absolute()
        self.sources: dict[str, sqlite3.Connection | None] = {}
        self.evidence: dict[str, Any] = {}
        self._monitor = monitor
        self._owns_monitor = monitor is None
        self._before: CaptureToken | None = None

    def __enter__(self) -> Self:
        try:
            if self._monitor is None:
                self._monitor = SourceMonitor()
            for name in SOURCES:
                path = self.root / f'{name}.sqlite'
                self._monitor.watch(path)
                self._monitor.watch(path.with_name(path.name + '-wal'))
                try:
                    path.lstat()
                    self._monitor.database(path)
                except FileNotFoundError:
                    pass
            if self._monitor.dirty_paths() is None:
                raise MonitoringUnavailable(self._monitor.failure or 'auxiliary monitoring unavailable')
            self._before = self._monitor.token()
            files: dict[str, Any] = {}
            for name in SOURCES:
                path = self.root / f'{name}.sqlite'
                try:
                    path.lstat()
                except FileNotFoundError:
                    self.sources[name] = None
                    files[name] = {'present': False}
                    continue
                self._monitor.database(path)
                conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)
                self.sources[name] = conn
                conn.execute('PRAGMA query_only=1')
                conn.execute('BEGIN')
                conn.execute('SELECT rootpage FROM sqlite_schema LIMIT 1').fetchone()
                stat = path.stat()
                files[name] = {'present': True, 'identity': [stat.st_dev, stat.st_ino],
                               'schema_version': conn.execute('PRAGMA schema_version').fetchone()[0]}
            self.validate()
            assert self._before is not None
            versions = dict(self._before.database_versions)
            self.evidence = {'format': 1, 'files': files, 'monitor_session': self._before.session,
                             'monitor_generation': self._before.generation,
                             'sqlite_versions': {name: versions.get(str(self.root / f'{name}.sqlite')) for name in SOURCES}}
            return self
        except (OSError, sqlite3.Error) as exc:
            self.close()
            raise MonitoringUnavailable(f'auxiliary source view unavailable: {type(exc).__name__}') from exc
        except BaseException:
            self.close()
            raise

    def validate(self) -> None:
        if self._monitor is None or self._before is None or self._monitor.token() != self._before:
            raise MonitoringUnavailable('optional WhatsApp databases changed during capture')

    def close(self) -> None:
        for source in self.sources.values():
            if source is not None:
                source.close()
        self.sources.clear()
        if self._monitor is not None and self._owns_monitor:
            self._monitor.close()
        self._monitor = None
        self._before = None
        self.evidence = {}

    def __exit__(self, *args: object) -> None:
        self.close()


class AuxiliarySession:
    """Retain optional-source monitoring and exact-parent evidence across polls."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._monitor: SourceMonitor | None = None
        self._binding: tuple[str, str, CaptureToken] | None = None

    @contextmanager
    def views(self):
        try:
            if self._monitor is None:
                self._monitor = SourceMonitor()
            with AuxiliaryViews(self.root, monitor=self._monitor) as views:
                yield views
        except BaseException:
            self.close()
            raise

    def prepare(self, conn: sqlite3.Connection, views: AuxiliaryViews, *, full: bool) -> None:
        views.validate()
        assert views._before is not None
        database = str(conn.execute('PRAGMA database_list').fetchone()[2])
        parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
        if point is None:
            raise MonitoringUnavailable('alias capture requires a candidate page point')
        unchanged = not full and parent is not None and self._binding == (database, parent[0], views._before)
        if not unchanged or not aliases.reuse(conn, parent[0]):
            aliases.prepare(conn, sources=views.sources)
        views.validate()
        self._binding = (database, point[0], views._before)

    def close(self) -> None:
        if self._monitor is not None:
            self._monitor.close()
        self._monitor = None
        self._binding = None
