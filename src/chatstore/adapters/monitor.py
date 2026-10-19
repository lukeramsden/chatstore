"""Live source-change detection. Tokens are valid only within this monitor's lifetime.

This is a prerequisite for bounded source polling, not a durable source cursor. A
caller must register every referenced media path before hashing it, and accept only
a successful capture whose pre-capture token is still current. New registrations
invalidate that token, requiring a reconciliation with stable watch coverage.
"""

from __future__ import annotations

import os
import select
import sqlite3
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Self


class MonitoringUnavailable(Exception):
    pass


@dataclass(frozen=True)
class CaptureToken:
    session: str
    generation: int
    database_versions: tuple[tuple[str, int], ...]


class SourceMonitor:
    def __init__(self) -> None:
        if sys.platform != 'darwin':
            raise MonitoringUnavailable('live vnode monitoring requires macOS')
        self.session = uuid.uuid4().hex
        try:
            self._queue = select.kqueue()
        except OSError as exc:
            raise MonitoringUnavailable('cannot create vnode monitor') from exc
        self._files: dict[Path, int] = {}
        self._flags: dict[Path, int] = {}
        self._fd_targets: dict[int, Path] = {}
        self._dependencies: dict[Path, dict[Path, int]] = {}
        self._routes: dict[Path, dict[int, set[Path]]] = {}
        self._note_bits = (select.KQ_NOTE_ATTRIB, select.KQ_NOTE_RENAME, select.KQ_NOTE_DELETE,
                           select.KQ_NOTE_REVOKE, select.KQ_NOTE_WRITE, select.KQ_NOTE_EXTEND, select.KQ_NOTE_LINK)
        self._requests: set[Path] = set()
        self._dirty: set[Path] = set()
        self._needs_rearm: set[Path] = set()
        self._databases: dict[Path, sqlite3.Connection] = {}
        self._database_versions: dict[Path, int] = {}
        self._generation = 0
        self._accepted: CaptureToken | None = None
        self._failure: str | None = None
        self._closed = False

    @property
    def failure(self) -> str | None:
        return self._failure

    def _fail(self, reason: str) -> None:
        self._failure = reason
        self._accepted = None

    def watch(self, path: Path) -> None:
        """Watch a file (including future creation) and each existing ancestor.

        Symlinks require additional target dependencies; fail closed until a caller
        can supply a complete, stable set. Registration errors never permit reuse.
        """
        if self._closed:
            raise MonitoringUnavailable('monitor is closed')
        path = path.absolute()
        if path not in self._requests:
            self._requests.add(path)
            self._dirty.add(path)
            self._generation += 1
        missing_child = False
        for target in (path, *path.parents):
            try:
                try:
                    st = target.lstat()
                except FileNotFoundError:
                    missing_child = True
                    if target in self._databases:
                        self._fail('monitored database identity changed')
                        return
                    old = self._files.pop(target, None)
                    self._flags.pop(target, None)
                    if old is not None:
                        self._invalidate_target(target)
                        self._fd_targets.pop(old, None)
                        os.close(old)
                        self._generation += 1
                    continue
                if target.is_symlink():
                    self._fail('symlink watch coverage is incomplete')
                    return
                flags = select.KQ_NOTE_ATTRIB | select.KQ_NOTE_RENAME | select.KQ_NOTE_DELETE | select.KQ_NOTE_REVOKE
                if target == path or missing_child:
                    flags |= select.KQ_NOTE_WRITE | select.KQ_NOTE_EXTEND | select.KQ_NOTE_LINK
                missing_child = False
                subscribed = self._set_dependency(target, path, flags)
                old = self._files.get(target)
                if old is not None:
                    previous = os.fstat(old)
                    if (previous.st_dev, previous.st_ino) == (st.st_dev, st.st_ino):
                        if subscribed != self._flags[target]:
                            self._flags[target] = subscribed
                            self._queue.control([select.kevent(old, filter=select.KQ_FILTER_VNODE,
                                flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR,
                                fflags=self._flags[target])], 0, 0)
                            self._generation += 1
                        continue
                    if target in self._databases:
                        self._fail('monitored database identity changed')
                        return
                    self._invalidate_target(target)
                    self._fd_targets.pop(old, None)
                    os.close(old)
                    del self._files[target]
                    del self._flags[target]
                fd = os.open(target, getattr(os, 'O_EVTONLY', os.O_RDONLY) | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    self._queue.control([select.kevent(fd, filter=select.KQ_FILTER_VNODE,
                        flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR,
                        fflags=subscribed)], 0, 0)
                except BaseException:
                    os.close(fd)
                    raise
                self._files[target] = fd
                self._fd_targets[fd] = target
                self._flags[target] = subscribed
                self._generation += 1
            except OSError as exc:
                self._fail(f'watch registration failed: {type(exc).__name__}')
                return
        self._needs_rearm.discard(path)

    def _invalidate_target(self, target: Path) -> None:
        affected = self._dependencies.get(target, {})
        self._dirty.update(affected)
        self._needs_rearm.update(affected)

    def _set_dependency(self, target: Path, path: Path, flags: int) -> int:
        dependencies = self._dependencies.setdefault(target, {})
        if dependencies.get(path, 0) != flags:
            routes = self._routes.setdefault(target, {})
            for bit in self._note_bits:
                members = routes.setdefault(bit, set())
                if flags & bit:
                    members.add(path)
                else:
                    members.discard(path)
            if flags:
                dependencies[path] = flags
            else:
                dependencies.pop(path, None)
        return sum(bit for bit, members in self._routes.get(target, {}).items() if members)

    def unwatch(self, path: Path) -> None:
        """Release retired media dependencies while preserving shared ancestors."""
        if self._closed:
            raise MonitoringUnavailable('monitor is closed')
        path = path.absolute()
        if path in self._databases:
            raise MonitoringUnavailable('database dependencies require a new monitor session')
        if path not in self._requests:
            return
        self._requests.remove(path)
        self._dirty.discard(path)
        self._needs_rearm.discard(path)
        self._generation += 1
        try:
            for target in (path, *path.parents):
                subscribed = self._set_dependency(target, path, 0)
                fd = self._files.get(target)
                if not subscribed:
                    if fd is not None:
                        os.close(fd)
                        self._fd_targets.pop(fd, None)
                        del self._files[target]
                        del self._flags[target]
                    self._dependencies.pop(target, None)
                    self._routes.pop(target, None)
                elif fd is not None and self._flags[target] != subscribed:
                    self._flags[target] = subscribed
                    self._queue.control([select.kevent(fd, filter=select.KQ_FILTER_VNODE,
                        flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR, fflags=subscribed)], 0, 0)
        except OSError as exc:
            self._fail(f'watch retirement failed: {type(exc).__name__}')

    def database(self, path: Path) -> None:
        path = path.absolute()
        self.watch(path)
        if path in self._databases or self._failure:
            return
        try:
            conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)
            try:
                conn.execute('PRAGMA query_only=1')
                conn.execute('PRAGMA data_version').fetchone()
            except BaseException:
                conn.close()
                raise
            self._databases[path] = conn
            self._generation += 1
        except sqlite3.Error as exc:
            self._fail(f'database monitoring failed: {type(exc).__name__}')

    def token(self) -> CaptureToken | None:
        if self._closed or self._failure:
            return None
        try:
            while events := self._queue.control(None, 256, 0):
                self._generation += 1
                if any(event.flags & select.KQ_EV_ERROR for event in events):
                    self._fail('vnode notification error')
                    return None
                for event in events:
                    target = self._fd_targets.get(event.ident)
                    if target is None:
                        self._fail('vnode notification has no registered identity')
                        return None
                    replaced = event.fflags & (select.KQ_NOTE_DELETE | select.KQ_NOTE_RENAME | select.KQ_NOTE_REVOKE)
                    if replaced and target in self._databases:
                        self._fail('monitored database identity changed')
                        return None
                    affected: set[Path] = set()
                    for bit, paths in self._routes[target].items():
                        if event.fflags & bit:
                            affected.update(paths)
                    self._dirty.update(affected)
                    for path in affected:
                        if replaced or (target != path and event.fflags & select.KQ_NOTE_WRITE):
                            self._needs_rearm.add(path)
            versions = tuple((str(path), conn.execute('PRAGMA data_version').fetchone()[0])
                             for path, conn in sorted(self._databases.items()))
            for filename, version in versions:
                key = Path(filename)
                if self._database_versions.get(key) != version:
                    self._dirty.add(key)
                    self._database_versions[key] = version
            return CaptureToken(self.session, self._generation, versions)
        except (OSError, sqlite3.Error) as exc:
            self._fail(f'notification read failed: {type(exc).__name__}')
            return None

    def dirty_paths(self) -> frozenset[Path] | None:
        """Poll and rearm changed dependencies; retain dirtiness until stable capture.

        Clean polls perform no path enumeration/stat/open. Namespace changes can
        affect several missing children, all of which remain dirty conservatively.
        """
        if self.token() is None:
            return None
        for path in tuple(self._needs_rearm):
            self.watch(path)
        if self.token() is None:
            return None
        return frozenset(self._dirty)

    def accept_capture(self, before: CaptureToken | None) -> bool:
        """Acknowledge successful capture only if nothing changed during it."""
        after = self.token()
        self._accepted = after if before is not None and before == after and not self._needs_rearm else None
        if self._accepted is not None:
            self._dirty.clear()
        return self._accepted is not None

    def unchanged(self) -> bool:
        current = self.token()
        return current is not None and self._accepted is not None and current == self._accepted

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._accepted = None
            for conn in self._databases.values():
                conn.close()
            for fd in self._files.values():
                os.close(fd)
            self._queue.close()
            self._databases.clear()
            self._database_versions.clear()
            self._files.clear()
            self._flags.clear()
            self._fd_targets.clear()
            self._dependencies.clear()
            self._routes.clear()
            self._requests.clear()
            self._dirty.clear()
            self._needs_rearm.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
