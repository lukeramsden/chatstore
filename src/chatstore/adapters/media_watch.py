"""Process-local attachment ownership for a live source monitor."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from .monitor import MonitoringUnavailable, SourceMonitor


class MediaWatch:
    def __init__(self, monitor: SourceMonitor) -> None:
        self.monitor = monitor
        self._paths: dict[int, Path] = {}
        self._owners: dict[Path, set[int]] = {}

    def update(self, attachments: Iterable[tuple[int, str | None]]) -> None:
        """Apply changed rows; a missing filename also retires deleted rows.

        Register new paths before capture. Callers must also capture every changed
        attachment row, including a new owner of an already watched shared path,
        and acknowledge only after stable media retention.
        """
        for row_id, filename in attachments:
            path = Path(filename).expanduser().absolute() if filename else None
            old = self._paths.get(row_id)
            if old == path:
                continue
            if path is not None:
                if path not in self._owners:
                    self.monitor.watch(path)
                    if self.monitor.failure:
                        raise MonitoringUnavailable(self.monitor.failure)
                    self._owners[path] = set()
                self._owners[path].add(row_id)
                self._paths[row_id] = path
            else:
                self._paths.pop(row_id, None)
            if old is not None:
                owners = self._owners[old]
                owners.remove(row_id)
                if not owners:
                    self.monitor.unwatch(old)
                    del self._owners[old]
                    if self.monitor.failure:
                        raise MonitoringUnavailable(self.monitor.failure)

    def dirty_ids(self) -> frozenset[int]:
        paths = self.monitor.dirty_paths()
        if paths is None:
            raise MonitoringUnavailable(self.monitor.failure or 'media monitoring unavailable')
        return frozenset(row_id for path in paths for row_id in self._owners.get(path, ()))
