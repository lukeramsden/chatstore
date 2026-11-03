import time

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "real_source" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="real-source tests are local-only"))


class _LaterClock:
    """`time` stand-in for the media ledger: every scan happens 10s from now."""

    def time_ns(self) -> int:
        return time.time_ns() + 10 * 10**9


@pytest.fixture(autouse=True)
def _media_ledger_clock(request, monkeypatch):
    # Tests poll right after writing files, which the racy-clean rule would
    # re-hash. Shift the scan clock instead of the window: the window is part
    # of the extraction policy that subprocesses recompute. Racy-clean tests
    # (marked `racy_window`) keep the real clock.
    if "racy_window" not in request.keywords:
        monkeypatch.setattr("chatstore.adapters.media_ledger.time", _LaterClock())
