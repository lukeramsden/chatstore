#!/usr/bin/env python3
"""Measure Python allocations across a real synthetic encrypted delta export.

Run with PYTHONPATH=src:. .venv/bin/python scripts/benchmark_export_memory.py.
"""

import base64
import json
import random
import tempfile
import tracemalloc
from contextlib import closing
from pathlib import Path

from chatstore.archive import incremental, verify
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from tests.unit.test_cache import msg


def main():
    rng = random.Random(10)
    with tempfile.TemporaryDirectory(prefix="chatstore-export-memory-") as root:
        dd = DataDir(Path(root))
        dd.create()
        with closing(Cache(dd.cache)) as cache:
            with cache.write():
                for i in range(200):
                    text = base64.b64encode(rng.randbytes(12000)).decode()
                    cache.upsert(msg(i + 1, text, None, None, 1000000), observed_at=100)
            original = verify.verify_archive
            samples = []

            def measured(*args, **kwargs):
                samples.append(tracemalloc.get_traced_memory())
                return original(*args, **kwargs)

            verify.verify_archive = measured
            tracemalloc.start()
            result = incremental.export_changes(
                cache,
                dd,
                Config(),
                Identity(),
                consumer="memory",
                output=dd.exports,
                password="test-password",
            )
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            print(
                json.dumps(
                    {
                        "verification_entry_current_bytes": samples[0][0],
                        "verification_entry_peak_bytes": samples[0][1],
                        "export_peak_bytes": peak,
                        "events": result["counts"]["changes"],
                        "scope": "Python traced allocations during actual encrypted export; synthetic 200 messages with deterministic base64 random 16000-character text. Excludes allocations before export and native-library memory. One run per variant.",
                    }
                )
            )


if __name__ == "__main__":
    main()
