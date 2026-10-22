#!/usr/bin/env python3
"""Profile the new-message capture in benchmark_native_whatsapp (same CLI options)."""

import cProfile
import io
import json
import pstats
import sqlite3
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import benchmark_native_whatsapp as benchmark


def profile_rows(profile):
    stats = pstats.Stats(profile)
    return [
        {'file': Path(key[0]).name, 'line': key[1], 'function': key[2],
         'primitive_calls': value[0], 'calls': value[1],
         'self_seconds': value[2], 'cumulative_seconds': value[3]}
        for key, value in sorted(stats.stats.items(), key=lambda item: item[1][3], reverse=True)[:30]
    ]


def main():
    original = benchmark.collect_native
    calls = 0
    diagnostic = {}

    def collect(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls != 3:
            return original(*args, **kwargs)
        counters = {}
        saved = sqlite3.connect

        def handler(label):
            def progress():
                counters[label] = counters.get(label, 0) + 1000
                return 0
            return progress

        def connect(database, *values, **options):
            conn = saved(database, *values, **options)
            name = str(database)
            label = 'native-mirror' if '/native/' in name else Path(name).name
            conn.set_progress_handler(handler(label), 1000)
            return conn

        cache = args[4]
        cache.conn.set_progress_handler(handler('canonical-cache'), 1000)
        profile = cProfile.Profile()
        try:
            with patch.object(sqlite3, 'connect', connect):
                result = profile.runcall(original, *args, **kwargs)
        finally:
            cache.conn.set_progress_handler(None, 0)
        diagnostic.update(sqlite_vm_steps_lower_bounds=counters, functions=profile_rows(profile))
        return result

    output = io.StringIO()
    with redirect_stdout(output), patch.object(benchmark, 'collect_native', collect):
        benchmark.main()
    report = json.loads(output.getvalue())
    captures = [row for row in report['measurements'] if row['stage'].startswith('sync-')]
    assert captures[2]['stage'] == 'sync-new-message' and diagnostic
    report['new_message_profile'] = diagnostic
    report['profiling_scope'] = (
        'cProfile and SQLite progress handlers apply only to the third capture (new-message). '
        'VM counts aggregate 1,000-step callbacks by connection path label, rounded down per '
        'connection; omitted labels had no complete callback interval. Existing canonical Cache '
        'and newly opened SQLite connections are instrumented. This adds diagnostic overhead. '
        'No pipeline stages run unless --pipeline is passed. Physical I/O remains process-attributed.'
    )
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
