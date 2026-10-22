#!/usr/bin/env python3
"""Measure synthetic vnode attachment routing, including descriptor exhaustion."""

import argparse
import json
import os
import resource
import statistics
import sys
import tempfile
import time
from pathlib import Path

from chatstore.adapters.media_watch import MediaWatch
from chatstore.adapters.monitor import MonitoringUnavailable, SourceMonitor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--files', type=int, default=10000)
    parser.add_argument('--polls', type=int, default=100)
    parser.add_argument('--fd-limit', type=int)
    args = parser.parse_args()
    if sys.platform != 'darwin':
        parser.error('macOS vnode monitoring is required')
    if args.files < 1 or args.polls < 1:
        parser.error('files and polls must be positive')
    if args.fd_limit is not None:
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (args.fd_limit, hard))
    before_fds = len(os.listdir('/dev/fd'))
    result = {'files': args.files, 'polls': args.polls,
              'fd_limit': resource.getrlimit(resource.RLIMIT_NOFILE)[0],
              'scope': 'Synthetic MediaWatch/SourceMonitor routing only. Excludes file setup/mutation, media copying, SQLite, canonical sync, archives and delivery. RSS is lifetime process peak including fixture setup. Logical source file bytes read by routing are zero; kernel I/O and energy are not measured.'}
    with tempfile.TemporaryDirectory(prefix='chatstore-media-watch-') as directory:
        root = Path(directory).resolve()
        for row_id in range(args.files):
            (root / str(row_id)).write_bytes(b'fixture')
        monitor = SourceMonitor()
        watch = MediaWatch(monitor)
        wall, cpu = time.perf_counter(), time.process_time()
        try:
            watch.update((row_id, str(root / str(row_id))) for row_id in range(args.files))
            assert len(watch.dirty_ids()) == args.files
            assert monitor.accept_capture(monitor.token())
            result['registration'] = {'wall_seconds': time.perf_counter() - wall,
                                      'cpu_seconds': time.process_time() - cpu,
                                      'open_fd_delta': len(os.listdir('/dev/fd')) - before_fds}
            polls = []
            for _ in range(args.polls):
                wall, cpu = time.perf_counter(), time.process_time()
                assert not watch.dirty_ids()
                polls.append((time.perf_counter() - wall, time.process_time() - cpu))
            result['clean_median'] = {'wall_seconds': statistics.median(row[0] for row in polls),
                                      'cpu_seconds': statistics.median(row[1] for row in polls)}
            changed = set(range(0, args.files, max(1, args.files // 10)))
            for row_id in changed:
                with (root / str(row_id)).open('r+b', buffering=0) as writer:
                    writer.write(b'changed')
                    os.fsync(writer.fileno())
            wall, cpu = time.perf_counter(), time.process_time()
            dirty = watch.dirty_ids()
            result['sparse'] = {'wall_seconds': time.perf_counter() - wall,
                                'cpu_seconds': time.process_time() - cpu, 'dirty_ids': len(dirty)}
            assert dirty == changed
            result['status'] = 'complete'
        except MonitoringUnavailable as exc:
            result['status'] = 'monitoring_unavailable'
            result['failure'] = str(exc)
            result['failure_wall_seconds'] = time.perf_counter() - wall
            assert monitor.token() is None and not monitor.unchanged()
        finally:
            monitor.close()
    result['fds_after_close_delta'] = len(os.listdir('/dev/fd')) - before_fds
    result['peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert result['fds_after_close_delta'] == 0
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
