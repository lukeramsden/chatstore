#!/usr/bin/env python3
"""Diagnose why drained FSEvents history cannot prove unchanged attachment bytes.

Uses only a temporary synthetic directory. Does not participate in source capture.
Run on macOS: .venv/bin/python scripts/diagnose_fsevents.py
"""

from __future__ import annotations

import ctypes as C
import json
import os
import select
import sqlite3
import subprocess
import sys
import tempfile
import threading
from contextlib import closing
from pathlib import Path
from typing import Any


def probe(root: Path, cursor: dict[str, Any] | None = None) -> dict[str, Any]:
    if sys.platform != 'darwin':
        return {'history_drained': False, 'changed': True, 'reason': 'filesystem history unavailable'}
    try:
        result = subprocess.run([sys.executable, __file__, '--worker'],
            input=json.dumps({'root': str(root), 'cursor': cursor}), text=True,
            capture_output=True, timeout=8, check=True)
        value = json.loads(result.stdout)
        if not isinstance(value, dict) or not isinstance(value.get('history_drained'), bool):
            raise TypeError('invalid filesystem history result')
        return value
    except (OSError, subprocess.SubprocessError, ValueError, TypeError) as exc:
        return {'history_drained': False, 'changed': True, 'reason': type(exc).__name__}


def _native(root: Path, cursor: dict[str, Any] | None) -> dict[str, Any]:
    fs = C.CDLL('/System/Library/Frameworks/CoreServices.framework/CoreServices')
    cf = C.CDLL('/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation')
    dispatch = C.CDLL('/usr/lib/libSystem.B.dylib')

    def fn(lib, name, restype, *args):
        function = getattr(lib, name)
        function.restype, function.argtypes = restype, args
        return function

    release = fn(cf, 'CFRelease', None, C.c_void_p)
    string = fn(cf, 'CFStringCreateWithCString', C.c_void_p, C.c_void_p, C.c_char_p, C.c_uint32)
    stringify = fn(cf, 'CFStringGetCString', C.c_bool, C.c_void_p, C.c_char_p, C.c_long, C.c_uint32)
    uuid_copy = fn(fs, 'FSEventsCopyUUIDForDevice', C.c_void_p, C.c_int32)
    uuid_string = fn(cf, 'CFUUIDCreateString', C.c_void_p, C.c_void_p, C.c_void_p)
    current = fn(fs, 'FSEventsGetCurrentEventId', C.c_uint64)
    root = root.resolve(strict=True)
    st = root.stat()
    if not root.is_dir():
        raise ValueError('watch root is not a directory')
    uuid = uuid_copy(st.st_dev)
    if not uuid:
        raise ValueError('volume has no filesystem history UUID')
    rendered = uuid_string(None, uuid)
    try:
        buffer = C.create_string_buffer(256)
        if not stringify(rendered, buffer, len(buffer), 0x08000100):
            raise ValueError('cannot read volume UUID')
        volume = buffer.value.decode('utf-8')
    finally:
        release(rendered)
        release(uuid)
    boot = subprocess.check_output(['/usr/sbin/sysctl', '-n', 'kern.bootsessionuuid'], text=True).strip()
    identity = {'root': str(root), 'device': st.st_dev, 'inode': st.st_ino, 'volume': volume, 'boot': boot}
    cutoff = current()
    next_cursor = identity | {'event_id': cutoff}
    if cursor is None:
        return {'history_drained': False, 'changed': True, 'cursor': next_cursor, 'reason': 'initial capture required'}
    if any(cursor.get(k) != v for k, v in identity.items()) or not isinstance(cursor.get('event_id'), int) or not 0 <= cursor['event_id'] <= cutoff:
        return {'history_drained': False, 'changed': True, 'cursor': next_cursor, 'reason': 'filesystem identity or event sequence changed'}

    callback_type = C.CFUNCTYPE(None, C.c_void_p, C.c_void_p, C.c_size_t, C.c_void_p,
                               C.POINTER(C.c_uint32), C.POINTER(C.c_uint64))
    history_done = threading.Event()
    changed = False
    uncertain = False

    @callback_type
    def callback(stream, info, count, paths, flags, ids):
        nonlocal changed, uncertain
        for i in range(count):
            flag = flags[i]
            if flag & 0x10:  # HistoryDone is a sentinel, not a path change.
                history_done.set()
            else:
                changed = True
            # MustScanSubDirs, UserDropped, KernelDropped, EventIdsWrapped,
            # RootChanged, Mount and Unmount all require a fresh capture.
            if flag & 0xEF:
                uncertain = True

    create_array = fn(cf, 'CFArrayCreate', C.c_void_p, C.c_void_p, C.POINTER(C.c_void_p), C.c_long, C.c_void_p)
    root_string = string(None, os.fsencode(root), 0x08000100)
    array = create_array(None, (C.c_void_p * 1)(root_string), 1, None)
    create = fn(fs, 'FSEventStreamCreate', C.c_void_p, C.c_void_p, callback_type,
                C.c_void_p, C.c_void_p, C.c_uint64, C.c_double, C.c_uint32)
    stream = create(None, callback, None, array, cursor['event_id'], 0, 0x16)  # NoDefer | WatchRoot | FileEvents
    queue_create = fn(dispatch, 'dispatch_queue_create', C.c_void_p, C.c_char_p, C.c_void_p)
    queue = queue_create(b'chatstore.filesystem-history', None)
    start = fn(fs, 'FSEventStreamStart', C.c_bool, C.c_void_p)
    stop = fn(fs, 'FSEventStreamStop', None, C.c_void_p)
    invalidate = fn(fs, 'FSEventStreamInvalidate', None, C.c_void_p)
    stream_release = fn(fs, 'FSEventStreamRelease', None, C.c_void_p)
    queue_release = fn(dispatch, 'dispatch_release', None, C.c_void_p)
    started = False
    try:
        if not stream or not queue:
            raise ValueError('cannot create filesystem stream')
        fn(fs, 'FSEventStreamSetDispatchQueue', None, C.c_void_p, C.c_void_p)(stream, queue)
        started = start(stream)
        if not started:
            raise ValueError('cannot start filesystem stream')
        fn(fs, 'FSEventStreamFlushSync', None, C.c_void_p)(stream)
        complete = history_done.wait(3)
        # Stop joins callbacks before inspecting their accumulated evidence.
        stop(stream)
        started = False
        return {'history_drained': complete and not uncertain, 'changed': changed or not complete or uncertain,
                'cursor': next_cursor, 'reason': 'history incomplete' if not complete or uncertain else None}
    finally:
        if stream:
            if started:
                stop(stream)
            invalidate(stream)
            stream_release(stream)
        if queue:
            queue_release(queue)
        release(array)
        release(root_string)


def worker() -> None:
    try:
        request = json.load(sys.stdin)
        value = _native(Path(request['root']), request.get('cursor'))
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as exc:
        value = {'history_drained': False, 'changed': True, 'reason': type(exc).__name__}
    print(json.dumps(value))


def main() -> None:
    if sys.platform != 'darwin':
        raise SystemExit('This diagnostic requires macOS FSEvents.')
    results = []
    with tempfile.TemporaryDirectory(prefix='chatstore-fsevents-open-') as directory:
        root = Path(directory)
        with (root / 'synthetic-attachment').open('wb', buffering=0) as handle, closing(select.kqueue()) as watcher:
            watcher.control([select.kevent(handle.fileno(), filter=select.KQ_FILTER_VNODE,
                flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR, fflags=select.KQ_NOTE_WRITE | select.KQ_NOTE_EXTEND)], 0, 0)
            cursor = probe(root).get('cursor')
            if cursor is None:
                raise SystemExit('FSEvents cursor unavailable')
            for iteration in range(5):
                handle.write(b'x')
                os.fsync(handle.fileno())
                result = probe(root, cursor)
                results.append({'write': iteration + 1, 'history_drained': result['history_drained'],
                                'reported_change': result['changed'],
                                'persistent_vnode_reported_change': bool(watcher.control(None, 1, 0))})
                cursor = result.get('cursor', cursor)
        with sqlite3.connect(root / 'synthetic.sqlite') as writer:
            writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,text TEXT)')
            writer.execute("INSERT INTO message VALUES (1,'old')")
            writer.commit()
            reader = sqlite3.connect(f"file:{root / 'synthetic.sqlite'}?mode=ro", uri=True)
            try:
                previous = reader.execute('PRAGMA data_version').fetchone()[0]
                versions = []
                for iteration in range(5):
                    writer.execute('UPDATE message SET text=? WHERE id=1', (str(iteration),))
                    writer.commit()
                    version = reader.execute('PRAGMA data_version').fetchone()[0]
                    versions.append(version != previous)
                    previous = version
            finally:
                reader.close()
    print(json.dumps({'persistent_sqlite_detected_commits': versions, 'scope': 'temporary synthetic file, open handle, write + fsync before each query',
                      'results': results,
                      'missed_known_write': any(r['history_drained'] and not r['reported_change'] for r in results),
                      'conclusion': 'Do not use drained FSEvents history alone to skip media-byte checks.'}, indent=2))


if __name__ == '__main__':
    worker() if '--worker' in sys.argv else main()
