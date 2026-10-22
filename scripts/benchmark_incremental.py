#!/usr/bin/env python3
"""Reproducible synthetic stage benchmarks; never reads configured personal sources.

Run from the repository: .venv/bin/python scripts/benchmark_incremental.py --messages 100000
Each stage runs in a fresh process. macOS storage counters come from proc_pid_rusage(2);
Linux uses /proc/self/io. Cached reads may report zero storage bytes. No energy estimate is made.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import platform
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.archive import export as E
from chatstore.archive.import_ import import_archive, order_paths
from chatstore.archive.incremental import export_changes
from chatstore.archive.verify import verify_archive
from chatstore.cache import Cache
from chatstore.config import Config, Identity, load_identity, save_config, save_identity
from chatstore.paths import DataDir
from chatstore.sync import run_sync
from chatstore.sync_session import SyncSession
from tests.fixtures.synthetic import build_messages, build_whatsapp

PASSWORD = 'synthetic-benchmark-only'


def io_counters(*, include_memory: bool = False) -> dict[str, int] | None:
    if sys.platform == 'darwin':
        class Usage(ctypes.Structure):
            _fields_ = [('uuid', ctypes.c_uint8 * 16)] + [(n, ctypes.c_uint64) for n in (
                'user', 'system', 'idle_wakeups', 'interrupt_wakeups', 'pageins', 'wired',
                'resident', 'footprint', 'start', 'exit', 'child_user', 'child_system',
                'child_idle', 'child_interrupt', 'child_pageins', 'child_elapsed', 'read', 'written')]
        usage = Usage()
        import os
        lib = ctypes.CDLL('/usr/lib/libproc.dylib')
        if lib.proc_pid_rusage(os.getpid(), 2, ctypes.byref(usage)) == 0:
            values = {'storage_read_bytes': usage.read, 'storage_written_bytes': usage.written,
                      'package_idle_wakeups': usage.idle_wakeups, 'interrupt_wakeups': usage.interrupt_wakeups}
            if include_memory:
                values.update(resident_bytes=usage.resident, physical_footprint_bytes=usage.footprint)
            return values
    if sys.platform.startswith('linux'):
        entries = dict(line.split(':', 1) for line in Path('/proc/self/io').read_text().splitlines())
        return {'storage_read_bytes': int(entries['read_bytes']), 'storage_written_bytes': int(entries['write_bytes'])}
    return None


def stage(root: Path, name: str, source: str = 'whatsapp', session: SyncSession | None = None) -> dict[str, Any]:
    dd = DataDir(root / 'data')
    cfg = Config(source_paths={source: str(root / ('wa' if source == 'whatsapp' else 'im'))})
    importing = name.startswith('import')
    ident = Identity() if importing else load_identity(dd)
    cache = None if importing else Cache(dd.cache)
    wall, cpu, before = time.perf_counter(), time.process_time(), io_counters()
    children_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    archives = []
    sync_counts = None
    if name.startswith('sync'):
        result = run_sync(WhatsAppAdapter() if source == 'whatsapp' else MessagesAdapter(), dd, cfg, ident, cache, session=session)
        assert result.exit_code == 0
        sync_counts = result.run['counts']
    elif name.startswith('monthly'):
        plans = [E.collect_catalogue(cache)] + [E.collect_bucket(cache, b) for b in E.plan_buckets(cache, None, None)]
        for plan in plans:
            result = E.write_archive(cache, dd, cfg, ident, plan, output=root / 'monthly', password=PASSWORD,
                                     media='text', force=False, adapter_versions={})
            if result.action == 'written':
                archives.append(result.path)
    elif name == 'export-cli-unchanged':
        import contextlib
        import io
        import os

        from chatstore.cli.app import main as cli_main
        password_file = root / 'benchmark-password'
        fd = os.open(password_file, os.O_WRONLY | os.O_CREAT, 0o600)
        with os.fdopen(fd, 'w') as f:
            f.write(PASSWORD)
        os.environ['CHATSTORE_PASSWORD_FILE'] = str(password_file)
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            code = cli_main(['--json', '--quiet', '--data-dir', str(dd.root), 'archive', 'export',
                             '--consumer', 'bench', '--output', str(root / 'delta')])
        assert code == 0 and json.loads(stdout.getvalue())['data']['written'] == 0
    elif name.startswith('export'):
        while True:
            result = export_changes(cache, dd, cfg, ident, consumer='bench', output=root / 'delta', password=PASSWORD)
            if result['action'] == 'written':
                archives.append(result['path'])
            if not result['has_more']:
                break
    elif name == 'verify-new-delta':
        path = Path(json.loads((root / 'export-old-edit.json').read_text())['archives'][-1])
        assert verify_archive(path, PASSWORD).ok
    elif name.startswith('import'):
        target = DataDir(root / ('cold' if name == 'import-cold-all' else 'restored'))
        target.create()
        dst = Cache(target.cache)
        dest_ident = Identity() if not target.identity.exists() else load_identity(target)
        if name == 'import-cold-all':
            paths = list((root / 'delta').glob('*.zip'))
        else:
            export_stage = {'import-baseline': 'export-baseline', 'import-poll': 'export-poll',
                      'import-new-message': 'export-new-message', 'import-old-edit': 'export-old-edit'}[name]
            paths = [Path(p) for p in json.loads((root / (export_stage + '.json')).read_text())['archives']]
        for p in order_paths(paths):
            result = import_archive(dst, target, dest_ident, p, PASSWORD)
            assert result.action == 'imported', result
            assert result.conflicts == 0, result
        dst.close()
    else:
        raise ValueError(name)
    if cache is not None:
        cache.close()
    after = io_counters()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    parent_cpu = time.process_time() - cpu
    child_cpu = (children.ru_utime - children_before.ru_utime) + (children.ru_stime - children_before.ru_stime)
    child_active = child_cpu > 0 or children.ru_nvcsw != children_before.ru_nvcsw or children.ru_nivcsw != children_before.ru_nivcsw
    rss_scale = 1 if sys.platform == 'darwin' else 1024
    child_peak = children.ru_maxrss * rss_scale if child_active else 0
    result = {'stage': name, 'wall_seconds': time.perf_counter() - wall,
              'cpu_seconds': parent_cpu + child_cpu, 'parent_cpu_seconds': parent_cpu, 'child_cpu_seconds': child_cpu,
              'peak_rss_bytes': usage.ru_maxrss * rss_scale, 'child_peak_rss_bytes': child_peak,
              'combined_peak_rss_upper_bound_bytes': usage.ru_maxrss * rss_scale + child_peak,
              'child_input_block_operations': children.ru_inblock - children_before.ru_inblock,
              'child_output_block_operations': children.ru_oublock - children_before.ru_oublock, 'io_scope': 'parent_process',
              'archive_bytes': sum(Path(p).stat().st_size for p in archives), 'archive_count': len(archives), 'archives': archives,
              'energy_joules': None, 'io': {k: after[k] - before[k] for k in before} if before and after else None}
    if sync_counts is not None:
        result['sync_counts'] = sync_counts
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=['whatsapp', 'messages'], default='whatsapp')
    parser.add_argument('--messages', type=int, default=100000)
    parser.add_argument('--workdir', type=Path)
    parser.add_argument('--stage')
    parser.add_argument('--monitored-polls', type=int, default=0, help='Measure N polls with a retained source monitor.')
    parser.add_argument('--sync-only', action='store_true', help='Measure native source sync cases without repeating archive baselines.')
    args = parser.parse_args()
    if args.stage:
        print(json.dumps(stage(args.workdir, args.stage, args.source)))
        return
    root = (args.workdir or Path(tempfile.mkdtemp(prefix='chatstore-bench-'))).resolve()
    root.mkdir(parents=True, exist_ok=True)
    native_root = root / ('wa' if args.source == 'whatsapp' else 'im')
    (build_whatsapp if args.source == 'whatsapp' else build_messages)(native_root, extra_messages=args.messages)
    native_db = native_root / ('ChatStorage.sqlite' if args.source == 'whatsapp' else 'chat.db')
    # Spread synthetic history across about 6 years, rather than one oversized month.
    with sqlite3.connect(native_db) as native:
        native.execute('UPDATE ZWAMESSAGE SET ZMESSAGEDATE=700001000-(Z_PK-100)*2000 WHERE Z_PK>=100' if args.source == 'whatsapp'
                       else 'UPDATE message SET date=(700001000-(ROWID-100)*2000)*1000000000 WHERE ROWID>=100')
        if args.source == 'messages':
            native.execute('CREATE INDEX fixture_chat_message ON chat_message_join(message_id,chat_id)')
            native.execute('CREATE INDEX fixture_attachment_message ON message_attachment_join(message_id,attachment_id)')
            native.execute('CREATE INDEX fixture_thread_originator ON message(thread_originator_guid)')
    dd = DataDir(root / 'data')
    dd.create()
    ident = Identity()
    ident.ensure_scope(args.source)
    ident.ensure_export_set()
    save_identity(dd, ident)
    save_config(dd, Config(source_paths={args.source: str(native_root)}))
    rows = []
    if args.monitored_polls:
        if args.monitored_polls < 3:
            parser.error('--monitored-polls must be at least 3 (includes coverage registration)')
        with SyncSession() as session:
            for i in range(args.monitored_polls):
                result = stage(root, f'sync-monitored-{i + 1}', args.source, session)
                rows.append(result)
                print(json.dumps(result), flush=True)
        summary = {'source': args.source, 'platform': platform.platform(), 'extra_messages': args.messages,
                   'scope': 'retained-process native monitor; per-poll process RSS includes earlier polls',
                   'measurements': rows}
        (root / 'results.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(f'Results: {root / "results.json"}', file=sys.stderr)
        return
    def run(name):
        offline = name == 'import-cold-all'
        if offline:
            dd.root.rename(root / 'offline-data')
            native_root.rename(root / 'offline-source')
        try:
            completed = subprocess.run([sys.executable, __file__, '--workdir', str(root), '--stage', name, '--source', args.source],
                                       check=True, capture_output=True, text=True)
        finally:
            if offline:
                (root / 'offline-data').rename(dd.root)
                (root / 'offline-source').rename(native_root)
        result = json.loads(completed.stdout)
        (root / (name + '.json')).write_text(json.dumps(result))
        rows.append(result)
        print(json.dumps(result), flush=True)
    for name in (('sync-initial', 'sync-poll') if args.sync_only else
                 ('sync-initial', 'monthly-baseline', 'export-baseline', 'sync-poll', 'monthly-poll', 'export-poll', 'export-unchanged', 'export-cli-unchanged')):
        run(name)
    with sqlite3.connect(native_db) as native:
        if args.source == 'whatsapp':
            native.execute("""INSERT INTO ZWAMESSAGE SELECT Z_PK+1000000,Z_ENT,Z_OPT,ZFLAGS,ZGROUPEVENTTYPE,
                ZISFROMME,ZMESSAGEERRORSTATUS,ZMESSAGESTATUS,ZMESSAGETYPE,ZSORT+1000000,ZSPOTLIGHTSTATUS,ZSTARRED,
                ZCHATSESSION,ZGROUPMEMBER,ZMEDIAITEM,ZPARENTMESSAGE,ZMESSAGEDATE,ZSENTDATE,ZFROMJID,ZPUSHNAME,
                'new-one-message','one new message',ZTOJID FROM ZWAMESSAGE WHERE Z_PK=1""")
        else:
            native.row_factory = sqlite3.Row
            record = dict(native.execute('SELECT * FROM message WHERE ROWID=1').fetchone())
            record.update(ROWID=1000001, guid='CCCCCCCC-0000-4000-8000-000000000001', text='one new message')
            native.execute(f"INSERT INTO message({','.join(record)}) VALUES ({','.join('?'*len(record))})", list(record.values()))
            native.execute('INSERT INTO chat_message_join VALUES (1,1000001,0)')
    for name in (('sync-new-message',) if args.sync_only else ('sync-new-message', 'monthly-new-message', 'export-new-message')):
        run(name)
    with sqlite3.connect(native_db) as native:
        native.execute("UPDATE ZWAMESSAGE SET ZTEXT='edit to a message from years ago' WHERE Z_PK=1" if args.source == 'whatsapp'
                       else "UPDATE message SET text='edit to a message from years ago' WHERE ROWID=1")
    for name in (('sync-old-edit',) if args.sync_only else
                 ('sync-old-edit', 'export-old-edit', 'verify-new-delta', 'import-baseline', 'import-poll',
                  'import-new-message', 'import-old-edit', 'import-cold-all')):
        run(name)
    summary = {'source': args.source, 'platform': platform.platform(), 'python': sys.version, 'extra_messages': args.messages,
               'workdir': str(root), 'sync_only': args.sync_only, 'measurements': [{k: v for k, v in r.items() if k != 'archives'} for r in rows]}
    (root / 'results.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(f'Results: {root / "results.json"}', file=sys.stderr)


if __name__ == '__main__':
    main()
