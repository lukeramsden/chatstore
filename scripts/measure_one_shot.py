#!/usr/bin/env python3
"""Measure fresh-process one-shot `chatstore sync` against real sources (issue #22).

Uses a new private temporary data directory, so the real chatstore data dir and
any running collector are untouched. Sources are only read. The temporary
directory holds copies of retained media; it is deleted at the end unless
--keep is given.

    .venv/bin/python scripts/measure_one_shot.py --source messages --polls 3

Messages needs a terminal with Full Disk Access.
"""

import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CHATSTORE = Path(__file__).resolve().parents[1] / '.venv/bin/chatstore'


def run(env, cwd, *args):
    began = time.monotonic()
    out = subprocess.run([str(CHATSTORE), '--json', '--quiet', *args], env=env, cwd=cwd,
                         capture_output=True, text=True, check=False)
    wall = time.monotonic() - began
    try:
        envelope = json.loads(out.stdout)
    except json.JSONDecodeError:
        return wall, {'ok': False, 'errors': [{'code': 'no_json', 'message': out.stderr[-500:]}]}
    return wall, envelope


def summary(label, wall, envelope):
    if not envelope.get('ok') and not envelope.get('data'):
        return f'{label}: {wall:.2f}s FAILED {envelope.get("errors")}'
    lines = []
    for r in envelope['data']:
        counts = r['counts']
        capture = (r.get('snapshot') or {}).get('prepared_native', {}).get('capture', {})
        path = ('baseline' if capture.get('baseline') else 'delta') if capture else 'snapshot'
        lines.append(f'{label}: {wall:.2f}s {r["source"]} {r["status"]} {r["mode"]} {path} '
                     f'rows_changed={capture.get("rows_changed")} messages={counts.get("messages", 0)} '
                     f'rebuilt={counts.get("canonical_units_rebuilt")} '
                     f'coverage_reused={counts.get("prepared_coverage_reused")} '
                     f'headers_reused={counts.get("prepared_headers_reused")}')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--source', choices=('all', 'whatsapp', 'messages'), default='all')
    parser.add_argument('--polls', type=int, default=3, help='One-shot runs after the baseline.')
    parser.add_argument('--interval', type=float, default=5, help='Seconds between polls.')
    parser.add_argument('--fd-limit', type=int, default=256)
    parser.add_argument('--keep', action='store_true', help='Keep the temporary data directory.')
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix='chatstore-measure-'))
    os.chmod(root, 0o700)
    env = dict(os.environ, CHATSTORE_DATA_DIR=str(root / 'data'))
    decoder = Path(__file__).resolve().parents[1] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if decoder.is_file():
        env.setdefault('CHATSTORE_MESSAGES_DECODER', str(decoder))
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (args.fd_limit, hard))
    try:
        subprocess.run([str(CHATSTORE), '--json', 'init', '--timezone', 'UTC'], env=env, check=True,
                       capture_output=True)
        print(summary('baseline (cwd /)', *run(env, '/', 'sync', '--source', args.source)), flush=True)
        for poll in range(args.polls):
            time.sleep(args.interval)
            cwd = '/tmp' if poll % 2 == 0 else '/'
            print(summary(f'poll {poll + 1} (cwd {cwd})', *run(env, cwd, 'sync', '--source', args.source)), flush=True)
    finally:
        if args.keep:
            print(f'kept {root}', file=sys.stderr)
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == '__main__':
    main()
