#!/usr/bin/env python3
"""Transfer prepared synthetic batches over SSH and invoke an isolated Linux importer.

Requires benchmark_remote_pipeline.py and the matching repository snapshot under
REMOTE_ROOT/repo. The remote interpreter only supplies dependencies; PYTHONPATH
selects the isolated snapshot. No configured production data directory is used.
"""
from __future__ import annotations

import argparse
import io
import json
import re
import resource
import shlex
import subprocess
import tarfile
import time
from pathlib import Path

STAGES = ('baseline', 'clean', 'new-message', 'old-edit', 'context', 'old-delete')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--remote-root', required=True)
    parser.add_argument('--remote-python', required=True)
    parser.add_argument('--local-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = []
    log = args.output.with_suffix('.jsonl')
    with log.open('x'):
        pass

    def save(row):
        rows.append(row)
        with log.open('a') as output:
            output.write(json.dumps(row) + '\n')
        print(json.dumps(row), flush=True)

    def ssh(command, **kwargs):
        return subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ServerAliveInterval=15',
            '-o', 'ServerAliveCountMax=3', args.host, command], check=True, capture_output=True, **kwargs)

    def invoke(operation, stage):
        code = args.remote_root + '/repo'
        command = shlex.join(['env', 'PYTHONPATH=' + code + '/src:' + code, args.remote_python,
            code + '/scripts/benchmark_remote_pipeline.py', operation, '--root', args.remote_root,
            '--stage', stage])
        completed = ssh(command, text=True)
        result = json.loads(completed.stdout)
        # The result file exists only after post-timing preservation assertions pass.
        verified = json.loads(ssh(shlex.join(['cat', args.remote_root + f'/{operation}-{stage}-result.json']), text=True).stdout)
        assert verified['assertions_passed'] and all(verified[k] == v for k, v in result.items())
        save(verified)

    def transfer(stage, *, partial=False):
        metadata = json.loads((args.local_root / f'{stage}.json').read_text())
        names = [entry['file'] for entry in metadata['objects']]
        assert all(Path(name).name == name for name in names)
        payload_bytes = sum((args.local_root / 'objects' / name).stat().st_size for name in names)
        metadata_bytes = (args.local_root / f'{stage}.json').stat().st_size
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        wall, cpu = time.perf_counter(), time.process_time()
        command = shlex.join(['tar', '-xf', '-', '-C', args.remote_root])
        receiver = subprocess.Popen(['ssh', '-v', '-o', 'BatchMode=yes', '-o', 'ServerAliveInterval=15',
            '-o', 'ServerAliveCountMax=3', args.host, command], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if partial:
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode='w', format=tarfile.USTAR_FORMAT) as archive:
                archive.add(args.local_root / f'{stage}.json', arcname=f'{stage}.json')
                archive.add(args.local_root / 'objects' / names[0], arcname='objects/' + names[0])
            prefix = 512 + ((metadata_bytes + 511) // 512) * 512 + 512
            sent = buffer.getvalue()[:prefix + (args.local_root / 'objects' / names[0]).stat().st_size // 2]
            stdout, stderr = receiver.communicate(sent)
            assert receiver.returncode != 0, 'partial TAR transfer unexpectedly succeeded'
        else:
            sender = subprocess.Popen(['tar', '-cf', '-', '-C', str(args.local_root), f'{stage}.json',
                *('objects/' + name for name in names)], stdout=receiver.stdin, stderr=subprocess.PIPE)
            receiver.stdin.close()
            receiver.stdin = None
            _, sender_error = sender.communicate()
            stdout, stderr = receiver.communicate()
            if sender.returncode or receiver.returncode:
                raise RuntimeError((sender_error + stderr + stdout).decode(errors='replace'))
        elapsed = time.perf_counter() - wall
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        match = re.search(rb'Transferred: sent (\d+), received (\d+) bytes', stderr)
        assert match is not None, 'SSH did not report its transfer byte counters'
        save({'stage': ('interrupted-transfer-' if partial else 'transfer-') + stage,
            'wall_seconds': elapsed,
            'local_cpu_seconds': time.process_time() - cpu + after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
            'ssh_sent_bytes': int(match[1]), 'ssh_received_bytes': int(match[2]),
            'ciphertext_payload_bytes': None if partial else payload_bytes,
            'metadata_payload_bytes': metadata_bytes, 'archives': 1 if partial else len(names),
            'remote_tar_exit_code': receiver.returncode})

    transfer('baseline', partial=True)
    invoke('reject', 'baseline')
    for stage in STAGES:
        transfer(stage)
        invoke('import', stage)
    invoke('retry', 'old-delete')
    summary = {'status': 'completed', 'messages': json.loads((args.local_root / 'baseline.json').read_text())['messages'],
        'measurements': rows, 'scope': 'Real SSH transfer to the supplied Linux host, separate isolated synthetic importer. Each transfer uses a new SSH connection. SSH client byte counters include SSH/TAR/metadata overhead, not IP-level packet capture. Transfer CPU is local parent plus child CPU; importer CPU/I/O/RSS are measured on Linux. Import includes cache open/close, archive validation, receipt-hash comparison and authenticated replay; post-import preservation assertions are outside its timer. No native capture or energy measurement. No production package or data directory is modified.'}
    args.output.write_text(json.dumps(summary, indent=2) + '\n')


if __name__ == '__main__':
    main()
