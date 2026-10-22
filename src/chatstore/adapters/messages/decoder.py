"""Bounded selection and checked JSONL transport for the read-only Rust decoder."""

from __future__ import annotations

import json
import subprocess
import tempfile
from collections.abc import Generator, Iterable, Sequence
from contextlib import ExitStack, closing
from itertools import islice
from pathlib import Path
from typing import Any

from ..base import IncompatibleSource


def decode(helper: Path, database: Path, rowids: Sequence[int] | None = None) -> Generator[dict[str, Any], None, None]:
    if rowids is not None and (len(rowids) > 10000 or any(type(row) is not int or not -(1 << 63) <= row < (1 << 63) for row in rowids)):
        raise IncompatibleSource('decoder selection must contain at most 10000 signed SQLite row IDs')
    selected = None if rowids is None else sorted(set(rowids))
    with ExitStack() as stack:
        errors = stack.enter_context(tempfile.TemporaryFile(mode='w+t', encoding='utf-8'))
        args = [str(helper), str(database)]
        if selected is not None:
            ids_file = stack.enter_context(tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=database.parent))
            json.dump(selected, ids_file)
            ids_file.flush()
            args.extend(['--rowids-file', ids_file.name])
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=errors, text=True, encoding='utf-8', bufsize=1 << 16)
        assert proc.stdout is not None
        try:
            header = json.loads(proc.stdout.readline() or '{}')
            if not header:
                rc = proc.wait()
                errors.seek(0)
                raise IncompatibleSource(f'helper exited {rc} without a header: {errors.read(200).strip()}')
            if not isinstance(header, dict):
                raise IncompatibleSource('helper header must be a JSON object')
            if header.get('type') != 'header' or header.get('format') != 'chatstore-messages-decoder/1':
                raise IncompatibleSource(f'helper produced unexpected header format {header.get("format")!r}')
            if selected is not None and (header.get('selection') != {'kind': 'rowids', 'requested': len(selected)}
                    or 'rowids-file-v1' not in header.get('capabilities', [])):
                raise IncompatibleSource('helper does not confirm exact row-ID selection support')
            yield header
            footer = None
            count = 0
            allowed = set(selected) if selected is not None else None
            for line in proc.stdout:
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise IncompatibleSource('helper JSONL record must be an object')
                if footer is not None:
                    raise IncompatibleSource('helper emitted output after its footer')
                if item.get('type') == 'footer':
                    footer = item
                elif item.get('type') == 'message':
                    if allowed is not None and (type(item.get('rowid')) is not int or item['rowid'] not in allowed):
                        raise IncompatibleSource('helper emitted an unselected native row')
                    count += 1
                    yield item
                else:
                    raise IncompatibleSource('helper emitted an unknown JSONL record')
            rc = proc.wait()
            errors.seek(0)
            detail = errors.read(200)
            if rc:
                raise IncompatibleSource(f'helper exited {rc}: {detail.strip()}')
            if (footer is None or type(footer.get('messages')) is not int or footer['messages'] != count
                    or type(footer.get('errors')) is not int or footer['errors'] < 0):
                raise IncompatibleSource('helper output truncated or footer count mismatched')
            yield footer
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise IncompatibleSource('helper emitted invalid UTF-8 JSONL') from exc
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            proc.stdout.close()


def decode_batches(helper: Path, database: Path, rowids: Iterable[int], *, requested: int) -> Generator[dict[str, Any], None, None]:
    """Stream one logical decode in caller-supplied date/rowid order.

    Each child owns at most 10,000 IDs. Only a verified final aggregate footer
    completes the logical stream; an interrupted later child fails the capture.
    """
    if type(requested) is not int or requested < 0:
        raise IncompatibleSource('decoder selection count must be a nonnegative integer')
    rows = iter(rowids)
    policy = None
    totals = {'messages': 0, 'errors': 0, 'query_fullscan_steps': 0, 'query_vm_steps': 0}
    consumed = batches = 0
    while True:
        batch = list(islice(rows, 10000))
        if not batch and batches:
            break
        consumed += len(batch)
        if consumed > requested:
            raise IncompatibleSource('decoder batch input exceeds its declared selection')
        with closing(decode(helper, database, batch)) as decoded:
            header = next(decoded)
            current = {key: value for key, value in header.items() if key != 'selection'}
            if policy is None:
                policy = current
                yield header | {'selection': {'kind': 'rowids', 'requested': requested}}
            elif current != policy:
                raise IncompatibleSource('decoder policy header changed between selection batches')
            for item in decoded:
                if item['type'] == 'footer':
                    for key in totals:
                        value = item.get(key, 0)
                        if type(value) is not int or value < 0:
                            raise IncompatibleSource('helper footer counter must be a nonnegative integer')
                        totals[key] += value
                else:
                    yield item
        batches += 1
        if not batch:
            break
    if consumed != requested:
        raise IncompatibleSource('decoder batch input is missing declared selected IDs')
    yield {'type': 'footer', **totals, 'batches': batches}
