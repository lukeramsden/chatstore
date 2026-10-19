"""Small adapter for durable native/canonical handoff integration tests."""

import sqlite3
from contextlib import closing

from chatstore.adapters.base import Emit, Observation
from chatstore.canonical.records import record
from chatstore.identity.urn import derive_urn


class MirrorAdapter:
    source = 'messages'
    version = 'mirror-test/1'

    def __init__(self):
        self.fail = False
        self.calls = 0
        self.partial = False

    def discover(self, cfg):
        raise AssertionError('prepared capture must not discover expired native sources')

    def diagnose(self, cfg):
        raise AssertionError('prepared capture must not diagnose expired native sources')

    def snapshot(self, cfg, staging):
        raise AssertionError('prepared capture must not copy expired native sources')

    def extract(self, snap, scope, cfg, stats, checkpoints, *, full):
        self.calls += 1
        with closing(sqlite3.connect(snap.files['chat'].snapshot_path)) as reader:
            for row_id, text in reader.execute('SELECT id,body FROM message ORDER BY id'):
                urn = derive_urn('messages', scope, 'message', f'mirror-{row_id}')
                yield Emit(record('messages', urn, 'messages', scope, native_key=f'mirror-{row_id}',
                    chat_urn=None, chat_assignment='none', sender_urn=None, is_from_me=False,
                    transport='imessage', kind='message', sent_at=None, state='unknown',
                    text=text, has_attachments=False, part_count=1, source_flags={}),
                           Observation('message', [row_id], f'mirror-{row_id}', text))
                stats.bump('messages')
                if self.fail:
                    raise RuntimeError('injected extractor interruption')
        stats.scanned_tables.add('message')
        stats.checkpoints['rows'] = stats.counts.get('messages', 0)
        if self.partial:
            stats.error('native_decode', 'injected partial extraction')

