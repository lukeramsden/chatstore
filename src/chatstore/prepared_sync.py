"""Crash-safe handoff from a prepared native mirror to canonical sync.

Selection and media preparation are separate concerns. This bridge currently runs
adapter extraction over the retained mirror, with selective Messages coverage when eligible.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

from .adapters import (
    media_watch,
    monitor,
    native_mirror,
    page_index,
    pages,
    source_view,
    wal,
    wal_image,
)
from .adapters.base import SourceAdapter
from .adapters.messages.adapter import MessagesAdapter, find_helper
from .adapters.native_mirror import MirrorUnavailable, NativeMirror, PreparedCapture
from .adapters.snapshot import Snapshot, SnapshotFile
from .adapters.whatsapp import dependencies as whatsapp_dependencies
from .adapters.whatsapp.adapter import WhatsAppAdapter
from .cache import Cache
from .cache.extraction import code_signature
from .config import Config, Identity
from .paths import DataDir
from .sync import SyncOutcome


def policy_signature(adapter: SourceAdapter, cfg: Config) -> str:
    from . import native_collector

    helper_digest = ''
    if type(adapter) is MessagesAdapter:
        helper = find_helper(cfg)
        if helper is None:
            raise MirrorUnavailable('prepared Messages capture requires its decoder')
        with helper.open('rb') as binary:
            helper_digest = hashlib.file_digest(binary, 'sha256').hexdigest()
    projection = (media_watch, monitor, native_mirror, page_index, pages, source_view, wal, wal_image, native_collector, whatsapp_dependencies)
    policy = [code_signature(adapter, cfg, extra_modules=projection), helper_digest, os.getcwd()]
    return hashlib.sha256(json.dumps(policy).encode()).hexdigest()


@dataclass(frozen=True)
class PreparedSync:
    mirror: NativeMirror
    capture: PreparedCapture
    file_name: str

    @property
    def run_urn(self) -> str:
        return uuid.uuid5(uuid.NAMESPACE_URL, 'chatstore-native-capture/' + self.capture.token).urn

    @property
    def binding(self) -> str:
        return hashlib.sha256(json.dumps({'capture': asdict(self.capture), 'file_name': self.file_name},
            sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()

    @property
    def observed_at_ms(self) -> int:
        value = datetime.fromisoformat(self.capture.observed_at)
        if value.tzinfo is None:
            raise MirrorUnavailable('prepared observation time must include a timezone')
        delta = value - datetime(1970, 1, 1, tzinfo=UTC)
        return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000

    def manifest(self) -> dict[str, Any]:
        return {'prepared_native': {'binding': self.binding, 'file_name': self.file_name,
                                    'capture': json.loads(json.dumps(asdict(self.capture)))}}

    def snapshot(self) -> Snapshot:
        prepared = self

        class BorrowedSnapshot(Snapshot):
            def manifest(self) -> dict[str, Any]:
                return prepared.manifest()

            def cleanup(self) -> None:
                pass  # The pending mirror must survive both success and failure.

        path = self.mirror.path
        at = self.observed_at_ms
        probe = None
        alias_pairs = None
        if 'whatsapp_aliases' in self.capture.context:
            from .adapters.whatsapp.aliases import read
            if self.capture.context['whatsapp_aliases'] != {'format': 1}:
                raise MirrorUnavailable('unsupported prepared WhatsApp alias evidence format')
            alias_pairs = read(self.mirror.conn, self.capture.token)
        if 'messages_media' in self.capture.context:
            from .adapters.prepared_media import read_media
            if self.capture.context['messages_media'] != {'format': 1}:
                raise MirrorUnavailable('unsupported prepared media evidence format')
            probe = partial(read_media, self.mirror.conn, self.capture.token)
        return BorrowedSnapshot(path.parent, {self.file_name: SnapshotFile(self.file_name,
            str(path), path, path.stat().st_size, '', 0, False, at, at)}, attachment_probe=probe,
            whatsapp_alias_pairs=alias_pairs)

    def validate(self, adapter: SourceAdapter, cfg: Config, scope: str) -> None:
        if self.mirror.pending() != self.capture:
            raise MirrorUnavailable('canonical sync requires the exact pending native capture')
        if (self.capture.namespace != f'{adapter.source}/{scope}'
                or self.capture.policy_tag != policy_signature(adapter, cfg)):
            raise MirrorUnavailable('prepared native capture source/scope or extraction policy mismatch')
        _ = self.observed_at_ms

    def receipt(self, cache: Cache, source: str, scope: str) -> str | None:
        row = cache.conn.execute('SELECT * FROM native_capture_receipts WHERE token=?', (self.capture.token,)).fetchone()
        if row is not None:
            if (row['binding'], row['source'], row['account_scope'], row['sync_run']) != (
                    self.binding, source, scope, self.run_urn):
                raise MirrorUnavailable('canonical native capture receipt binding conflict')
            completed = cache.revision_record(self.run_urn, row['receipt'].split(':', 2)[2])
            if (completed is None or completed.get('status') != 'complete'
                    or completed.get('snapshot') != self.manifest()):
                raise MirrorUnavailable('canonical native capture receipt has no retained complete run')
            return row['receipt']
        # A cold archive import preserves the complete sync-run record, even
        # though this local receipt projection is not itself an archive record.
        run = cache.get(self.run_urn)
        if run is not None and run.get('status') == 'complete':
            if (run.get('source'), run.get('account_scope'), run.get('snapshot')) != (source, scope, self.manifest()):
                raise MirrorUnavailable('restored native capture run binding conflict')
            with cache.write():
                return self.commit_receipt(cache, source, scope, run)
        return None

    def check_parent(self, cache: Cache, source: str, scope: str) -> None:
        if self.capture.parent is None:
            return
        parent_run = uuid.uuid5(uuid.NAMESPACE_URL, 'chatstore-native-capture/' + self.capture.parent).urn
        run = cache.get(parent_run)
        if (run is None or run.get('status') != 'complete' or run.get('source') != source
                or run.get('account_scope') != scope):
            raise MirrorUnavailable('prepared capture parent has no complete canonical receipt')
        native = (run.get('snapshot') or {}).get('prepared_native', {})
        if native.get('capture', {}).get('token') != self.capture.parent:
            raise MirrorUnavailable('prepared capture parent binding is missing')

    def commit_receipt(self, cache: Cache, source: str, scope: str, run: dict[str, Any]) -> str:
        if not cache.conn.in_transaction or run.get('status') != 'complete' or run['urn'] != self.run_urn:
            raise MirrorUnavailable('native capture receipt requires the complete canonical transaction')
        receipt = 'native-capture:' + self.binding + ':' + run['revision_digest']
        existing = cache.conn.execute('SELECT binding,receipt FROM native_capture_receipts WHERE token=?', (self.capture.token,)).fetchone()
        if existing is not None and (existing['binding'], existing['receipt']) != (self.binding, receipt):
            raise MirrorUnavailable('native capture receipt already binds another result')
        cache.conn.execute('INSERT OR IGNORE INTO native_capture_receipts VALUES(?,?,?,?,?,?)',
            (self.capture.token, self.binding, source, scope, self.run_urn, receipt))
        cache.set_checkpoint(source, scope, 'native_capture', self.manifest()['prepared_native'])
        return receipt


@contextmanager
def _capture_lock(path: Path):
    # A separate advisory lock permits adapter-owned index creation on the mirror
    # while serializing canonical attempts across independent collector processes.
    import fcntl

    mirror_path = path.resolve()
    fd = os.open(mirror_path.with_name(mirror_path.name + '.capture-lock'), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _messages_coverage(adapter, cfg, cache, prepared, scope, full):
    from .adapters.messages.dependencies import RoutingUnavailable, route_changes
    from .cache.capture_coverage import CaptureCoverage
    from .cache.projection_epoch import read_source
    from .cache.unit_coverage import CoverageUnavailable, UnitWithdrawal
    from .identity.urn import messages_inferred_chat_key

    capture = prepared.capture
    args = {'source': adapter.source, 'scope': scope, 'token': capture.token,
            'binding': prepared.binding, 'parent': capture.parent}
    with cache.write():
        try:
            state = CaptureCoverage.resume(cache, token=capture.token, binding=prepared.binding)
            if full:
                return CaptureCoverage.restart_full(cache, **args), None
            if state is not None and state.full:
                return state, None
            if capture.baseline:
                return CaptureCoverage.begin(cache, **args, full=True), None
            prior = cache.get_checkpoint(adapter.source, scope, 'adapter') or {}
            if 'rowids-file-v1' not in (prior.get('helper', {}).get('capabilities') or []):
                return CaptureCoverage.restart_full(cache, **args), None
            media_ids = ()
            if capture.context.get('messages_media') == {'format': 1}:
                # Older prepared points may have frozen bytes but no selective
                # file coverage proof. Reconcile them without assuming clean media.
                exists = prepared.mirror.conn.execute("SELECT 1 FROM sqlite_schema WHERE name='cs_native_media_coverage' AND type='table'").fetchone()
                coverage = prepared.mirror.conn.execute('SELECT point,full FROM cs_native_media_coverage WHERE id=1').fetchone() if exists else None
                if coverage is None or coverage['point'] != capture.token or coverage['full']:
                    return CaptureCoverage.restart_full(cache, **args), None
                media_ids = tuple(row[0] for row in prepared.mirror.conn.execute('SELECT row_id FROM cs_native_media_changes'))
            elif prepared.mirror.conn.execute("SELECT 1 FROM attachment WHERE filename>'' LIMIT 1").fetchone():
                return CaptureCoverage.restart_full(cache, **args), None
            selection = route_changes(prepared.mirror, capture, media_attachment_ids=media_ids)
            for key in selection.old_unit_keys | selection.new_unit_keys:
                if not key.isascii():
                    return CaptureCoverage.restart_full(cache, **args), None
                try:
                    uuid.UUID(key)
                except ValueError:
                    return CaptureCoverage.restart_full(cache, **args), None
            unique_guid = False
            for index in prepared.mirror.conn.execute('PRAGMA index_list(message)'):
                if index['unique'] and not index['partial']:
                    columns = [row['name'] for row in prepared.mirror.conn.execute('PRAGMA index_info("' + index['name'].replace('"', '""') + '")')]
                    unique_guid = unique_guid or columns == ['guid']
            if not unique_guid:
                return CaptureCoverage.restart_full(cache, **args), None
            if state is None:
                proof = UnitWithdrawal.take(cache, adapter.source, scope, selection.old_unit_keys,
                    expected_epoch=read_source(cache.conn, adapter.source, scope)).with_headers(cache,
                    ['handle', 'chat', 'chat_handle_join', 'me'],
                    inferred_keys=[messages_inferred_chat_key(key) for key in selection.inferred_chat_inputs])
                state = CaptureCoverage.begin(cache, **args, full=False, withdrawal=proof)
            return state, set(selection.rowids)
        except (CoverageUnavailable, RoutingUnavailable):
            return CaptureCoverage.restart_full(cache, **args), None


def _whatsapp_coverage(adapter, cache, prepared, scope, full):
    from .cache.capture_coverage import CaptureCoverage
    from .cache.projection_epoch import read_source
    from .cache.unit_coverage import CoverageUnavailable, UnitWithdrawal

    capture = prepared.capture
    args = {'source': adapter.source, 'scope': scope, 'token': capture.token,
            'binding': prepared.binding, 'parent': capture.parent}
    with cache.write():
        try:
            state = CaptureCoverage.resume(cache, token=capture.token, binding=prepared.binding)
            if full:
                return CaptureCoverage.restart_full(cache, **args), None
            if state is not None and state.full:
                return state, None
            if capture.baseline:
                return CaptureCoverage.begin(cache, **args, full=True), None
            if capture.context.get('whatsapp_aliases') != {'format': 1}:
                return CaptureCoverage.restart_full(cache, **args), None
            # No file-backed unit can be re-sighted from a SQL-only point.
            if prepared.mirror.conn.execute("SELECT 1 FROM ZWAMEDIAITEM INDEXED BY cs_wa_media_file WHERE ZMEDIALOCALPATH>'' LIMIT 1").fetchone():
                return CaptureCoverage.restart_full(cache, **args), None
            selection = whatsapp_dependencies.route_changes(prepared.mirror, capture)
            if state is None:
                proof = UnitWithdrawal.take(cache, adapter.source, scope, selection.old_unit_keys,
                    expected_epoch=read_source(cache.conn, adapter.source, scope)).with_headers(cache,
                    ['ZWACHATSESSION', 'ZWAGROUPMEMBER', 'ZWAGROUPMEMBERSCHANGE', 'jid', 'me', 'lid_phone_pair'])
                state = CaptureCoverage.begin(cache, **args, full=False, withdrawal=proof)
            return state, set(selection.rowids)
        except (CoverageUnavailable, whatsapp_dependencies.RoutingUnavailable):
            return CaptureCoverage.restart_full(cache, **args), None


def run_prepared_sync(adapter: SourceAdapter, dd: DataDir, cfg: Config, ident: Identity,
                      cache: Cache, prepared: PreparedSync, *, full: bool = False) -> SyncOutcome:
    with _capture_lock(prepared.mirror.path):
        return _run_prepared_sync(adapter, dd, cfg, ident, cache, prepared, full=full)


def _run_prepared_sync(adapter: SourceAdapter, dd: DataDir, cfg: Config, ident: Identity,
                       cache: Cache, prepared: PreparedSync, *, full: bool = False) -> SyncOutcome:
    from .sync import run_sync

    scope = ident.scope_for(adapter.source)
    # Receipt lookup precedes policy checks: a completed capture can be
    # acknowledged after a code upgrade without reinterpreting its old rows.
    receipt = prepared.receipt(cache, adapter.source, scope)
    if receipt is not None:
        state = prepared.mirror.pending() or prepared.mirror.continuation()
        if state != prepared.capture:
            raise MirrorUnavailable('canonical receipt does not match current native mirror point')
        prepared.mirror.acknowledge(prepared.capture.token, receipt)
        run = cache.revision_record(prepared.run_urn, receipt.split(':', 2)[2])
        if run is None:
            raise MirrorUnavailable('canonical native capture run is missing')
        return SyncOutcome(run, 0)
    prepared.validate(adapter, cfg, scope)
    if type(adapter) is WhatsAppAdapter and prepared.capture.context.get('whatsapp_aliases') != {'format': 1}:
        raise MirrorUnavailable('prepared WhatsApp capture requires frozen auxiliary alias evidence')
    prepared.check_parent(cache, adapter.source, scope)
    capture_state, selected = None, None
    if type(adapter) is MessagesAdapter:
        capture_state, selected = _messages_coverage(adapter, cfg, cache, prepared, scope, full)
    elif type(adapter) is WhatsAppAdapter:
        capture_state, selected = _whatsapp_coverage(adapter, cache, prepared, scope, full)
    outcome = run_sync(adapter, dd, cfg, ident, cache, full=full, prepared=prepared,
                       capture_state=capture_state, selected_message_rowids=selected)
    if outcome.exit_code == 0:
        receipt = prepared.receipt(cache, adapter.source, scope)
        if receipt is None:
            raise MirrorUnavailable('complete prepared sync has no committed canonical receipt')
        prepared.mirror.acknowledge(prepared.capture.token, receipt)
    return outcome
