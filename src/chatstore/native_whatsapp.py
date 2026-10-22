"""WhatsApp collector dispatch with guarded, frozen optional alias sources."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime

from .adapters.base import check_schema
from .adapters.native_mirror import NativeMirror
from .adapters.pages import PageUnavailable
from .adapters.source_view import WalSource
from .adapters.wal import WalPages, WalUnavailable
from .adapters.whatsapp import aliases, auxiliary
from .adapters.whatsapp.adapter import REQUIRED, WhatsAppAdapter
from .cache import Cache
from .config import Config, Identity
from .paths import DataDir
from .prepared_sync import PreparedSync, _capture_lock, _run_prepared_sync, policy_signature
from .sync import SyncOutcome

POLICY_MODULES = (auxiliary,)


class MediaCaptureRequired(Exception):
    pass


def collect(adapter: WhatsAppAdapter, dd: DataDir, cfg: Config, ident: Identity,
            cache: Cache, *, full: bool = False) -> SyncOutcome | None:
    namespace = f'whatsapp/{ident.scope_for(adapter.source)}'
    directory = dd.root / 'native'
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / (hashlib.sha256(namespace.encode()).hexdigest() + '.sqlite3')
    with _capture_lock(path), NativeMirror(path) as mirror:
        pending = mirror.pending()
        if pending is not None:
            return _run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, pending, 'ChatStorage'), full=full)
        previous = mirror.continuation()
        policy = policy_signature(adapter, cfg)
        source_path = adapter.discover(cfg)['ChatStorage']
        for continuation in (True, False):
            after = previous.cursor if continuation and previous and previous.policy_tag == policy else None
            try:
                with WalSource(source_path) as source, source.view(after) as view:
                    if check_schema(view.connection, REQUIRED):
                        return None
                    baseline = previous is None or after is None or previous.schema_version != view.schema_version
                    if baseline and view.connection.execute("SELECT 1 FROM ZWAMEDIAITEM WHERE ZMEDIALOCALPATH>'' LIMIT 1").fetchone():
                        return None
                    with auxiliary.AuxiliaryViews(cfg.source_root('whatsapp')) as optional:
                        context = {'collector': 'whatsapp-native-v1', 'whatsapp_aliases': {'format': 1},
                                   'whatsapp_auxiliary': optional.evidence}
                        def validate(conn: sqlite3.Connection, baseline: bool = baseline) -> None:
                            if not baseline and conn.execute("SELECT 1 FROM ZWAMEDIAITEM INDEXED BY cs_wa_media_file WHERE ZMEDIALOCALPATH>'' LIMIT 1").fetchone():
                                raise MediaCaptureRequired
                            aliases.prepare(conn, sources=optional.sources)
                            optional.validate()
                        observed_at = datetime.now(UTC).isoformat()
                        if previous is None:
                            pending = mirror.initialize(view, REQUIRED, namespace=namespace, policy_tag=policy,
                                observed_at=observed_at, context=context, validate_rows=validate)
                        elif after is None or previous.schema_version != view.schema_version:
                            pending = mirror.rebase(view, REQUIRED, expected_token=previous.token, namespace=namespace,
                                policy_tag=policy, observed_at=observed_at, context=context, validate_rows=validate)
                        else:
                            with WalPages(source_path.with_name(source_path.name + '-wal'), view.changes) as pages:
                                pending = mirror.prepare(view, pages, expected_token=previous.token, namespace=namespace,
                                    policy_tag=policy, observed_at=observed_at, context=context, validate_rows=validate)
                break
            except MediaCaptureRequired:
                return None
            except (WalUnavailable, PageUnavailable):
                if not continuation or after is None:
                    return None
        assert pending is not None
        return _run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, pending, 'ChatStorage'), full=full)
