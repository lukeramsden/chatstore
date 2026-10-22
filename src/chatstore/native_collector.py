"""Durable Messages projection dispatch for ordinary collector polls."""

from __future__ import annotations

import hashlib
import sqlite3
import sys
from datetime import UTC, datetime
from functools import partial
from typing import Any

from . import native_whatsapp
from .adapters.base import SourceAdapter, check_schema
from .adapters.media_watch import PreparedMediaWatch
from .adapters.messages.adapter import REQUIRED, MessagesAdapter, find_helper
from .adapters.native_mirror import NativeMirror
from .adapters.pages import PageUnavailable
from .adapters.source_view import WalSource
from .adapters.wal import WalPages, WalUnavailable
from .adapters.whatsapp.adapter import WhatsAppAdapter
from .adapters.whatsapp.auxiliary import AuxiliarySession
from .adapters.whatsapp.media_watch import PreparedWhatsAppMediaWatch
from .cache import Cache
from .config import Config, Identity
from .paths import DataDir
from .prepared_sync import PreparedSync, _capture_lock, _run_prepared_sync, policy_signature
from .sync import SyncOutcome

POLICY_MODULES = (native_whatsapp,)


class MediaCaptureRequired(Exception):
    """A candidate contains file paths and must use ordinary media capture."""


def _validate_text_only(conn: sqlite3.Connection) -> None:
    # The baseline adapter installs this private index before acknowledgement.
    if conn.execute("SELECT 1 FROM attachment INDEXED BY cs_route_media_file WHERE filename>'' LIMIT 1").fetchone():
        raise MediaCaptureRequired


def collect_native(adapter: SourceAdapter, dd: DataDir, cfg: Config, ident: Identity,
                   cache: Cache, *, full: bool = False, media_watch: PreparedMediaWatch | PreparedWhatsAppMediaWatch | None = None,
                   auxiliary_session: AuxiliarySession | None = None) -> SyncOutcome | None:
    """Return None only when ordinary snapshot capture is required.

    A pending point is always handled before opening the source. Errors after
    prepare never authorize fallback to another capture or advance its cursor.
    """
    if sys.platform != 'darwin':
        return None
    if type(adapter) is WhatsAppAdapter:
        if media_watch is not None and not isinstance(media_watch, PreparedWhatsAppMediaWatch):
            raise ValueError('WhatsApp capture requires its own media watcher')
        return native_whatsapp.collect(adapter, dd, cfg, ident, cache, full=full,
                                       auxiliary_session=auxiliary_session, media_watch=media_watch)
    if type(adapter) is not MessagesAdapter:
        return None
    if media_watch is not None and not isinstance(media_watch, PreparedMediaWatch):
        raise ValueError('Messages capture requires its own media watcher')
    scope = ident.scope_for(adapter.source)
    namespace = f'{adapter.source}/{scope}'
    directory = dd.root / 'native'
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    name = hashlib.sha256(namespace.encode()).hexdigest() + '.sqlite3'
    with _capture_lock(directory / name), NativeMirror(directory / name) as mirror:
        pending = mirror.pending()
        if pending is not None:
            return _run_prepared_sync(adapter, dd, cfg, ident, cache,
                                      PreparedSync(mirror, pending, 'chat'), full=full)
        previous = mirror.continuation()
        if find_helper(cfg) is None:
            return None
        policy = policy_signature(adapter, cfg)
        path = adapter.discover(cfg)['chat']
        observed_at = datetime.now(UTC).isoformat()
        # A rejected continuation may still permit a pinned fresh baseline.
        # No prepared point exists at this stage, so fallback cannot lose one.
        for continuation in (True, False):
            after = previous.cursor if continuation and previous and previous.policy_tag == policy and not previous.context.get('native_rebase_required') else None
            try:
                with WalSource(path) as source, source.view(after) as view:
                    if check_schema(view.connection, REQUIRED):
                        return None
                    baseline = previous is None or after is None or previous.schema_version != view.schema_version
                    # A full source eligibility scan is limited to full baselines.
                    # Deltas validate the indexed candidate before its commit.
                    if baseline and media_watch is None and view.connection.execute("SELECT 1 FROM attachment WHERE filename>'' LIMIT 1").fetchone():
                        return None
                    tables = list(REQUIRED)
                    if view.connection.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='chat_recoverable_message_join'").fetchone():
                        tables.append('chat_recoverable_message_join')
                    context: dict[str, Any] = {'collector': 'messages-native-v1'}
                    validator = _validate_text_only if media_watch is None and not baseline else None
                    if media_watch is not None:
                        context['messages_media'] = {'format': 1}
                        validator = partial(media_watch.prepare, full=baseline)
                    if previous is None:
                        pending = mirror.initialize(view, tables, namespace=namespace, policy_tag=policy,
                            observed_at=observed_at, context=context, validate_rows=validator)
                    elif after is None or previous.schema_version != view.schema_version:
                        pending = mirror.rebase(view, tables, expected_token=previous.token,
                            namespace=namespace, policy_tag=policy, observed_at=observed_at, context=context, validate_rows=validator)
                    else:
                        with WalPages(path.with_name(path.name + '-wal'), view.changes) as pages:
                            pending = mirror.prepare(view, pages, expected_token=previous.token,
                                namespace=namespace, policy_tag=policy, observed_at=observed_at, context=context,
                                validate_rows=validator)
                break
            except MediaCaptureRequired:
                return None
            except (WalUnavailable, PageUnavailable):
                if not continuation or after is None:
                    return None
        assert pending is not None
        return _run_prepared_sync(adapter, dd, cfg, ident, cache,
                                  PreparedSync(mirror, pending, 'chat'), full=full)
