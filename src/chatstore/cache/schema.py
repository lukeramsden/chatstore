"""SQLite schema. A rebuildable projection of canonical records (specs/canonical-model.md)."""

SCHEMA_VERSION = 1

DDL = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA foreign_keys=ON",
    """CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)""",
    # Current view of every canonical entity, one row per URN.
    """CREATE TABLE IF NOT EXISTS records (
        urn TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        source TEXT,
        account_scope TEXT,
        revision_digest TEXT NOT NULL,
        observed_at INTEGER NOT NULL,
        sync_run TEXT,
        tombstoned INTEGER NOT NULL DEFAULT 0,
        record TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS records_kind ON records(kind, source, account_scope)",
    # Immutable observed revisions.
    """CREATE TABLE IF NOT EXISTS revisions (
        id INTEGER PRIMARY KEY,
        entity_urn TEXT NOT NULL,
        revision_digest TEXT NOT NULL,
        entity_kind TEXT NOT NULL,
        observed_at INTEGER NOT NULL,
        sync_run TEXT,
        supersedes TEXT,
        record TEXT NOT NULL,
        UNIQUE (entity_urn, revision_digest)
    )""",
    "CREATE INDEX IF NOT EXISTS revisions_observed ON revisions(entity_urn, observed_at)",
    # Query indexes (derived from records; rebuildable).
    """CREATE TABLE IF NOT EXISTS messages_idx (
        id INTEGER PRIMARY KEY,
        urn TEXT NOT NULL UNIQUE REFERENCES records(urn) ON DELETE CASCADE,
        chat_urn TEXT,
        sender_urn TEXT,
        utc_ms INTEGER,
        kind TEXT NOT NULL,
        transport TEXT NOT NULL,
        source TEXT NOT NULL,
        account_scope TEXT NOT NULL,
        has_attachments INTEGER NOT NULL,
        is_from_me INTEGER NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS messages_chat_time ON messages_idx(chat_urn, utc_ms, urn)",
    "CREATE INDEX IF NOT EXISTS messages_time ON messages_idx(utc_ms, urn)",
    "CREATE INDEX IF NOT EXISTS messages_sender ON messages_idx(sender_urn, utc_ms)",
    """CREATE TABLE IF NOT EXISTS events_idx (
        urn TEXT PRIMARY KEY REFERENCES records(urn) ON DELETE CASCADE,
        target_urn TEXT, kind TEXT NOT NULL, at_ms INTEGER, actor_urn TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS events_target ON events_idx(target_urn, at_ms)",
    """CREATE TABLE IF NOT EXISTS attachments_idx (
        urn TEXT PRIMARY KEY REFERENCES records(urn) ON DELETE CASCADE,
        message_urn TEXT, availability TEXT NOT NULL, blob_sha256 TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS attachments_message ON attachments_idx(message_urn)",
    "CREATE INDEX IF NOT EXISTS attachments_blob ON attachments_idx(blob_sha256)",
    """CREATE TABLE IF NOT EXISTS parts_idx (
        urn TEXT PRIMARY KEY REFERENCES records(urn) ON DELETE CASCADE,
        message_urn TEXT NOT NULL, idx INTEGER NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS parts_message ON parts_idx(message_urn, idx)",
    """CREATE TABLE IF NOT EXISTS memberships_idx (
        urn TEXT PRIMARY KEY REFERENCES records(urn) ON DELETE CASCADE,
        chat_urn TEXT NOT NULL, identity_urn TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS memberships_chat ON memberships_idx(chat_urn)",
    "CREATE INDEX IF NOT EXISTS memberships_identity ON memberships_idx(identity_urn)",
    """CREATE TABLE IF NOT EXISTS links_idx (
        urn TEXT PRIMARY KEY REFERENCES records(urn) ON DELETE CASCADE,
        person_urn TEXT NOT NULL, identity_urn TEXT NOT NULL, state TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS links_person ON links_idx(person_urn)",
    "CREATE INDEX IF NOT EXISTS links_identity ON links_idx(identity_urn)",
    """CREATE TABLE IF NOT EXISTS aliases_idx (
        urn TEXT PRIMARY KEY REFERENCES records(urn) ON DELETE CASCADE,
        old_urn TEXT NOT NULL, new_urn TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS aliases_old ON aliases_idx(old_urn)",
    "CREATE INDEX IF NOT EXISTS aliases_new ON aliases_idx(new_urn)",
    # Provenance.
    """CREATE TABLE IF NOT EXISTS source_observations (
        entity_urn TEXT NOT NULL,
        source TEXT NOT NULL,
        account_scope TEXT NOT NULL,
        native_table TEXT NOT NULL,
        native_row_ids TEXT NOT NULL,
        native_key TEXT,
        minted INTEGER NOT NULL,
        fingerprint TEXT NOT NULL,
        adapter_version TEXT NOT NULL,
        observed_at INTEGER NOT NULL,
        sync_run TEXT,
        present TEXT NOT NULL,
        PRIMARY KEY (entity_urn, source, account_scope, native_table)
    )""",
    "CREATE INDEX IF NOT EXISTS observations_native ON source_observations(source, account_scope, native_table, native_key)",
    # Sync checkpoints per source/scope.
    """CREATE TABLE IF NOT EXISTS checkpoints (
        source TEXT NOT NULL, account_scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
        PRIMARY KEY (source, account_scope, key)
    )""",
    # Conflicts needing resolution.
    """CREATE TABLE IF NOT EXISTS conflicts (
        id INTEGER PRIMARY KEY,
        kind TEXT NOT NULL,
        entity_urn TEXT,
        detail TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        resolved_at INTEGER,
        resolution TEXT
    )""",
    # Archive lineage (Phase 3).
    """CREATE TABLE IF NOT EXISTS imports (
        export_id TEXT PRIMARY KEY,
        export_set TEXT NOT NULL,
        kind TEXT NOT NULL,
        bucket_label TEXT,
        start_utc_ms INTEGER,
        end_utc_ms INTEGER,
        revision INTEGER NOT NULL,
        lineage TEXT NOT NULL,
        imported_at INTEGER NOT NULL,
        superseded_by TEXT,
        manifest TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS bucket_membership (
        export_id TEXT NOT NULL REFERENCES imports(export_id) ON DELETE CASCADE,
        urn TEXT NOT NULL,
        PRIMARY KEY (export_id, urn)
    )""",
    "CREATE INDEX IF NOT EXISTS bucket_membership_urn ON bucket_membership(urn)",
    # Full-text search over current message text. rowid == messages_idx.id / revisions.id.
    """CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
        text, tokenize = "unicode61 remove_diacritics 2 categories 'L* N* Co So Sk'"
    )""",
    """CREATE VIRTUAL TABLE IF NOT EXISTS revisions_fts USING fts5(
        text, tokenize = "unicode61 remove_diacritics 2 categories 'L* N* Co So Sk'"
    )""",
]
