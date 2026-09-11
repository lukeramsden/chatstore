# Security and privacy

## Threat model in one paragraph

Your message history is the asset. chatstore keeps it in a user-local data directory (mode
0700, files 0600) and, optionally, in encrypted archives you choose to copy elsewhere (for
example a private Git repository). Someone who can read the archives but not the password
should learn nothing beyond month labels, revision numbers and sizes. Someone who knows the
password can read everything; encryption authenticates against outsiders, not against them.

## Source access

- Sources are opened read-only (`mode=ro` URI) and copied with the SQLite online backup API into
  the data dir's `snapshots/`; adapters only ever read the snapshot. The tool never writes to
  `~/Library/Messages` or the WhatsApp container.
- macOS needs Full Disk Access for the terminal/process. `chatstore doctor` reports exit 3 with a
  hint if it is missing. Nothing is escalated.
- Only allow-listed fields are copied from source rows (`specs/schemas/allowlist.json`). Media
  encryption keys, receipt blobs and other opaque fields are never stored.

## Archive format

- WinZip AES-256 (AE-2) via `pyzipper`, one `payload` member holding a TAR. Legacy ZipCrypto is
  never written and never accepted.
- The KDF is PBKDF2-SHA1 with 1000 iterations — weak against guessing. Use
  `chatstore archive password suggest` (256 bits) and a password manager. A stronger envelope
  would be a new, versioned format, not a tweak to this one.
- Import is fail-closed: password/auth failure, any checksum mismatch, extra ZIP members,
  unsafe TAR member names (absolute, `..`, backslashes, drive letters), non-regular members,
  declared-size or compression-ratio limits exceeded → exit 7 and the cache is untouched.
- Filenames reveal the calendar month and revision (documented, accepted). The 8-hex prefix is
  derived from the export set, not the account.

## Passwords

Order of lookup: `CHATSTORE_PASSWORD_FILE` (must be a regular file, mode 0600) → macOS keychain
(`security -i` with the password on stdin, never argv) → interactive prompt on a tty. Passwords
never appear in argv, environment *values*, logs, exceptions or JSON output. `password suggest`
refuses to run under `--json`.

## Archives in Git

You may commit archives to a private repository; never commit the password beside them. Git
history is immutable: rotating a password does not un-share archives already pushed. Repository
size grows with each bucket revision; text-only exports keep this manageable.

## Retention and purge

All observed revisions are retained by default (edits, retractions, and historical text stay
searchable with `--history`). `chatstore purge --entity|--source --confirm` hard-deletes from the
local cache only; already exported archives still contain the data and the command says so.

## Agents

The skill in `skills/chatstore/SKILL.md` treats message text as untrusted data, cites URNs and
revision digests instead of pasting whole chats, and requires explicit approval for export,
import, linking, mapping, and purge.

## Helper binary downloads

`chatstore helper install` fetches `chatstore-messages-decoder-macos-<arch>` from
`https://github.com/lukeramsden/chatstore/releases/download/v<version>/` over HTTPS, together with
the release's `SHA256SUMS`. The binary is written only if its SHA-256 matches; a mismatch, missing
checksum or oversized download (>64 MiB) aborts with nothing installed. The file goes to
`<data-dir>/bin/` (mode 0700 directory, 0755 file) and `config.helper_path` is pointed at it. The
checksums are produced by the release workflow from the binaries it built on GitHub-hosted macOS
runners; if you prefer, build locally with `scripts/build-helper.sh` instead — nothing requires
the download.
