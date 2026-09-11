#!/usr/bin/env sh
# Bootstrap the chatstore CLI for the chatstore agent skill.
# Idempotent: safe to re-run. Installs nothing if chatstore is already on PATH
# and initialised. Never touches WhatsApp or Apple Messages databases.
set -eu

REPO="git+https://github.com/lukeramsden/chatstore"

log() { printf '%s\n' "$*" >&2; }

if [ "$(uname -s)" != "Darwin" ]; then
  log "chatstore ingests WhatsApp for macOS and Apple Messages; it only runs on macOS."
  exit 1
fi

if ! command -v chatstore >/dev/null 2>&1; then
  if command -v pipx >/dev/null 2>&1; then
    log "Installing chatstore with pipx..."
    pipx install "$REPO"
  elif command -v python3 >/dev/null 2>&1; then
    log "pipx not found; installing chatstore with pip --user..."
    python3 -m pip install --user "$REPO"
    USER_BIN="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))')"
    case ":$PATH:" in *":$USER_BIN:"*) ;; *) PATH="$USER_BIN:$PATH"; export PATH ;; esac
  else
    log "Need python3 (>= 3.11) or pipx. Install one and re-run."
    exit 1
  fi
fi

if ! command -v chatstore >/dev/null 2>&1; then
  log "chatstore installed but not on PATH. Add your pip/pipx bin directory to PATH and re-run."
  exit 1
fi

# `init` is a no-op if the data dir already exists.
chatstore init >/dev/null 2>&1 || chatstore init

# Prebuilt Apple Messages decoder (sha256-verified download; no Rust needed).
if ! chatstore --json helper status 2>/dev/null | grep -q '"found": *true'; then
  log "Installing the Apple Messages helper..."
  chatstore helper install
fi

log "Running chatstore doctor..."
# Exit code 3 = Full Disk Access missing; report but do not fail the bootstrap.
chatstore doctor || {
  rc=$?
  if [ "$rc" -eq 3 ]; then
    log "Grant Full Disk Access to your terminal (System Settings > Privacy & Security), then run: chatstore sync"
    exit 0
  fi
  exit "$rc"
}

log "chatstore is ready. First sync: chatstore sync"
