#!/usr/bin/env bash
# Builds the Rust messages-decoder helper. Uses the toolchain pinned in rust-toolchain.toml.
set -euo pipefail
cd "$(dirname "$0")/../helpers/messages-decoder"
if [[ "$(uname)" == "Darwin" ]] && ! xcrun --find clang >/dev/null 2>&1; then
  if [[ -d /Library/Developer/CommandLineTools ]]; then
    export DEVELOPER_DIR=/Library/Developer/CommandLineTools
    echo "note: using Command Line Tools (xcodebuild unavailable)" >&2
  fi
fi
cargo build --release "$@"
echo "built: $(pwd)/target/release/chatstore-messages-decoder"
