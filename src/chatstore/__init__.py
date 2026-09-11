"""chatstore: local-first ingestion, search, and encrypted archiving of personal messages."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("chatstore")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0+unknown"
