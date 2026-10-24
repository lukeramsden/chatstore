#!/usr/bin/env python3
"""Verify WhatsApp file-only collection through the CLI in an isolated fixture.

Run with PYTHONPATH=src:. .venv/bin/python scripts/verify_whatsapp_media_watch.py.
The configured 900-second sleep is replaced so this check completes immediately.
"""

import hashlib
import io
import json
import os
import sqlite3
import tempfile
from contextlib import closing, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from chatstore.cache import Cache
from chatstore.cli.app import main
from chatstore.paths import DataDir
from tests.fixtures.synthetic import build_whatsapp


def main_check():
    with tempfile.TemporaryDirectory(prefix="chatstore-wa-cli-") as directory:
        root = build_whatsapp(Path(directory).resolve() / "source")
        dd = DataDir(Path(directory).resolve() / "data")
        with (
            closing(sqlite3.connect(root / "ChatStorage.sqlite")) as writer,
            patch.dict(os.environ, {"CHATSTORE_DATA_DIR": str(dd.root)}),
        ):
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("PRAGMA user_version=1")
            writer.commit()
            output = io.StringIO()
            with redirect_stdout(output):
                assert main(["--json", "init", "--whatsapp-path", str(root)]) == 0
            output = io.StringIO()
            waits = []
            changed = b"WhatsApp CLI file-only edit"

            def between(seconds):
                waits.append(seconds)
                (root / "Message/Media/x/pic.jpg").write_bytes(changed)

            with patch("chatstore.cli.app.time.sleep", between), redirect_stdout(output):
                code = main(
                    [
                        "--json",
                        "sync",
                        "--source",
                        "whatsapp",
                        "--watch-interval",
                        "900",
                        "--max-polls",
                        "2",
                    ]
                )
            polls = [json.loads(line) for line in output.getvalue().splitlines()]
            assert code == 0 and len(polls) == 2 and len(waits) == 1
            assert all(row["ok"] for row in polls)
            runs = [row["data"][0] for row in polls]
            assert runs[0]["snapshot"]["prepared_native"]["capture"]["context"][
                "whatsapp_media"
            ] == {"format": 1}
            assert (
                runs[1]["counts"]["messages"] == 1
                and runs[1]["counts"]["prepared_headers_reused"] == 1
            )
            assert runs[1]["snapshot"]["prepared_native"]["capture"]["rows_changed"] == 0
            with closing(Cache(dd.cache)) as cache:
                assert (
                    next(cache.iter_records("attachments"))["blob_sha256"]
                    == hashlib.sha256(changed).hexdigest()
                )
            print(
                json.dumps(
                    {
                        "polls": 2,
                        "cadence_seconds": 900,
                        "wait_replaced_for_check": True,
                        "native_media": True,
                        "second_capture_sql_rows_changed": 0,
                        "second_capture_messages": 1,
                        "second_capture_headers_reused": True,
                        "latest_attachment_digest_verified": True,
                    }
                )
            )


if __name__ == "__main__":
    main_check()
