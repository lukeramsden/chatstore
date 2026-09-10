"""Archive password handling. Passwords never appear on argv, in env values, logs or JSON.

Sources, in order: `CHATSTORE_PASSWORD_FILE` (path to a 0600 file), OS keychain (macOS
`security`), interactive prompt (tty only).
"""

from __future__ import annotations

import base64
import getpass
import os
import platform
import secrets
import stat
import subprocess
import sys
from pathlib import Path

KEYCHAIN_SERVICE = "chatstore-archive"


class PasswordUnavailable(Exception):
    pass


def suggest() -> str:
    """256-bit random password, base32 without padding, grouped for readability."""
    raw = base64.b32encode(secrets.token_bytes(32)).decode().rstrip("=").lower()
    return "-".join(raw[i:i + 8] for i in range(0, len(raw), 8))


def keychain_account(data_dir: Path) -> str:
    return f"data-dir:{data_dir.resolve()}"


def _from_file() -> str | None:
    p = os.environ.get("CHATSTORE_PASSWORD_FILE")
    if not p:
        return None
    path = Path(p).expanduser()
    st = path.stat()
    if stat.S_ISREG(st.st_mode) is False:
        raise PasswordUnavailable("CHATSTORE_PASSWORD_FILE is not a regular file")
    if os.name == "posix" and st.st_mode & 0o077:
        raise PasswordUnavailable("CHATSTORE_PASSWORD_FILE must not be readable by group/others (chmod 600)")
    pw = path.read_text(encoding="utf-8").rstrip("\r\n")
    if not pw:
        raise PasswordUnavailable("CHATSTORE_PASSWORD_FILE is empty")
    return pw


def keychain_available() -> bool:
    return platform.system() == "Darwin"


def keychain_get(data_dir: Path) -> str | None:
    if not keychain_available():
        return None
    r = subprocess.run(["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", keychain_account(data_dir), "-w"],
                       capture_output=True, text=True, check=False)
    if r.returncode != 0:
        return None
    pw = r.stdout.rstrip("\n")
    return pw or None


def keychain_set(data_dir: Path, password: str) -> bool:
    if not keychain_available():
        return False
    # `security -i` reads commands from stdin, so the password never appears in the process list.
    quoted = '"' + password.replace("\\", "\\\\").replace('"', '\\"') + '"'
    cmd = f'add-generic-password -U -s {KEYCHAIN_SERVICE} -a "{keychain_account(data_dir)}" -w {quoted}\n'
    r = subprocess.run(["security", "-i"], input=cmd, capture_output=True, text=True, check=False)
    return r.returncode == 0


def keychain_delete(data_dir: Path) -> bool:
    if not keychain_available():
        return False
    r = subprocess.run(["security", "delete-generic-password", "-s", KEYCHAIN_SERVICE, "-a", keychain_account(data_dir)],
                       capture_output=True, text=True, check=False)
    return r.returncode == 0


def obtain(data_dir: Path, *, confirm: bool = False, allow_prompt: bool = True) -> tuple[str, str]:
    """Return (password, origin). origin in {file, keychain, prompt}."""
    pw = _from_file()
    if pw:
        return pw, "file"
    pw = keychain_get(data_dir)
    if pw:
        return pw, "keychain"
    if allow_prompt and sys.stdin.isatty():
        pw = getpass.getpass("archive password: ")
        if confirm:
            again = getpass.getpass("confirm password: ")
            if again != pw:
                raise PasswordUnavailable("passwords did not match")
        if not pw:
            raise PasswordUnavailable("empty password")
        return pw, "prompt"
    raise PasswordUnavailable(
        "no archive password available: run `chatstore archive password set`, set CHATSTORE_PASSWORD_FILE, or use a terminal")
