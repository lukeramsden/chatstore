"""Download the prebuilt `chatstore-messages-decoder` helper from a GitHub release.

The helper is a Rust binary that decodes Apple Messages `attributedBody` blobs. A `pip install`
ships only the Python package, so this module fetches the binary matching the installed chatstore
version, verifies it against the release's SHA256SUMS, and installs it into the data dir.
"""

from __future__ import annotations

import hashlib
import os
import platform
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from .paths import DataDir

REPO = "lukeramsden/chatstore"
RELEASES_URL = f"https://github.com/{REPO}/releases"
HELPER_NAME = "chatstore-messages-decoder"
SUMS_NAME = "SHA256SUMS"
MAX_HELPER_BYTES = 64 * 1024 * 1024
TIMEOUT_S = 60


class HelperInstallError(Exception):
    pass


@dataclass
class Installed:
    path: Path
    tag: str
    asset: str
    sha256: str


def package_version() -> str | None:
    try:
        return version("chatstore")
    except PackageNotFoundError:
        return None


def release_tag(explicit: str | None = None) -> str:
    """Tag to download from: explicit (`v0.1.0` or `0.1.0`), else the installed package version."""
    if explicit:
        return explicit if explicit.startswith("v") else f"v{explicit}"
    v = package_version()
    if not v or v == "0.0.0":
        raise HelperInstallError("cannot determine the chatstore version; pass --tag vX.Y.Z")
    return f"v{v}"


def asset_name(system: str | None = None, machine: str | None = None) -> str:
    system = (system or platform.system()).lower()
    machine = (machine or platform.machine()).lower()
    if system != "darwin":
        raise HelperInstallError(f"no prebuilt helper for {system}; build it with scripts/build-helper.sh")
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "x86_64", "amd64": "x86_64"}.get(machine)
    if arch is None:
        raise HelperInstallError(f"no prebuilt helper for macOS/{machine}; build it with scripts/build-helper.sh")
    return f"{HELPER_NAME}-macos-{arch}"


def asset_url(tag: str, name: str, base: str = RELEASES_URL) -> str:
    return f"{base}/download/{tag}/{name}"


def install_dir(dd: DataDir) -> Path:
    return dd.root / "bin"


def _fetch(url: str, limit: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": f"chatstore/{package_version() or 'dev'}"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            data = r.read(limit + 1)
    except urllib.error.HTTPError as e:
        raise HelperInstallError(f"{url}: HTTP {e.code}") from e
    except (urllib.error.URLError, OSError) as e:
        raise HelperInstallError(f"{url}: {e}") from e
    if len(data) > limit:
        raise HelperInstallError(f"{url}: larger than {limit} bytes")
    return data


def parse_sums(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and len(parts[0]) == 64:
            out[parts[1].lstrip("*")] = parts[0].lower()
    return out


def install(dd: DataDir, *, tag: str | None = None, base_url: str = RELEASES_URL) -> Installed:
    """Download, verify and install the helper. Returns the installed path. Fails closed on any mismatch."""
    t = release_tag(tag)
    name = asset_name()
    sums = parse_sums(_fetch(asset_url(t, SUMS_NAME, base_url), 64 * 1024).decode("utf-8", "replace"))
    expected = sums.get(name)
    if not expected:
        raise HelperInstallError(f"release {t} has no checksum for {name}; see {base_url}/tag/{t}")
    data = _fetch(asset_url(t, name, base_url), MAX_HELPER_BYTES)
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        raise HelperInstallError(f"{name}: sha256 mismatch (expected {expected[:12]}…, got {actual[:12]}…); not installed")
    target_dir = install_dir(dd)
    target_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(target_dir, 0o700)
    dst = target_dir / HELPER_NAME
    fd, tmp = tempfile.mkstemp(prefix=".helper-", dir=str(target_dir))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.chmod(tmp, 0o755)
        os.replace(tmp, dst)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return Installed(dst, t, name, actual)
