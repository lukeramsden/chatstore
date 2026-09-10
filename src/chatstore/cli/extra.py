"""Registration of archive / identity / scope / conflicts / purge commands (Phases 3 and 4)."""

from __future__ import annotations

import argparse
from typing import Any


def register_extra(sub: Any) -> None:
    """Extended commands are registered here as they land."""
    _ = argparse
