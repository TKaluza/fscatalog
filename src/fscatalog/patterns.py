"""Load FilePattern definitions from TOML files."""

from __future__ import annotations

import tomllib
from pathlib import Path

from fscatalog.models import FilePattern


def load_pattern(path: str | Path) -> FilePattern:
    """Load a single FilePattern from a TOML file.

    Expected format::

        [pattern]
        name = "whatsapp-video"
        description = "WhatsApp videos"
        regex = "VID-(?P<year>\\d{4})..."
        extensions = [".mp4", ".3gp"]
    """
    with open(path, "rb") as fh:
        data = tomllib.load(fh)

    p = data["pattern"]
    return FilePattern(
        name=p["name"],
        description=p["description"],
        regex=p["regex"],
        extensions=tuple(p["extensions"]),
    )


def load_patterns_from_dir(directory: str | Path) -> list[FilePattern]:
    """Load all ``*.toml`` pattern files from *directory*."""
    directory = Path(directory)
    patterns: list[FilePattern] = []
    for toml_path in sorted(directory.glob("*.toml")):
        patterns.append(load_pattern(toml_path))
    return patterns
