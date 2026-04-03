"""Frozen dataclasses for file catalog data."""

from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class FilePattern:
    """A pattern defining a category of files to search for.

    Can be loaded from TOML or constructed directly.
    """

    name: str
    description: str
    regex: str
    extensions: tuple[str, ...]

    def extensions_lower(self) -> tuple[str, ...]:
        """Return extensions normalised to lowercase with leading dot."""
        return tuple(
            ext if ext.startswith(".") else f".{ext}" for ext in self.extensions
        )


@dataclass(frozen=True, slots=True)
class DiskInfo:
    """Partition / disk metadata for a scanned path."""

    uuid: str | None = None
    model: str | None = None
    serial: str | None = None
    device: str | None = None
    label: str | None = None
    fstype: str | None = None


@dataclass(slots=True)
class ScanStats:
    """Counters collected while running a scan."""

    fd_candidates: int = 0
    files_seen: int = 0
    files_inserted: int = 0
    stat_failures: int = 0
    hash_failures: int = 0
    insert_batches: int = 0
    insert_failures: int = 0


@dataclass(frozen=True, slots=True)
class ScanMeta:
    """Metadata for a single scan run."""

    scan_id: str
    scan_epoch: float
    root_path: str
    disk: DiskInfo
    username: str
    library_version: str
    follow_symlinks: bool
    patterns: tuple[FilePattern, ...] = ()
    stats: ScanStats = field(default_factory=ScanStats)


@dataclass(frozen=True, slots=True)
class FileEntry:
    """A single catalogued file."""

    scan_id: str
    absolute_path: str
    filename: str
    extension: str
    xxhash: str
    size_bytes: int
    mtime_epoch: float
    ctime_epoch: float
    is_symlink: bool = False
    pattern_name: str | None = None
    pattern_groups: str | None = None  # JSON-encoded dict of captured groups

    def decoded_groups(self) -> dict[str, str] | None:
        """Return captured regex groups as a dict, or None."""
        if self.pattern_groups is None:
            return None
        return json.loads(self.pattern_groups)


@dataclass(frozen=True, slots=True)
class DuplicateGroup:
    """A group of files sharing the same xxhash."""

    xxhash: str
    size_bytes: int
    files: tuple[FileEntry, ...]
