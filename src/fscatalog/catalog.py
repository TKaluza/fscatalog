"""High-level scan orchestration."""

from __future__ import annotations

import getpass
import json
import logging
import re
import time
import uuid
from collections.abc import Callable
from pathlib import Path

from fscatalog._version import __version__
from fscatalog.diskinfo import collect_disk_info
from fscatalog.hasher import hash_file
from fscatalog.models import FileEntry, FilePattern, ScanMeta
from fscatalog.scanner import scan_files
from fscatalog.storage import CatalogDB

log = logging.getLogger(__name__)


def _collect_extensions(patterns: list[FilePattern]) -> tuple[str, ...] | None:
    """Merge extensions from all patterns.  Returns None if no patterns (= scan all)."""
    if not patterns:
        return None
    exts: set[str] = set()
    for p in patterns:
        exts.update(p.extensions_lower())
    return tuple(sorted(exts))


def _match_patterns(
    filename: str, patterns: list[FilePattern], compiled: dict[str, re.Pattern[str]]
) -> tuple[str | None, str | None]:
    """Try each pattern against *filename*.  Return (name, groups_json) or (None, None)."""
    for p in patterns:
        m = compiled[p.name].search(filename)
        if m:
            groups = m.groupdict()
            return p.name, json.dumps(groups) if groups else None
    return None, None


def run_scan(
    root: str | Path,
    db: CatalogDB,
    *,
    patterns: list[FilePattern] | None = None,
    follow_symlinks: bool = False,
    one_file_system: bool = True,
    skip_hash: bool = False,
    progress_callback: Callable[[int], None] | None = None,
    batch_size: int = 5000,
) -> ScanMeta:
    """Execute a full scan and persist results.

    Parameters
    ----------
    root:
        Directory tree to scan.
    db:
        Open :class:`CatalogDB` to write into.
    patterns:
        Optional list of :class:`FilePattern` to match.  If omitted, all
        files are catalogued without pattern matching.
    follow_symlinks:
        Follow symbolic links during scan.
    one_file_system:
        Stay on one filesystem (default *True*).
    skip_hash:
        Skip xxhash computation (useful for quick metadata-only scans).
    progress_callback:
        Called with the running file count after each batch is inserted.
    batch_size:
        Number of file entries to accumulate before bulk-inserting.

    Returns
    -------
    ScanMeta
        Metadata object for the completed scan.
    """
    root = Path(root).resolve()
    patterns = patterns or []

    # Pre-compile regexes
    compiled: dict[str, re.Pattern[str]] = {
        p.name: re.compile(p.regex) for p in patterns
    }

    extensions = _collect_extensions(patterns)
    disk = collect_disk_info(root)
    scan_id = uuid.uuid4().hex[:16]
    scan_epoch = time.time()

    meta = ScanMeta(
        scan_id=scan_id,
        scan_epoch=scan_epoch,
        root_path=str(root),
        disk=disk,
        username=getpass.getuser(),
        library_version=__version__,
        follow_symlinks=follow_symlinks,
        patterns=tuple(patterns),
    )
    db.insert_scan(meta)

    batch: list[FileEntry] = []
    total_inserted = 0

    for raw in scan_files(
        root,
        extensions,
        follow_symlinks=follow_symlinks,
        one_file_system=one_file_system,
    ):
        # Pattern matching
        pname, pgroups = (None, None)
        if patterns:
            pname, pgroups = _match_patterns(raw.filename, patterns, compiled)
            # If patterns are given but none matched, still catalogue the file
            # (it matched by extension via fd).

        # Hashing
        xxh = ""
        if not skip_hash:
            try:
                xxh = hash_file(raw.absolute_path)
            except (PermissionError, OSError) as exc:
                log.debug("hash failed for %s: %s", raw.absolute_path, exc)
                xxh = "ERROR"

        entry = FileEntry(
            scan_id=scan_id,
            absolute_path=raw.absolute_path,
            filename=raw.filename,
            extension=raw.extension,
            xxhash=xxh,
            size_bytes=raw.size_bytes,
            mtime_epoch=raw.mtime_epoch,
            ctime_epoch=raw.ctime_epoch,
            is_symlink=raw.is_symlink,
            pattern_name=pname,
            pattern_groups=pgroups,
        )
        batch.append(entry)

        if len(batch) >= batch_size:
            db.insert_files(batch)
            total_inserted += len(batch)
            batch.clear()
            if progress_callback:
                progress_callback(total_inserted)

    # Flush remaining
    if batch:
        db.insert_files(batch)
        total_inserted += len(batch)
        if progress_callback:
            progress_callback(total_inserted)

    log.info("Scan %s complete: %d files catalogued", scan_id, total_inserted)
    return meta
