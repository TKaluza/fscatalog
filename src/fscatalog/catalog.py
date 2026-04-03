"""High-level scan orchestration."""

from __future__ import annotations

import getpass
import errno
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
from fscatalog.models import FileEntry, FilePattern, ScanMeta, ScanStats
from fscatalog.scanner import scan_files
from fscatalog.storage import CatalogDB

log = logging.getLogger(__name__)


def _log_timing(
    step: str, start: float, *, scan_id: str | None = None, **details: object
) -> None:
    """Emit a debug log line with a measured duration."""
    elapsed = time.perf_counter() - start
    parts = [f"step={step}", f"elapsed={elapsed:.3f}s"]
    if scan_id is not None:
        parts.insert(0, f"scan_id={scan_id}")
    for key, value in details.items():
        parts.append(f"{key}={value}")
    log.debug(" ".join(parts))


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
    on_fd_start: Callable[[], None] | None = None,
    on_fd_done: Callable[[int], None] | None = None,
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
        Called with the running file count after each processed file.
    on_fd_start:
        Called right before the ``fd`` subprocess starts.
    on_fd_done:
        Called once ``fd`` finishes and the candidate count is known.
    batch_size:
        Number of file entries to accumulate before bulk-inserting.

    Returns
    -------
    ScanMeta
        Metadata object for the completed scan.
    """
    root = Path(root).resolve()
    patterns = patterns or []
    stats = ScanStats()

    def _record_stat_failure(_path: str, _exc: Exception) -> None:
        stats.stat_failures += 1

    # Pre-compile regexes
    regex_start = time.perf_counter()
    compiled: dict[str, re.Pattern[str]] = {
        p.name: re.compile(p.regex) for p in patterns
    }
    if patterns:
        _log_timing("compile_patterns", regex_start, pattern_count=len(patterns))

    disk_start = time.perf_counter()
    extensions = _collect_extensions(patterns)
    disk = collect_disk_info(root)
    scan_id = uuid.uuid4().hex[:16]
    scan_epoch = time.time()
    _log_timing("collect_disk_info", disk_start, scan_id=scan_id, root=root)

    meta_start = time.perf_counter()
    meta = ScanMeta(
        scan_id=scan_id,
        scan_epoch=scan_epoch,
        root_path=str(root),
        disk=disk,
        username=getpass.getuser(),
        library_version=__version__,
        follow_symlinks=follow_symlinks,
        patterns=tuple(patterns),
        stats=stats,
    )
    _log_timing(
        "build_scan_meta",
        meta_start,
        scan_id=scan_id,
        pattern_count=len(patterns),
    )

    txn_start = time.perf_counter()
    db.begin()
    _log_timing("begin_transaction", txn_start, scan_id=scan_id)

    insert_scan_start = time.perf_counter()
    try:
        db.insert_scan(meta)
        _log_timing("insert_scan_meta", insert_scan_start, scan_id=scan_id)

        batch: list[FileEntry] = []
        total_inserted = 0
        insert_time = 0.0
        hash_time = 0.0
        pattern_time = 0.0
        entry_time = 0.0
        batch_count = 0

        def _record_fd_done(candidates: int) -> None:
            stats.fd_candidates = candidates
            if on_fd_done:
                on_fd_done(candidates)

        scan_start = time.perf_counter()
        for raw in scan_files(
            root,
            extensions,
            follow_symlinks=follow_symlinks,
            one_file_system=one_file_system,
            on_fd_start=on_fd_start,
            on_fd_done=_record_fd_done,
            on_stat_failure=_record_stat_failure,
        ):
            stats.files_seen += 1
            # Pattern matching
            pname, pgroups = (None, None)
            if patterns:
                pattern_start = time.perf_counter()
                pname, pgroups = _match_patterns(raw.filename, patterns, compiled)
                pattern_time += time.perf_counter() - pattern_start
                # If patterns are given but none matched, still catalogue the file
                # (it matched by extension via fd).

            # Hashing
            xxh = ""
            if not skip_hash:
                try:
                    hash_start = time.perf_counter()
                    xxh = hash_file(raw.absolute_path)
                    hash_time += time.perf_counter() - hash_start
                except FileNotFoundError as exc:
                    stats.hash_failures += 1
                    log.debug("hash failed for %s: %s", raw.absolute_path, exc)
                    xxh = "ERROR"
                except PermissionError as exc:
                    stats.hash_failures += 1
                    log.debug("hash failed for %s: %s", raw.absolute_path, exc)
                    xxh = "ERROR"
                except OSError as exc:
                    stats.hash_failures += 1
                    if getattr(exc, "errno", None) in {
                        errno.EIO,
                        errno.ESTALE,
                        errno.ENXIO,
                    }:
                        log.warning("hash failed for %s: %s", raw.absolute_path, exc)
                    else:
                        log.debug("hash failed for %s: %s", raw.absolute_path, exc)
                    xxh = "ERROR"

            entry_start = time.perf_counter()
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
            entry_time += time.perf_counter() - entry_start
            batch.append(entry)
            total_inserted += 1
            stats.files_inserted += 1
            if progress_callback:
                progress_callback(total_inserted)

            if len(batch) >= batch_size:
                insert_start = time.perf_counter()
                try:
                    db.insert_files(batch)
                except Exception:
                    stats.insert_failures += 1
                    log.exception(
                        "scan_id=%s failed to insert batch of %d files after %d inserted",
                        scan_id,
                        len(batch),
                        total_inserted,
                    )
                    raise
                insert_time += time.perf_counter() - insert_start
                batch_count += 1
                stats.insert_batches += 1
                batch.clear()

        # Flush remaining
        if batch:
            insert_start = time.perf_counter()
            try:
                db.insert_files(batch)
            except Exception:
                stats.insert_failures += 1
                log.exception(
                    "scan_id=%s failed to insert final batch of %d files after %d inserted",
                    scan_id,
                    len(batch),
                    total_inserted,
                )
                raise
            insert_time += time.perf_counter() - insert_start
            batch_count += 1
            stats.insert_batches += 1
        commit_start = time.perf_counter()
        db.commit()
        _log_timing(
            "commit_transaction",
            commit_start,
            scan_id=scan_id,
            files=total_inserted,
            batches=batch_count,
        )
    except Exception:
        db.rollback()
        raise

    scan_elapsed = time.perf_counter() - scan_start
    failure_summary = (
        f"stat_failures={stats.stat_failures} "
        f"hash_failures={stats.hash_failures} "
        f"insert_failures={stats.insert_failures}"
    )
    log.debug(
        "scan_id=%s step=process_files elapsed=%.3fs files=%d batches=%d pattern_time=%.3fs hash_time=%.3fs entry_time=%.3fs %s",
        scan_id,
        scan_elapsed,
        total_inserted,
        batch_count,
        pattern_time,
        hash_time,
        entry_time,
        failure_summary,
    )
    log.debug(
        "scan_id=%s step=insert_batches elapsed=%.3fs batches=%d files=%d %s",
        scan_id,
        insert_time,
        batch_count,
        total_inserted,
        failure_summary,
    )
    end_level = logging.INFO
    if stats.stat_failures or stats.hash_failures or stats.insert_failures:
        end_level = logging.WARNING
    log.log(
        end_level,
        "Scan %s complete: %d files catalogued (%s)",
        scan_id,
        total_inserted,
        failure_summary,
    )
    return meta
