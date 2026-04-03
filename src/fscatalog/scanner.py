"""Fast file discovery using fd + Python os.stat / os.lstat."""

from __future__ import annotations

from collections.abc import Callable
import errno
import logging
import os
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawFileInfo:
    """Raw stat result for a discovered file."""

    absolute_path: str
    filename: str
    extension: str  # includes leading dot, or "" if none
    size_bytes: int
    mtime_epoch: float
    ctime_epoch: float
    is_symlink: bool


def _build_fd_cmd(
    root: Path,
    extensions: tuple[str, ...] | None = None,
    *,
    follow_symlinks: bool = False,
    one_file_system: bool = True,
) -> list[str]:
    """Build the ``fd`` command list."""
    cmd = [
        "fd",
        "--type",
        "file",
        "--base-directory",
        str(root),
        "--color",
        "never",
        "--print0",
        "--no-ignore",  # don't skip gitignored files
        "--hidden",  # include dotfiles
    ]
    if follow_symlinks:
        cmd.append("--follow")
    if one_file_system:
        cmd.append("--one-file-system")
    if extensions:
        for ext in extensions:
            # strip leading dot for fd
            cmd.extend(["-e", ext.lstrip(".")])
    return cmd


def scan_files(
    root: str | Path,
    extensions: tuple[str, ...] | None = None,
    *,
    follow_symlinks: bool = False,
    one_file_system: bool = True,
    on_fd_start: Callable[[], None] | None = None,
    on_fd_done: Callable[[int], None] | None = None,
    on_stat_failure: Callable[[str, Exception], None] | None = None,
) -> Iterator[RawFileInfo]:
    """Discover files under *root* using ``fd`` and yield :class:`RawFileInfo`.

    Parameters
    ----------
    root:
        Directory to scan.
    extensions:
        Optional tuple of extensions to filter (e.g. ``(".jpg", ".png")``).
        If *None*, all files are returned.
    follow_symlinks:
        If *True*, fd follows symlinks and stat() resolves them.
    one_file_system:
        If *True* (default), fd stays on one filesystem.

    Yields
    ------
    RawFileInfo
        One entry per successfully stat'd file.
    """
    root = Path(root).resolve()
    if not root.is_dir():
        raise NotADirectoryError(f"scan root is not a directory: {root}")

    cmd = _build_fd_cmd(
        root,
        extensions,
        follow_symlinks=follow_symlinks,
        one_file_system=one_file_system,
    )

    log.debug("Running: %s", " ".join(cmd))
    if on_fd_start:
        on_fd_start()
    fd_start = time.perf_counter()
    try:
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace").strip() if exc.stderr else ""
        log.error(
            "fd failed for %s with exit code %s%s",
            root,
            exc.returncode,
            f": {stderr}" if stderr else "",
        )
        if on_fd_done:
            on_fd_done(0)
        raise RuntimeError(f"fd failed while scanning {root}") from exc
    fd_elapsed = time.perf_counter() - fd_start

    if completed.stderr:
        for line in completed.stderr.decode(errors="replace").splitlines():
            log.warning("fd stderr: %s", line)

    rel_paths = [rel for rel in completed.stdout.split(b"\0") if rel]
    candidates = len(rel_paths)
    if on_fd_done:
        on_fd_done(candidates)

    scanned = 0
    yielded = 0
    stat_failures = 0
    stat_elapsed = 0.0
    try:
        for rel in rel_paths:
            scanned += 1
            try:
                stat_start = time.perf_counter()
                rel_str = rel.decode(errors="surrogateescape")
                full_path = root / rel_str
                abs_str = str(full_path)

                # Detect symlink before potentially resolving
                is_symlink = full_path.is_symlink()

                # Use stat (follows symlinks) or lstat (does not)
                if follow_symlinks:
                    st = os.stat(abs_str)
                else:
                    st = os.lstat(abs_str)

                stat_elapsed += time.perf_counter() - stat_start

                name = full_path.name
                _, ext = os.path.splitext(name)

                yielded += 1
                yield RawFileInfo(
                    absolute_path=abs_str,
                    filename=name,
                    extension=ext.lower(),
                    size_bytes=st.st_size,
                    mtime_epoch=st.st_mtime,
                    ctime_epoch=st.st_ctime,
                    is_symlink=is_symlink,
                )
            except (FileNotFoundError, PermissionError, OSError) as exc:
                stat_failures += 1
                if on_stat_failure:
                    on_stat_failure(rel.decode(errors="surrogateescape"), exc)
                level = logging.DEBUG
                if isinstance(exc, OSError) and getattr(exc, "errno", None) not in {
                    errno.ENOENT,
                    errno.EACCES,
                }:
                    level = logging.WARNING
                log.log(level, "stat failed for %s: %s", rel, exc)
    finally:
        log.debug(
            "scan_files root=%s step=fd_run elapsed=%.3fs candidates=%d yielded=%d stat_failures=%d stat_time=%.3fs",
            root,
            fd_elapsed,
            scanned,
            yielded,
            stat_failures,
            stat_elapsed,
        )
