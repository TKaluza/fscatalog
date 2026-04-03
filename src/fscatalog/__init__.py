"""fscatalog — fast file cataloging with fd, xxhash, and DuckDB."""

from fscatalog._version import __version__
from fscatalog.catalog import run_scan
from fscatalog.hasher import hash_file
from fscatalog.models import (
    DiskInfo,
    DuplicateGroup,
    FileEntry,
    FilePattern,
    ScanMeta,
    ScanStats,
)
from fscatalog.patterns import load_pattern, load_patterns_from_dir
from fscatalog.scanner import RawFileInfo, scan_files
from fscatalog.storage import CatalogDB

__all__ = [
    "__version__",
    "CatalogDB",
    "DiskInfo",
    "DuplicateGroup",
    "FileEntry",
    "FilePattern",
    "RawFileInfo",
    "ScanMeta",
    "ScanStats",
    "hash_file",
    "load_pattern",
    "load_patterns_from_dir",
    "run_scan",
    "scan_files",
]
