# fscatalog

Fast file cataloging with **fd**, **xxhash** (XXH3_64), and **DuckDB**.

Scans directory trees at high speed using [fd](https://github.com/sharkdp/fd) for
file discovery, computes content hashes with xxhash, and stores everything in a
DuckDB database for instant querying, deduplication, and change detection.

## Requirements

- Python ≥ 3.10
- [fd](https://github.com/sharkdp/fd) installed and on `PATH`
- [uv](https://docs.astral.sh/uv/) for project management

## Installation

```bash
# install as library
uv pip install .

# install with CLI progress bars (tqdm)
uv pip install ".[cli]"

# or in a project
uv add fscatalog
```

## CLI Usage

```bash
# Scan a directory (all files)
fscatalog scan /home/tim/Bilder

# Scan with pattern matching
fscatalog scan /home/tim/Bilder -p patterns/whatsapp-video.toml -p patterns/bsc-camera.toml

# Scan a whole directory of patterns
fscatalog scan /mnt/backup -p patterns/

# Quick metadata scan (skip hashing)
fscatalog scan /mnt/nas --no-hash

# Follow symlinks
fscatalog scan /home -L

# Custom database path
fscatalog scan /data -d /tmp/my_catalog.duckdb

# Show scan metadata
fscatalog info catalog.duckdb

# Find duplicate files
fscatalog dupes catalog.duckdb
fscatalog dupes catalog.duckdb --min-size 1048576  # only files ≥ 1 MiB

# Run raw SQL
fscatalog query "SELECT extension, count(*), sum(size_bytes) FROM files GROUP BY extension ORDER BY 3 DESC"

# Verbose logging
fscatalog -vv scan /home/tim
```

`-vv` enables debug logs with phase timings for pattern compilation, disk-info
collection, file discovery, hashing, and database inserts. If installed with
the `cli` extra, the scan command shows a spinner during `fd` discovery and a
tqdm bar for file processing.

## Library Usage

```python
from fscatalog import CatalogDB, FilePattern, run_scan

# Define patterns in code
patterns = [
    FilePattern(
        name="whatsapp-video",
        description="WhatsApp videos",
        regex=r"VID-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-WA(?P<sequence>\d+)",
        extensions=(".mp4", ".3gp"),
    ),
]

# Run a scan
with CatalogDB("my_catalog.duckdb") as db:
    meta = run_scan("/home/tim/Bilder", db, patterns=patterns)
    print(f"Scanned {db.file_count(scan_id=meta.scan_id)} files")

    # Find duplicates
    for group in db.find_duplicates():
        print(f"Hash {group.xxhash}: {len(group.files)} copies, {group.size_bytes:,} bytes each")
        for f in group.files:
            print(f"  {f.absolute_path}")

    # Iterate with filters
    for entry in db.iter_files(extension=".mp4", pattern_name="whatsapp-video"):
        groups = entry.decoded_groups()
        print(f"{entry.filename} -> {groups}")

    # Raw SQL via DuckDB
    result = db.execute("""
        SELECT extension, count(*) as cnt, sum(size_bytes) as total
        FROM files
        GROUP BY extension
        ORDER BY total DESC
        LIMIT 10
    """)
    for row in result.fetchall():
        print(row)
```

Typical post-scan deduplication workflow:

```python
from __future__ import annotations

from pathlib import Path

from fscatalog import CatalogDB, run_scan


def iter_duplicates_to_delete(
    db: CatalogDB,
    *,
    scan_id: str,
):
    """Yield (duplicate, keeper) for duplicate files.

    Strategy:
    - group by content hash (`find_duplicates`)
    - sort each group by oldest mtime first
    - keep the oldest file
    - delete the rest
    """
    for group in db.find_duplicates(scan_id=scan_id, min_size=1):
        # Oldest file wins. Add `absolute_path` as a stable tie-breaker.
        ordered = sorted(group.files, key=lambda f: (f.mtime_epoch, f.absolute_path))
        keeper = ordered[0]

        for duplicate in ordered[1:]:
            yield Path(duplicate.absolute_path), Path(keeper.absolute_path)


with CatalogDB("my_catalog.duckdb") as db:
    meta = run_scan("/srv/photos", db)

    for duplicate, keeper in iter_duplicates_to_delete(
        db,
        scan_id=meta.scan_id,
    ):
        print(f"KEEP {keeper}")
        print(f"DELETE {duplicate}")
        duplicate.unlink()
```

If you want a safer first pass, remove `duplicate.unlink()` and keep the `print(...)`.
If you prefer creation time sorting, replace `mtime_epoch` with `ctime_epoch`.

## Pattern TOML Format

```toml
[pattern]
name = "whatsapp-video"
description = "WhatsApp videos"
regex = "VID-(?P<year>\\d{4})(?P<month>\\d{2})(?P<day>\\d{2})-WA(?P<sequence>\\d+)"
extensions = [".mp4", ".3gp"]
```

## DuckDB Schema

**scans** — one row per scan run:
`scan_id`, `scan_epoch`, `root_path`, `follow_symlinks`, `disk_uuid`, `disk_model`,
`disk_serial`, `disk_device`, `disk_label`, `disk_fstype`, `username`, `library_version`,
`patterns_json`

**files** — one row per catalogued file:
`scan_id`, `absolute_path`, `filename`, `extension`, `xxhash`, `size_bytes`,
`mtime_epoch`, `ctime_epoch`, `is_symlink`, `pattern_name`, `pattern_groups`

Indexes on `xxhash`, `scan_id`, and `extension`.
