"""DuckDB storage backend for file catalog data."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path

import duckdb

from fscatalog.models import DiskInfo, DuplicateGroup, FileEntry, FilePattern, ScanMeta

log = logging.getLogger(__name__)

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS scans (
    scan_id         TEXT PRIMARY KEY,
    scan_epoch      DOUBLE  NOT NULL,
    root_path       TEXT    NOT NULL,
    follow_symlinks BOOLEAN NOT NULL DEFAULT FALSE,
    -- disk info
    disk_uuid       TEXT,
    disk_model      TEXT,
    disk_serial     TEXT,
    disk_device     TEXT,
    disk_label      TEXT,
    disk_fstype     TEXT,
    -- meta
    username        TEXT    NOT NULL,
    library_version TEXT    NOT NULL,
    patterns_json   TEXT    -- JSON array of pattern dicts
);

CREATE TABLE IF NOT EXISTS files (
    scan_id        TEXT    NOT NULL,
    absolute_path  TEXT    NOT NULL,
    filename       TEXT    NOT NULL,
    extension      TEXT    NOT NULL,
    xxhash         TEXT    NOT NULL,
    size_bytes     BIGINT  NOT NULL,
    mtime_epoch    DOUBLE  NOT NULL,
    ctime_epoch    DOUBLE  NOT NULL,
    is_symlink     BOOLEAN NOT NULL DEFAULT FALSE,
    pattern_name   TEXT,
    pattern_groups TEXT,   -- JSON dict of captured groups
    FOREIGN KEY (scan_id) REFERENCES scans(scan_id)
);

CREATE INDEX IF NOT EXISTS idx_files_xxhash ON files (xxhash);
CREATE INDEX IF NOT EXISTS idx_files_scan   ON files (scan_id);
CREATE INDEX IF NOT EXISTS idx_files_ext    ON files (extension);
"""


class CatalogDB:
    """Manage a DuckDB-backed file catalog.

    Parameters
    ----------
    db_path:
        Path to the DuckDB file.  Use ``":memory:"`` for in-memory.
    """

    def __init__(self, db_path: str | Path = "catalog.duckdb") -> None:
        self._path = str(db_path)
        self._con = duckdb.connect(self._path)
        self._con.execute("BEGIN")
        self._con.execute(_SCHEMA_SQL)
        self._con.execute("COMMIT")

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_scan(self, meta: ScanMeta) -> None:
        """Insert scan metadata."""
        patterns_json = json.dumps(
            [
                {
                    "name": p.name,
                    "description": p.description,
                    "regex": p.regex,
                    "extensions": list(p.extensions),
                }
                for p in meta.patterns
            ]
        )
        self._con.execute(
            """
            INSERT INTO scans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                meta.scan_id,
                meta.scan_epoch,
                meta.root_path,
                meta.follow_symlinks,
                meta.disk.uuid,
                meta.disk.model,
                meta.disk.serial,
                meta.disk.device,
                meta.disk.label,
                meta.disk.fstype,
                meta.username,
                meta.library_version,
                patterns_json,
            ],
        )

    def insert_files(self, entries: list[FileEntry], *, batch_size: int = 5000) -> int:
        """Bulk-insert file entries.  Returns count inserted."""
        if not entries:
            return 0

        total = 0
        for i in range(0, len(entries), batch_size):
            batch = entries[i : i + batch_size]
            rows = [
                (
                    e.scan_id,
                    e.absolute_path,
                    e.filename,
                    e.extension,
                    e.xxhash,
                    e.size_bytes,
                    e.mtime_epoch,
                    e.ctime_epoch,
                    e.is_symlink,
                    e.pattern_name,
                    e.pattern_groups,
                )
                for e in batch
            ]
            self._con.executemany(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            total += len(batch)
        return total

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def _row_to_entry(self, row: tuple) -> FileEntry:
        return FileEntry(
            scan_id=row[0],
            absolute_path=row[1],
            filename=row[2],
            extension=row[3],
            xxhash=row[4],
            size_bytes=row[5],
            mtime_epoch=row[6],
            ctime_epoch=row[7],
            is_symlink=row[8],
            pattern_name=row[9],
            pattern_groups=row[10],
        )

    def iter_files(
        self,
        *,
        scan_id: str | None = None,
        extension: str | None = None,
        pattern_name: str | None = None,
    ) -> Iterator[FileEntry]:
        """Iterate over stored file entries with optional filters."""
        clauses: list[str] = []
        params: list[object] = []
        if scan_id is not None:
            clauses.append("scan_id = ?")
            params.append(scan_id)
        if extension is not None:
            clauses.append("extension = ?")
            params.append(
                extension.lower() if not extension.startswith(".") else extension
            )
        if pattern_name is not None:
            clauses.append("pattern_name = ?")
            params.append(pattern_name)

        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM files{where} ORDER BY absolute_path"

        for row in self._con.execute(sql, params).fetchall():
            yield self._row_to_entry(row)

    def find_duplicates(
        self, *, scan_id: str | None = None, min_size: int = 1
    ) -> Iterator[DuplicateGroup]:
        """Yield groups of files that share the same xxhash (= identical content).

        Only groups with 2+ files are returned.
        """
        where_parts = ["size_bytes >= ?"]
        params: list[object] = [min_size]
        if scan_id is not None:
            where_parts.append("scan_id = ?")
            params.append(scan_id)

        where = " AND ".join(where_parts)
        sql = f"""
            SELECT xxhash, size_bytes
            FROM files
            WHERE {where}
            GROUP BY xxhash, size_bytes
            HAVING count(*) > 1
            ORDER BY size_bytes DESC
        """
        for hash_val, size in self._con.execute(sql, params).fetchall():
            detail_params: list[object] = [hash_val]
            scan_filter = ""
            if scan_id:
                scan_filter = " AND scan_id = ?"
                detail_params.append(scan_id)
            rows = self._con.execute(
                f"SELECT * FROM files WHERE xxhash = ?{scan_filter} ORDER BY absolute_path",
                detail_params,
            ).fetchall()
            entries = tuple(self._row_to_entry(r) for r in rows)
            yield DuplicateGroup(xxhash=hash_val, size_bytes=size, files=entries)

    def list_scans(self) -> list[ScanMeta]:
        """Return metadata for all scans."""
        rows = self._con.execute(
            "SELECT * FROM scans ORDER BY scan_epoch DESC"
        ).fetchall()
        result: list[ScanMeta] = []
        for r in rows:
            pats_raw = json.loads(r[12]) if r[12] else []
            patterns = tuple(
                FilePattern(
                    name=p["name"],
                    description=p["description"],
                    regex=p["regex"],
                    extensions=tuple(p["extensions"]),
                )
                for p in pats_raw
            )
            result.append(
                ScanMeta(
                    scan_id=r[0],
                    scan_epoch=r[1],
                    root_path=r[2],
                    follow_symlinks=r[3],
                    disk=DiskInfo(
                        uuid=r[4],
                        model=r[5],
                        serial=r[6],
                        device=r[7],
                        label=r[8],
                        fstype=r[9],
                    ),
                    username=r[10],
                    library_version=r[11],
                    patterns=patterns,
                )
            )
        return result

    def file_count(self, *, scan_id: str | None = None) -> int:
        """Return total number of catalogued files."""
        if scan_id:
            row = self._con.execute(
                "SELECT count(*) FROM files WHERE scan_id = ?", [scan_id]
            ).fetchone()
        else:
            row = self._con.execute("SELECT count(*) FROM files").fetchone()
        return row[0] if row else 0

    def execute(
        self, sql: str, params: list | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Run arbitrary SQL for advanced queries."""
        return self._con.execute(sql, params or [])

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> CatalogDB:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
