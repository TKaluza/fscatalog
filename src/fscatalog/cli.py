"""Command-line interface for fscatalog."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from fscatalog._version import __version__
from fscatalog.catalog import run_scan
from fscatalog.models import FilePattern
from fscatalog.patterns import load_pattern, load_patterns_from_dir
from fscatalog.storage import CatalogDB


def _make_progress_callback():
    """Return a progress callback.  Uses tqdm if available, else simple print."""
    try:
        from tqdm import tqdm  # type: ignore[import-untyped]

        pbar = tqdm(unit=" files", desc="Cataloguing")

        def _cb(n: int) -> None:
            pbar.n = n
            pbar.refresh()

        _cb._pbar = pbar  # prevent GC  # type: ignore[attr-defined]
        return _cb
    except ImportError:
        _last = [0]

        def _cb(n: int) -> None:
            if n - _last[0] >= 1000 or n < _last[0]:
                print(f"\r  {n} files catalogued", end="", flush=True)
                _last[0] = n

        return _cb


# ── scan ──────────────────────────────────────────────────────────────


def cmd_scan(args: argparse.Namespace) -> None:
    root = Path(args.path).resolve()
    if not root.is_dir():
        print(f"Error: {root} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Load patterns
    patterns: list[FilePattern] = []
    for p in args.pattern or []:
        p = Path(p)
        if p.is_dir():
            patterns.extend(load_patterns_from_dir(p))
        else:
            patterns.append(load_pattern(p))

    db_path = args.db or "catalog.duckdb"
    cb = _make_progress_callback()

    t0 = time.perf_counter()
    with CatalogDB(db_path) as db:
        meta = run_scan(
            root,
            db,
            patterns=patterns,
            follow_symlinks=args.follow_symlinks,
            skip_hash=args.no_hash,
            progress_callback=cb,
        )
    elapsed = time.perf_counter() - t0

    # Close tqdm if present
    if hasattr(cb, "_pbar"):
        cb._pbar.close()  # type: ignore[attr-defined]
    else:
        print()  # newline after \r progress

    print(f"\nScan complete in {elapsed:.1f}s")
    print(f"  scan_id:  {meta.scan_id}")
    print(f"  root:     {meta.root_path}")
    print(
        f"  disk:     uuid={meta.disk.uuid}  model={meta.disk.model}  serial={meta.disk.serial}"
    )
    print(f"  db:       {db_path}")


# ── info ──────────────────────────────────────────────────────────────


def cmd_info(args: argparse.Namespace) -> None:
    with CatalogDB(args.db) as db:
        scans = db.list_scans()
        if not scans:
            print("No scans found in database.")
            return

        for s in scans:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s.scan_epoch))
            count = db.file_count(scan_id=s.scan_id)
            print(f"[{s.scan_id}]  {ts}  {s.root_path}  ({count} files)")
            print(
                f"    disk: uuid={s.disk.uuid}  model={s.disk.model}  serial={s.disk.serial}"
            )
            if s.patterns:
                names = ", ".join(p.name for p in s.patterns)
                print(f"    patterns: {names}")
            print()


# ── duplicates ────────────────────────────────────────────────────────


def cmd_dupes(args: argparse.Namespace) -> None:
    min_size = args.min_size or 1
    with CatalogDB(args.db) as db:
        groups = list(db.find_duplicates(scan_id=args.scan_id, min_size=min_size))
        if not groups:
            print("No duplicates found.")
            return

        total_waste = 0
        for g in groups:
            waste = g.size_bytes * (len(g.files) - 1)
            total_waste += waste
            print(
                f"xxhash={g.xxhash}  size={g.size_bytes:,}  copies={len(g.files)}  waste={waste:,}"
            )
            for f in g.files:
                sym = " [symlink]" if f.is_symlink else ""
                print(f"    {f.absolute_path}{sym}")
            print()

        print(f"Total duplicate groups: {len(groups)}")
        print(
            f"Total wasted space:     {total_waste:,} bytes ({total_waste / (1 << 20):.1f} MiB)"
        )


# ── query ─────────────────────────────────────────────────────────────


def cmd_query(args: argparse.Namespace) -> None:
    with CatalogDB(args.db) as db:
        result = db.execute(args.sql)
        rows = result.fetchall()
        if not rows:
            print("No results.")
            return
        # Print header from description
        if result.description:
            header = "\t".join(d[0] for d in result.description)
            print(header)
            print("-" * len(header))
        for row in rows:
            print("\t".join(str(v) for v in row))


# ── main ──────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="fscatalog",
        description="Fast file cataloging with fd, xxhash, and DuckDB",
    )
    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # scan
    p_scan = sub.add_parser("scan", help="Scan a directory tree")
    p_scan.add_argument("path", help="Root directory to scan")
    p_scan.add_argument("-d", "--db", default="catalog.duckdb", help="DuckDB file path")
    p_scan.add_argument(
        "-p",
        "--pattern",
        action="append",
        help="Pattern TOML file or directory of TOML files (repeatable)",
    )
    p_scan.add_argument(
        "-L", "--follow-symlinks", action="store_true", help="Follow symlinks"
    )
    p_scan.add_argument(
        "--no-hash", action="store_true", help="Skip xxhash (metadata-only scan)"
    )
    p_scan.set_defaults(func=cmd_scan)

    # info
    p_info = sub.add_parser("info", help="Show scan metadata")
    p_info.add_argument(
        "db", nargs="?", default="catalog.duckdb", help="DuckDB file path"
    )
    p_info.set_defaults(func=cmd_info)

    # dupes
    p_dupes = sub.add_parser("dupes", help="Find duplicate files")
    p_dupes.add_argument(
        "db", nargs="?", default="catalog.duckdb", help="DuckDB file path"
    )
    p_dupes.add_argument("-s", "--scan-id", help="Limit to a specific scan")
    p_dupes.add_argument(
        "--min-size", type=int, default=1, help="Minimum file size in bytes"
    )
    p_dupes.set_defaults(func=cmd_dupes)

    # query
    p_query = sub.add_parser("query", help="Run raw SQL against the catalog")
    p_query.add_argument("sql", help="SQL query string")
    p_query.add_argument(
        "-d", "--db", default="catalog.duckdb", help="DuckDB file path"
    )
    p_query.set_defaults(func=cmd_query)

    args = parser.parse_args(argv)

    level = logging.WARNING
    if args.verbose == 1:
        level = logging.INFO
    elif args.verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    args.func(args)


if __name__ == "__main__":
    main()
