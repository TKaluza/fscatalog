# fscatalog
Fast file cataloging tool using fd for discovery, xxh3_64 for content hashing, and DuckDB for storage/querying. Scans directory trees, collects metadata (size, timestamps, disk UUID/serial), matches filenames against configurable patterns (TOML), and detects duplicates. Importable Python library with CLI. Build on fd.
