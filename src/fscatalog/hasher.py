"""Fast file hashing with xxhash (XXH3_64)."""

from __future__ import annotations

import xxhash

# 1 MiB read buffer — good balance between syscall overhead and memory
_CHUNK_SIZE = 1 << 20  # 1 048 576


def hash_file(path: str, *, chunk_size: int = _CHUNK_SIZE) -> str:
    """Return the XXH3_64 hex digest of the file at *path*.

    Streams the file in chunks so arbitrarily large files are fine.
    """
    h = xxhash.xxh3_64()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
