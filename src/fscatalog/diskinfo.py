"""Collect disk / partition metadata for a given path (Linux only)."""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path

from fscatalog.models import DiskInfo

log = logging.getLogger(__name__)


def _resolve_device(path: str | Path) -> str | None:
    """Return the block device backing *path* using ``df``."""
    try:
        result = subprocess.run(
            ["df", "--output=source", str(path)],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = result.stdout.strip().splitlines()
        if len(lines) >= 2:
            return lines[-1].strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        log.debug("df failed for %s", path)
    return None


def _lsblk_json(device: str) -> dict | None:
    """Query lsblk in JSON mode for *device*."""
    try:
        result = subprocess.run(
            [
                "lsblk",
                "-Jno",
                "UUID,MODEL,SERIAL,LABEL,FSTYPE",
                device,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(result.stdout)
        devs = data.get("blockdevices", [])
        if devs:
            return devs[0]
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError):
        log.debug("lsblk failed for %s", device)
    return None


def collect_disk_info(path: str | Path) -> DiskInfo:
    """Best-effort collection of disk metadata for the filesystem holding *path*.

    Returns a :class:`DiskInfo` with as many fields populated as possible.
    On non-Linux systems or when tools are unavailable, fields will be ``None``.
    """
    device = _resolve_device(path)
    if device is None:
        return DiskInfo()

    info = _lsblk_json(device)
    if info is None:
        return DiskInfo(device=device)

    return DiskInfo(
        uuid=info.get("uuid") or None,
        model=(info.get("model") or "").strip() or None,
        serial=(info.get("serial") or "").strip() or None,
        device=device,
        label=info.get("label") or None,
        fstype=info.get("fstype") or None,
    )
