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
                "-Jpo",
                "PATH,TYPE,PKNAME,UUID,PARTUUID,MODEL,SERIAL,LABEL,PARTLABEL,FSTYPE",
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


def _clean(value: object) -> str | None:
    """Normalize lsblk string values."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parent_device_path(info: dict) -> str | None:
    """Return the parent block-device path if lsblk exposed one."""
    pkname = _clean(info.get("pkname"))
    if pkname is None:
        return None
    if pkname.startswith("/dev/"):
        return pkname
    return f"/dev/{pkname}"


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

    parent_info: dict | None = None
    parent_device = _parent_device_path(info)
    if parent_device is not None and parent_device != device:
        parent_info = _lsblk_json(parent_device)

    model = _clean(info.get("model")) or (
        _clean(parent_info.get("model")) if parent_info else None
    )
    serial = _clean(info.get("serial")) or (
        _clean(parent_info.get("serial")) if parent_info else None
    )
    label = _clean(info.get("label")) or _clean(info.get("partlabel"))

    return DiskInfo(
        uuid=_clean(info.get("uuid")) or _clean(info.get("partuuid")),
        model=model,
        serial=serial,
        device=device,
        label=label,
        fstype=_clean(info.get("fstype")),
    )
