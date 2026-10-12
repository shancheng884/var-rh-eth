from __future__ import annotations

import gzip
import hashlib
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path


ORDER_METRICS_ROTATE_BYTES = 512 * 1024 * 1024
ORDER_METRICS_COMPRESS_AGE = timedelta(hours=24)
ORDER_METRICS_MIN_FREE_BYTES = int(3.5 * 1024**3)
_SEGMENT_RE = re.compile(r"\.segment-(\d{8}T\d{12}Z)\.jsonl$")


def segment_paths(path: Path) -> list[Path]:
    return sorted(path.parent.glob(f"{path.name}.segment-*.jsonl"))


def seal_order_metrics_file(
    path: Path,
    *,
    now: datetime | None = None,
) -> Path | None:
    """Atomically move the active log to a unique immutable segment."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    observed = now or datetime.now(timezone.utc)
    observed = observed.astimezone(timezone.utc)
    stamp_at = observed
    while True:
        stamp = stamp_at.strftime("%Y%m%dT%H%M%S%fZ")
        segment = path.with_name(f"{path.name}.segment-{stamp}.jsonl")
        if not segment.exists():
            break
        stamp_at += timedelta(microseconds=1)
    os.replace(path, segment)
    path.touch()
    return segment


def append_order_metrics_line(
    path: Path,
    line: str,
    *,
    rotate_bytes: int = ORDER_METRICS_ROTATE_BYTES,
    now: datetime | None = None,
) -> Path | None:
    """Append one complete row, rotating the active file before it crosses a boundary."""
    observed = now or datetime.now(timezone.utc)
    observed = observed.astimezone(timezone.utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    rotated: Path | None = None

    if path.exists() and path.stat().st_size:
        modified_day = datetime.fromtimestamp(
            path.stat().st_mtime, tz=timezone.utc
        ).date()
        crosses_size = path.stat().st_size + len(line.encode("utf-8")) > rotate_bytes
        crosses_day = modified_day < observed.date()
        if crosses_size or crosses_day:
            rotated = seal_order_metrics_file(path, now=observed)

    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
        handle.flush()
    return rotated


def _compress_verified(source_path: Path) -> Path:
    target_path = Path(str(source_path) + ".gz")
    temporary_path = Path(str(target_path) + ".tmp")
    if target_path.exists() or temporary_path.exists():
        raise FileExistsError(f"archive already exists: {target_path.name}")

    source_digest = hashlib.sha256()
    try:
        with source_path.open("rb") as source, gzip.open(
            temporary_path, "wb", compresslevel=6
        ) as destination:
            while chunk := source.read(1024 * 1024):
                source_digest.update(chunk)
                destination.write(chunk)

        archive_digest = hashlib.sha256()
        with gzip.open(temporary_path, "rb") as archived:
            while chunk := archived.read(1024 * 1024):
                archive_digest.update(chunk)
        if archive_digest.digest() != source_digest.digest():
            raise OSError("order metrics archive verification failed")

        os.replace(temporary_path, target_path)
        source_path.unlink()
        return target_path
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def compress_closed_order_metrics(
    path: Path,
    *,
    now: datetime | None = None,
    minimum_free_bytes: int = ORDER_METRICS_MIN_FREE_BYTES,
) -> list[Path]:
    """Compress sealed segments after a grace period, keeping originals on failure."""
    observed = now or datetime.now(timezone.utc)
    observed = observed.astimezone(timezone.utc)
    if not path.parent.exists():
        return []
    if shutil.disk_usage(path.parent).free < minimum_free_bytes:
        return []

    archives: list[Path] = []
    for source_path in segment_paths(path):
        match = _SEGMENT_RE.search(source_path.name[len(path.name) :])
        if match is None:
            continue
        segment_at = datetime.strptime(
            match.group(1), "%Y%m%dT%H%M%S%fZ"
        ).replace(tzinfo=timezone.utc)
        if observed - segment_at < ORDER_METRICS_COMPRESS_AGE:
            continue
        archives.append(_compress_verified(source_path))
    return archives
