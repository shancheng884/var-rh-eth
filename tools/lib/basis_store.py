from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = "3"


def _utc_day(value: str | None = None) -> str:
    if value:
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(text).astimezone(timezone.utc).date().isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).date().isoformat()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def compress_jsonl(path: Path) -> Path:
    """Compress a closed JSONL file atomically, preserving the source on failure."""
    target = Path(str(path) + ".gz")
    temporary = Path(str(target) + ".tmp")
    with path.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as destination:
        while chunk := source.read(1024 * 1024):
            destination.write(chunk)
    os.replace(temporary, target)
    path.unlink()
    return target


@dataclass
class DayStats:
    rows: int = 0
    baseline_rows: int = 0
    burst_rows: int = 0
    first_at: str | None = None
    last_at: str | None = None
    sha256: Any = None

    def __post_init__(self) -> None:
        if self.sha256 is None:
            self.sha256 = hashlib.sha256()


class BasisSampleStore:
    """Append-only daily samples with lossless compression and bounded age."""

    def __init__(
        self,
        root: Path,
        *,
        config_hash: str,
        commit: str,
        retention_days: int = 45,
    ) -> None:
        if retention_days < 30:
            raise ValueError("basis sample retention must be at least 30 days")
        self.root = root
        self.config_hash = config_hash
        self.commit = commit
        self.retention_days = retention_days
        self.stats: dict[tuple[str, str], DayStats] = {}
        self._last_prune_day: str | None = None
        self.root.mkdir(parents=True, exist_ok=True)

    def append(self, row: dict[str, Any]) -> Path:
        asset = str(row.get("asset") or "UNKNOWN").upper()
        logged_at = str(row.get("logged_at") or datetime.now(timezone.utc).isoformat())
        day = _utc_day(logged_at)
        asset_dir = self.root / asset
        asset_dir.mkdir(parents=True, exist_ok=True)
        path = asset_dir / f"{day}.jsonl"
        payload = {
            "schema_version": SCHEMA_VERSION,
            "collector_config_hash": self.config_hash,
            "collector_commit": self.commit,
            **row,
        }
        line = json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
        key = (asset, day)
        stats = self.stats.setdefault(key, DayStats())
        stats.rows += 1
        sample_kind = str(payload.get("sample_kind") or "baseline")
        if sample_kind == "burst":
            stats.burst_rows += 1
        else:
            stats.baseline_rows += 1
        stats.first_at = stats.first_at or logged_at
        stats.last_at = logged_at
        stats.sha256.update(line.encode("utf-8"))
        return path

    def write_manifests(self) -> None:
        for (asset, day), stats in self.stats.items():
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "asset": asset,
                "utc_day": day,
                "rows_this_process": stats.rows,
                "baseline_rows_this_process": stats.baseline_rows,
                "burst_rows_this_process": stats.burst_rows,
                "first_at_this_process": stats.first_at,
                "last_at_this_process": stats.last_at,
                "process_rows_sha256": stats.sha256.hexdigest(),
                "collector_config_hash": self.config_hash,
                "collector_commit": self.commit,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_json(self.root / asset / f"{day}.manifest.json", manifest)

    def rotate_closed_days(self, *, current_day: str | None = None) -> list[Path]:
        today = current_day or _utc_day()
        compressed: list[Path] = []
        for path in sorted(self.root.glob("*/*.jsonl")):
            if path.stem >= today:
                continue
            compressed.append(compress_jsonl(path))
        if self._last_prune_day != today:
            self.prune_expired_days(current_day=today)
            self._last_prune_day = today
        return compressed

    def prune_expired_days(self, *, current_day: str | None = None) -> list[Path]:
        """Remove only closed sample files older than the configured history."""
        today = date.fromisoformat(current_day or _utc_day())
        cutoff = today - timedelta(days=self.retention_days)
        removed: list[Path] = []
        for path in self.root.glob("*/*"):
            if not path.is_file() or path.name.endswith(".tmp"):
                continue
            try:
                sample_day = date.fromisoformat(path.name[:10])
            except ValueError:
                continue
            if sample_day >= cutoff:
                continue
            if path.name[10:] not in (".jsonl", ".jsonl.gz", ".manifest.json"):
                continue
            path.unlink()
            removed.append(path)
        return removed


def basis_sample_paths(root: Path, asset_filter: str | None = None) -> list[Path]:
    if not root.exists():
        return []
    assets: Iterable[Path]
    if asset_filter:
        assets = [root / asset_filter.upper()]
    else:
        assets = [path for path in root.iterdir() if path.is_dir()]
    paths: list[Path] = []
    for asset_dir in assets:
        if not asset_dir.exists():
            continue
        paths.extend(asset_dir.glob("*.jsonl"))
        paths.extend(asset_dir.glob("*.jsonl.gz"))
    return sorted(paths, key=lambda path: (path.parent.name, path.name))


def read_basis_samples(
    root: Path,
    *,
    limit: int,
    asset_filter: str | None = None,
    sample_kind_filter: str | None = None,
    sample_quality_filter: str | None = None,
    quote_size_mode_filter: str | None = None,
    max_total_rows: int | None = None,
    max_total_bytes: int | None = None,
    max_line_bytes: int | None = None,
    stats: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    paths_by_asset: dict[str, list[Path]] = {}
    for path in basis_sample_paths(root, asset_filter):
        paths_by_asset.setdefault(path.parent.name, []).append(path)
    assets = sorted(paths_by_asset)
    total_row_limit = max(
        1, max_total_rows if max_total_rows is not None else limit
    )
    base_rows, extra_rows = divmod(total_row_limit, max(1, len(assets)))
    total_byte_limit = max_total_bytes
    base_bytes, extra_bytes = (
        divmod(total_byte_limit, max(1, len(assets)))
        if total_byte_limit is not None
        else (None, None)
    )
    total_retained_bytes = 0
    counters = stats if stats is not None else {}
    counters.setdefault("oversized_lines", 0)
    counters.setdefault("evicted_rows", 0)
    for asset_index, asset in enumerate(assets):
        paths = paths_by_asset[asset]
        asset_row_limit = (
            base_rows + (1 if asset_index < extra_rows else 0)
            if max_total_rows is not None
            else max(1, limit)
        )
        asset_byte_limit = (
            base_bytes + (1 if asset_index < extra_bytes else 0)
            if base_bytes is not None
            else None
        )
        if asset_row_limit <= 0 or asset_byte_limit == 0:
            continue
        asset_rows: deque[tuple[dict[str, Any], int]] = deque()
        retained_bytes = 0
        for path in sorted(paths, key=lambda item: item.name):
            opener = gzip.open if path.suffix == ".gz" else open
            try:
                mode = "rb" if max_line_bytes is not None else "rt"
                kwargs = {} if mode == "rb" else {"encoding": "utf-8", "errors": "replace"}
                with opener(path, mode, **kwargs) as handle:
                    while True:
                        line = (
                            handle.readline(max_line_bytes + 1)
                            if max_line_bytes is not None
                            else handle.readline()
                        )
                        if not line:
                            break
                        if max_line_bytes is not None and len(line) > max_line_bytes:
                            counters["oversized_lines"] += 1
                            while line and not line.endswith(b"\n"):
                                line = handle.readline(max_line_bytes + 1)
                            continue
                        line_size = len(line) if isinstance(line, bytes) else len(line.encode("utf-8"))
                        try:
                            row = json.loads(line)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue
                        if not isinstance(row, dict):
                            continue
                        if (
                            sample_kind_filter is not None
                            and str(row.get("sample_kind") or "")
                            != sample_kind_filter
                        ):
                            continue
                        if (
                            sample_quality_filter is not None
                            and str(row.get("sample_quality") or "")
                            != sample_quality_filter
                        ):
                            continue
                        if (
                            quote_size_mode_filter is not None
                            and str(row.get("quote_size_mode") or "")
                            != quote_size_mode_filter
                        ):
                            continue
                        asset_rows.append((row, line_size))
                        retained_bytes += line_size
                        while len(asset_rows) > asset_row_limit or (
                            asset_byte_limit is not None
                            and retained_bytes > asset_byte_limit
                        ):
                            _, removed_bytes = asset_rows.popleft()
                            retained_bytes -= removed_bytes
                            counters["evicted_rows"] += 1
            except OSError:
                continue
        rows.extend(row for row, _ in asset_rows)
        total_retained_bytes += retained_bytes
    rows.sort(key=lambda row: str(row.get("logged_at") or ""))
    result = rows[-total_row_limit:]
    counters["retained_rows"] = len(result)
    counters["retained_bytes"] = total_retained_bytes
    return result
