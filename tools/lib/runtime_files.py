from __future__ import annotations

import json
import gzip
import os
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "log"
BASIS_SAMPLES_DIR = LOG_DIR / "basis_samples"
ORDER_METRICS = LOG_DIR / "order_metrics.jsonl"
RUNTIME_LOG = LOG_DIR / "runtime.log"
COLLECTOR_LOG = LOG_DIR / "basis_collector.log"
LIVE_STATE = LOG_DIR / "live_inventory_state.json"
LIVE_CONTROL = LOG_DIR / "live_inventory_control.json"
JSONL_TAIL_MAX_ROWS = 10000
JSONL_TAIL_MAX_BYTES = 2 * 1024 * 1024
JSONL_TAIL_MAX_LINE_BYTES = 256 * 1024


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _tail_jsonl_handles(
    handles: Iterable[Any],
    *,
    limit: int,
    max_bytes: int,
    max_line_bytes: int,
    stats: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    row_limit = min(JSONL_TAIL_MAX_ROWS, max(1, limit))
    byte_limit = max(1, max_bytes)
    line_limit = max(1, max_line_bytes)
    raw_rows: deque[bytes] = deque()
    retained_bytes = 0
    counters = stats if stats is not None else {}
    counters.setdefault("oversized_lines", 0)
    counters.setdefault("evicted_rows", 0)

    for handle in handles:
        while True:
            line = handle.readline(line_limit + 1)
            if not line:
                break
            if len(line) > line_limit:
                counters["oversized_lines"] += 1
                while line and not line.endswith(b"\n"):
                    line = handle.readline(line_limit + 1)
                continue
            raw_rows.append(line)
            retained_bytes += len(line)
            while len(raw_rows) > row_limit or retained_bytes > byte_limit:
                retained_bytes -= len(raw_rows.popleft())
                counters["evicted_rows"] += 1

    parsed: list[dict[str, Any]] = []
    for line in raw_rows:
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(row, dict):
            parsed.append(row)
    counters["retained_rows"] = len(parsed)
    counters["retained_bytes"] = retained_bytes
    return parsed


def tail_jsonl(
    path: Path,
    limit: int,
    *,
    max_bytes: int = JSONL_TAIL_MAX_BYTES,
    max_line_bytes: int = JSONL_TAIL_MAX_LINE_BYTES,
    stats: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    try:
        with path.open("rb") as handle:
            return _tail_jsonl_handles(
                [handle],
                limit=limit,
                max_bytes=max_bytes,
                max_line_bytes=max_line_bytes,
                stats=stats,
            )
    except FileNotFoundError:
        return []


def rotated_jsonl_paths(path: Path) -> list[Path]:
    parent = path.parent
    name = path.name
    if not parent.exists():
        return [path] if path.exists() else []
    rotated = [item for item in parent.iterdir() if item.name.startswith(name + ".")]
    def sort_key(item: Path) -> tuple[int, str]:
        suffix = item.name[len(name) + 1 :]
        first = suffix.split(".", 1)[0]
        try:
            return (int(first), item.name)
        except ValueError:
            return (9999, item.name)
    paths = list(reversed(sorted(rotated, key=sort_key)))
    if path.exists():
        paths.append(path)
    return paths


def tail_jsonl_many(
    paths: Iterable[Path],
    limit: int,
    *,
    max_bytes: int = JSONL_TAIL_MAX_BYTES,
    max_line_bytes: int = JSONL_TAIL_MAX_LINE_BYTES,
    stats: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    row_limit = min(JSONL_TAIL_MAX_ROWS, max(1, limit))
    byte_limit = max(1, max_bytes)
    line_limit = max(1, max_line_bytes)
    raw_rows: deque[bytes] = deque()
    retained_bytes = 0
    counters = stats if stats is not None else {}
    counters.setdefault("oversized_lines", 0)
    counters.setdefault("evicted_rows", 0)
    for path in paths:
        try:
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rb") as handle:
                while True:
                    line = handle.readline(line_limit + 1)
                    if not line:
                        break
                    if len(line) > line_limit:
                        counters["oversized_lines"] += 1
                        while line and not line.endswith(b"\n"):
                            line = handle.readline(line_limit + 1)
                        continue
                    raw_rows.append(line)
                    retained_bytes += len(line)
                    while len(raw_rows) > row_limit or retained_bytes > byte_limit:
                        retained_bytes -= len(raw_rows.popleft())
                        counters["evicted_rows"] += 1
        except (FileNotFoundError, OSError):
            continue
    parsed: list[dict[str, Any]] = []
    for line in raw_rows:
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(row, dict):
            parsed.append(row)
    counters["retained_rows"] = len(parsed)
    counters["retained_bytes"] = retained_bytes
    return parsed


def tail_text(path: Path, limit: int) -> list[str]:
    rows: deque[str] = deque(maxlen=max(1, limit))
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                rows.append(line.rstrip("\n"))
    except FileNotFoundError:
        return []
    return list(rows)


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def fmt_decimal(value: Decimal | None, places: str = "0.01") -> str:
    if value is None:
        return "-"
    return format(value.quantize(Decimal(places)), "f")


def avg(values: Iterable[Decimal]) -> Decimal | None:
    items = list(values)
    return sum(items) / Decimal(len(items)) if items else None


def percentile(values: Iterable[Decimal], pct: Decimal) -> Decimal | None:
    ordered = sorted(values)
    if not ordered:
        return None
    index = int((Decimal(len(ordered) - 1) * pct / Decimal("100")).to_integral_value(rounding="ROUND_HALF_UP"))
    return ordered[max(0, min(index, len(ordered) - 1))]


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                continue
    return total


def human_bytes(value: int) -> str:
    units = ["B", "K", "M", "G", "T"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
