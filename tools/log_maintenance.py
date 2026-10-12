#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.compact_order_metrics import compact
from tools.lib.basis_store import BasisSampleStore


LOG_DIR = ROOT / "log"
ORDER_METRICS = LOG_DIR / "order_metrics.jsonl"
LIVE_STATE = LOG_DIR / "live_inventory_state.json"
ORDER_COMPACT_THRESHOLD = 512 * 1024 * 1024
ROTATE_THRESHOLD = 128 * 1024 * 1024
LOG_BUDGET_BYTES = 4 * 1024**3
MIN_FREE_FOR_COMPACTION_BYTES = 512 * 1024**2
SAMPLE_RETENTION_DAYS = 45
LEGACY_RETENTION_DAYS = {
    "ws_events.jsonl": 14,
    "rest_events.jsonl": 14,
    "market_samples.jsonl": 45,
    "opportunities.jsonl": 45,
    "inventory_paper.jsonl": 45,
}


def log_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def active_writers() -> list[str]:
    pattern = (
        r"python.*(main\.py|tools/live\.py|tools/basis_collector\.py|"
        r"tools/robinhood_basis_collector\.py)"
    )
    try:
        result = subprocess.run(
            ["pgrep", "-af", pattern], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        return ["process_check_unavailable"]
    return [
        line
        for line in result.stdout.splitlines()
        if line.strip() and "pgrep -af" not in line
    ]


def flat_state() -> tuple[bool, str]:
    try:
        state = json.loads(LIVE_STATE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"state_unavailable:{type(exc).__name__}"
    lots = state.get("open_lots") or []
    pending = state.get("pending_actions") or []
    is_flat = state.get("status") == "flat" and not lots and not pending
    return is_flat, (
        f"status={state.get('status')} lots={len(lots)} pending={len(pending)}"
    )


def expired_basis_files(root: Path, today: date) -> list[Path]:
    cutoff = today - timedelta(days=SAMPLE_RETENTION_DAYS)
    expired: list[Path] = []
    if not root.exists():
        return expired
    for path in root.glob("*/*"):
        if not path.is_file() or path.name.endswith(".tmp"):
            continue
        try:
            sample_day = date.fromisoformat(path.name[:10])
        except ValueError:
            continue
        if sample_day < cutoff and path.name[10:] in (
            ".jsonl",
            ".jsonl.gz",
            ".manifest.json",
        ):
            expired.append(path)
    return sorted(expired)


def legacy_archives_to_prune(now: datetime) -> list[Path]:
    candidates: list[Path] = []
    for name, retention_days in LEGACY_RETENTION_DAYS.items():
        cutoff = now - timedelta(days=retention_days)
        for path in LOG_DIR.glob(f"{name}.legacy-*.gz"):
            stamp = path.name.removeprefix(f"{name}.legacy-").removesuffix(".gz")
            try:
                archived_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            if archived_at < cutoff:
                candidates.append(path)
    return sorted(candidates)


def archive_closed_log(path: Path, stamp: str) -> Path:
    target = path.with_name(f"{path.name}.legacy-{stamp}.gz")
    temporary = target.with_suffix(target.suffix + ".tmp")
    if target.exists() or temporary.exists():
        raise FileExistsError(f"archive already exists: {target.name}")
    source_digest = hashlib.sha256()
    try:
        with path.open("rb") as source, gzip.open(
            temporary, "wb", compresslevel=6
        ) as destination:
            while chunk := source.read(1024 * 1024):
                source_digest.update(chunk)
                destination.write(chunk)
        archived_digest = hashlib.sha256()
        with gzip.open(temporary, "rb") as archived:
            while chunk := archived.read(1024 * 1024):
                archived_digest.update(chunk)
        if archived_digest.digest() != source_digest.digest():
            raise OSError("archive verification failed")
        os.replace(temporary, target)
        path.write_bytes(b"")
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return target


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preview or safely maintain local trading and market-data logs."
    )
    parser.add_argument(
        "--execute", action="store_true", help="Apply the maintenance plan."
    )
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    today = now.date()
    roots = (LOG_DIR / "basis_samples", LOG_DIR / "robinhood_basis_samples")
    old_samples = [path for root in roots for path in expired_basis_files(root, today)]
    old_raw_archives = legacy_archives_to_prune(now)
    rotate_candidates = [
        LOG_DIR / name
        for name in LEGACY_RETENTION_DAYS
        if (LOG_DIR / name).exists()
        and (LOG_DIR / name).stat().st_size >= ROTATE_THRESHOLD
    ]
    metrics_size = ORDER_METRICS.stat().st_size if ORDER_METRICS.exists() else 0
    protected_archives = [
        *LOG_DIR.glob("order_metrics.jsonl.full-*.gz"),
        *LOG_DIR.glob("order_metrics.jsonl.segment-*.jsonl.gz"),
    ]
    protected_archive_bytes = sum(path.stat().st_size for path in protected_archives)
    current_log_bytes = log_size(LOG_DIR)
    free_bytes = shutil.disk_usage(LOG_DIR if LOG_DIR.exists() else ROOT).free
    processes = active_writers()
    is_flat, state_summary = flat_state()

    print(f"log_dir_gb={current_log_bytes / 1024**3:.2f}")
    print(f"free_gb={free_bytes / 1024**3:.2f}")
    print(f"order_metrics_mb={metrics_size / 1024**2:.1f}")
    print(f"protected_full_archives_gb={protected_archive_bytes / 1024**3:.2f}")
    print(f"expired_basis_files={len(old_samples)} retention_days={SAMPLE_RETENTION_DAYS}")
    print(f"expired_raw_archives={len(old_raw_archives)}")
    print("large_raw_logs=" + (",".join(path.name for path in rotate_candidates) or "none"))
    print(f"writers={'running' if processes else 'stopped'} state={state_summary}")
    print(f"budget={'over' if current_log_bytes > LOG_BUDGET_BYTES else 'within'}_4_gb")

    if not args.execute:
        print("DRY_RUN add --execute only while all writers are stopped and state is flat")
        return 0
    if processes:
        print("REFUSE_MAINTENANCE reason=writers_running")
        return 2
    if not is_flat:
        print("REFUSE_MAINTENANCE reason=local_state_not_confirmed_flat")
        return 2
    if metrics_size >= ORDER_COMPACT_THRESHOLD:
        required_free = int(metrics_size * 0.35) + MIN_FREE_FOR_COMPACTION_BYTES
        if free_bytes < required_free:
            print(
                "REFUSE_MAINTENANCE reason=insufficient_compaction_space "
                f"required_gb={required_free / 1024**3:.2f}"
            )
            return 2
        archive, source_lines, _ = compact(ORDER_METRICS)
        print(
            f"order_metrics_archived=YES source_lines={source_lines} "
            f"archive={archive.name} archive_bytes={archive.stat().st_size} "
            f"active_bytes={ORDER_METRICS.stat().st_size}"
        )
    else:
        print("order_metrics_compacted=NO reason=below_512_mb")

    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    for path in rotate_candidates:
        archive = archive_closed_log(path, stamp)
        print(f"raw_log_archived={path.name} archive={archive.name}")
    for root in roots:
        store = BasisSampleStore(root, config_hash="maintenance", commit="maintenance")
        compressed = store.rotate_closed_days(current_day=today.isoformat())
        removed = store.prune_expired_days(current_day=today.isoformat())
        print(
            f"basis_root={root.name} compressed={len(compressed)} "
            f"expired_removed={len(removed)}"
        )
    for path in old_raw_archives:
        path.unlink()
    print(f"expired_raw_archives_removed={len(old_raw_archives)}")
    print(f"log_dir_after_gb={log_size(LOG_DIR) / 1024**3:.2f}")
    print("order_metrics_archives=preserved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
