#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.platform_pnl import (  # noqa: E402
    DEFAULT_START,
    DEFAULT_VARIATIONAL_COVERAGE_FILE,
    DEFAULT_VARIATIONAL_EXPORT_DIR,
)
from tools.lib.pnl_baseline import BEIJING_TIMEZONE, parse_timestamp  # noqa: E402


def coverage_timestamp(value: str, *, end_of_date: bool = False) -> datetime:
    if len(value) == 10:
        day = date.fromisoformat(value)
        if end_of_date:
            day += timedelta(days=1)
        result = datetime.combine(day, time.min, tzinfo=BEIJING_TIMEZONE)
        return result.astimezone(timezone.utc)
    parsed = parse_timestamp(value)
    if parsed is None:
        raise argparse.ArgumentTypeError("use YYYY-MM-DD or an ISO timestamp")
    return parsed


def file_hashes(export_dir: Path) -> tuple[dict[str, str], list[Path], list[Path]]:
    files = sorted(export_dir.glob("*.csv")) if export_dir.exists() else []
    trades = [path for path in files if "trade" in path.stem.lower()]
    transfers = [
        path for path in files
        if "transfer" in path.stem.lower() or "fund" in path.stem.lower()
    ]
    if not trades or not transfers:
        raise ValueError("need at least one trade CSV and one transfer/funding CSV")
    for path in files:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            if sum(1 for _ in csv.DictReader(handle)) >= 10_000:
                raise ValueError(f"{path.name} reached the platform 10,000-row export cap; split the export")
    hashes = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }
    return hashes, trades, transfers


def write_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Locally attest that Variational CSV exports cover the requested history."
    )
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_VARIATIONAL_EXPORT_DIR)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_VARIATIONAL_COVERAGE_FILE)
    parser.add_argument("--complete-from", required=True, help="Beijing date or ISO timestamp")
    parser.add_argument("--complete-through", required=True, help="Beijing date or ISO timestamp")
    parser.add_argument("--i-confirm-complete", action="store_true")
    args = parser.parse_args()

    if not args.i_confirm_complete:
        raise SystemExit("REFUSE: verify the exports cover every trade, realized PnL, funding, fee, and transfer, then add --i-confirm-complete")
    try:
        start = coverage_timestamp(args.complete_from)
        through = coverage_timestamp(args.complete_through, end_of_date=True)
        statistics_start = parse_timestamp(DEFAULT_START)
        if statistics_start is None or start > statistics_start:
            raise ValueError("coverage must start at or before the configured statistics start")
        if through <= start:
            raise ValueError("coverage end must be after coverage start")
        if through > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("coverage end cannot be in the future")
        hashes, trades, transfers = file_hashes(args.export_dir)
    except (OSError, ValueError, argparse.ArgumentTypeError) as exc:
        raise SystemExit(f"REFUSE: {exc}") from exc

    manifest = {
        "schema_version": 1,
        "confirmed_complete": True,
        "complete_from": start.isoformat(),
        "complete_through": through.isoformat(),
        "confirmed_at_utc": datetime.now(timezone.utc).isoformat(),
        "trade_files": [path.name for path in trades],
        "transfer_files": [path.name for path in transfers],
        "files_sha256": hashes,
    }
    write_atomic(args.manifest, manifest)
    print(f"coverage_manifest=WRITTEN path={args.manifest}")
    print(f"complete_from={manifest['complete_from']} complete_through={manifest['complete_through']}")
    print(f"trade_files={len(trades)} transfer_files={len(transfers)} hashed_csv_files={len(hashes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
