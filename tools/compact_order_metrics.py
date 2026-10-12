#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess

from tools.lib.order_metrics_store import _compress_verified, seal_order_metrics_file

ROOT = Path(__file__).resolve().parents[1]
ORDER_METRICS = ROOT / "log" / "order_metrics.jsonl"


def running_strategies() -> list[str]:
    result = subprocess.run(
        ["pgrep", "-af", "python.*(main.py|tools/basis_collector.py)"],
        capture_output=True,
        text=True,
        check=False,
    )
    return [
        line
        for line in result.stdout.splitlines()
        if line.strip() and "compact_order_metrics.py" not in line
    ]


def compact(path: Path) -> tuple[Path, int, int]:
    segment = seal_order_metrics_file(path)
    if segment is None:
        raise ValueError("active order metrics file is empty")
    with segment.open("rb") as handle:
        source_lines = sum(1 for _ in handle)
    archive = _compress_verified(segment)
    print(f"source_lines={source_lines}")
    print("retained_lines=all")
    return archive, source_lines, source_lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Losslessly seal and gzip the active order_metrics.jsonl file."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform compaction. Without this flag, print a safe plan.",
    )
    args = parser.parse_args()

    processes = running_strategies()
    if processes:
        print("REFUSE_COMPACT reason=collector_or_main_process_running")
        for process in processes:
            print(process)
        return 2
    if not ORDER_METRICS.exists():
        print("REFUSE_COMPACT reason=order_metrics_missing")
        return 2

    print(f"source={ORDER_METRICS}")
    print(f"source_bytes={ORDER_METRICS.stat().st_size}")
    print("preserve=all_events_in_verified_segment_archive")
    if not args.execute:
        print("DRY_RUN add --execute only while strategy is stopped and venues are flat")
        return 0

    archive, _, _ = compact(ORDER_METRICS)
    print(f"archive={archive}")
    print(f"archive_bytes={archive.stat().st_size}")
    print(f"active_bytes={ORDER_METRICS.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
