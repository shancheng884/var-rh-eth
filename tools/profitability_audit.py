"""Stream V4 opportunity and confirmed price-PnL history with bounded memory."""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (  # noqa: E402
    JSONL_TAIL_MAX_LINE_BYTES,
    ORDER_METRICS,
    rotated_jsonl_paths,
)

DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")
ENTRY_RELAXATIONS = (Decimal("0"), Decimal("1"), Decimal("2"))
EXIT_TARGETS = (Decimal("3.5"), Decimal("4.0"), Decimal("4.5"))


def decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except ValueError:
        return None


def update_peak(lot: dict[str, Any], field: str, value: Any) -> None:
    observed = decimal(value)
    if observed is not None:
        previous = lot.get(field)
        lot[field] = observed if previous is None else max(previous, observed)


def audit_files(
    paths: list[Path], *, since: datetime, asset: str
) -> dict[str, Any]:
    events: Counter[str] = Counter()
    entry_blocks: Counter[str] = Counter()
    exit_blocks: Counter[str] = Counter()
    entry_signals: Counter[tuple[str, Decimal]] = Counter()
    lots: dict[tuple[str, str], dict[str, Any]] = {}
    last_sample_index: dict[str, str] = {}
    scanned_lines = valid_rows = oversized_lines = 0
    first_at = last_at = None
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rb") as handle:
            while True:
                line = handle.readline(JSONL_TAIL_MAX_LINE_BYTES + 1)
                if not line:
                    break
                scanned_lines += 1
                if len(line) > JSONL_TAIL_MAX_LINE_BYTES:
                    oversized_lines += 1
                    while line and not line.endswith(b"\n"):
                        line = handle.readline(JSONL_TAIL_MAX_LINE_BYTES + 1)
                    continue
                try:
                    row = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(row, dict):
                    continue
                at = parse_time(row.get("logged_at"))
                if at is None or at < since:
                    continue
                if str(row.get("asset") or "").upper() != asset:
                    continue
                if not str(row.get("strategy_version") or "").startswith(
                    "basis-v4-live"
                ):
                    continue
                valid_rows += 1
                first_at = at if first_at is None else min(first_at, at)
                last_at = at if last_at is None else max(last_at, at)
                event = str(row.get("event") or "")
                events[event] += 1
                run_id = str(row.get("run_id") or "unknown")

                if event == "live_inventory_basis_state":
                    sample_index = row.get("sample_index")
                    if sample_index is not None:
                        sample_key = str(sample_index)
                        if last_sample_index.get(run_id) == sample_key:
                            continue
                        last_sample_index[run_id] = sample_key
                    if row.get("sample_pair_valid") is False:
                        continue
                    edges = row.get("v4_direction_edges_bps") or {}
                    thresholds = row.get("v4_direction_thresholds_bps") or {}
                    if not isinstance(edges, dict) or not isinstance(
                        thresholds, dict
                    ):
                        continue
                    for direction in DIRECTIONS:
                        edge = decimal(edges.get(direction))
                        threshold = decimal(thresholds.get(direction))
                        if edge is None or threshold is None:
                            continue
                        for relaxation in ENTRY_RELAXATIONS:
                            if edge >= threshold - relaxation:
                                entry_signals[(direction, relaxation)] += 1
                    continue

                if event == "live_inventory_entry_blocked":
                    entry_blocks[str(row.get("reason") or "unknown")] += 1
                elif event == "live_inventory_exit_blocked":
                    reason = str(row.get("reason") or "unknown")
                    exit_blocks[reason] += 1
                    if reason == "v4_executable_pnl_below_threshold":
                        lot_id = row.get("lot_id")
                        if lot_id is not None:
                            lot = lots.setdefault((run_id, str(lot_id)), {})
                            update_peak(lot, "blocked_log_peak_bps", row.get("pnl_bps"))
                elif event == "live_inventory_v4_exit_observation":
                    for item in row.get("lots") or []:
                        if not isinstance(item, dict) or item.get("lot_id") is None:
                            continue
                        lot = lots.setdefault((run_id, str(item["lot_id"])), {})
                        update_peak(
                            lot,
                            "matched_quote_peak_bps",
                            item.get("executable_mfe_pnl_bps"),
                        )
                elif event == "live_inventory_exited":
                    lot_id = row.get("lot_id")
                    if lot_id is not None:
                        lot = lots.setdefault((run_id, str(lot_id)), {})
                        lot["exited"] = True
                        update_peak(
                            lot,
                            "matched_quote_peak_bps",
                            row.get("executable_exit_mfe_pnl_bps"),
                        )
                elif event == "live_inventory_actual_pnl":
                    if row.get("actual_pnl_status") != "lighter_final_fill_confirmed":
                        continue
                    lot_id = row.get("lot_id")
                    if lot_id is not None:
                        lot = lots.setdefault((run_id, str(lot_id)), {})
                        lot["actual_pnl_usd"] = decimal(row.get("actual_pnl_usd"))
                        lot["actual_pnl_bps"] = decimal(row.get("actual_pnl_bps"))
                        lot["closed_child_lots"] = row.get("closed_child_lots")

    confirmed = [
        lot for lot in lots.values() if lot.get("actual_pnl_usd") is not None
    ]
    actual_bps = [
        lot["actual_pnl_bps"]
        for lot in confirmed
        if lot.get("actual_pnl_bps") is not None
    ]
    target_counts = []
    for target in EXIT_TARGETS:
        target_counts.append(
            {
                "target_bps": target,
                "matched_quote_peaks": sum(
                    lot.get("matched_quote_peak_bps") is not None
                    and lot["matched_quote_peak_bps"] >= target
                    for lot in lots.values()
                ),
                "throttled_block_log_peaks": sum(
                    lot.get("blocked_log_peak_bps") is not None
                    and lot["blocked_log_peak_bps"] >= target
                    for lot in lots.values()
                ),
            }
        )
    return {
        "scanned_lines": scanned_lines,
        "valid_rows": valid_rows,
        "oversized_lines": oversized_lines,
        "first_at": first_at,
        "last_at": last_at,
        "events": events,
        "entry_blocks": entry_blocks,
        "exit_blocks": exit_blocks,
        "entry_signals": entry_signals,
        "confirmed_close_groups": len(confirmed),
        "confirmed_price_pnl_usd": sum(
            (lot["actual_pnl_usd"] for lot in confirmed), Decimal("0")
        ),
        "confirmed_price_pnl_median_bps": median(actual_bps) if actual_bps else None,
        "target_counts": target_counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset", default="ETH")
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--since-beijing", help="Beijing calendar date YYYY-MM-DD")
    window.add_argument("--hours", type=float, default=24)
    parser.add_argument("--include-rotated", action="store_true")
    args = parser.parse_args()
    if args.since_beijing:
        try:
            beijing_day = datetime.strptime(args.since_beijing, "%Y-%m-%d")
        except ValueError:
            parser.error("--since-beijing must be YYYY-MM-DD")
        since = beijing_day.replace(tzinfo=timezone(timedelta(hours=8))).astimezone(
            timezone.utc
        )
    else:
        if args.hours <= 0:
            parser.error("--hours must be positive")
        since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    paths = rotated_jsonl_paths(ORDER_METRICS) if args.include_rotated else [ORDER_METRICS]
    paths = [path for path in paths if path.exists()]
    if not paths:
        parser.error(f"order metrics file not found: {ORDER_METRICS}")
    result = audit_files(paths, since=since, asset=args.asset.upper())
    print(f"window_start_utc={since.isoformat()} asset={args.asset.upper()}")
    print(
        f"source_files={len(paths)} scanned_lines={result['scanned_lines']} "
        f"v4_rows_in_window={result['valid_rows']} "
        f"oversized_lines={result['oversized_lines']} "
        f"first_at={result['first_at']} last_at={result['last_at']}"
    )
    events = result["events"]
    print(
        f"entered={events['live_inventory_entered']} "
        f"exited={events['live_inventory_exited']} "
        f"confirmed_close_groups={result['confirmed_close_groups']} "
        f"confirmed_price_pnl_usd={result['confirmed_price_pnl_usd']} "
        f"confirmed_price_pnl_median_bps="
        f"{result['confirmed_price_pnl_median_bps']}"
    )
    print(f"entry_block_reasons={dict(result['entry_blocks'].most_common(10))}")
    print(f"exit_block_reasons={dict(result['exit_blocks'].most_common(10))}")
    for direction in DIRECTIONS:
        print(
            f"entry_signal_samples direction={direction} "
            + " ".join(
                f"relax_{relaxation}_bps={result['entry_signals'][(direction, relaxation)]}"
                for relaxation in ENTRY_RELAXATIONS
            )
        )
    for target in result["target_counts"]:
        print(
            f"exit_peak_crossings target_bps={target['target_bps']} "
            f"matched_quote={target['matched_quote_peaks']} "
            f"throttled_block_log={target['throttled_block_log_peaks']}"
        )
    print("price_pnl_excludes_funding_and_fees; peak_crossings_are_not_fills")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
