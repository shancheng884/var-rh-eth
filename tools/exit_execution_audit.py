"""Read-only V4 exit execution and refresh sensitivity audit."""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
from tools.profitability_audit import decimal, parse_time  # noqa: E402

REFRESH_REASONS = {
    "basis_exit_refresh_pnl_below_threshold",
    "v4_portfolio_exit_refresh_below_threshold",
    "basis_exit_refresh_quote_unavailable",
}
SHADOW_TARGETS = tuple(Decimal(value) for value in ("3.0", "3.5", "4.0", "4.5", "5.0"))


def _latest_numeric(row: dict[str, Any], field: str, observation_field: str) -> Decimal | None:
    value = decimal(row.get(field))
    if value is not None:
        return value
    observations = row.get("fast_refresh_observations") or []
    if not isinstance(observations, list):
        return None
    values = [
        decimal(item.get(observation_field))
        for item in observations
        if isinstance(item, dict)
    ]
    return max((item for item in values if item is not None), default=None)


def _newer(rows: dict[tuple[str, str], dict[str, Any]], key: tuple[str, str], row: dict[str, Any]) -> None:
    previous = rows.get(key)
    if previous is None or str(row.get("logged_at") or "") >= str(previous.get("logged_at") or ""):
        rows[key] = row


def audit_files(paths: list[Path], *, since: datetime, asset: str) -> dict[str, Any]:
    actual: dict[tuple[str, str], dict[str, Any]] = {}
    final: dict[tuple[str, str], dict[str, Any]] = {}
    refresh_counts: Counter[str] = Counter()
    refresh_lots: dict[str, set[tuple[str, str]]] = defaultdict(set)
    refresh_numeric: Counter[str] = Counter()
    shadow_attempts: Counter[Decimal] = Counter()
    shadow_lots: dict[Decimal, set[tuple[str, str]]] = defaultdict(set)
    scanned_lines = oversized_lines = 0

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
                if not str(row.get("strategy_version") or "").startswith("basis-v4-live"):
                    continue

                event = row.get("event")
                lot_id = row.get("lot_id")
                if lot_id is None:
                    continue
                key = (str(row.get("run_id") or "unknown"), str(lot_id))
                if event == "live_inventory_actual_pnl":
                    if (
                        row.get("actual_pnl_status") == "lighter_final_fill_confirmed"
                        and decimal(row.get("actual_pnl_usd")) is not None
                    ):
                        _newer(actual, key, row)
                elif event == "live_inventory_final_pnl":
                    if row.get("final_pnl_status") == "var_and_lighter_final_fills_confirmed":
                        _newer(final, key, row)
                elif event == "live_inventory_exit_blocked":
                    reason = str(row.get("reason") or "")
                    if reason not in REFRESH_REASONS:
                        continue
                    refresh_counts[reason] += 1
                    refresh_lots[reason].add(key)
                    top = _latest_numeric(row, "max_refreshed_pnl_bps", "refreshed_pnl_bps")
                    executable = _latest_numeric(row, "max_executable_pnl_bps", "executable_pnl_bps")
                    if executable is None:
                        executable = decimal(row.get("refreshed_pnl_bps")) if reason == "v4_portfolio_exit_refresh_below_threshold" else None
                    refresh_numeric["with_top_quote" if top is not None else "without_top_quote"] += 1
                    refresh_numeric["with_executable_quote" if executable is not None else "without_executable_quote"] += 1
                    if executable is not None:
                        for target in SHADOW_TARGETS:
                            if executable >= target:
                                shadow_attempts[target] += 1
                                shadow_lots[target].add(key)

    groups = []
    for key, row in actual.items():
        fill = final.get(key, {})
        direction = str(row.get("direction") or fill.get("direction") or "")
        var_drift = decimal(fill.get("exit_var_fill_drift_bps"))
        rh_drift = decimal(fill.get("exit_lighter_fill_drift_bps"))
        if direction == "short_var_long_lighter":
            var_adverse = var_drift
            rh_adverse = -rh_drift if rh_drift is not None else None
        elif direction == "long_var_short_lighter":
            var_adverse = -var_drift if var_drift is not None else None
            rh_adverse = rh_drift
        else:
            var_adverse = rh_adverse = None
        groups.append({
            "run_id": key[0],
            "lot_id": key[1],
            "logged_at": row.get("logged_at"),
            "direction": direction,
            "child_lots": row.get("closed_child_lots"),
            "actual_pnl_usd": decimal(row.get("actual_pnl_usd")),
            "actual_pnl_bps": decimal(row.get("actual_pnl_bps")),
            "estimated_pnl_bps": decimal(row.get("estimated_pnl_bps")),
            "shortfall_bps": decimal(row.get("estimated_vs_actual_pnl_shortfall_bps")),
            "var_adverse_bps": var_adverse,
            "rh_adverse_bps": rh_adverse,
            "fill_gap_ms": decimal(row.get("exit_var_fill_to_lighter_fill_ms")),
            "final_fill_detail_present": bool(fill),
        })
    groups.sort(key=lambda row: (str(row["logged_at"] or ""), row["run_id"], row["lot_id"]))
    shortfalls = [row["shortfall_bps"] for row in groups if row["shortfall_bps"] is not None]
    rh_adverse = [row["rh_adverse_bps"] for row in groups if row["rh_adverse_bps"] is not None]
    child_lots = 0
    for row in groups:
        try:
            child_lots += max(1, int(row["child_lots"]))
        except (TypeError, ValueError):
            child_lots += 1
    return {
        "scanned_lines": scanned_lines,
        "oversized_lines": oversized_lines,
        "groups": groups,
        "closed_child_lots": child_lots,
        "price_pnl_usd": sum((row["actual_pnl_usd"] or Decimal("0") for row in groups), Decimal("0")),
        "shortfall_median_bps": median(shortfalls) if shortfalls else None,
        "positive_shortfall_groups": sum(value > 0 for value in shortfalls),
        "rh_adverse_median_bps": median(rh_adverse) if rh_adverse else None,
        "refresh_counts": refresh_counts,
        "refresh_lots": {reason: len(keys) for reason, keys in refresh_lots.items()},
        "refresh_numeric": refresh_numeric,
        "shadow_attempts": shadow_attempts,
        "shadow_lots": {target: len(keys) for target, keys in shadow_lots.items()},
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
            day = datetime.strptime(args.since_beijing, "%Y-%m-%d")
        except ValueError:
            parser.error("--since-beijing must be YYYY-MM-DD")
        since = day.replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc)
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
    print(f"source_files={len(paths)} scanned_lines={result['scanned_lines']} oversized_lines={result['oversized_lines']}")
    print(f"confirmed_close_groups={len(result['groups'])} closed_child_lots={result['closed_child_lots']} price_pnl_usd={result['price_pnl_usd']} price_only=True fees_and_funding_excluded=True")
    print(f"shortfall_median_bps={result['shortfall_median_bps']} positive_shortfall_groups={result['positive_shortfall_groups']} rh_adverse_median_bps={result['rh_adverse_median_bps']} low_sample_warning={len(result['groups']) < 20}")
    for row in result["groups"]:
        print(
            f"close at={row['logged_at']} run={row['run_id']} lot={row['lot_id']} children={row['child_lots']} "
            f"direction={row['direction']} actual_usd={row['actual_pnl_usd']} actual_bps={row['actual_pnl_bps']} "
            f"estimated_bps={row['estimated_pnl_bps']} shortfall_bps={row['shortfall_bps']} "
            f"var_adverse_bps={row['var_adverse_bps']} rh_adverse_bps={row['rh_adverse_bps']} "
            f"fill_gap_ms={row['fill_gap_ms']} fill_detail={row['final_fill_detail_present']}"
        )
    for reason in sorted(REFRESH_REASONS):
        print(f"refresh_block reason={reason} logged_blocks={result['refresh_counts'][reason]} distinct_lots={result['refresh_lots'].get(reason, 0)}")
    print(f"refresh_numeric={dict(result['refresh_numeric'])}")
    for target in SHADOW_TARGETS:
        print(
            f"shadow_price_ceiling target_bps={target} logged_blocks={result['shadow_attempts'][target]} "
            f"distinct_lots={result['shadow_lots'].get(target, 0)}"
        )
    print("positive_adverse_bps_means_worse_fill; negative_means_better_fill")
    print("shadow_price_ceiling_is_not_a_fill_or_confirmation_backtest; no_fee_or_funding_model")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
