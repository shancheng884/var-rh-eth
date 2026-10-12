#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.pnl_baseline import (  # noqa: E402
    PNL_BASELINE_FILE_NAME,
    load_pnl_baseline,
    record_pnl_cycle,
)
from tools.lib.runtime_files import iter_jsonl_lines  # noqa: E402


DEFAULT_LOG = ROOT / "log" / "order_metrics.jsonl"
DEFAULT_BASELINE = ROOT / "log" / PNL_BASELINE_FILE_NAME


def decimal_value(*values: Any) -> Decimal | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            continue
    return None


def actual_pnl_four_leg_volume(row: dict[str, Any]) -> Decimal | None:
    planned_qty = decimal_value(row.get("planned_qty"), row.get("qty"))
    entry_var_qty = decimal_value(
        row.get("entry_var_final_fill_qty"), planned_qty
    )
    entry_lighter_qty = decimal_value(
        row.get("entry_lighter_final_fill_qty"), planned_qty
    )
    exit_var_qty = decimal_value(row.get("exit_var_final_fill_qty"), planned_qty)
    exit_lighter_qty = decimal_value(
        row.get("exit_lighter_final_fill_qty"), planned_qty
    )
    entry_var_price = decimal_value(
        row.get("entry_var_final_fill_price"), row.get("entry_var_price")
    )
    entry_lighter_price = decimal_value(
        row.get("entry_lighter_final_fill_price"),
        row.get("entry_lighter_price"),
    )
    exit_var_price = decimal_value(
        row.get("exit_var_final_fill_price"), row.get("exit_var_price")
    )
    exit_lighter_price = decimal_value(row.get("exit_lighter_final_fill_price"))
    values = (
        entry_var_qty,
        entry_lighter_qty,
        exit_var_qty,
        exit_lighter_qty,
        entry_var_price,
        entry_lighter_price,
        exit_var_price,
        exit_lighter_price,
    )
    if any(value is None for value in values):
        return None
    return (
        entry_var_qty * entry_var_price
        + entry_lighter_qty * entry_lighter_price
        + exit_var_qty * exit_var_price
        + exit_lighter_qty * exit_lighter_price
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-time migration of four-leg volume into the small PnL ledger."
    )
    parser.add_argument("--path", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--baseline-path", type=Path, default=DEFAULT_BASELINE)
    args = parser.parse_args()
    baseline = load_pnl_baseline(args.baseline_path)
    if baseline is None:
        raise SystemExit("backfill=FAILED baseline_missing")
    counted = {str(value) for value in baseline.get("counted_cycle_keys", [])}
    volume_counted_before = {
        str(value) for value in baseline.get("volume_counted_cycle_keys", [])
    }
    matched = 0
    migrated = 0
    missing_fields = 0
    for line in iter_jsonl_lines(args.path):
        if '"event": "live_inventory_actual_pnl"' not in line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("actual_pnl_status") != "lighter_final_fill_confirmed":
            continue
        run_id = str(row.get("run_id") or "")
        asset = str(row.get("asset") or "").upper()
        lot_id = row.get("lot_id")
        key = f"{run_id}:{asset}:{lot_id}"
        if key not in counted:
            continue
        matched += 1
        volume = actual_pnl_four_leg_volume(row)
        if volume is None:
            missing_fields += 1
            continue
        record_pnl_cycle(
            args.baseline_path,
            run_id=run_id,
            asset=asset,
            lot_id=lot_id,
            actual_pnl_usd=row.get("actual_pnl_usd"),
            observed_at=row.get("confirmed_at") or row.get("logged_at"),
            four_leg_volume_usd=volume,
            closed_child_lots=(
                row.get("closed_child_lots")
                or row.get("portfolio_component_lot_count")
                or 1
            ),
        )
        if key not in volume_counted_before:
            migrated += 1
    updated = load_pnl_baseline(args.baseline_path) or {}
    print("backfill=PASS")
    print(f"matched_confirmed_closes={matched}")
    print(f"migrated_volume_records={migrated}")
    print(f"missing_price_or_qty_records={missing_fields}")
    print(
        "confirmed_four_leg_volume_usd="
        f"{updated.get('confirmed_four_leg_volume_usd')}"
    )
    print(f"tracked_closed_child_lots={updated.get('tracked_closed_child_lots')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
