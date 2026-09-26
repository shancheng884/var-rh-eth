#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (  # noqa: E402
    LIVE_STATE,
    ORDER_METRICS,
    read_json,
    to_decimal,
    write_json_atomic,
)

DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"


def _reverse_lines(path: Path, *, block_size: int = 1024 * 1024):
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        remainder = b""
        while position > 0:
            size = min(block_size, position)
            position -= size
            handle.seek(position)
            parts = (handle.read(size) + remainder).split(b"\n")
            remainder = parts[0]
            for line in reversed(parts[1:]):
                if line:
                    yield line
        if remainder:
            yield remainder


def _recovery_rows_from_end(
    path: Path,
    *,
    limit: int,
    lot_id: Any,
    lighter_record_key: str,
    run_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    submit_found = False
    lighter_found = False
    try:
        lines = _reverse_lines(path)
        for index, raw in enumerate(lines, 1):
            if index > max(1, limit):
                break
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            is_submit = (
                row.get("event") == "live_inventory_var_entry_submitted"
                and str(row.get("lot_id")) == str(lot_id)
                and str(row.get("run_id") or "") == run_id
            )
            is_lighter = (
                row.get("event") == "lighter_fill"
                and str(row.get("trade_key") or "") == lighter_record_key
            )
            if is_submit and not submit_found:
                rows.append(row)
                submit_found = True
            elif is_lighter and not lighter_found:
                rows.append(row)
                lighter_found = True
            if submit_found and lighter_found:
                return rows
    except FileNotFoundError:
        pass
    missing = []
    if not submit_found:
        missing.append("Variational submit")
    if not lighter_found:
        missing.append("Lighter fill")
    raise ValueError(
        f"recovery evidence not found in last {limit} log lines: {', '.join(missing)}"
    )


def _find_pending_backup(state_file: Path) -> tuple[Path, dict[str, Any]]:
    candidates = sorted(
        state_file.parent.glob(state_file.name + ".before_diagnostic_restart.*.bak"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        state = read_json(path)
        pending = state.get("pending_actions")
        if isinstance(pending, list) and pending:
            return path, state
    raise ValueError("no interrupted-entry state backup with pending actions found")


def _var_position(context: dict[str, Any], asset: str) -> tuple[Decimal, Decimal]:
    result = context.get("variational_positions_result")
    result = result.get("result") if isinstance(result, dict) else None
    positions = result.get("positions") if isinstance(result, dict) else None
    if isinstance(positions, dict):
        iterable = positions.values()
    elif isinstance(positions, list):
        iterable = positions
    else:
        raise ValueError("Variational positions are missing from manual review context")
    for position in iterable:
        if not isinstance(position, dict):
            continue
        info = position.get("position_info") if isinstance(position, dict) else None
        instrument = info.get("instrument") if isinstance(info, dict) else None
        direct_instrument = position.get("instrument")
        candidates = [
            position.get("asset"),
            position.get("market"),
            position.get("symbol"),
            position.get("underlying"),
            info.get("asset") if isinstance(info, dict) else None,
            info.get("market") if isinstance(info, dict) else None,
            info.get("symbol") if isinstance(info, dict) else None,
            info.get("underlying") if isinstance(info, dict) else None,
            instrument.get("underlying") if isinstance(instrument, dict) else None,
            direct_instrument.get("underlying")
            if isinstance(direct_instrument, dict)
            else None,
        ]
        if asset not in {str(value).upper() for value in candidates if value is not None}:
            continue
        qty = next(
            (
                value
                for source in (position, info)
                if isinstance(source, dict)
                for key in (
                    "qty",
                    "quantity",
                    "size",
                    "position",
                    "position_size",
                    "base_amount",
                    "amount",
                )
                if (value := to_decimal(source.get(key))) is not None
            ),
            None,
        )
        avg_price = next(
            (
                value
                for source in (position, info)
                if isinstance(source, dict)
                for key in ("avg_entry_price", "average_entry_price", "entry_price")
                if (value := to_decimal(source.get(key))) is not None
            ),
            None,
        )
        if qty is None or avg_price is None:
            break
        return qty, avg_price
    raise ValueError(f"Variational {asset} position is missing")


def _lighter_position(context: dict[str, Any], asset: str) -> tuple[Decimal, Decimal]:
    result = context.get("lighter_account_result")
    accounts = result.get("accounts") if isinstance(result, dict) else None
    if not isinstance(accounts, list):
        raise ValueError("Lighter account is missing from manual review context")
    for account in accounts:
        positions = account.get("positions") if isinstance(account, dict) else None
        if not isinstance(positions, list):
            continue
        for position in positions:
            if not isinstance(position, dict):
                continue
            if str(position.get("symbol") or "").upper() != asset:
                continue
            qty = to_decimal(position.get("position"))
            sign = to_decimal(position.get("sign"))
            avg_price = to_decimal(position.get("avg_entry_price"))
            if qty is None or avg_price is None:
                break
            signed_qty = abs(qty) * (Decimal("1") if sign is None or sign >= 0 else Decimal("-1"))
            return signed_qty, avg_price
    raise ValueError(f"Lighter {asset} position is missing")


def _weighted_existing(lots: list[dict[str, Any]], price_field: str) -> tuple[Decimal, Decimal]:
    qty_total = Decimal("0")
    value_total = Decimal("0")
    for lot in lots:
        qty = to_decimal(lot.get("qty"))
        price = to_decimal(lot.get(price_field))
        if qty is None or qty <= 0 or price is None or price <= 0:
            raise ValueError(f"existing lot has invalid {price_field}")
        qty_total += qty
        value_total += qty * price
    return qty_total, value_total


def _matching_rows(
    rows: list[dict[str, Any]],
    *,
    lot_id: Any,
    lighter_record_key: str,
    run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    submitted = next(
        (
            row
            for row in reversed(rows)
            if row.get("event") == "live_inventory_var_entry_submitted"
            and str(row.get("lot_id")) == str(lot_id)
            and str(row.get("run_id") or "") == run_id
        ),
        None,
    )
    lighter_fill = next(
        (
            row
            for row in reversed(rows)
            if row.get("event") == "lighter_fill"
            and str(row.get("trade_key") or "") == lighter_record_key
        ),
        None,
    )
    if submitted is None:
        raise ValueError(f"lot {lot_id} submit event is missing")
    if lighter_fill is None:
        raise ValueError(f"Lighter fill {lighter_record_key} is missing")
    return submitted, lighter_fill


def build_repaired_state(
    *,
    current: dict[str, Any],
    interrupted: dict[str, Any],
    rows: list[dict[str, Any]],
    asset: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    asset = asset.upper()
    if current.get("status") != "manual_review_required":
        raise ValueError("current state is not manual_review_required")
    if current.get("manual_review_reason") != "startup_reconcile_exchange_position_mismatch":
        raise ValueError("current manual review reason is not an exchange position mismatch")
    open_lots = current.get("open_lots")
    if not isinstance(open_lots, list) or not open_lots:
        raise ValueError("current state has no open lots")
    if current.get("pending_actions"):
        raise ValueError("current state still has pending actions")
    pending_actions = interrupted.get("pending_actions")
    if not isinstance(pending_actions, list) or len(pending_actions) != 1:
        raise ValueError("backup must contain exactly one pending action")
    pending = pending_actions[0]
    if pending.get("role") != "live_inventory_entry_pending_var_fill":
        raise ValueError("pending action is not an interrupted entry")
    if str(pending.get("asset") or "").upper() != asset:
        raise ValueError("pending action asset does not match")
    direction = str(pending.get("direction") or "")
    if direction not in {DIRECTION_LONG, DIRECTION_SHORT}:
        raise ValueError("pending action direction is invalid")
    if any(str(lot.get("direction") or "") != direction for lot in open_lots):
        raise ValueError("pending direction does not match existing lots")
    pending_qty = to_decimal(pending.get("qty"))
    if pending_qty is None or pending_qty <= 0:
        raise ValueError("pending quantity is invalid")

    context = current.get("manual_review_context")
    if not isinstance(context, dict):
        raise ValueError("manual review context is missing")
    var_qty, var_avg = _var_position(context, asset)
    lighter_qty, lighter_avg = _lighter_position(context, asset)
    expected_var_sign = Decimal("-1") if direction == DIRECTION_SHORT else Decimal("1")
    expected_lighter_sign = -expected_var_sign
    if var_qty * expected_var_sign <= 0 or lighter_qty * expected_lighter_sign <= 0:
        raise ValueError("exchange position direction does not match pending entry")

    existing_qty, existing_var_value = _weighted_existing(open_lots, "entry_var_fill_price")
    lighter_existing_qty, existing_lighter_value = _weighted_existing(
        open_lots, "entry_lighter_fill_price"
    )
    if lighter_existing_qty != existing_qty:
        raise ValueError("existing lot quantities disagree")
    expected_total = existing_qty + pending_qty
    tolerance = max(Decimal("0.00000001"), expected_total * Decimal("0.0001"))
    if abs(abs(var_qty) - expected_total) > tolerance:
        raise ValueError("Variational quantity does not equal existing plus pending")
    if abs(abs(lighter_qty) - expected_total) > tolerance:
        raise ValueError("Lighter quantity does not equal existing plus pending")

    lighter_record_key = str(pending.get("lighter_record_key") or "")
    interrupted_run_id = str(interrupted.get("run_id") or "")
    if not interrupted_run_id:
        raise ValueError("backup run id is missing")
    submitted, lighter_fill = _matching_rows(
        rows,
        lot_id=pending.get("lot_id"),
        lighter_record_key=lighter_record_key,
        run_id=interrupted_run_id,
    )
    if str(submitted.get("direction") or "") != direction:
        raise ValueError("submit event direction does not match pending entry")
    submitted_qty = to_decimal(submitted.get("qty"))
    if submitted_qty is None or abs(submitted_qty - pending_qty) > tolerance:
        raise ValueError("submit event quantity does not match pending entry")
    lighter_fill_qty = to_decimal(lighter_fill.get("lighter_filled_base_amount"))
    lighter_fill_price = to_decimal(lighter_fill.get("lighter_filled_price"))
    if lighter_fill_qty is None or abs(lighter_fill_qty - pending_qty) > tolerance:
        raise ValueError("Lighter fill quantity does not match pending quantity")
    if lighter_fill_price is None or lighter_fill_price <= 0:
        raise ValueError("Lighter final fill price is missing")

    inferred_var_fill = (abs(var_qty) * var_avg - existing_var_value) / pending_qty
    if inferred_var_fill <= 0:
        raise ValueError("inferred Variational fill price is invalid")
    reconstructed_lighter_avg = (
        existing_lighter_value + pending_qty * lighter_fill_price
    ) / expected_total
    if abs(reconstructed_lighter_avg - lighter_avg) > Decimal("0.011"):
        raise ValueError("Lighter aggregate average does not validate the recovered fill")

    tranche_index = int(current.get("v4_next_tranche_index") or len(open_lots) + 1)
    active_tier = int(
        submitted.get("v4_real_gradient_active_tier")
        or submitted.get("v4_real_gradient_market_tier")
        or tranche_index
    )
    var_price_field = "var_bid" if direction == DIRECTION_SHORT else "var_ask"
    lighter_price_field = (
        "lighter_buy_price" if direction == DIRECTION_SHORT else "lighter_sell_price"
    )
    lot = {
        "lot_id": pending.get("lot_id"),
        "basis_trace_id": submitted.get("basis_trace_id"),
        "episode_id": current.get("v4_episode_id"),
        "tranche_index": tranche_index,
        "signal_mode": "basis",
        "direction": direction,
        "qty": str(pending_qty),
        "entry_var_fill_price": str(inferred_var_fill),
        "entry_lighter_fill_price": str(lighter_fill_price),
        "entry_estimated_var_price": submitted.get(var_price_field),
        "entry_estimated_lighter_price": submitted.get(lighter_price_field),
        "entry_var_final_fill_qty": str(pending_qty),
        "entry_lighter_final_fill_qty": str(lighter_fill_qty),
        "entry_var_price_source": "aggregate_position_inference",
        "entry_lighter_price_source": "final_fill",
        "entry_cost_status": "final_fills_confirmed",
        "entry_edge_bps": submitted.get("edge_bps"),
        "entry_roundtrip_pnl_bps": submitted.get("roundtrip_pnl_bps"),
        "entry_basis_bps": submitted.get("basis_bps"),
        "entry_z": submitted.get("z"),
        "entry_normalized_edge_bps": submitted.get("normalized_edge_bps"),
        "entry_normalized_roundtrip_pnl_bps": submitted.get(
            "normalized_short_roundtrip_pnl_bps"
            if direction == DIRECTION_SHORT
            else "normalized_long_roundtrip_pnl_bps"
        ),
        "entry_stablecoin_edge_share": submitted.get(
            "short_stablecoin_edge_share"
            if direction == DIRECTION_SHORT
            else "long_stablecoin_edge_share"
        ),
        "entry_v4_profile": submitted.get("basis_v4_profile"),
        "entry_v4_threshold_bps": submitted.get("v4_entry_threshold_bps"),
        "entry_gradient_tier": active_tier,
        "entry_gradient_capacity_notional_usd": submitted.get(
            "v4_real_gradient_capacity_notional_usd"
        ),
        "entry_gradient_capacity_child_lots": submitted.get(
            "v4_real_gradient_capacity_child_lots"
        ),
        "entry_v4_baseline_window_seconds": submitted.get(
            "v4_baseline_window_seconds"
        ),
        "entry_var_side": str(pending.get("side") or "").upper(),
        "entry_var_order_quote_id": submitted.get("quote_id"),
        "entry_var_order_quote_bid": submitted.get("var_bid"),
        "entry_var_order_quote_ask": submitted.get("var_ask"),
        "entry_var_order_quote_timestamp": submitted.get("quote_timestamp"),
        "entry_var_order_quote_execution_price": submitted.get(var_price_field),
        "entry_var_submit_ms": submitted.get("var_submit_ms"),
        "entry_lighter_submit_ms": submitted.get("lighter_submit_ms"),
        "entry_lighter_record_key": lighter_record_key,
        "entry_lighter_payload": submitted.get("lighter_payload"),
        "entry_kind": "basis_v4_eth_short_p97_5",
        "entered_at": pending.get("submitted_at") or submitted.get("logged_at"),
        "entered_sample_index": submitted.get("sample_index"),
        "status": "open",
        "recovery_source": "strict_exchange_position_and_execution_log",
    }
    repaired = copy.deepcopy(current)
    repaired["status"] = "open"
    repaired["open_lots"] = sorted(
        [*open_lots, lot], key=lambda item: int(item.get("lot_id") or 0)
    )
    repaired["pending_actions"] = []
    repaired["next_lot_id"] = max(
        int(repaired.get("next_lot_id") or 1), int(pending.get("lot_id") or 0) + 1
    )
    repaired["v4_next_tranche_index"] = max(
        int(repaired.get("v4_next_tranche_index") or 1), tranche_index + 1
    )
    repaired["reason"] = "operator_recovered_interrupted_completed_entry"
    repaired["updated_at"] = datetime.now(timezone.utc).isoformat()
    repaired["state_mutation_revision"] = int(
        repaired.get("state_mutation_revision") or 0
    ) + 1
    repaired.pop("manual_review_reason", None)
    repaired.pop("manual_review_context", None)
    summary = {
        "asset": asset,
        "direction": direction,
        "existing_qty": str(existing_qty),
        "recovered_qty": str(pending_qty),
        "expected_total_qty": str(expected_total),
        "variational_position_qty": str(var_qty),
        "lighter_position_qty": str(lighter_qty),
        "recovered_var_fill_price": str(inferred_var_fill),
        "recovered_lighter_fill_price": str(lighter_fill_price),
        "recovered_lot_id": pending.get("lot_id"),
        "recovered_tranche_index": tranche_index,
        "recovered_gradient_tier": active_tier,
    }
    return repaired, summary


def _strategy_running() -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-af", r"python.*main\.py"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return False
    for line in result.stdout.splitlines():
        try:
            process_id = int(line.split(maxsplit=1)[0])
            process_cwd = Path(os.readlink(f"/proc/{process_id}/cwd")).resolve()
            process_cwd.relative_to(ROOT.resolve())
        except (IndexError, OSError, ValueError):
            continue
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recover a fully filled gradient lot left pending by an interrupted runtime."
    )
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--state-file", type=Path, default=LIVE_STATE)
    parser.add_argument("--orders-file", type=Path, default=ORDER_METRICS)
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if _strategy_running():
        raise SystemExit("repair=REFUSED reason=strategy_process_running")
    current = read_json(args.state_file)
    backup_path, interrupted = _find_pending_backup(args.state_file)
    pending_actions = interrupted.get("pending_actions")
    if not isinstance(pending_actions, list) or len(pending_actions) != 1:
        raise SystemExit("repair=REFUSED reason=backup_pending_action_count")
    pending = pending_actions[0]
    interrupted_run_id = str(interrupted.get("run_id") or "")
    rows = _recovery_rows_from_end(
        args.orders_file,
        limit=args.tail,
        lot_id=pending.get("lot_id"),
        lighter_record_key=str(pending.get("lighter_record_key") or ""),
        run_id=interrupted_run_id,
    )
    repaired, summary = build_repaired_state(
        current=current,
        interrupted=interrupted,
        rows=rows,
        asset=args.asset,
    )
    print("repair_validation=PASS")
    print(f"pending_backup={backup_path}")
    for key, value in summary.items():
        print(f"{key}={value}")
    if not args.apply:
        print("repair_apply=NO dry_run=true")
        return 0

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safety_backup = args.state_file.with_name(
        args.state_file.name + f".before_interrupted_entry_repair.{timestamp}.bak"
    )
    safety_backup.write_text(
        json.dumps(current, ensure_ascii=True, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.chmod(safety_backup, 0o600)
    write_json_atomic(args.state_file, repaired)
    os.chmod(args.state_file, 0o600)
    print("repair_apply=PASS")
    print(f"safety_backup={safety_backup}")
    print(f"open_lots={len(repaired['open_lots'])}")
    print("pending_actions=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
