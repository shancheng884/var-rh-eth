#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_pnl_volume import actual_pnl_four_leg_volume  # noqa: E402
from tools.lib.pnl_baseline import (  # noqa: E402
    PNL_BASELINE_FILE_NAME,
    beijing_day,
    load_pnl_baseline,
    new_pnl_baseline,
    record_pnl_cycle,
    roll_pnl_beijing_day,
    set_pnl_account_baseline,
    set_pnl_latest_account_snapshot,
    write_pnl_baseline,
)
from tools.pnl_report import (  # noqa: E402
    complete_snapshots,
    deduplicated_actual_pnl,
    load_rows,
    parse_time,
    to_decimal,
)

DEFAULT_LOG = ROOT / "log" / "order_metrics.jsonl"
DEFAULT_BASELINE = ROOT / "log" / PNL_BASELINE_FILE_NAME
DEFAULT_STATE = ROOT / "log" / "live_inventory_state.json"


@dataclass(frozen=True)
class HistoryPlan:
    first_run_at: str
    first_snapshot: dict[str, Any]
    latest_snapshot: dict[str, Any]
    cycles: list[dict[str, Any]]
    last_flat_residual_usd: Decimal | None
    digest: str


def _rh_live(row: dict[str, Any]) -> bool:
    return (
        row.get("mode") == "live"
        and row.get("execution_mode") == "live"
        and "robinhood-chain" in str(row.get("strategy_variant") or "")
    )


def _snapshot_time(row: dict[str, Any]) -> datetime | None:
    return parse_time(row.get("snapshot_captured_at") or row.get("logged_at"))


def _snapshot_equity(row: dict[str, Any]) -> Decimal:
    combined = to_decimal(row.get("combined_equity_usd"))
    variational = to_decimal(row.get("variational_equity_usd"))
    lighter = to_decimal(row.get("lighter_equity_usd"))
    if (
        combined is None
        or variational is None
        or lighter is None
        or not all(value.is_finite() for value in (combined, variational, lighter))
        or combined <= 0
        or abs(combined - variational - lighter) > Decimal("0.000001")
    ):
        raise ValueError("account snapshot has inconsistent two-venue equity")
    return combined


def plan_history(rows: list[dict[str, Any]]) -> HistoryPlan:
    foreign_closes = [
        row for row in rows
        if row.get("event") == "live_inventory_actual_pnl"
        and row.get("actual_pnl_status") == "lighter_final_fill_confirmed"
        and not _rh_live(row)
    ]
    if foreign_closes:
        raise ValueError("order log contains confirmed closes outside the RH live strategy")

    rh_rows = [row for row in rows if _rh_live(row)]
    run_times = [
        parse_time(row.get("logged_at"))
        for row in rh_rows
        if row.get("event") == "live_inventory_run_config"
    ]
    first_run = min((value for value in run_times if value is not None), default=None)
    if first_run is None:
        raise ValueError("RH live run configuration is missing")

    snapshots = complete_snapshots(rh_rows)
    flat_starts = [
        row for row in snapshots
        if row.get("snapshot_stage") == "startup_flat"
        and row.get("account_snapshot_flat") is True
        and _snapshot_time(row) is not None
        and _snapshot_time(row) >= first_run
    ]
    if not flat_starts:
        raise ValueError("first complete RH startup-flat account snapshot is missing")
    first_snapshot = flat_starts[0]
    snapshot_at = _snapshot_time(first_snapshot)
    assert snapshot_at is not None
    if beijing_day(snapshot_at) != beijing_day(first_run):
        raise ValueError("first complete flat account snapshot is not on RH start day")
    initial_equity = _snapshot_equity(first_snapshot)

    confirmed_rows = [
        row for row in rh_rows
        if row.get("event") == "live_inventory_actual_pnl"
        and row.get("actual_pnl_status") == "lighter_final_fill_confirmed"
    ]
    for row in confirmed_rows:
        pnl = to_decimal(row.get("actual_pnl_usd"))
        if (
            not row.get("run_id")
            or row.get("lot_id") is None
            or pnl is None
            or not pnl.is_finite()
            or parse_time(row.get("confirmed_at") or row.get("logged_at")) is None
        ):
            raise ValueError("confirmed RH close has incomplete identity, time, or PnL")
    cycles = deduplicated_actual_pnl(confirmed_rows)
    if len(cycles) > 5000:
        raise ValueError("more than 5000 closes exceed the baseline deduplication window")
    cycles.sort(key=lambda row: parse_time(row.get("confirmed_at") or row.get("logged_at")) or first_run)
    for row in cycles:
        observed = parse_time(row.get("confirmed_at") or row.get("logged_at"))
        if observed is None or observed < snapshot_at:
            raise ValueError("a confirmed close predates the first complete flat snapshot")
        volume = to_decimal(row.get("four_leg_volume_usd"))
        if volume is None or not volume.is_finite() or volume <= 0:
            volume = actual_pnl_four_leg_volume(row)
        if volume is None or not volume.is_finite() or volume <= 0:
            raise ValueError(f"four-leg volume cannot be verified for lot {row.get('lot_id')}")
        row["_verified_four_leg_volume_usd"] = str(volume)

    latest_snapshot = snapshots[-1]
    _snapshot_equity(latest_snapshot)
    last_flat = next(
        (row for row in reversed(snapshots) if row.get("account_snapshot_flat") is True),
        None,
    )
    residual = None
    if last_flat is not None:
        flat_time = _snapshot_time(last_flat)
        if flat_time is not None:
            pnl_through_flat = sum(
                (
                    to_decimal(row.get("actual_pnl_usd")) or Decimal("0")
                    for row in cycles
                    if (parse_time(row.get("confirmed_at") or row.get("logged_at")) or first_run)
                    <= flat_time
                ),
                Decimal("0"),
            )
            residual = _snapshot_equity(last_flat) - initial_equity - pnl_through_flat

    latest_snapshot_at = _snapshot_time(latest_snapshot)
    if latest_snapshot_at is None:
        raise ValueError("latest RH account snapshot has no valid timestamp")
    digest_payload = {
        "first_run_at": first_run.isoformat(),
        "first_snapshot_at": snapshot_at.isoformat(),
        "initial_equity_usd": str(initial_equity),
        "latest_snapshot_at": latest_snapshot_at.isoformat(),
        "latest_equity_usd": str(_snapshot_equity(latest_snapshot)),
        "last_flat_residual_usd": str(residual),
        "cycles": [
            {
                "run_id": row["run_id"],
                "lot_id": row["lot_id"],
                "confirmed_at": row.get("confirmed_at") or row.get("logged_at"),
                "actual_pnl_usd": row["actual_pnl_usd"],
                "four_leg_volume_usd": row["_verified_four_leg_volume_usd"],
            }
            for row in cycles
        ],
    }
    digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    return HistoryPlan(
        first_run_at=first_run.isoformat(),
        first_snapshot=first_snapshot,
        latest_snapshot=latest_snapshot,
        cycles=cycles,
        last_flat_residual_usd=residual,
        digest=digest,
    )


def build_baseline(plan: HistoryPlan, path: Path, *, observed_at: str) -> dict[str, Any]:
    write_pnl_baseline(
        path,
        new_pnl_baseline(
            asset="ETH",
            realized_pnl_usd="0",
            completed_cycles=0,
            started_at=plan.first_run_at,
        ),
    )
    started = plan.first_snapshot
    set_pnl_account_baseline(
        path,
        combined_equity_usd=_snapshot_equity(started),
        captured_at=str(started.get("snapshot_captured_at") or started.get("logged_at")),
    )
    for row in plan.cycles:
        record_pnl_cycle(
            path,
            run_id=str(row["run_id"]),
            asset="ETH",
            lot_id=row["lot_id"],
            actual_pnl_usd=row["actual_pnl_usd"],
            observed_at=row.get("confirmed_at") or row.get("logged_at"),
            four_leg_volume_usd=row["_verified_four_leg_volume_usd"],
            closed_child_lots=(
                row.get("closed_child_lots")
                or row.get("portfolio_component_lot_count")
                or 1
            ),
        )
    latest = plan.latest_snapshot
    set_pnl_latest_account_snapshot(
        path,
        variational_equity_usd=latest["variational_equity_usd"],
        lighter_equity_usd=latest["lighter_equity_usd"],
        combined_equity_usd=latest["combined_equity_usd"],
        captured_at=str(latest.get("snapshot_captured_at") or latest.get("logged_at")),
        account_snapshot_flat=bool(latest.get("account_snapshot_flat")),
    )
    baseline = load_pnl_baseline(path)
    assert baseline is not None
    rolled, _ = roll_pnl_beijing_day(baseline, observed_at=observed_at)
    write_pnl_baseline(path, rolled)
    result = load_pnl_baseline(path)
    assert result is not None
    return result


def strategy_running() -> bool:
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
            pid = int(line.split(maxsplit=1)[0])
            process_cwd = Path(os.readlink(f"/proc/{pid}/cwd")).resolve()
            process_cwd.relative_to(ROOT.resolve())
        except (IndexError, OSError, ValueError):
            continue
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild RH ETH reporting from verified account snapshots and final paired fills."
    )
    parser.add_argument("--log-path", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--baseline-path", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expect-digest")
    parser.add_argument("--i-confirm-no-unrecorded-transfers", action="store_true")
    args = parser.parse_args()
    if not args.log_path.exists():
        raise SystemExit("bootstrap=REFUSED reason=order_log_missing")
    if args.baseline_path.exists():
        raise SystemExit("bootstrap=REFUSED reason=baseline_already_exists")
    try:
        plan = plan_history(load_rows(args.log_path, asset="ETH"))
    except ValueError as exc:
        raise SystemExit(f"bootstrap=REFUSED reason={exc}") from exc

    with tempfile.TemporaryDirectory(prefix="rh-pnl-preview-") as directory:
        candidate = build_baseline(
            plan,
            Path(directory) / PNL_BASELINE_FILE_NAME,
            observed_at=datetime.now(timezone.utc).isoformat(),
        )
    print(f"rh_first_live_run_at={plan.first_run_at}")
    print(f"rh_starting_capital_usd={candidate['account_baseline_equity_usd']}")
    print(f"rh_account_baseline_at={candidate['account_baseline_at']}")
    print(f"confirmed_close_groups={candidate['tracked_completed_cycles']}")
    print(f"confirmed_pnl_usd={candidate['confirmed_pnl_usd']}")
    print(f"four_leg_volume_usd={candidate['confirmed_four_leg_volume_usd']}")
    print(f"latest_account_snapshot_at={candidate['latest_account_snapshot_at']}")
    print(f"latest_combined_equity_usd={candidate['latest_combined_equity_usd']}")
    print(f"last_flat_unexplained_change_usd={plan.last_flat_residual_usd}")
    print(f"history_digest={plan.digest}")
    if not args.apply:
        print("bootstrap=DRY_RUN")
        return 0
    if not args.expect_digest or args.expect_digest != plan.digest:
        raise SystemExit("bootstrap=REFUSED reason=preview_digest_mismatch")
    if not args.i_confirm_no_unrecorded_transfers:
        raise SystemExit("bootstrap=REFUSED reason=transfer_history_not_confirmed")
    if strategy_running():
        raise SystemExit("bootstrap=REFUSED reason=strategy_running")
    if not args.state_path.exists():
        raise SystemExit("bootstrap=REFUSED reason=local_state_missing")
    try:
        state = json.loads(args.state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit("bootstrap=REFUSED reason=local_state_invalid") from exc
    if (
        not isinstance(state, dict)
        or state.get("asset") != "ETH"
        or state.get("status") not in {"open", "manual_review_required"}
        or state.get("pending_actions")
    ):
        raise SystemExit("bootstrap=REFUSED reason=local_state_not_safe_for_open_position_history")
    if plan.last_flat_residual_usd is not None and abs(plan.last_flat_residual_usd) > Decimal("1"):
        raise SystemExit("bootstrap=REFUSED reason=unexplained_account_change_requires_cashflow_review")
    if args.baseline_path.exists():
        raise SystemExit("bootstrap=REFUSED reason=baseline_created_during_preview")
    write_pnl_baseline(args.baseline_path, candidate)
    print(f"bootstrap=APPLIED path={args.baseline_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
