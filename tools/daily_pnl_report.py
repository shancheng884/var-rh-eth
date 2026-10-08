#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.pnl_baseline import (  # noqa: E402
    BEIJING_TIMEZONE,
    PNL_BASELINE_FILE_NAME,
    beijing_calendar_days,
    load_pnl_baseline,
    parse_timestamp,
    pnl_day_summary,
)
from tools.lib.account_equity_ledger import (  # noqa: E402
    VAR_EQUITY_FORMULA_VERSION,
    complete_beijing_equity_day,
    load_account_equity_state,
    read_fresh_account_equity,
    record_account_equity_sample,
)
from tools.lib.runtime_files import (  # noqa: E402
    JSONL_TAIL_MAX_LINE_BYTES,
    ORDER_METRICS,
    rotated_jsonl_paths,
)
from tools.lib.platform_pnl import (  # noqa: E402
    DEFAULT_LEDGER_PATH as DEFAULT_PLATFORM_LEDGER,
    DEFAULT_START as DEFAULT_PLATFORM_START,
    aggregate_period,
    load_platform_ledger,
    platform_source_completeness,
    sync_platform_ledger,
    weighted_capital,
)
from tools.lib.telegram_notifier import (  # noqa: E402
    TelegramNotifier,
    format_telegram_trade_message,
)


DEFAULT_BASELINE = ROOT / "log" / PNL_BASELINE_FILE_NAME
DEFAULT_SEND_STATE = ROOT / "log" / "pnl_daily_telegram_state.json"
DEFAULT_EQUITY_STATE = ROOT / "log" / "account_equity_daily_state.json"
DEFAULT_RISK_HEALTH = ROOT / "log" / "live_inventory_risk_health.json"


def build_platform_activity_payload(
    ledger: dict[str, Any],
    *,
    asset: str,
    day: date,
    equity_state: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    start = datetime.combine(day, datetime.min.time(), tzinfo=BEIJING_TIMEZONE)
    next_midnight = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=BEIJING_TIMEZONE)
    start_utc = start.astimezone(timezone.utc)
    end = min(current.astimezone(timezone.utc), next_midnight.astimezone(timezone.utc))
    if end <= start_utc:
        raise ValueError("cannot report a future Beijing date")
    daily = aggregate_period(ledger, start_utc, end)
    statistics_start = parse_timestamp(ledger.get("statistics_start") or DEFAULT_PLATFORM_START)
    if statistics_start is None:
        raise RuntimeError("platform PnL statistics start is invalid")
    cumulative = aggregate_period(ledger, statistics_start, end)
    average_capital, current_capital = weighted_capital(ledger, statistics_start, end)
    elapsed_days = Decimal(str((end - statistics_start).total_seconds())) / Decimal("86400")
    annualized = (
        cumulative["net_pnl_usd"] / average_capital * Decimal("100") * Decimal("365") / elapsed_days
        if average_capital > 0 and elapsed_days > 0 else None
    )

    complete, source_detail = platform_source_completeness(
        ledger, now=current, required_through=end
    )

    return {
        "summary_scope": "realized_platform_activity",
        "summary_status": "complete" if complete else "partial",
        "asset": asset.upper(),
        "beijing_day": day.isoformat(),
        "daily_trade_count": daily["trade_count"],
        "daily_volume_usd": str(daily["volume_usd"]),
        "daily_realized_pnl_usd": str(daily["realized_pnl_usd"]),
        "daily_funding_usd": str(daily["funding_usd"]),
        "daily_net_pnl_usd": str(daily["net_pnl_usd"]),
        "cumulative_trade_count": cumulative["trade_count"],
        "cumulative_volume_usd": str(cumulative["volume_usd"]),
        "cumulative_realized_pnl_usd": str(cumulative["realized_pnl_usd"]),
        "cumulative_funding_usd": str(cumulative["funding_usd"]),
        "cumulative_net_pnl_usd": str(cumulative["net_pnl_usd"]),
        "annualized_simple_pct": str(annualized) if complete and annualized is not None else None,
        "statistics_start_day": statistics_start.astimezone(BEIJING_TIMEZONE).date().isoformat(),
        "capital_usd": str(current_capital),
        "initial_capital_usd": str(ledger.get("starting_capital_usd") or "241.774564"),
        "variational_equity_usd": equity_state.get("latest_variational_equity_usd"),
        "lighter_equity_usd": equity_state.get("latest_lighter_equity_usd"),
        "combined_equity_usd": equity_state.get("latest_combined_equity_usd"),
        "account_snapshot_at": equity_state.get("last_sample_at"),
        "external_cashflow_usd": str(cumulative["cashflow_usd"]),
        "source_status": "complete" if complete else "partial",
        "source_detail": source_detail,
    }


def decimal_value(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value)) if value not in (None, "") else None
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result is not None and result.is_finite() else None


def _realized_snapshot(row: dict[str, Any]) -> dict[str, Any] | None:
    if (
        row.get("event") != "live_inventory_account_snapshot"
        or row.get("snapshot_status") != "complete"
        or row.get("snapshot_errors") not in ({}, None)
    ):
        return None
    captured = parse_timestamp(row.get("snapshot_captured_at") or row.get("logged_at"))
    var_balance = decimal_value(row.get("variational_balance_usd"))
    var_upnl = decimal_value(row.get("variational_upnl_usd"))
    var_equity = decimal_value(row.get("variational_equity_usd"))
    rh_balance = decimal_value(row.get("lighter_collateral_usd"))
    rh_upnl = decimal_value(row.get("lighter_unrealized_pnl_usd"))
    rh_equity = decimal_value(row.get("lighter_equity_usd"))
    combined = decimal_value(row.get("combined_equity_usd"))
    if (
        captured is None
        or None in {var_balance, var_upnl, var_equity, rh_balance, rh_equity, combined}
        or abs(var_equity + rh_equity - combined) > Decimal("0.05")
        or rh_upnl is not None
        and abs(rh_equity - rh_balance - rh_upnl) > Decimal("0.05")
    ):
        return None
    return {
        "captured_at": captured.isoformat(),
        "variational_equity_usd": str(var_equity),
        "lighter_equity_usd": str(rh_equity),
        "combined_equity_usd": str(combined),
        "variational_realized_balance_usd": str(var_balance - var_upnl),
        "lighter_realized_balance_usd": str(rh_balance),
        "combined_realized_balance_usd": str(var_balance - var_upnl + rh_balance),
    }


def _scan_order_metrics(
    *, asset: str, tracking_start: datetime | None, day_start: datetime, day_end: datetime
) -> tuple[dict[str, Any] | None, dict[str, Any], list[dict[str, Any]]]:
    earliest: tuple[datetime, dict[str, Any]] | None = None
    totals: dict[str, Any] = {
        "daily_trade_count": 0,
        "daily_volume_usd": Decimal("0"),
        "cumulative_trade_count": 0,
        "cumulative_volume_usd": Decimal("0"),
    }
    snapshots: list[dict[str, Any]] = []
    for path in rotated_jsonl_paths(ORDER_METRICS):
        opener = gzip.open if path.suffix == ".gz" else open
        try:
            with opener(path, "rb") as handle:
                while True:
                    line = handle.readline(JSONL_TAIL_MAX_LINE_BYTES + 1)
                    if not line:
                        break
                    if len(line) > JSONL_TAIL_MAX_LINE_BYTES:
                        while line and not line.endswith(b"\n"):
                            line = handle.readline(JSONL_TAIL_MAX_LINE_BYTES + 1)
                        continue
                    try:
                        row = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if not isinstance(row, dict) or str(row.get("asset") or "").upper() != asset.upper():
                        continue
                    event = row.get("event")
                    if event == "live_inventory_account_snapshot":
                        snapshot = _realized_snapshot(row)
                        snap_at = parse_timestamp(snapshot.get("captured_at")) if snapshot else None
                        if snapshot is not None and snap_at is not None:
                            snapshots.append(snapshot)
                            if earliest is None or snap_at < earliest[0]:
                                earliest = (snap_at, snapshot)
                    at = parse_timestamp(row.get("logged_at"))
                    if at is None:
                        continue
                    if event == "live_inventory_entered":
                        qty = decimal_value(row.get("qty"))
                        var_price = decimal_value(row.get("var_price"))
                        rh_price = decimal_value(row.get("lighter_price"))
                        if None in {qty, var_price, rh_price}:
                            continue
                        volume = abs(qty) * (var_price + rh_price)
                        trade_count = 1
                    elif (
                        event == "live_inventory_actual_pnl"
                        and row.get("actual_pnl_status") == "lighter_final_fill_confirmed"
                    ):
                        var_qty = decimal_value(row.get("exit_var_final_fill_qty"))
                        var_price = decimal_value(
                            row.get("exit_var_final_fill_price") or row.get("exit_var_price")
                        )
                        rh_qty = decimal_value(row.get("exit_lighter_final_fill_qty"))
                        rh_price = decimal_value(row.get("exit_lighter_final_fill_price"))
                        if None in {var_qty, var_price, rh_qty, rh_price}:
                            continue
                        volume = abs(var_qty * var_price) + abs(rh_qty * rh_price)
                        try:
                            trade_count = max(1, int(row.get("closed_child_lots") or 1))
                        except (TypeError, ValueError):
                            trade_count = 1
                    else:
                        continue
                    if tracking_start is not None and at >= tracking_start:
                        totals["cumulative_trade_count"] += trade_count
                        totals["cumulative_volume_usd"] += volume
                    if (
                        day_start <= at < day_end
                        and (tracking_start is None or at >= tracking_start)
                    ):
                        totals["daily_trade_count"] += trade_count
                        totals["daily_volume_usd"] += volume
        except OSError:
            continue
    snapshots.sort(key=lambda item: str(item["captured_at"]))
    return (earliest[1] if earliest else None), totals, snapshots


def _tracking_baseline(
    equity_state: dict[str, Any], earliest_snapshot: dict[str, Any] | None
) -> dict[str, Any] | None:
    existing = equity_state.get("realized_tracking")
    if isinstance(existing, dict) and decimal_value(
        existing.get("start_realized_balance_usd")
    ) is not None:
        return existing
    if earliest_snapshot is None:
        return None
    baseline = {
        "start_at": earliest_snapshot["captured_at"],
        "start_equity_usd": earliest_snapshot["combined_equity_usd"],
        "start_variational_realized_balance_usd": earliest_snapshot[
            "variational_realized_balance_usd"
        ],
        "start_lighter_realized_balance_usd": earliest_snapshot[
            "lighter_realized_balance_usd"
        ],
        "start_realized_balance_usd": earliest_snapshot[
            "combined_realized_balance_usd"
        ],
    }
    equity_state["realized_tracking"] = baseline
    return baseline


def _cashflows_between(
    baseline: dict[str, Any], ledger: dict[str, Any], start: datetime, end: datetime
) -> Decimal:
    events: dict[str, Decimal] = {}
    candidates = list(baseline.get("external_cashflow_events") or [])
    candidates.extend(
        event for event in ledger.get("events", [])
        if isinstance(event, dict) and event.get("kind") == "cashflow"
    )
    for index, event in enumerate(candidates):
        if not isinstance(event, dict):
            continue
        at = parse_timestamp(event.get("observed_at") or event.get("timestamp"))
        amount = decimal_value(event.get("amount_usd"))
        if at is None or amount is None or not (start <= at < end):
            continue
        key = str(event.get("event_id") or f"{at.isoformat()}:{amount}")
        events[key] = amount
    return sum(events.values(), Decimal("0"))


def _cashflow_sources_verified(ledger: dict[str, Any]) -> bool:
    sources = ledger.get("source_status") or {}
    var = sources.get("variational") or {}
    rh = sources.get("rh") or {}
    rh_pages = rh.get("page_status") or {}
    return bool(
        var.get("coverage_manifest_valid") is True
        and var.get("status") == "available"
        and rh.get("status") == "available"
        and rh_pages
        and all(
            value.get("complete") is True
            for value in rh_pages.values()
            if isinstance(value, dict)
        )
    )


def build_realized_balance_payload(
    baseline: dict[str, Any],
    *,
    equity_state: dict[str, Any],
    platform_ledger: dict[str, Any],
    asset: str,
    day: date,
    now: datetime | None = None,
) -> dict[str, Any]:
    observed = now or datetime.now(timezone.utc)
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=BEIJING_TIMEZONE).astimezone(timezone.utc)
    day_end = min(
        day_start + timedelta(days=1), observed.astimezone(timezone.utc)
    )
    tracking = _tracking_baseline(equity_state, None)
    tracking_start = parse_timestamp(
        tracking.get("start_at") if isinstance(tracking, dict) else None
    )
    earliest, activity, snapshots = _scan_order_metrics(
        asset=asset,
        tracking_start=tracking_start,
        day_start=day_start,
        day_end=day_end,
    )
    if tracking is None:
        tracking = _tracking_baseline(equity_state, earliest)
        discovered_start = parse_timestamp(
            tracking.get("start_at") if isinstance(tracking, dict) else None
        )
        if discovered_start is not None:
            _, activity, snapshots = _scan_order_metrics(
                asset=asset,
                tracking_start=discovered_start,
                day_start=day_start,
                day_end=day_end,
            )
    if tracking is None:
        return {
            "summary_scope": "realized_balance_daily",
            "summary_status": "unavailable",
            "asset": asset.upper(),
            "beijing_day": day.isoformat(),
            "pnl_data_reason": "complete_balance_snapshot_missing",
        }
    start_at = parse_timestamp(tracking.get("start_at"))
    if start_at is None:
        raise RuntimeError("realized balance tracking start is invalid")
    if day_end <= start_at:
        return {
            "summary_scope": "realized_balance_daily",
            "summary_status": "unavailable",
            "asset": asset.upper(),
            "beijing_day": day.isoformat(),
            "pnl_data_reason": "requested_day_precedes_first_complete_balance_snapshot",
        }
    if not snapshots:
        snapshots = []

    history = equity_state.get("realized_daily_history") or {}
    day_record = history.get(day.isoformat()) or {}
    points: dict[str, dict[str, Any]] = {
        str(row["captured_at"]): row
        for row in snapshots
        if parse_timestamp(row.get("captured_at")) is not None
        and day_start <= parse_timestamp(row.get("captured_at")) < day_end
    }
    first_history_at = parse_timestamp(day_record.get("first_sample_at"))
    last_history_at = parse_timestamp(day_record.get("latest_sample_at"))
    if first_history_at is not None and day_record.get("start_realized_balance_usd") is not None:
        points[first_history_at.isoformat()] = {
            "captured_at": first_history_at.isoformat(),
            "combined_realized_balance_usd": day_record.get("start_realized_balance_usd"),
            "combined_equity_usd": day_record.get("start_equity_usd"),
            "variational_equity_usd": None,
            "lighter_equity_usd": None,
        }
    if last_history_at is not None and day_record.get("latest_realized_balance_usd") is not None:
        points[last_history_at.isoformat()] = {
            "captured_at": last_history_at.isoformat(),
            "combined_realized_balance_usd": day_record.get("latest_realized_balance_usd"),
            "combined_equity_usd": day_record.get("latest_equity_usd"),
            "variational_equity_usd": day_record.get("latest_variational_equity_usd"),
            "lighter_equity_usd": day_record.get("latest_lighter_equity_usd"),
        }
    ordered_points = sorted(
        points.values(), key=lambda row: str(row.get("captured_at") or "")
    )
    first_point = ordered_points[0] if ordered_points else None
    latest_point = ordered_points[-1] if ordered_points else None
    first_realized = decimal_value(
        first_point.get("combined_realized_balance_usd") if first_point else None
    )
    latest_realized = decimal_value(
        latest_point.get("combined_realized_balance_usd") if latest_point else None
    )
    observed_daily_change = (
        latest_realized - first_realized
        if len(ordered_points) >= 2
        and first_realized is not None
        and latest_realized is not None
        else None
    )
    max_gap = Decimal("0")
    times = [parse_timestamp(row.get("captured_at")) for row in ordered_points]
    times = [value for value in times if value is not None]
    if len(times) > 1:
        max_gap = max(
            Decimal(str((right - left).total_seconds()))
            for left, right in zip(times, times[1:])
        )
    recorded_gap = decimal_value(day_record.get("max_sample_gap_seconds"))
    if recorded_gap is not None:
        max_gap = max(max_gap, recorded_gap)
    day_complete = bool(
        day_record.get("sample_count", 0) >= 2
        and day_record.get("coverage_complete") is True
        and complete_beijing_equity_day(
            {
                "first_sample_at": day_record.get("first_sample_at"),
                "latest_sample_at": day_record.get("latest_sample_at"),
                "sample_count": day_record.get("sample_count"),
                "max_sample_gap_seconds": day_record.get("max_sample_gap_seconds"),
            },
            day.isoformat(),
        )
    )
    daily_cashflow = _cashflows_between(baseline, platform_ledger, day_start, day_end)
    daily_pnl = (
        observed_daily_change - daily_cashflow
        if observed_daily_change is not None
        else None
    )
    latest_captured = parse_timestamp(equity_state.get("last_sample_at"))
    latest_realized_captured = parse_timestamp(
        equity_state.get("latest_realized_sample_at")
    )
    latest_total = decimal_value(equity_state.get("latest_combined_equity_usd"))
    latest_var_equity = equity_state.get("latest_variational_equity_usd")
    latest_rh_equity = equity_state.get("latest_lighter_equity_usd")
    last_snapshot = snapshots[-1] if snapshots else None
    snapshot_at = parse_timestamp(last_snapshot.get("captured_at")) if last_snapshot else None
    if last_snapshot and (
        latest_captured is None
        or snapshot_at is not None and snapshot_at > latest_captured
    ):
        latest_captured = snapshot_at
        latest_total = decimal_value(last_snapshot.get("combined_equity_usd"))
        latest_var_equity = last_snapshot.get("variational_equity_usd")
        latest_rh_equity = last_snapshot.get("lighter_equity_usd")
    if latest_captured is None or latest_total is None:
        if last_snapshot:
            latest_captured = snapshot_at
            latest_total = decimal_value(last_snapshot.get("combined_equity_usd"))
            latest_var_equity = last_snapshot.get("variational_equity_usd")
            latest_rh_equity = last_snapshot.get("lighter_equity_usd")
    latest_realized = decimal_value(
        equity_state.get("latest_combined_realized_balance_usd")
    ) or decimal_value(
        (history.get(
            (latest_realized_captured or latest_captured).astimezone(
                BEIJING_TIMEZONE
            ).date().isoformat()
        ) or {}).get(
            "latest_realized_balance_usd"
        )
        if latest_realized_captured is not None or latest_captured is not None
        else None
    )
    if last_snapshot and snapshot_at is not None and (
        latest_realized_captured is None or snapshot_at > latest_realized_captured
    ):
        latest_realized_captured = snapshot_at
        latest_realized = decimal_value(
            last_snapshot.get("combined_realized_balance_usd")
        )
    start_realized = decimal_value(tracking.get("start_realized_balance_usd"))
    cumulative_cashflow = _cashflows_between(
        baseline, platform_ledger, start_at, latest_realized_captured or latest_captured or observed
    )
    cumulative_pnl = (
        latest_realized - start_realized - cumulative_cashflow
        if latest_realized is not None and start_realized is not None
        else None
    )
    capital = decimal_value(tracking.get("start_equity_usd"))
    elapsed_days = (
        Decimal(str(((latest_realized_captured or latest_captured or observed) - start_at).total_seconds()))
        / Decimal("86400")
    )
    cumulative_return = (
        cumulative_pnl / capital * Decimal("100")
        if cumulative_pnl is not None and capital is not None and capital > 0
        else None
    )
    annualized = (
        cumulative_return * Decimal("365") / elapsed_days
        if cumulative_return is not None and elapsed_days > 0
        else None
    )
    cashflow_verified = _cashflow_sources_verified(platform_ledger)
    status = "complete" if day_complete and cashflow_verified else "partial"
    tracking_day = start_at.astimezone(BEIJING_TIMEZONE).date().isoformat()
    return {
        "summary_scope": "realized_balance_daily",
        "summary_status": status,
        "asset": asset.upper(),
        "beijing_day": day.isoformat(),
        "daily_trade_count": activity["daily_trade_count"],
        "daily_volume_usd": str(activity["daily_volume_usd"]),
        "cumulative_trade_count": activity["cumulative_trade_count"],
        "cumulative_volume_usd": str(activity["cumulative_volume_usd"]),
        "daily_net_pnl_usd": str(daily_pnl) if daily_pnl is not None else None,
        "observed_daily_change_usd": str(observed_daily_change) if observed_daily_change is not None else None,
        "daily_return_pct": str(daily_pnl / capital * Decimal("100")) if daily_pnl is not None and capital else None,
        "cumulative_net_pnl_usd": str(cumulative_pnl) if cumulative_pnl is not None else None,
        "annualized_simple_pct": str(annualized) if annualized is not None else None,
        "statistics_start_day": tracking_day,
        "statistics_start_at": tracking.get("start_at"),
        "capital_usd": str(capital) if capital is not None else None,
        "external_cashflow_usd": str(cumulative_cashflow),
        "cashflow_verified": cashflow_verified,
        "daily_sample_start_at": first_point.get("captured_at") if first_point else None,
        "daily_sample_end_at": latest_point.get("captured_at") if latest_point else None,
        "daily_max_sample_gap_seconds": str(max_gap),
        "daily_coverage_complete": day_complete,
        "combined_equity_usd": str(latest_total) if latest_total is not None else None,
        "variational_equity_usd": latest_var_equity,
        "lighter_equity_usd": latest_rh_equity,
        "account_snapshot_at": latest_captured.isoformat() if latest_captured else None,
        "realized_balance_snapshot_at": (
            latest_realized_captured.isoformat()
            if latest_realized_captured
            else None
        ),
        "realized_pnl_basis": "change_in_var_balance_minus_upnl_plus_rh_collateral",
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def reset_realized_tracking_baseline(
    *,
    asset: str,
    equity_state_path: Path,
    risk_health_path: Path,
    apply: bool,
) -> int:
    if asset.upper() != "ETH":
        raise RuntimeError("cumulative baseline reset currently supports ETH only")
    sample, reason = read_fresh_account_equity(
        risk_health_path,
        asset=asset,
    )
    if sample is None:
        raise RuntimeError(f"fresh complete account snapshot unavailable: {reason}")

    state = load_account_equity_state(equity_state_path)
    health_at = parse_timestamp(sample.get("captured_at"))
    state_at = parse_timestamp(state.get("latest_realized_sample_at"))
    if state_at is not None and health_at is not None and state_at > health_at:
        state_age = (datetime.now(timezone.utc) - state_at).total_seconds()
        state_realized_values = {
            "variational_realized_balance_usd": state.get(
                "latest_variational_realized_balance_usd"
            ),
            "lighter_realized_balance_usd": state.get(
                "latest_lighter_realized_balance_usd"
            ),
            "combined_realized_balance_usd": state.get(
                "latest_combined_realized_balance_usd"
            ),
        }
        if (
            0 <= state_age <= 60
            and state.get("variational_equity_formula_version")
            == VAR_EQUITY_FORMULA_VERSION
            and all(decimal_value(value) is not None for value in state_realized_values.values())
            and abs(
                decimal_value(state_realized_values["variational_realized_balance_usd"])
                + decimal_value(state_realized_values["lighter_realized_balance_usd"])
                - decimal_value(state_realized_values["combined_realized_balance_usd"])
            ) <= Decimal("0.01")
        ):
            sample = {
                "captured_at": state_at.isoformat(),
                "variational_equity_usd": state["latest_variational_equity_usd"],
                "lighter_equity_usd": state["latest_lighter_equity_usd"],
                "combined_equity_usd": state["latest_combined_equity_usd"],
                "variational_equity_formula_version": state.get(
                    "variational_equity_formula_version"
                ),
                **state_realized_values,
            }

    realized_var = decimal_value(sample.get("variational_realized_balance_usd"))
    realized_rh = decimal_value(sample.get("lighter_realized_balance_usd"))
    realized_total = decimal_value(sample.get("combined_realized_balance_usd"))
    total_equity = decimal_value(sample.get("combined_equity_usd"))
    captured_at = parse_timestamp(sample.get("captured_at"))
    if (
        None in {realized_var, realized_rh, realized_total, total_equity, captured_at}
        or total_equity <= 0
        or abs(realized_var + realized_rh - realized_total) > Decimal("0.01")
    ):
        raise RuntimeError("latest snapshot lacks consistent realized balances")

    new_tracking = {
        "start_at": captured_at.isoformat(),
        "start_equity_usd": str(total_equity),
        "start_variational_realized_balance_usd": str(realized_var),
        "start_lighter_realized_balance_usd": str(realized_rh),
        "start_realized_balance_usd": str(realized_total),
    }
    print("reset_start_at=" + new_tracking["start_at"])
    print("reset_starting_capital_usd=" + new_tracking["start_equity_usd"])
    print("reset_start_realized_balance_usd=" + new_tracking["start_realized_balance_usd"])
    if not apply:
        print("cumulative_reset=DRY_RUN add --apply-cumulative-reset to apply")
        return 0

    backup_path: Path | None = None
    if equity_state_path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = equity_state_path.with_name(
            f"{equity_state_path.name}.before_cumulative_reset.{stamp}.bak"
        )
        if backup_path.exists():
            backup_path = equity_state_path.with_name(
                f"{equity_state_path.name}.before_cumulative_reset.{stamp}.{os.getpid()}.bak"
            )
        shutil.copy2(equity_state_path, backup_path)

    latest_at = parse_timestamp(state.get("last_sample_at"))
    if latest_at is None or captured_at > latest_at:
        state = record_account_equity_sample(equity_state_path, sample)
    else:
        state = load_account_equity_state(equity_state_path)

    reset_history = list(state.get("realized_tracking_reset_history") or [])
    previous = state.get("realized_tracking")
    if isinstance(previous, dict):
        reset_history.append(
            {
                "reset_at": datetime.now(timezone.utc).isoformat(),
                "previous_tracking": previous,
            }
        )
    state["realized_tracking"] = new_tracking
    state["realized_tracking_reset_history"] = reset_history[-20:]
    write_json_atomic(equity_state_path, state)
    print("backup=" + (str(backup_path) if backup_path else "none_new_state_file"))
    print("cumulative_reset=PASS")
    return 0


def load_send_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "sent_keys": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema_version": 1, "sent_keys": []}
    return value if isinstance(value, dict) else {"schema_version": 1, "sent_keys": []}


def resolve_day(value: str | None, *, now: datetime | None = None) -> date:
    observed = (now or datetime.now(timezone.utc)).astimezone(BEIJING_TIMEZONE)
    if value in (None, "yesterday"):
        return observed.date() - timedelta(days=1)
    if value == "today":
        return observed.date()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--day must be today, yesterday, or YYYY-MM-DD"
        ) from exc


def build_daily_payload(
    baseline: dict[str, Any],
    *,
    asset: str,
    day: date,
    now: datetime | None = None,
    equity_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if baseline.get("return_basis") == "account_equity_delta":
        return build_account_equity_payload(
            baseline,
            equity_state=equity_state or {},
            asset=asset,
            day=day,
            now=now,
        )
    record = pnl_day_summary(baseline, day.isoformat())
    capital = decimal_value(os.getenv("PNL_REPORT_CAPITAL_USD"))
    capital_source = "PNL_REPORT_CAPITAL_USD"
    if capital is None or capital <= 0:
        capital = decimal_value(baseline.get("account_baseline_equity_usd"))
        capital_source = "tracking_account_baseline"
    daily_pnl = decimal_value(record.get("confirmed_pnl_usd")) or Decimal("0")
    cumulative_pnl = (
        decimal_value(baseline.get("confirmed_pnl_usd")) or Decimal("0")
    )
    daily_return = (
        daily_pnl / capital * Decimal("100")
        if capital is not None and capital > 0
        else None
    )
    cumulative_return = (
        cumulative_pnl / capital * Decimal("100")
        if capital is not None and capital > 0
        else None
    )
    observed = now or datetime.now(timezone.utc)
    covered_days = beijing_calendar_days(
        parse_timestamp(
            baseline.get("account_baseline_at") or baseline.get("started_at")
        ),
        observed,
    )
    cumulative_annualized = (
        cumulative_return * Decimal("365") / covered_days
        if cumulative_return is not None
        and covered_days is not None
        and covered_days > 0
        else None
    )
    return {
        "asset": asset.upper(),
        "summary_scope": "beijing_daily",
        "summary_status": "complete",
        "beijing_day": day.isoformat(),
        "reporting_timezone": "Asia/Shanghai",
        "daily_closed_child_lots": int(record.get("closed_child_lots") or 0),
        "daily_completed_close_groups": int(
            record.get("tracked_completed_cycles") or 0
        ),
        "daily_four_leg_volume_usd": str(
            record.get("four_leg_volume_usd") or "0"
        ),
        "beijing_day_actual_pnl_usd": str(daily_pnl),
        "beijing_day_return_pct": str(daily_return) if daily_return is not None else None,
        "daily_annualized_simple_pct": (
            str(daily_return * Decimal("365"))
            if daily_return is not None
            else None
        ),
        "run_actual_pnl_usd": str(cumulative_pnl),
        "cumulative_four_leg_volume_usd": str(
            baseline.get("confirmed_four_leg_volume_usd") or "0"
        ),
        "cumulative_closed_child_lots": int(
            baseline.get("tracked_closed_child_lots") or 0
        ),
        "return_pct": (
            str(cumulative_return) if cumulative_return is not None else None
        ),
        "annualized_simple_pct": (
            str(cumulative_annualized)
            if cumulative_annualized is not None
            else None
        ),
        "covered_beijing_days": (
            str(covered_days) if covered_days is not None else None
        ),
        "capital_usd": str(capital) if capital is not None else None,
        "capital_source": capital_source if capital is not None else "unavailable",
        "variational_equity_usd": baseline.get(
            "latest_variational_equity_usd"
        ),
        "lighter_equity_usd": baseline.get("latest_lighter_equity_usd"),
        "combined_equity_usd": baseline.get("latest_combined_equity_usd"),
        "account_snapshot_at": baseline.get("latest_account_snapshot_at"),
        "return_pnl_source": "beijing_daily_confirmed_pair_fills",
        "annualized_reliability": "beijing_daily_projection",
    }


def build_account_equity_payload(
    baseline: dict[str, Any],
    *,
    equity_state: dict[str, Any],
    asset: str,
    day: date,
    now: datetime | None = None,
) -> dict[str, Any]:
    history = equity_state.get("daily_history") or {}
    record = history.get(day.isoformat()) or {}
    fill_record = pnl_day_summary(baseline, day.isoformat())
    configured_start = parse_timestamp(DEFAULT_PLATFORM_START)
    if configured_start is None:
        raise RuntimeError("configured account-equity statistics start is invalid")
    statistics_start_day = configured_start.astimezone(BEIJING_TIMEZONE).date()
    tracking_record = history.get(statistics_start_day.isoformat()) or {}
    capital = decimal_value(tracking_record.get("start_equity_usd"))
    tracking_started_at = parse_timestamp(tracking_record.get("first_sample_at"))
    daily_start = decimal_value(record.get("start_equity_usd"))
    daily_latest = decimal_value(record.get("latest_equity_usd"))
    sample_count = int(record.get("sample_count") or 0)
    observed_period_change = (
        daily_latest - daily_start
        if sample_count >= 2 and daily_start is not None and daily_latest is not None
        else None
    )
    complete_day = complete_beijing_equity_day(record, day.isoformat())
    equity_state_at = parse_timestamp(equity_state.get("last_sample_at"))
    baseline_at = parse_timestamp(baseline.get("latest_account_snapshot_at"))
    state_total = decimal_value(equity_state.get("latest_combined_equity_usd"))
    baseline_total = decimal_value(baseline.get("latest_combined_equity_usd"))
    use_baseline_snapshot = baseline_total is not None and (
        baseline_at is not None
        and (equity_state_at is None or baseline_at > equity_state_at)
    )
    if use_baseline_snapshot:
        latest_total = baseline_total
        current_sample_at = baseline_at
        latest_variational = baseline.get("latest_variational_equity_usd")
        latest_lighter = baseline.get("latest_lighter_equity_usd")
    else:
        latest_total = state_total if state_total is not None else baseline_total
        current_sample_at = equity_state_at or baseline_at
        latest_variational = (
            equity_state.get("latest_variational_equity_usd")
            or baseline.get("latest_variational_equity_usd")
        )
        latest_lighter = (
            equity_state.get("latest_lighter_equity_usd")
            or baseline.get("latest_lighter_equity_usd")
        )
    observed = (
        now
        or current_sample_at
        or datetime.now(timezone.utc)
    )
    daily_period_start = datetime.combine(
        day, datetime.min.time(), tzinfo=BEIJING_TIMEZONE
    ).astimezone(timezone.utc)
    daily_period_end = daily_period_start + timedelta(days=1)
    if observed.astimezone(timezone.utc) < daily_period_end:
        daily_period_end = observed.astimezone(timezone.utc)
    daily_cashflow = _registered_cashflow_between(
        baseline, daily_period_start, daily_period_end
    )
    daily_pnl = (
        observed_period_change - daily_cashflow
        if complete_day and observed_period_change is not None and daily_cashflow is not None
        else None
    )
    cumulative_cashflow = (
        _registered_cashflow_between(baseline, tracking_started_at, current_sample_at)
        if tracking_started_at is not None and current_sample_at is not None
        else None
    )
    cumulative_pnl = (
        latest_total - capital - cumulative_cashflow
        if latest_total is not None and capital is not None and cumulative_cashflow is not None
        else None
    )
    adjusted_capital = (
        capital + cumulative_cashflow
        if capital is not None and cumulative_cashflow is not None
        else None
    )
    daily_capital = (
        daily_start + daily_cashflow
        if daily_start is not None and daily_cashflow is not None
        else None
    )
    daily_return = (
        daily_pnl / daily_capital * Decimal("100")
        if daily_pnl is not None and daily_capital is not None and daily_capital > 0
        else None
    )
    daily_annualized = daily_return * Decimal("365") if daily_return is not None else None
    cumulative_return = (
        cumulative_pnl / adjusted_capital * Decimal("100")
        if cumulative_pnl is not None and adjusted_capital is not None and adjusted_capital > 0
        else None
    )
    covered_days = beijing_calendar_days(tracking_started_at, current_sample_at)
    annualized = (
        cumulative_return * Decimal("365") / covered_days
        if cumulative_return is not None and covered_days and covered_days > 0
        else None
    )
    status = (
        "complete" if daily_pnl is not None
        else "partial" if observed_period_change is not None
        else "unavailable"
    )
    return {
        "asset": asset.upper(),
        "summary_scope": "account_equity_daily",
        "summary_status": status,
        "beijing_day": day.isoformat(),
        "reporting_timezone": "Asia/Shanghai",
        "beijing_day_actual_pnl_usd": str(daily_pnl) if daily_pnl is not None else None,
        "observed_period_change_usd": (
            str(observed_period_change)
            if not complete_day and observed_period_change is not None
            else None
        ),
        "beijing_day_return_pct": str(daily_return) if daily_return is not None else None,
        "daily_annualized_simple_pct": (
            str(daily_annualized) if daily_annualized is not None else None
        ),
        "daily_equity_start_usd": str(daily_start) if daily_start is not None else None,
        "daily_equity_latest_usd": str(daily_latest) if daily_latest is not None else None,
        "daily_first_sample_at": record.get("first_sample_at"),
        "daily_latest_sample_at": record.get("latest_sample_at"),
        "daily_sample_count": sample_count,
        "daily_cashflow_usd": str(daily_cashflow) if daily_cashflow is not None else None,
        "daily_closed_child_lots": int(fill_record.get("closed_child_lots") or 0),
        "daily_completed_close_groups": int(
            fill_record.get("tracked_completed_cycles") or 0
        ),
        "daily_four_leg_volume_usd": str(
            fill_record.get("four_leg_volume_usd") or "0"
        ),
        "daily_confirmed_pnl_usd": str(
            fill_record.get("confirmed_pnl_usd") or "0"
        ),
        "run_actual_pnl_usd": str(cumulative_pnl) if cumulative_pnl is not None else None,
        "tracking_start_equity_usd": str(capital) if capital is not None else None,
        "tracking_start_at": tracking_started_at.isoformat() if tracking_started_at else None,
        "tracking_cashflow_usd": str(cumulative_cashflow) if cumulative_cashflow is not None else None,
        "cumulative_confirmed_pnl_usd": str(
            baseline.get("confirmed_pnl_usd") or "0"
        ),
        "cumulative_four_leg_volume_usd": str(
            baseline.get("confirmed_four_leg_volume_usd") or "0"
        ),
        "cumulative_completed_close_groups": int(
            baseline.get("tracked_completed_cycles") or 0
        ),
        "cumulative_closed_child_lots": int(
            baseline.get("tracked_closed_child_lots") or 0
        ),
        "return_pct": str(cumulative_return) if cumulative_return is not None else None,
        "annualized_simple_pct": str(annualized) if annualized is not None else None,
        "covered_beijing_days": str(covered_days) if covered_days is not None else None,
        "account_baseline_day": statistics_start_day.isoformat(),
        "account_baseline_at": tracking_started_at.isoformat() if tracking_started_at else None,
        "external_cashflow_usd": str(cumulative_cashflow) if cumulative_cashflow is not None else None,
        "capital_usd": str(adjusted_capital) if adjusted_capital is not None else None,
        "capital_source": "first_statistics_day_equity_sample" if capital is not None else "unavailable",
        "variational_equity_usd": latest_variational,
        "lighter_equity_usd": latest_lighter,
        "combined_equity_usd": str(latest_total) if latest_total is not None else None,
        "account_snapshot_at": current_sample_at.isoformat() if current_sample_at else None,
        "return_pnl_source": "account_equity_delta",
        "annualized_reliability": "account_equity_observation_period",
    }


def _registered_cashflow_between(
    baseline: dict[str, Any], start: datetime | None, end: datetime | None
) -> Decimal | None:
    if start is None or end is None or end < start:
        return None
    events = baseline.get("external_cashflow_events") or []
    aggregate = decimal_value(baseline.get("external_cashflow_usd") or "0")
    if aggregate is None:
        return None
    parsed_events: list[tuple[datetime, Decimal]] = []
    for event in events:
        if not isinstance(event, dict):
            return None
        at = parse_timestamp(event.get("observed_at"))
        amount = decimal_value(event.get("amount_usd"))
        if at is None or amount is None:
            return None
        parsed_events.append((at, amount))
    if sum((amount for _, amount in parsed_events), Decimal("0")) != aggregate:
        if aggregate != 0 or parsed_events:
            return None
    return sum(
        (amount for at, amount in parsed_events if start <= at <= end),
        Decimal("0"),
    )


def send_with_retry(
    notifier: TelegramNotifier,
    message: str,
    *,
    attempts: int = 3,
) -> tuple[bool, str]:
    detail = "not_attempted"
    for attempt in range(1, max(1, attempts) + 1):
        ok, detail = notifier.send_now(message)
        if ok:
            return True, detail
        if attempt < attempts:
            time.sleep(min(2 ** (attempt - 1), 4))
    return False, detail


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a deduplicated Beijing-day Telegram PnL report."
    )
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--day", default="yesterday")
    parser.add_argument("--baseline-path", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_SEND_STATE)
    parser.add_argument("--equity-state-path", type=Path, default=DEFAULT_EQUITY_STATE)
    parser.add_argument("--risk-health-path", type=Path, default=DEFAULT_RISK_HEALTH)
    parser.add_argument("--platform-ledger-path", type=Path, default=DEFAULT_PLATFORM_LEDGER)
    parser.add_argument("--sync-platform-data", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset-cumulative-baseline", action="store_true")
    parser.add_argument("--apply-cumulative-reset", action="store_true")
    parser.add_argument("--attempts", type=int, default=3)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.apply_cumulative_reset and not args.reset_cumulative_baseline:
        raise SystemExit("--apply-cumulative-reset requires --reset-cumulative-baseline")
    if args.apply_cumulative_reset and args.dry_run:
        raise SystemExit("--apply-cumulative-reset cannot be combined with --dry-run")
    load_dotenv(ROOT / ".env")
    if args.reset_cumulative_baseline:
        try:
            return reset_realized_tracking_baseline(
                asset=args.asset,
                equity_state_path=args.equity_state_path,
                risk_health_path=args.risk_health_path,
                apply=args.apply_cumulative_reset,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"cumulative_reset=FAILED reason={type(exc).__name__}:{exc}")
            return 1
    baseline = load_pnl_baseline(args.baseline_path)
    if (
        baseline is None
        and not args.platform_ledger_path.exists()
        and not args.sync_platform_data
        and args.asset.upper() != "ETH"
    ):
        print("daily_pnl=SKIP baseline_missing")
        return 0
    if baseline is not None and str(baseline.get("asset") or "").upper() not in {"", args.asset.upper()}:
        raise SystemExit("daily_pnl=FAILED baseline_asset_mismatch")
    equity_state = load_account_equity_state(args.equity_state_path)
    account_equity_mode = baseline is not None and baseline.get("return_basis") == "account_equity_delta"
    realized_balance_mode = args.asset.upper() == "ETH"
    if realized_balance_mode and not args.dry_run:
        sample, sample_reason = read_fresh_account_equity(
            args.risk_health_path,
            asset=args.asset,
        )
        if sample is not None:
            equity_state = record_account_equity_sample(
                args.equity_state_path,
                sample,
            )
            print(f"account_equity_sample=RECORDED at={sample['captured_at']}")
        else:
            print(f"account_equity_sample=SKIPPED reason={sample_reason}")
    if args.sync_platform_data:
        try:
            sync_platform_ledger(path=args.platform_ledger_path)
            print("platform_pnl_sync=PASS")
        except Exception as exc:
            print(f"platform_pnl_sync=FAILED reason={type(exc).__name__}:{exc}")
            return 1
    target_day = resolve_day(args.day)
    if realized_balance_mode:
        tracking = equity_state.get("realized_tracking")
        if not isinstance(tracking, dict):
            day_start = datetime.combine(
                target_day, datetime.min.time(), tzinfo=BEIJING_TIMEZONE
            ).astimezone(timezone.utc)
            earliest, _, _ = _scan_order_metrics(
                asset=args.asset,
                tracking_start=None,
                day_start=day_start,
                day_end=day_start + timedelta(days=1),
            )
            tracking = _tracking_baseline(equity_state, earliest)
            if tracking is not None and not args.dry_run:
                write_json_atomic(args.equity_state_path, equity_state)
        tracking_start = parse_timestamp(
            tracking.get("start_at") if isinstance(tracking, dict) else None
        )
        if tracking_start is None:
            print("daily_pnl=SKIP complete_balance_snapshot_missing")
            return 0
        started_day = tracking_start.astimezone(BEIJING_TIMEZONE).date()
    else:
        started_day = date.fromisoformat(
            str((baseline or {}).get("current_beijing_day") or target_day.isoformat())
        )
    if not realized_balance_mode and args.platform_ledger_path.exists():
        platform_ledger = load_platform_ledger(args.platform_ledger_path)
        platform_started = parse_timestamp(platform_ledger.get("statistics_start"))
        if platform_started is not None:
            started_day = platform_started.astimezone(BEIJING_TIMEZONE).date()
    elif not realized_balance_mode:
        baseline_started = parse_timestamp((baseline or {}).get("started_at"))
        if baseline_started is not None:
            started_day = baseline_started.astimezone(BEIJING_TIMEZONE).date()
    if target_day < started_day:
        print("daily_pnl=SKIP target_before_baseline")
        return 0

    send_state = load_send_state(args.state_path)
    if args.day == "yesterday" and not args.force:
        try:
            last_sent_day = date.fromisoformat(
                str(send_state.get("last_sent_day") or "")
            )
        except ValueError:
            last_sent_day = None
        if (
            last_sent_day is not None
            and last_sent_day + timedelta(days=1) < target_day
        ):
            target_day = last_sent_day + timedelta(days=1)
    send_key = f"{args.asset.upper()}:{target_day.isoformat()}"
    sent_keys = [str(value) for value in send_state.get("sent_keys", [])]
    if send_key in sent_keys and not args.force:
        print(f"daily_pnl=SKIP already_sent key={send_key}")
        return 0

    if realized_balance_mode:
        platform_ledger = (
            load_platform_ledger(args.platform_ledger_path)
            if args.platform_ledger_path.exists()
            else {"events": [], "source_status": {}}
        )
        payload = build_realized_balance_payload(
            baseline or {},
            asset=args.asset,
            day=target_day,
            equity_state=equity_state,
            platform_ledger=platform_ledger,
        )
    elif account_equity_mode:
        payload = build_daily_payload(
            baseline,
            asset=args.asset,
            day=target_day,
            equity_state=equity_state,
        )
    elif args.platform_ledger_path.exists():
        payload = build_platform_activity_payload(
            load_platform_ledger(args.platform_ledger_path),
            asset=args.asset,
            day=target_day,
            equity_state=equity_state,
        )
    else:
        if baseline is None:
            print("daily_pnl=SKIP baseline_and_platform_ledger_missing")
            return 0
        payload = build_daily_payload(
            baseline,
            asset=args.asset,
            day=target_day,
            equity_state=equity_state,
        )
    message = format_telegram_trade_message(
        "live_inventory_pnl_summary",
        payload,
    )
    print(message)
    if args.dry_run:
        print("daily_pnl=DRY_RUN")
        return 0
    notifier = TelegramNotifier.from_env(
        logger=logging.getLogger("daily_pnl_report.telegram")
    )
    ok, detail = send_with_retry(
        notifier,
        message,
        attempts=max(1, args.attempts),
    )
    print(f"daily_pnl={'PASS' if ok else 'FAILED'} {detail}")
    if not ok:
        return 1
    if not args.force:
        write_json_atomic(
            args.state_path,
            {
                "schema_version": 1,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "last_sent_day": target_day.isoformat(),
                "sent_keys": [*sent_keys, send_key][-400:],
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
