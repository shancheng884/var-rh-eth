#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
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
    complete_beijing_equity_day,
    load_account_equity_state,
    read_fresh_account_equity,
    record_account_equity_sample,
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


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


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
    observed = (
        now
        or parse_timestamp(
            equity_state.get("last_sample_at") or baseline.get("latest_account_snapshot_at")
        )
        or datetime.now(timezone.utc)
    )
    current_sample_at = parse_timestamp(
        equity_state.get("last_sample_at") or baseline.get("latest_account_snapshot_at")
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
    latest_total = decimal_value(equity_state.get("latest_combined_equity_usd"))
    if latest_total is None:
        latest_total = decimal_value(baseline.get("latest_combined_equity_usd"))
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
        "variational_equity_usd": equity_state.get("latest_variational_equity_usd") or baseline.get("latest_variational_equity_usd"),
        "lighter_equity_usd": equity_state.get("latest_lighter_equity_usd") or baseline.get("latest_lighter_equity_usd"),
        "combined_equity_usd": str(latest_total) if latest_total is not None else None,
        "account_snapshot_at": equity_state.get("last_sample_at") or baseline.get("latest_account_snapshot_at"),
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
    parser.add_argument("--attempts", type=int, default=3)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_dotenv(ROOT / ".env")
    baseline = load_pnl_baseline(args.baseline_path)
    if baseline is None and not args.platform_ledger_path.exists() and not args.sync_platform_data:
        print("daily_pnl=SKIP baseline_missing")
        return 0
    if baseline is not None and str(baseline.get("asset") or "").upper() not in {"", args.asset.upper()}:
        raise SystemExit("daily_pnl=FAILED baseline_asset_mismatch")
    equity_state = load_account_equity_state(args.equity_state_path)
    account_equity_mode = baseline is not None and baseline.get("return_basis") == "account_equity_delta"
    if account_equity_mode and not args.dry_run:
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
    if args.sync_platform_data and not account_equity_mode:
        try:
            sync_platform_ledger(path=args.platform_ledger_path)
            print("platform_pnl_sync=PASS")
        except Exception as exc:
            print(f"platform_pnl_sync=FAILED reason={type(exc).__name__}:{exc}")
            return 1
    target_day = resolve_day(args.day)
    if account_equity_mode:
        configured_start = parse_timestamp(DEFAULT_PLATFORM_START)
        started_day = (
            configured_start.astimezone(BEIJING_TIMEZONE).date()
            if configured_start is not None else target_day
        )
        if args.sync_platform_data:
            print("platform_pnl_sync=SKIP account_equity_mode")
    else:
        started_day = date.fromisoformat(
            str((baseline or {}).get("current_beijing_day") or target_day.isoformat())
        )
    if not account_equity_mode and args.platform_ledger_path.exists():
        platform_ledger = load_platform_ledger(args.platform_ledger_path)
        platform_started = parse_timestamp(platform_ledger.get("statistics_start"))
        if platform_started is not None:
            started_day = platform_started.astimezone(BEIJING_TIMEZONE).date()
    elif not account_equity_mode:
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

    if account_equity_mode:
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
