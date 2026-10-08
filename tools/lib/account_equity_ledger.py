from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from tools.lib.pnl_baseline import BEIJING_TIMEZONE, beijing_day, parse_timestamp


RISK_HEALTH_MAX_AGE_SECONDS = 60
DAILY_EQUITY_STATE_SCHEMA = 2
MAX_DAILY_SAMPLE_GAP_SECONDS = 900
VAR_EQUITY_FORMULA_VERSION = "balance_includes_upnl_v1"


def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def load_account_equity_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": DAILY_EQUITY_STATE_SCHEMA, "daily_history": {}}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"account equity state is unreadable: {path}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("daily_history"), dict):
        raise RuntimeError(f"account equity state has an invalid structure: {path}")
    return value


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def read_fresh_account_equity(
    path: Path,
    *,
    now: datetime | None = None,
    asset: str = "ETH",
) -> tuple[dict[str, str] | None, str]:
    try:
        health = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, "risk_health_missing_or_invalid"
    if not isinstance(health, dict) or str(health.get("asset") or "").upper() != asset.upper():
        return None, "risk_health_asset_mismatch"
    observed = parse_timestamp(health.get("updated_at"))
    current = now or datetime.now(BEIJING_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=BEIJING_TIMEZONE)
    age = (current.astimezone(observed.tzinfo) - observed).total_seconds() if observed else None
    if age is None or age < -5 or age > RISK_HEALTH_MAX_AGE_SECONDS:
        return None, "risk_health_stale"
    if health.get("variational_account_snapshot_fresh") is not True:
        return None, "variational_equity_stale"
    if health.get("variational_account_snapshot_usable") is not True:
        return None, "variational_equity_unusable"
    if health.get("variational_equity_formula") != "balance_includes_upnl":
        return None, "variational_equity_formula_unverified"
    if health.get("lighter_risk_fetch_error"):
        return None, "lighter_equity_fetch_failed"
    try:
        pending = int(health.get("pending_actions_total") or 0)
    except (TypeError, ValueError):
        return None, "pending_action_count_invalid"
    if pending:
        return None, "pending_actions_unresolved"

    variational = _decimal(health.get("variational_equity_usd"))
    lighter = _decimal(health.get("lighter_equity_usd"))
    combined = _decimal(health.get("combined_equity_usd"))
    if (
        variational is None
        or lighter is None
        or combined is None
        or min(variational, lighter, combined) <= 0
        or abs(variational + lighter - combined) > Decimal("0.01")
    ):
        return None, "two_venue_equity_incomplete_or_inconsistent"
    sample = {
            "captured_at": observed.isoformat(),
            "variational_equity_usd": str(variational),
            "lighter_equity_usd": str(lighter),
            "combined_equity_usd": str(combined),
            "variational_equity_formula_version": VAR_EQUITY_FORMULA_VERSION,
    }
    var_balance = _decimal(health.get("variational_balance_usd"))
    var_upnl = _decimal(health.get("variational_upnl_usd"))
    lighter_collateral = _decimal(health.get("lighter_collateral_usd"))
    lighter_upnl = _decimal(health.get("lighter_unrealized_pnl_usd"))
    lighter_check = _decimal(health.get("lighter_realized_balance_check_usd"))
    if (
        var_balance is not None
        and var_upnl is not None
        and lighter_collateral is not None
        and lighter_upnl is not None
        and lighter_check is not None
        and abs(lighter_check) <= Decimal("0.05")
    ):
        sample.update(
            {
                "variational_realized_balance_usd": str(var_balance - var_upnl),
                "lighter_realized_balance_usd": str(lighter_collateral),
                "combined_realized_balance_usd": str(
                    var_balance - var_upnl + lighter_collateral
                ),
                "lighter_realized_balance_check_usd": str(lighter_check),
            }
        )
    return sample, "ok"


def complete_beijing_equity_day(record: dict[str, Any], day: str) -> bool:
    try:
        local_day = datetime.fromisoformat(day).date()
        sample_count = int(record.get("sample_count") or 0)
    except (TypeError, ValueError):
        return False
    first_at = parse_timestamp(record.get("first_sample_at"))
    latest_at = parse_timestamp(record.get("latest_sample_at"))
    max_gap = _decimal(record.get("max_sample_gap_seconds"))
    if sample_count < 2 or first_at is None or latest_at is None or max_gap is None:
        return False
    day_start = datetime.combine(local_day, time.min, tzinfo=BEIJING_TIMEZONE)
    day_end = day_start + timedelta(days=1)
    return (
        day_start <= first_at <= day_start + timedelta(minutes=10)
        and day_end - timedelta(minutes=10) <= latest_at < day_end
        and Decimal("0") <= max_gap <= MAX_DAILY_SAMPLE_GAP_SECONDS
    )


def record_account_equity_sample(
    path: Path,
    sample: dict[str, str],
) -> dict[str, Any]:
    state = load_account_equity_state(path)
    observed = parse_timestamp(sample.get("captured_at"))
    if observed is None:
        return state
    previous = parse_timestamp(state.get("last_sample_at"))
    if previous is not None and observed <= previous:
        return state
    day = beijing_day(observed)
    if day is None:
        return state
    formula_version = sample.get("variational_equity_formula_version")
    if formula_version != VAR_EQUITY_FORMULA_VERSION:
        return state

    history = dict(state.get("daily_history") or {})
    record = dict(history.get(day) or {})
    if record.get("variational_equity_formula_version") != formula_version:
        record = {}
    first_at = parse_timestamp(record.get("first_sample_at"))
    latest_at = parse_timestamp(record.get("latest_sample_at"))
    max_gap = (
        Decimal("0") if first_at is None
        else _decimal(record.get("max_sample_gap_seconds"))
    )
    start_equity = record.get("start_equity_usd")
    if first_at is None:
        start_equity = sample["combined_equity_usd"]
        first_at = observed
    if latest_at is not None and max_gap is not None:
        max_gap = max(max_gap, Decimal(str((observed - latest_at).total_seconds())))
    if latest_at is None or observed > latest_at:
        latest_at = observed
    record.update(
        {
            "start_equity_usd": start_equity,
            "latest_equity_usd": sample["combined_equity_usd"],
            "first_sample_at": first_at.isoformat(),
            "latest_sample_at": latest_at.isoformat(),
            "sample_count": int(record.get("sample_count") or 0) + 1,
            "max_sample_gap_seconds": str(max_gap) if max_gap is not None else None,
            "variational_equity_formula_version": formula_version,
        }
    )
    record["coverage_complete"] = complete_beijing_equity_day(record, day)
    history[day] = record
    realized_history = dict(state.get("realized_daily_history") or {})
    realized_total = _decimal(sample.get("combined_realized_balance_usd"))
    realized_var = _decimal(sample.get("variational_realized_balance_usd"))
    realized_lighter = _decimal(sample.get("lighter_realized_balance_usd"))
    if (
        realized_total is not None
        and realized_var is not None
        and realized_lighter is not None
        and abs(realized_var + realized_lighter - realized_total)
        <= Decimal("0.01")
    ):
        realized_record = dict(realized_history.get(day) or {})
        realized_first = parse_timestamp(realized_record.get("first_sample_at"))
        realized_last = parse_timestamp(realized_record.get("latest_sample_at"))
        realized_gap = (
            Decimal("0")
            if realized_first is None
            else _decimal(realized_record.get("max_sample_gap_seconds"))
        )
        if realized_first is None:
            realized_record.update(
                {
                    "first_sample_at": observed.isoformat(),
                    "start_realized_balance_usd": str(realized_total),
                    "start_variational_realized_balance_usd": str(realized_var),
                    "start_lighter_realized_balance_usd": str(realized_lighter),
                    "start_equity_usd": sample["combined_equity_usd"],
                }
            )
        if realized_last is not None and realized_gap is not None:
            realized_gap = max(
                realized_gap,
                Decimal(str((observed - realized_last).total_seconds())),
            )
        realized_record.update(
            {
                "latest_sample_at": observed.isoformat(),
                "latest_realized_balance_usd": str(realized_total),
                "latest_variational_realized_balance_usd": str(realized_var),
                "latest_lighter_realized_balance_usd": str(realized_lighter),
                "latest_equity_usd": sample["combined_equity_usd"],
                "sample_count": int(realized_record.get("sample_count") or 0) + 1,
                "max_sample_gap_seconds": (
                    str(realized_gap) if realized_gap is not None else None
                ),
            }
        )
        realized_record["coverage_complete"] = complete_beijing_equity_day(
            realized_record, day
        )
        realized_history[day] = realized_record
    state.update(
        {
            "schema_version": DAILY_EQUITY_STATE_SCHEMA,
            "asset": "ETH",
            "updated_at": datetime.now(BEIJING_TIMEZONE).isoformat(),
            "last_sample_at": observed.isoformat(),
            "latest_variational_equity_usd": sample["variational_equity_usd"],
            "latest_lighter_equity_usd": sample["lighter_equity_usd"],
            "latest_combined_equity_usd": sample["combined_equity_usd"],
            "variational_equity_formula_version": formula_version,
            "daily_history": dict(sorted(history.items())[-400:]),
            "realized_daily_history": dict(
                sorted(realized_history.items())[-400:]
            ),
        }
    )
    if sample.get("combined_realized_balance_usd") is not None:
        state.update(
            {
                "latest_variational_realized_balance_usd": sample.get(
                    "variational_realized_balance_usd"
                ),
                "latest_lighter_realized_balance_usd": sample.get(
                    "lighter_realized_balance_usd"
                ),
                "latest_combined_realized_balance_usd": sample.get(
                    "combined_realized_balance_usd"
                ),
                "latest_realized_sample_at": observed.isoformat(),
            }
        )
    _write_state(path, state)
    return state
