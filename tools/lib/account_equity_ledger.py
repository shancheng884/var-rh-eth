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
    return (
        {
            "captured_at": observed.isoformat(),
            "variational_equity_usd": str(variational),
            "lighter_equity_usd": str(lighter),
            "combined_equity_usd": str(combined),
        },
        "ok",
    )


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

    history = dict(state.get("daily_history") or {})
    record = dict(history.get(day) or {})
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
        }
    )
    record["coverage_complete"] = complete_beijing_equity_day(record, day)
    history[day] = record
    state.update(
        {
            "schema_version": DAILY_EQUITY_STATE_SCHEMA,
            "asset": "ETH",
            "updated_at": datetime.now(BEIJING_TIMEZONE).isoformat(),
            "last_sample_at": observed.isoformat(),
            "latest_variational_equity_usd": sample["variational_equity_usd"],
            "latest_lighter_equity_usd": sample["lighter_equity_usd"],
            "latest_combined_equity_usd": sample["combined_equity_usd"],
            "daily_history": dict(sorted(history.items())[-400:]),
        }
    )
    _write_state(path, state)
    return state
