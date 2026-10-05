from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


PNL_BASELINE_FILE_NAME = "pnl_reporting_baseline.json"
BEIJING_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
PNL_REPORTING_TIMEZONE = "Asia/Shanghai"
_BASELINE_LOCK = threading.RLock()
_MAX_DAILY_HISTORY_DAYS = 400


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def beijing_day(value: Any) -> str | None:
    parsed = value if isinstance(value, datetime) else parse_timestamp(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(BEIJING_TIMEZONE).date().isoformat()


def beijing_calendar_days(start: datetime | None, end: datetime | None) -> Decimal | None:
    if start is None or end is None or end < start:
        return None
    start_day = start.astimezone(BEIJING_TIMEZONE).date()
    end_day = end.astimezone(BEIJING_TIMEZONE).date()
    return Decimal((end_day - start_day).days + 1)


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value not in (None, "") else default))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _empty_day_record() -> dict[str, Any]:
    return {
        "confirmed_pnl_usd": "0",
        "four_leg_volume_usd": "0",
        "tracked_completed_cycles": 0,
        "closed_child_lots": 0,
        "counted_cycle_keys": [],
        "volume_counted_cycle_keys": [],
        "external_cashflow_usd": "0",
    }


def _day_record_from_current(baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "confirmed_pnl_usd": str(baseline.get("daily_confirmed_pnl_usd") or "0"),
        "four_leg_volume_usd": str(
            baseline.get("daily_four_leg_volume_usd") or "0"
        ),
        "tracked_completed_cycles": int(
            baseline.get("daily_tracked_completed_cycles") or 0
        ),
        "closed_child_lots": int(
            baseline.get("daily_closed_child_lots") or 0
        ),
        "counted_cycle_keys": [
            str(value)
            for value in baseline.get("daily_counted_cycle_keys", [])
        ],
        "volume_counted_cycle_keys": [
            str(value)
            for value in baseline.get("daily_volume_counted_cycle_keys", [])
        ],
        "external_cashflow_usd": str(
            baseline.get("daily_external_cashflow_usd") or "0"
        ),
    }


def normalize_pnl_baseline(value: dict[str, Any]) -> dict[str, Any]:
    return {
        **value,
        "reporting_timezone": PNL_REPORTING_TIMEZONE,
        "confirmed_four_leg_volume_usd": str(
            value.get("confirmed_four_leg_volume_usd") or "0"
        ),
        "tracked_closed_child_lots": int(
            value.get("tracked_closed_child_lots") or 0
        ),
        "daily_four_leg_volume_usd": str(
            value.get("daily_four_leg_volume_usd") or "0"
        ),
        "daily_closed_child_lots": int(
            value.get("daily_closed_child_lots") or 0
        ),
        "daily_history": {
            str(day): record
            for day, record in (value.get("daily_history") or {}).items()
            if isinstance(record, dict)
        },
        "volume_counted_cycle_keys": [
            str(item) for item in value.get("volume_counted_cycle_keys", [])
        ],
        "daily_volume_counted_cycle_keys": [
            str(item)
            for item in value.get("daily_volume_counted_cycle_keys", [])
        ],
        "latest_variational_equity_usd": value.get(
            "latest_variational_equity_usd"
        ),
        "latest_lighter_equity_usd": value.get("latest_lighter_equity_usd"),
        "latest_combined_equity_usd": value.get("latest_combined_equity_usd"),
        "latest_account_snapshot_at": value.get("latest_account_snapshot_at"),
        "latest_account_snapshot_flat": value.get(
            "latest_account_snapshot_flat"
        ),
        "return_basis": str(value.get("return_basis") or "confirmed_pair_fills"),
        "account_baseline_source": value.get("account_baseline_source"),
        "account_equity_tracking_started_at": value.get(
            "account_equity_tracking_started_at"
        ),
    }


def roll_pnl_beijing_day(
    baseline: dict[str, Any],
    *,
    observed_at: Any,
) -> tuple[dict[str, Any], bool]:
    baseline = normalize_pnl_baseline(baseline)
    day = beijing_day(observed_at) or beijing_day(utc_now())
    current_day = str(baseline.get("current_beijing_day") or "")
    if not day or current_day == day:
        return baseline, False
    # Late fill reconciliation belongs in history and must never roll the
    # active day backwards.
    if current_day and day < current_day:
        return baseline, False
    history = dict(baseline.get("daily_history") or {})
    if current_day:
        history[current_day] = _day_record_from_current(baseline)
    history = dict(sorted(history.items())[-_MAX_DAILY_HISTORY_DAYS:])
    return (
        {
            **baseline,
            "current_beijing_day": day,
            "daily_confirmed_pnl_usd": "0",
            "daily_four_leg_volume_usd": "0",
            "daily_tracked_completed_cycles": 0,
            "daily_closed_child_lots": 0,
            "daily_counted_cycle_keys": [],
            "daily_volume_counted_cycle_keys": [],
            "daily_external_cashflow_usd": "0",
            "daily_history": history,
        },
        True,
    )


def load_pnl_baseline(path: Path) -> dict[str, Any] | None:
    with _BASELINE_LOCK:
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return normalize_pnl_baseline(value) if isinstance(value, dict) else None


def write_pnl_baseline(path: Path, value: dict[str, Any]) -> None:
    with _BASELINE_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 2,
            "updated_at": utc_now(),
            **normalize_pnl_baseline(value),
        }
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)


def new_pnl_baseline(
    *,
    asset: str,
    realized_pnl_usd: Any,
    completed_cycles: Any,
    started_at: str | None = None,
) -> dict[str, Any]:
    timestamp = started_at or utc_now()
    return normalize_pnl_baseline(
        {
            "asset": asset.upper(),
            "started_at": timestamp,
            "current_beijing_day": beijing_day(timestamp),
            "realized_pnl_baseline_usd": str(realized_pnl_usd or "0"),
            "completed_cycles_baseline": int(completed_cycles or 0),
            "confirmed_pnl_usd": "0",
            "confirmed_four_leg_volume_usd": "0",
            "tracked_completed_cycles": 0,
            "tracked_closed_child_lots": 0,
            "counted_cycle_keys": [],
            "volume_counted_cycle_keys": [],
            "account_baseline_equity_usd": None,
            "account_baseline_at": None,
            "external_cashflow_usd": "0",
            "external_cashflow_events": [],
            "daily_confirmed_pnl_usd": "0",
            "daily_four_leg_volume_usd": "0",
            "daily_tracked_completed_cycles": 0,
            "daily_closed_child_lots": 0,
            "daily_counted_cycle_keys": [],
            "daily_volume_counted_cycle_keys": [],
            "daily_external_cashflow_usd": "0",
            "daily_history": {},
            "return_basis": "confirmed_pair_fills",
            "account_baseline_source": None,
            "account_equity_tracking_started_at": None,
        }
    )


def set_pnl_account_baseline(
    path: Path,
    *,
    combined_equity_usd: Any,
    captured_at: str,
) -> dict[str, Any] | None:
    with _BASELINE_LOCK:
        baseline = load_pnl_baseline(path)
        if baseline is None:
            return None
        if baseline.get("account_baseline_equity_usd") not in (None, ""):
            return baseline
        updated = {
            **baseline,
            "account_baseline_equity_usd": str(combined_equity_usd),
            "account_baseline_at": captured_at,
        }
        write_pnl_baseline(path, updated)
        return load_pnl_baseline(path)


def set_pnl_latest_account_snapshot(
    path: Path,
    *,
    variational_equity_usd: Any,
    lighter_equity_usd: Any,
    combined_equity_usd: Any,
    captured_at: str,
    account_snapshot_flat: bool,
) -> dict[str, Any] | None:
    with _BASELINE_LOCK:
        baseline = load_pnl_baseline(path)
        if baseline is None:
            return None
        updated = {
            **baseline,
            "latest_variational_equity_usd": str(variational_equity_usd),
            "latest_lighter_equity_usd": str(lighter_equity_usd),
            "latest_combined_equity_usd": str(combined_equity_usd),
            "latest_account_snapshot_at": captured_at,
            "latest_account_snapshot_flat": bool(account_snapshot_flat),
        }
        write_pnl_baseline(path, updated)
        return load_pnl_baseline(path)


def record_pnl_cycle(
    path: Path,
    *,
    run_id: str,
    asset: str,
    lot_id: Any,
    actual_pnl_usd: Any,
    observed_at: Any = None,
    four_leg_volume_usd: Any = None,
    closed_child_lots: Any = 1,
) -> dict[str, Any] | None:
    with _BASELINE_LOCK:
        baseline = load_pnl_baseline(path)
        if baseline is None:
            return None
        observed = observed_at or utc_now()
        baseline, rolled = roll_pnl_beijing_day(baseline, observed_at=observed)
        cycle_key = f"{run_id}:{asset.upper()}:{lot_id}"
        try:
            cycle_pnl = Decimal(str(actual_pnl_usd))
            cycle_volume = Decimal(str(four_leg_volume_usd or "0"))
            child_lots = int(closed_child_lots or 1)
        except (InvalidOperation, TypeError, ValueError):
            return baseline
        counted = [str(value) for value in baseline.get("counted_cycle_keys", [])]
        volume_counted = [
            str(value)
            for value in baseline.get("volume_counted_cycle_keys", [])
        ]
        if cycle_key in counted:
            if cycle_volume <= 0 or cycle_key in volume_counted:
                if rolled:
                    write_pnl_baseline(path, baseline)
                    return load_pnl_baseline(path)
                return baseline
            # Schema-v1 baselines already contain PnL but not volume. A
            # one-time backfill adds only volume/child counts, never PnL.
            migrated = {
                **baseline,
                "confirmed_four_leg_volume_usd": str(
                    _decimal(baseline.get("confirmed_four_leg_volume_usd"))
                    + cycle_volume
                ),
                "tracked_closed_child_lots": int(
                    baseline.get("tracked_closed_child_lots") or 0
                )
                + child_lots,
                "volume_counted_cycle_keys": [
                    *volume_counted,
                    cycle_key,
                ][-5000:],
            }
            cycle_day = beijing_day(observed)
            current_day = str(migrated.get("current_beijing_day") or "")
            if cycle_day == current_day:
                migrated["daily_four_leg_volume_usd"] = str(
                    _decimal(migrated.get("daily_four_leg_volume_usd"))
                    + cycle_volume
                )
                migrated["daily_closed_child_lots"] = int(
                    migrated.get("daily_closed_child_lots") or 0
                ) + child_lots
                migrated["daily_volume_counted_cycle_keys"] = [
                    *[
                        str(value)
                        for value in migrated.get(
                            "daily_volume_counted_cycle_keys", []
                        )
                    ],
                    cycle_key,
                ][-5000:]
            elif cycle_day:
                history = dict(migrated.get("daily_history") or {})
                record = {
                    **_empty_day_record(),
                    **dict(history.get(cycle_day) or {}),
                }
                record["four_leg_volume_usd"] = str(
                    _decimal(record.get("four_leg_volume_usd")) + cycle_volume
                )
                record["closed_child_lots"] = int(
                    record.get("closed_child_lots") or 0
                ) + child_lots
                record["volume_counted_cycle_keys"] = [
                    *[
                        str(value)
                        for value in record.get("volume_counted_cycle_keys", [])
                    ],
                    cycle_key,
                ][-5000:]
                history[cycle_day] = record
                migrated["daily_history"] = history
            write_pnl_baseline(path, migrated)
            return load_pnl_baseline(path)

        updated = {
            **baseline,
            "confirmed_pnl_usd": str(
                _decimal(baseline.get("confirmed_pnl_usd")) + cycle_pnl
            ),
            "confirmed_four_leg_volume_usd": str(
                _decimal(baseline.get("confirmed_four_leg_volume_usd"))
                + cycle_volume
            ),
            "tracked_completed_cycles": int(
                baseline.get("tracked_completed_cycles") or 0
            )
            + 1,
            "tracked_closed_child_lots": int(
                baseline.get("tracked_closed_child_lots") or 0
            )
            + (child_lots if cycle_volume > 0 else 0),
            "counted_cycle_keys": [*counted, cycle_key][-5000:],
            "volume_counted_cycle_keys": (
                [*volume_counted, cycle_key][-5000:]
                if cycle_volume > 0
                else volume_counted
            ),
        }

        cycle_day = beijing_day(observed)
        current_day = str(updated.get("current_beijing_day") or "")
        if cycle_day == current_day:
            updated.update(
                {
                    "daily_confirmed_pnl_usd": str(
                        _decimal(updated.get("daily_confirmed_pnl_usd"))
                        + cycle_pnl
                    ),
                    "daily_four_leg_volume_usd": str(
                        _decimal(updated.get("daily_four_leg_volume_usd"))
                        + cycle_volume
                    ),
                    "daily_tracked_completed_cycles": int(
                        updated.get("daily_tracked_completed_cycles") or 0
                    )
                    + 1,
                    "daily_closed_child_lots": int(
                        updated.get("daily_closed_child_lots") or 0
                    )
                    + (child_lots if cycle_volume > 0 else 0),
                    "daily_counted_cycle_keys": [
                        *[
                            str(value)
                            for value in updated.get("daily_counted_cycle_keys", [])
                        ],
                        cycle_key,
                    ][-5000:],
                    "daily_volume_counted_cycle_keys": (
                        [
                            *[
                                str(value)
                                for value in updated.get(
                                    "daily_volume_counted_cycle_keys", []
                                )
                            ],
                            cycle_key,
                        ][-5000:]
                        if cycle_volume > 0
                        else list(
                            updated.get("daily_volume_counted_cycle_keys", [])
                        )
                    ),
                }
            )
        elif cycle_day:
            history = dict(updated.get("daily_history") or {})
            record = {
                **_empty_day_record(),
                **dict(history.get(cycle_day) or {}),
            }
            record["confirmed_pnl_usd"] = str(
                _decimal(record.get("confirmed_pnl_usd")) + cycle_pnl
            )
            record["four_leg_volume_usd"] = str(
                _decimal(record.get("four_leg_volume_usd")) + cycle_volume
            )
            record["tracked_completed_cycles"] = int(
                record.get("tracked_completed_cycles") or 0
            ) + 1
            record["closed_child_lots"] = int(
                record.get("closed_child_lots") or 0
            ) + (child_lots if cycle_volume > 0 else 0)
            record["counted_cycle_keys"] = [
                *[str(value) for value in record.get("counted_cycle_keys", [])],
                cycle_key,
            ][-5000:]
            if cycle_volume > 0:
                record["volume_counted_cycle_keys"] = [
                    *[
                        str(value)
                        for value in record.get("volume_counted_cycle_keys", [])
                    ],
                    cycle_key,
                ][-5000:]
            history[cycle_day] = record
            updated["daily_history"] = dict(
                sorted(history.items())[-_MAX_DAILY_HISTORY_DAYS:]
            )
        write_pnl_baseline(path, updated)
        return load_pnl_baseline(path)


def pnl_day_summary(baseline: dict[str, Any], day: str) -> dict[str, Any]:
    baseline = normalize_pnl_baseline(baseline)
    if day == str(baseline.get("current_beijing_day") or ""):
        record = _day_record_from_current(baseline)
    else:
        record = {
            **_empty_day_record(),
            **dict((baseline.get("daily_history") or {}).get(day) or {}),
        }
    return {"beijing_day": day, **record}


def record_external_cashflow(
    path: Path,
    *,
    amount_usd: Any,
    observed_at: str,
    reason: str,
) -> dict[str, Any] | None:
    with _BASELINE_LOCK:
        baseline = load_pnl_baseline(path)
        if baseline is None:
            return None
        baseline, _ = roll_pnl_beijing_day(baseline, observed_at=observed_at)
        try:
            amount = Decimal(str(amount_usd))
        except (InvalidOperation, TypeError, ValueError):
            return baseline
        events = [
            value
            for value in list(baseline.get("external_cashflow_events") or [])
            if isinstance(value, dict)
        ]
        updated = {
            **baseline,
            "external_cashflow_usd": str(
                _decimal(baseline.get("external_cashflow_usd")) + amount
            ),
            "external_cashflow_events": [
                *events,
                {
                    "amount_usd": str(amount),
                    "observed_at": observed_at,
                    "reason": reason,
                },
            ][-100:],
        }
        observed_day = beijing_day(observed_at)
        if observed_day == str(updated.get("current_beijing_day") or ""):
            updated["daily_external_cashflow_usd"] = str(
                _decimal(updated.get("daily_external_cashflow_usd")) + amount
            )
        elif observed_day:
            history = dict(updated.get("daily_history") or {})
            record = {
                **_empty_day_record(),
                **dict(history.get(observed_day) or {}),
            }
            record["external_cashflow_usd"] = str(
                _decimal(record.get("external_cashflow_usd")) + amount
            )
            history[observed_day] = record
            updated["daily_history"] = history
        write_pnl_baseline(path, updated)
        return load_pnl_baseline(path)
