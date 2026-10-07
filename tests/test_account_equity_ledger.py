import json
from datetime import datetime, timezone

from tools.lib.account_equity_ledger import (
    VAR_EQUITY_FORMULA_VERSION,
    complete_beijing_equity_day,
    read_fresh_account_equity,
    record_account_equity_sample,
)


def _sample(captured_at: str, variational: str, combined: str) -> dict[str, str]:
    return {
        "captured_at": captured_at,
        "variational_equity_usd": variational,
        "lighter_equity_usd": "115",
        "combined_equity_usd": combined,
        "variational_equity_formula_version": VAR_EQUITY_FORMULA_VERSION,
    }


def test_full_day_requires_boundary_samples_and_known_continuity() -> None:
    record = {
        "first_sample_at": "2026-10-04T16:05:00+00:00",
        "latest_sample_at": "2026-10-05T15:55:00+00:00",
        "sample_count": 288,
        "max_sample_gap_seconds": "300",
    }
    assert complete_beijing_equity_day(record, "2026-10-05")
    assert not complete_beijing_equity_day(
        {**record, "max_sample_gap_seconds": None, "coverage_complete": True},
        "2026-10-05",
    )
    assert not complete_beijing_equity_day(
        {**record, "max_sample_gap_seconds": "1800"}, "2026-10-05"
    )
    assert not complete_beijing_equity_day(
        {**record, "first_sample_at": "2026-10-05T06:55:00+00:00"},
        "2026-10-05",
    )


def test_sample_ledger_records_gaps_without_promoting_partial_day(tmp_path) -> None:
    path = tmp_path / "account_equity_daily_state.json"
    first = record_account_equity_sample(
        path,
        _sample("2026-10-04T16:05:00+00:00", "125", "240"),
    )
    assert first["daily_history"]["2026-10-05"]["max_sample_gap_seconds"] == "0"
    last = record_account_equity_sample(
        path,
        _sample("2026-10-05T15:55:00+00:00", "126", "241"),
    )
    record = last["daily_history"]["2026-10-05"]
    assert record["max_sample_gap_seconds"] == "85800.0"
    assert record["coverage_complete"] is False


def test_new_formula_restarts_only_the_affected_daily_record(tmp_path) -> None:
    path = tmp_path / "account_equity_daily_state.json"
    path.write_text(
        json.dumps(
            {
                "last_sample_at": "2026-10-07T17:00:00+08:00",
                "daily_history": {
                    "2026-10-07": {
                        "start_equity_usd": "245",
                        "latest_equity_usd": "246",
                        "first_sample_at": "2026-10-07T16:10:00+08:00",
                        "latest_sample_at": "2026-10-07T17:00:00+08:00",
                        "sample_count": 12,
                        "max_sample_gap_seconds": "300",
                        "coverage_complete": False,
                    },
                    "2026-10-06": {"start_equity_usd": "240"},
                }
            }
        ),
        encoding="utf-8",
    )
    updated = record_account_equity_sample(
        path,
        _sample("2026-10-07T17:01:00+08:00", "137.451292", "241.898309"),
    )
    today = updated["daily_history"]["2026-10-07"]
    assert today["start_equity_usd"] == "241.898309"
    assert today["sample_count"] == 1
    assert today["coverage_complete"] is False
    assert updated["daily_history"]["2026-10-06"]["start_equity_usd"] == "240"


def test_fresh_account_equity_rejects_unverified_formula(tmp_path) -> None:
    path = tmp_path / "risk_health.json"
    health = {
        "asset": "ETH",
        "updated_at": "2026-10-07T17:00:00+00:00",
        "variational_account_snapshot_fresh": True,
        "variational_account_snapshot_usable": True,
        "variational_equity_formula": "balance_plus_upnl",
        "lighter_risk_fetch_error": None,
        "pending_actions_total": 0,
        "variational_equity_usd": "141.222669",
        "lighter_equity_usd": "104.453824",
        "combined_equity_usd": "245.676493",
    }
    path.write_text(json.dumps(health), encoding="utf-8")
    sample, reason = read_fresh_account_equity(
        path, now=datetime(2026, 10, 7, 17, 0, 1, tzinfo=timezone.utc)
    )
    assert sample is None
    assert reason == "variational_equity_formula_unverified"
