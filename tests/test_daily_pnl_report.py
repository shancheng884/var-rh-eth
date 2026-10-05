from datetime import date, datetime, timezone
from decimal import Decimal

from tools.daily_pnl_report import build_daily_payload, resolve_day
from tools.lib.pnl_baseline import new_pnl_baseline


def test_daily_payload_uses_small_persistent_ledger(monkeypatch) -> None:
    monkeypatch.delenv("PNL_REPORT_CAPITAL_USD", raising=False)
    baseline = new_pnl_baseline(
        asset="ETH",
        realized_pnl_usd="0",
        completed_cycles=0,
        started_at="2026-08-29T00:00:00+00:00",
    )
    baseline.update(
        {
            "account_baseline_equity_usd": "200",
            "account_baseline_at": "2026-08-29T00:00:00+00:00",
            "current_beijing_day": "2026-08-31",
            "daily_confirmed_pnl_usd": "0.08",
            "daily_four_leg_volume_usd": "240",
            "daily_tracked_completed_cycles": 3,
            "daily_closed_child_lots": 12,
            "confirmed_pnl_usd": "0.44",
            "confirmed_four_leg_volume_usd": "1720",
            "tracked_closed_child_lots": 71,
            "latest_combined_equity_usd": "212.35",
        }
    )

    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 8, 31),
        now=datetime(2026, 8, 31, 16, 5, tzinfo=timezone.utc),
    )

    assert payload["daily_closed_child_lots"] == 12
    assert payload["daily_completed_close_groups"] == 3
    assert payload["daily_four_leg_volume_usd"] == "240"
    assert payload["beijing_day_actual_pnl_usd"] == "0.08"
    assert payload["beijing_day_return_pct"] == "0.0400"
    assert payload["daily_annualized_simple_pct"] == "14.6000"
    assert payload["run_actual_pnl_usd"] == "0.44"
    assert payload["combined_equity_usd"] == "212.35"


def test_account_equity_payload_keeps_fill_ledger_as_separate_stats() -> None:
    baseline = new_pnl_baseline(
        asset="ETH",
        realized_pnl_usd="0",
        completed_cycles=0,
        started_at="2026-09-26T10:53:31+00:00",
    )
    baseline.update(
        {
            "return_basis": "account_equity_delta",
            "account_baseline_equity_usd": "241.774564",
            "account_baseline_at": "2026-09-26T11:11:38+00:00",
            "confirmed_pnl_usd": "0.064454",
            "confirmed_four_leg_volume_usd": "557.93743",
            "tracked_completed_cycles": 7,
            "tracked_closed_child_lots": 11,
            "latest_variational_equity_usd": "127.269645",
            "latest_lighter_equity_usd": "115.121734",
            "latest_combined_equity_usd": "242.391379",
            "latest_account_snapshot_at": "2026-10-04T19:06:18+00:00",
        }
    )
    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 5),
        now=datetime(2026, 10, 5, 5, tzinfo=timezone.utc),
        equity_state={
            "latest_variational_equity_usd": "127.4",
            "latest_lighter_equity_usd": "115.2",
            "latest_combined_equity_usd": "242.6",
            "last_sample_at": "2026-10-05T05:00:00+00:00",
            "daily_history": {
                "2026-10-05": {
                    "start_equity_usd": "242.5",
                    "latest_equity_usd": "242.6",
                    "sample_count": 2,
                    "first_sample_at": "2026-10-04T16:00:00+00:00",
                    "latest_sample_at": "2026-10-05T05:00:00+00:00",
                    "coverage_complete": False,
                }
            },
        },
    )

    assert payload["beijing_day_actual_pnl_usd"] == "0.1"
    assert payload["beijing_day_return_pct"] == str(Decimal("0.1") / Decimal("241.774564") * 100)
    assert payload["daily_confirmed_pnl_usd"] == "0"
    assert payload["cumulative_confirmed_pnl_usd"] == "0.064454"
    assert payload["run_actual_pnl_usd"] == "0.825436"
    assert payload["cumulative_four_leg_volume_usd"] == "557.93743"


def test_daily_report_defaults_to_previous_completed_beijing_day() -> None:
    assert resolve_day(
        "yesterday",
        now=datetime(2026, 8, 31, 16, 1, tzinfo=timezone.utc),
    ) == date(2026, 8, 31)
