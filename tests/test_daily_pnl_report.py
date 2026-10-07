import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

import tools.daily_pnl_report as daily_report
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

    assert payload["summary_status"] == "partial"
    assert payload["beijing_day_actual_pnl_usd"] is None
    assert payload["observed_period_change_usd"] == "0.1"
    assert payload["beijing_day_return_pct"] is None
    assert payload["daily_annualized_simple_pct"] is None
    assert payload["daily_confirmed_pnl_usd"] == "0"
    assert payload["cumulative_confirmed_pnl_usd"] == "0.064454"
    assert payload["run_actual_pnl_usd"] == "0.825436"
    assert payload["cumulative_four_leg_volume_usd"] == "557.93743"


def test_account_equity_payload_prefers_newer_flat_snapshot_over_stale_state() -> None:
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
            "latest_variational_equity_usd": "137.451292",
            "latest_lighter_equity_usd": "104.447017",
            "latest_combined_equity_usd": "241.898309",
            "latest_account_snapshot_at": "2026-10-07T16:04:19.687018+00:00",
        }
    )
    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 8),
        equity_state={
            "latest_variational_equity_usd": "141.3463973042380864",
            "latest_lighter_equity_usd": "104.387816",
            "latest_combined_equity_usd": "245.7342133042380764",
            "last_sample_at": "2026-10-07T16:03:16.465490+00:00",
            "daily_history": {},
        },
    )
    assert payload["variational_equity_usd"] == "137.451292"
    assert payload["lighter_equity_usd"] == "104.447017"
    assert payload["combined_equity_usd"] == "241.898309"
    assert payload["account_snapshot_at"] == "2026-10-07T16:04:19.687018+00:00"


def test_account_equity_payload_uses_sep_26_capital_not_partial_day_change() -> None:
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
            "external_cashflow_usd": "0",
        }
    )
    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 5),
        equity_state={
            "latest_variational_equity_usd": "126.4236246912204016",
            "latest_lighter_equity_usd": "115.653125",
            "latest_combined_equity_usd": "242.0767496912204016",
            "last_sample_at": "2026-10-05T17:17:29+00:00",
            "daily_history": {
                "2026-10-05": {
                    "start_equity_usd": "240.2550703824405072",
                    "latest_equity_usd": "242.4738446912203276",
                    "sample_count": 105,
                    "first_sample_at": "2026-10-05T06:55:17+00:00",
                    "latest_sample_at": "2026-10-05T15:59:49+00:00",
                    "coverage_complete": False,
                }
            },
        },
    )

    assert payload["summary_status"] == "partial"
    assert payload["beijing_day_actual_pnl_usd"] is None
    assert payload["observed_period_change_usd"] == "2.2187743087798204"
    assert payload["beijing_day_return_pct"] is None
    assert payload["daily_annualized_simple_pct"] is None
    assert payload["run_actual_pnl_usd"] == "0.3021856912204016"
    assert payload["account_baseline_day"] == "2026-09-26"


def test_account_equity_payload_subtracts_recorded_cashflow() -> None:
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
            "external_cashflow_usd": "10",
        }
    )
    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 5),
        equity_state={
            "latest_combined_equity_usd": "252.0767496912204016",
            "last_sample_at": "2026-10-05T17:17:29+00:00",
            "daily_history": {},
        },
    )

    assert payload["run_actual_pnl_usd"] == "0.3021856912204016"
    assert payload["external_cashflow_usd"] == "10"


def test_account_equity_payload_reports_verified_full_day() -> None:
    baseline = new_pnl_baseline(
        asset="ETH", realized_pnl_usd="0", completed_cycles=0,
        started_at="2026-09-26T10:53:31+00:00",
    )
    baseline.update(
        {
            "return_basis": "account_equity_delta",
            "account_baseline_equity_usd": "200",
            "account_baseline_at": "2026-09-26T11:11:38+00:00",
        }
    )
    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 5),
        equity_state={
            "latest_combined_equity_usd": "202",
            "last_sample_at": "2026-10-05T15:55:00+00:00",
            "daily_history": {
                "2026-10-05": {
                    "first_sample_at": "2026-10-04T16:05:00+00:00",
                    "latest_sample_at": "2026-10-05T15:55:00+00:00",
                    "start_equity_usd": "201",
                    "latest_equity_usd": "202",
                    "sample_count": 288,
                    "max_sample_gap_seconds": "300",
                    "coverage_complete": True,
                }
            },
        },
    )

    assert payload["summary_status"] == "complete"
    assert payload["beijing_day_actual_pnl_usd"] == "1"
    assert payload["beijing_day_return_pct"] == "0.500"
    assert payload["daily_annualized_simple_pct"] == "182.500"
    assert payload["observed_period_change_usd"] is None


def test_account_equity_full_day_deducts_daily_cashflow() -> None:
    baseline = new_pnl_baseline(
        asset="ETH", realized_pnl_usd="0", completed_cycles=0,
        started_at="2026-09-26T10:53:31+00:00",
    )
    baseline.update(
        {
            "return_basis": "account_equity_delta",
            "account_baseline_equity_usd": "200",
            "account_baseline_at": "2026-09-26T11:11:38+00:00",
            "external_cashflow_usd": "10",
            "current_beijing_day": "2026-10-05",
            "daily_external_cashflow_usd": "10",
        }
    )
    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 5),
        equity_state={
            "latest_combined_equity_usd": "212",
            "last_sample_at": "2026-10-05T15:55:00+00:00",
            "daily_history": {
                "2026-10-05": {
                    "first_sample_at": "2026-10-04T16:05:00+00:00",
                    "latest_sample_at": "2026-10-05T15:55:00+00:00",
                    "start_equity_usd": "201",
                    "latest_equity_usd": "212",
                    "sample_count": 288,
                    "max_sample_gap_seconds": "300",
                }
            },
        },
    )

    assert payload["summary_status"] == "complete"
    assert payload["beijing_day_actual_pnl_usd"] == "1"
    assert payload["run_actual_pnl_usd"] == "2"


def test_report_sends_insufficient_day_and_advances_catchup(tmp_path, monkeypatch) -> None:
    baseline = new_pnl_baseline(
        asset="ETH", realized_pnl_usd="0", completed_cycles=0,
        started_at="2026-09-26T10:53:31+00:00",
    )
    baseline.update(
        {
            "return_basis": "account_equity_delta",
            "account_baseline_equity_usd": "241.774564",
            "account_baseline_at": "2026-09-26T11:11:38+00:00",
            "latest_combined_equity_usd": "242.0767496912204016",
        }
    )
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    equity_path = tmp_path / "account_equity_daily_state.json"
    send_path = tmp_path / "pnl_daily_telegram_state.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    equity_path.write_text(json.dumps({"daily_history": {}}), encoding="utf-8")
    monkeypatch.setattr(daily_report, "load_dotenv", lambda *_: None)
    monkeypatch.setattr(daily_report, "read_fresh_account_equity", lambda *_args, **_kwargs: (None, "stale"))

    class FakeNotifier:
        @classmethod
        def from_env(cls, **_kwargs):
            return cls()

    sent = []
    monkeypatch.setattr(daily_report, "TelegramNotifier", FakeNotifier)
    monkeypatch.setattr(
        daily_report, "send_with_retry",
        lambda _notifier, message, **_kwargs: (sent.append(message) or True, "ok"),
    )
    monkeypatch.setattr(
        sys, "argv",
        [
            "daily_pnl_report.py", "--asset", "ETH", "--day", "2026-10-05",
            "--baseline-path", str(baseline_path),
            "--equity-state-path", str(equity_path),
            "--state-path", str(send_path),
        ],
    )

    assert daily_report.main() == 0
    assert "状态：数据不足" in sent[0]
    assert "当日账户权益变化：暂不可用" in sent[0]
    assert "当日收益率：" not in sent[0]
    assert json.loads(send_path.read_text())["last_sent_day"] == "2026-10-05"


def test_daily_report_defaults_to_previous_completed_beijing_day() -> None:
    assert resolve_day(
        "yesterday",
        now=datetime(2026, 8, 31, 16, 1, tzinfo=timezone.utc),
    ) == date(2026, 8, 31)
