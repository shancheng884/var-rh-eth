import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import tools.daily_pnl_report as report
from tools.lib.account_equity_ledger import record_account_equity_sample
from tools.lib.telegram_notifier import format_telegram_trade_message


RESET_AT = "2026-10-08T16:09:32.991765+00:00"


def _flat_snapshot(at: str, *, var: str = "135.960472", rh: str = "137.964262") -> dict:
    total = str(Decimal(var) + Decimal(rh))
    return {
        "event": "live_inventory_account_snapshot",
        "asset": "ETH",
        "logged_at": at,
        "snapshot_captured_at": at,
        "snapshot_stage": "startup_flat",
        "snapshot_status": "complete",
        "snapshot_errors": {},
        "account_snapshot_flat": True,
        "variational_equity_formula": "balance_includes_upnl",
        "variational_balance_usd": var,
        "variational_upnl_usd": "0",
        "variational_equity_usd": var,
        "lighter_collateral_usd": rh,
        "lighter_unrealized_pnl_usd": "0",
        "lighter_equity_usd": rh,
        "combined_equity_usd": total,
    }


def _metrics(monkeypatch, tmp_path: Path, rows: list[dict]) -> None:
    path = tmp_path / "order_metrics.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    monkeypatch.setattr(report, "rotated_jsonl_paths", lambda _path: [path])


def test_historical_reset_requires_exact_complete_flat_snapshot(monkeypatch, tmp_path, capsys) -> None:
    _metrics(monkeypatch, tmp_path, [_flat_snapshot(RESET_AT)])
    state_path = tmp_path / "equity_state.json"
    state_path.write_text(json.dumps({
        "daily_history": {},
        "realized_tracking": {
            "start_at": "2026-09-28T01:12:50+00:00",
            "start_realized_balance_usd": "241",
        },
    }), encoding="utf-8")
    target = datetime.fromisoformat(RESET_AT)

    assert report.reset_realized_tracking_baseline(
        asset="ETH", equity_state_path=state_path,
        risk_health_path=tmp_path / "missing_health.json",
        apply=False, snapshot_at=target,
    ) == 0
    assert json.loads(state_path.read_text())["realized_tracking"]["start_at"].startswith("2026-09")
    assert "reset_starting_capital_usd=273.924734" in capsys.readouterr().out

    assert report.reset_realized_tracking_baseline(
        asset="ETH", equity_state_path=state_path,
        risk_health_path=tmp_path / "missing_health.json",
        apply=True, snapshot_at=target,
    ) == 0
    state = json.loads(state_path.read_text())
    assert state["realized_tracking"]["start_at"] == RESET_AT
    assert state["realized_tracking"]["start_realized_balance_usd"] == "273.924734"
    assert list(tmp_path.glob("equity_state.json.before_cumulative_reset.*.bak"))


def test_historical_reset_rejects_legacy_or_open_snapshots(monkeypatch, tmp_path) -> None:
    legacy = _flat_snapshot(RESET_AT)
    legacy["variational_equity_formula"] = "balance_plus_upnl"
    _metrics(monkeypatch, tmp_path, [legacy])
    state_path = tmp_path / "equity_state.json"
    target = datetime.fromisoformat(RESET_AT)
    with pytest.raises(RuntimeError, match="exact complete flat"):
        report.reset_realized_tracking_baseline(
            asset="ETH", equity_state_path=state_path,
            risk_health_path=tmp_path / "missing_health.json",
            apply=True, snapshot_at=target,
        )
    assert not state_path.exists()

    open_snapshot = _flat_snapshot(RESET_AT)
    open_snapshot["account_snapshot_flat"] = False
    _metrics(monkeypatch, tmp_path, [open_snapshot])
    with pytest.raises(RuntimeError, match="exact complete flat"):
        report.reset_realized_tracking_baseline(
            asset="ETH", equity_state_path=state_path,
            risk_health_path=tmp_path / "missing_health.json",
            apply=True, snapshot_at=target,
        )
    assert not state_path.exists()


def test_reset_day_starts_at_zero_and_ignores_old_trades_and_future_equity(monkeypatch, tmp_path) -> None:
    old_trade = {
        "event": "live_inventory_entered", "asset": "ETH",
        "logged_at": "2026-10-08T16:00:00+00:00", "qty": "0.01",
        "var_price": "2500", "lighter_price": "2500",
    }
    _metrics(monkeypatch, tmp_path, [
        old_trade,
        _flat_snapshot(RESET_AT),
        _flat_snapshot("2026-10-09T16:15:00+00:00", var="140", rh="140"),
    ])
    state = {
        "realized_tracking": {
            "start_at": RESET_AT,
            "start_equity_usd": "273.924734",
            "start_realized_balance_usd": "273.924734",
        },
        "last_sample_at": "2026-10-09T16:15:00+00:00",
        "latest_realized_sample_at": "2026-10-09T16:15:00+00:00",
        "variational_equity_formula_version": "balance_includes_upnl_v1",
        "latest_variational_equity_usd": "140",
        "latest_lighter_equity_usd": "140",
        "latest_combined_equity_usd": "280",
        "latest_combined_realized_balance_usd": "280",
        "realized_daily_history": {},
    }
    payload = report.build_realized_balance_payload(
        {}, equity_state=state, platform_ledger={}, asset="ETH",
        day=date(2026, 10, 9),
        now=datetime(2026, 10, 9, 17, 0, tzinfo=timezone.utc),
    )
    assert payload["summary_status"] == "partial"
    assert payload["daily_trade_count"] == 0
    assert payload["daily_volume_usd"] == "0"
    assert Decimal(payload["daily_net_pnl_usd"]) == 0
    assert payload["cumulative_trade_count"] == 0
    assert payload["cumulative_volume_usd"] == "0"
    assert Decimal(payload["cumulative_net_pnl_usd"]) == 0
    assert payload["annualized_simple_pct"] == "0"
    assert payload["statistics_start_day"] == "2026-10-09"
    assert payload["variational_equity_usd"] == "135.960472"
    assert payload["lighter_equity_usd"] == "137.964262"


def test_reset_day_message_has_only_requested_fields() -> None:
    message = format_telegram_trade_message("live_inventory_pnl_summary", {
        "asset": "ETH", "summary_scope": "realized_balance_daily",
        "summary_status": "partial", "beijing_day": "2026-10-09",
        "daily_trade_count": 0, "daily_volume_usd": "0",
        "daily_net_pnl_usd": "0", "cumulative_trade_count": 0,
        "cumulative_volume_usd": "0", "cumulative_net_pnl_usd": "0",
        "annualized_simple_pct": "0", "statistics_start_day": "2026-10-09",
        "variational_equity_usd": "135.960472", "lighter_equity_usd": "137.964262",
        "cashflow_verified": False,
    })
    assert message.splitlines() == [
        "[Var/RH] 北京时间每日收益",
        "日期：2026-10-09｜资产：ETH｜状态：部分时段",
        "今日平台成交笔数：0 笔（开仓、平仓子单均计）",
        "今日平台总成交量：0 U",
        "今日双平台总盈亏：0 U",
        "累计双平台成交笔数：0 笔",
        "累计双平台总成交量：0 U",
        "累计双平台总盈亏：0 U",
        "累计简单年化：0%",
        "统计起始日：2026-10-09",
        "Variational 权益：135.960472 U",
        "RH 权益：137.964262 U",
    ]


def test_realized_daily_history_keeps_venue_equities(tmp_path) -> None:
    path = tmp_path / "equity_state.json"
    state = record_account_equity_sample(path, {
        "captured_at": RESET_AT,
        "variational_equity_usd": "135.960472",
        "lighter_equity_usd": "137.964262",
        "combined_equity_usd": "273.924734",
        "variational_realized_balance_usd": "135.960472",
        "lighter_realized_balance_usd": "137.964262",
        "combined_realized_balance_usd": "273.924734",
        "variational_equity_formula_version": "balance_includes_upnl_v1",
    })
    day = state["realized_daily_history"]["2026-10-09"]
    assert day["latest_variational_equity_usd"] == "135.960472"
    assert day["latest_lighter_equity_usd"] == "137.964262"
    assert day["variational_equity_formula_version"] == "balance_includes_upnl_v1"
