import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from tools.bootstrap_rh_pnl_baseline import build_baseline, main, plan_history
from tools.daily_pnl_report import build_daily_payload


RH_FIELDS = {
    "asset": "ETH",
    "mode": "live",
    "execution_mode": "live",
    "strategy_variant": "eth-short-robinhood-chain",
}


def _row(event: str, at: str, **fields):
    return {**RH_FIELDS, "event": event, "logged_at": at, **fields}


def _snapshot(at: str, *, stage: str, var: str, lighter: str, flat: bool):
    return _row(
        "live_inventory_account_snapshot",
        at,
        snapshot_status="complete",
        snapshot_stage=stage,
        snapshot_captured_at=at,
        account_snapshot_flat=flat,
        variational_equity_usd=var,
        lighter_equity_usd=lighter,
        combined_equity_usd=str(Decimal(var) + Decimal(lighter)),
    )


def _close(at: str, *, lot: int, pnl: str, volume: str = "80"):
    return _row(
        "live_inventory_actual_pnl",
        at,
        run_id=f"rh-run-{lot}",
        lot_id=lot,
        actual_pnl_status="lighter_final_fill_confirmed",
        actual_pnl_usd=pnl,
        four_leg_volume_usd=volume,
        confirmed_at=at,
    )


def _history():
    return [
        _row("live_inventory_run_config", "2026-09-26T11:11:17+00:00"),
        _snapshot(
            "2026-09-26T11:11:23+00:00",
            stage="startup_flat",
            var="120",
            lighter="120",
            flat=True,
        ),
        _close("2026-09-28T15:50:00+00:00", lot=1, pnl="0.5"),
        _close("2026-09-28T15:50:00+00:00", lot=1, pnl="0.5"),
        _snapshot(
            "2026-09-28T16:00:00+00:00",
            stage="exit_confirmed_flat",
            var="120.25",
            lighter="120.25",
            flat=True,
        ),
        _close("2026-10-04T15:00:00+00:00", lot=2, pnl="-0.2"),
        _snapshot(
            "2026-10-04T15:10:00+00:00",
            stage="entry_confirmed",
            var="121",
            lighter="119",
            flat=False,
        ),
    ]


def test_bootstrap_replays_only_rh_closes_and_uses_first_flat_capital(tmp_path, monkeypatch):
    monkeypatch.delenv("PNL_REPORT_CAPITAL_USD", raising=False)
    plan = plan_history(_history())
    assert len(plan.cycles) == 2
    assert plan.last_flat_residual_usd == 0

    baseline = build_baseline(
        plan,
        tmp_path / "pnl_reporting_baseline.json",
        observed_at="2026-10-05T05:00:00+00:00",
    )
    assert baseline["account_baseline_equity_usd"] == "240"
    assert baseline["confirmed_pnl_usd"] == "0.3"
    assert baseline["confirmed_four_leg_volume_usd"] == "160"
    assert baseline["tracked_completed_cycles"] == 2
    assert baseline["current_beijing_day"] == "2026-10-05"
    assert baseline["daily_history"]["2026-09-28"]["confirmed_pnl_usd"] == "0.5"
    assert baseline["daily_history"]["2026-10-04"]["confirmed_pnl_usd"] == "-0.2"

    payload = build_daily_payload(
        baseline,
        asset="ETH",
        day=date(2026, 10, 4),
        now=datetime(2026, 10, 5, 5, tzinfo=timezone.utc),
    )
    assert payload["capital_usd"] == "240"
    assert payload["capital_source"] == "tracking_account_baseline"
    assert payload["beijing_day_actual_pnl_usd"] == "-0.2"
    assert payload["return_pct"] == str(Decimal("0.3") / Decimal("240") * 100)


def test_bootstrap_rejects_mixed_mainnet_close():
    foreign = {
        **_close("2026-09-29T00:00:00+00:00", lot=3, pnl="1"),
        "strategy_variant": "eth-short-mainnet",
    }
    with pytest.raises(ValueError, match="outside the RH live strategy"):
        plan_history([*_history(), foreign])


def test_bootstrap_requires_first_complete_flat_snapshot():
    rows = [row for row in _history() if row.get("snapshot_stage") != "startup_flat"]
    with pytest.raises(ValueError, match="startup-flat account snapshot is missing"):
        plan_history(rows)


def test_bootstrap_rejects_missing_verified_volume():
    rows = _history()
    for row in rows:
        if row.get("event") == "live_inventory_actual_pnl":
            row.pop("four_leg_volume_usd")
    with pytest.raises(ValueError, match="four-leg volume cannot be verified"):
        plan_history(rows)


def test_bootstrap_exposes_unexplained_flat_account_change():
    rows = _history()
    for row in rows:
        if row.get("snapshot_stage") == "exit_confirmed_flat":
            row["variational_equity_usd"] = "125.25"
            row["combined_equity_usd"] = "245.50"
    assert plan_history(rows).last_flat_residual_usd == Decimal("5")


def test_bootstrap_apply_requires_transfer_confirmation_and_stopped_strategy(
    tmp_path, monkeypatch
):
    rows = _history()
    log_path = tmp_path / "order_metrics.jsonl"
    log_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps({
            "asset": "ETH",
            "status": "manual_review_required",
            "manual_review_reason": "basis_entry_lighter_submit_after_var_fill_failed",
            "open_lots": [{"qty": "0.0074"}] * 11,
            "pending_actions": [],
        }),
        encoding="utf-8",
    )
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    digest = plan_history(rows).digest
    args = [
        "bootstrap_rh_pnl_baseline.py",
        "--log-path", str(log_path),
        "--state-path", str(state_path),
        "--baseline-path", str(baseline_path),
        "--apply", "--expect-digest", digest,
    ]
    monkeypatch.setattr(sys, "argv", args)
    monkeypatch.setattr(
        "tools.bootstrap_rh_pnl_baseline.strategy_running", lambda: False
    )
    with pytest.raises(SystemExit, match="transfer_history_not_confirmed"):
        main()
    assert not baseline_path.exists()

    monkeypatch.setattr(sys, "argv", [*args, "--i-confirm-no-unrecorded-transfers"])
    monkeypatch.setattr(
        "tools.bootstrap_rh_pnl_baseline.strategy_running", lambda: True
    )
    with pytest.raises(SystemExit, match="strategy_running"):
        main()
    assert not baseline_path.exists()

    monkeypatch.setattr(
        "tools.bootstrap_rh_pnl_baseline.strategy_running", lambda: False
    )
    assert main() == 0
    assert baseline_path.exists()
