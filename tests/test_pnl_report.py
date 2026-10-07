import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from main import (
    VariationalToLighterRuntime,
    account_risk_context,
    extract_lighter_account_metrics,
    extract_variational_account_metrics,
)
from tools.pnl_report import (
    build_report,
    daily_pnl_breakdown,
    deduplicated_actual_pnl,
    latest_pnl_telegram_payload,
    parse_since,
    print_report,
    previous_flat_snapshot,
    reset_pnl_reporting_baseline,
    reporting_period_since,
)
from tools.lib.pnl_baseline import (
    load_pnl_baseline,
    new_pnl_baseline,
    pnl_day_summary,
    record_external_cashflow,
    record_pnl_cycle,
    write_pnl_baseline,
)


def test_extract_variational_account_metrics_does_not_double_count_upnl() -> None:
    metrics = extract_variational_account_metrics(
        {
            "pool_portfolio_result": {
                "balance": "14.80",
                "upnl": "0.05",
            }
        }
    )

    assert metrics["balance_usd"] == Decimal("14.80")
    assert metrics["upnl_usd"] == Decimal("0.05")
    assert metrics["equity_usd"] == Decimal("14.80")
    assert metrics["equity_formula"] == "balance_includes_upnl"


def test_variational_equity_matches_observed_flat_account_snapshot() -> None:
    metrics = extract_variational_account_metrics(
        {"pool_portfolio_result": {"balance": "137.451292", "upnl": "0"}}
    )
    assert metrics["equity_usd"] == Decimal("137.451292")


def test_extract_lighter_account_metrics_uses_total_asset_value() -> None:
    metrics = extract_lighter_account_metrics(
        {
            "accounts": [
                {
                    "collateral": "20.25",
                    "available_balance": "19.00",
                    "total_asset_value": "20.50",
                    "cross_initial_margin_requirement": "4.10",
                    "cross_maintenance_margin_requirement": "2.05",
                }
            ]
        }
    )

    assert metrics["collateral_usd"] == Decimal("20.25")
    assert metrics["available_balance_usd"] == Decimal("19.00")
    assert metrics["total_asset_value_usd"] == Decimal("20.50")
    assert metrics["equity_usd"] == Decimal("20.50")
    assert metrics["equity_formula"] == "total_asset_value"
    assert metrics["initial_margin_usage_pct"] == Decimal("20")
    assert metrics["maintenance_margin_usage_pct"] == Decimal("10")


def test_account_risk_context_enforces_per_venue_leverage_and_balance() -> None:
    context = account_risk_context(
        variational_metrics={"equity_usd": Decimal("100")},
        lighter_metrics={"equity_usd": Decimal("40")},
        current_notional_usd=Decimal("180"),
        proposed_notional_usd=Decimal("220"),
        max_venue_leverage=Decimal("5"),
        margin_warning_pct=Decimal("40"),
        margin_block_entry_pct=Decimal("50"),
        margin_reduce_pct=Decimal("60"),
        margin_emergency_pct=Decimal("75"),
        balance_warning_ratio=Decimal("0.80"),
        balance_block_ratio=Decimal("0.50"),
    )

    assert context["risk_action"] == "block_entry"
    assert context["risk_reason"] == "venue_leverage_exceeds_hard_entry_limit"
    assert context["lighter_projected_leverage"] == "5.5"


def test_account_risk_context_builds_manual_rebalance_plan() -> None:
    context = account_risk_context(
        variational_metrics={"equity_usd": Decimal("100")},
        lighter_metrics={"equity_usd": Decimal("70")},
        current_notional_usd=Decimal("20"),
        proposed_notional_usd=Decimal("40"),
        max_venue_leverage=Decimal("5"),
        margin_warning_pct=Decimal("40"),
        margin_block_entry_pct=Decimal("50"),
        margin_reduce_pct=Decimal("60"),
        margin_emergency_pct=Decimal("75"),
        balance_warning_ratio=Decimal("0.82"),
        balance_block_ratio=Decimal("0.74"),
    )

    assert context["risk_action"] == "block_entry"
    assert context["risk_reason"] == "venue_equity_imbalance_blocks_entry"
    assert context["rebalance_from_venue"] == "variational"
    assert context["rebalance_to_venue"] == "lighter"
    assert context["rebalance_suggested_amount_usd"] == "12.2375"
    assert context["rebalance_target_imbalance_pct"] == "6.5"


def test_deduplicated_actual_pnl_requires_confirmed_fill() -> None:
    base = {
        "event": "live_inventory_actual_pnl",
        "run_id": "run-1",
        "asset": "ETH",
        "lot_id": 1,
        "logged_at": "2026-08-25T00:00:00+00:00",
    }
    rows = [
        {
            **base,
            "actual_pnl_status": "pending_lighter_final_fill",
            "actual_pnl_usd": "1.00",
        },
        {
            **base,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.10",
        },
        {
            **base,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.20",
        },
    ]

    selected = deduplicated_actual_pnl(rows)

    assert len(selected) == 1
    assert selected[0]["actual_pnl_usd"] == "0.20"


def test_reset_pnl_reporting_baseline_starts_empty_period(tmp_path) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "open_lots": [],
                "pending_actions": [],
                "realized_pnl_usd": "-0.081547",
                "completed_cycles": 29,
            }
        ),
        encoding="utf-8",
    )

    baseline = reset_pnl_reporting_baseline(
        asset="ETH",
        state_path=state_path,
        baseline_path=baseline_path,
        started_at="2026-08-26T00:00:00+00:00",
    )

    assert baseline["started_at"] == "2026-08-26T00:00:00+00:00"
    assert baseline["realized_pnl_baseline_usd"] == "-0.081547"
    assert baseline["completed_cycles_baseline"] == 29
    assert baseline["confirmed_pnl_usd"] == "0"
    assert baseline["tracked_completed_cycles"] == 0
    assert baseline["account_baseline_equity_usd"] is None
    assert baseline["reporting_timezone"] == "Asia/Shanghai"
    assert baseline["current_beijing_day"] == "2026-08-26"


def test_reset_pnl_reporting_baseline_refuses_open_state(tmp_path) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "open",
                "open_lots": [{"lot_id": 1}],
                "pending_actions": [],
            }
        ),
        encoding="utf-8",
    )

    try:
        reset_pnl_reporting_baseline(
            asset="ETH",
            state_path=state_path,
            baseline_path=tmp_path / "pnl_reporting_baseline.json",
        )
    except RuntimeError as exc:
        assert str(exc) == "local_live_inventory_state_not_flat"
    else:
        raise AssertionError("open state must refuse PnL baseline reset")


def test_pnl_baseline_records_cycles_once_across_runs(tmp_path) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "open_lots": [],
                "pending_actions": [],
                "realized_pnl_usd": "0",
                "completed_cycles": 0,
            }
        ),
        encoding="utf-8",
    )
    reset_pnl_reporting_baseline(
        asset="ETH",
        state_path=state_path,
        baseline_path=baseline_path,
    )

    record_pnl_cycle(
        baseline_path,
        run_id="run-1",
        asset="ETH",
        lot_id=1,
        actual_pnl_usd="0.01",
        four_leg_volume_usd="80",
        closed_child_lots=2,
    )
    record_pnl_cycle(
        baseline_path,
        run_id="run-1",
        asset="ETH",
        lot_id=1,
        actual_pnl_usd="0.01",
        four_leg_volume_usd="80",
        closed_child_lots=2,
    )
    record_pnl_cycle(
        baseline_path,
        run_id="run-2",
        asset="ETH",
        lot_id=1,
        actual_pnl_usd="-0.004",
        four_leg_volume_usd="40",
    )

    baseline = load_pnl_baseline(baseline_path)
    assert baseline is not None
    assert baseline["confirmed_pnl_usd"] == "0.006"
    assert baseline["tracked_completed_cycles"] == 2
    assert baseline["confirmed_four_leg_volume_usd"] == "120"
    assert baseline["tracked_closed_child_lots"] == 3


def test_pnl_baseline_rolls_at_beijing_midnight(tmp_path) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "open_lots": [],
                "pending_actions": [],
                "realized_pnl_usd": "0",
                "completed_cycles": 0,
            }
        ),
        encoding="utf-8",
    )
    reset_pnl_reporting_baseline(
        asset="ETH",
        state_path=state_path,
        baseline_path=baseline_path,
        started_at="2026-08-27T15:00:00+00:00",
    )

    record_pnl_cycle(
        baseline_path,
        run_id="run-1",
        asset="ETH",
        lot_id=1,
        actual_pnl_usd="0.01",
        observed_at="2026-08-27T15:59:59+00:00",
        four_leg_volume_usd="80",
    )
    record_pnl_cycle(
        baseline_path,
        run_id="run-1",
        asset="ETH",
        lot_id=2,
        actual_pnl_usd="0.02",
        observed_at="2026-08-27T16:00:01+00:00",
        four_leg_volume_usd="120",
        closed_child_lots=3,
    )

    baseline = load_pnl_baseline(baseline_path)
    assert baseline is not None
    assert baseline["confirmed_pnl_usd"] == "0.03"
    assert baseline["current_beijing_day"] == "2026-08-28"
    assert baseline["daily_confirmed_pnl_usd"] == "0.02"
    assert baseline["daily_tracked_completed_cycles"] == 1
    assert baseline["daily_four_leg_volume_usd"] == "120"
    assert baseline["daily_closed_child_lots"] == 3
    prior_day = pnl_day_summary(baseline, "2026-08-27")
    assert prior_day["confirmed_pnl_usd"] == "0.01"
    assert prior_day["four_leg_volume_usd"] == "80"


def test_pnl_baseline_backfills_volume_without_double_counting_pnl(tmp_path) -> None:
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    write_pnl_baseline(
        baseline_path,
        new_pnl_baseline(
            asset="ETH",
            realized_pnl_usd="0",
            completed_cycles=0,
            started_at="2026-08-31T00:00:00+00:00",
        ),
    )
    record_pnl_cycle(
        baseline_path,
        run_id="run-old",
        asset="ETH",
        lot_id=1,
        actual_pnl_usd="0.01",
        observed_at="2026-08-31T01:00:00+00:00",
    )
    record_pnl_cycle(
        baseline_path,
        run_id="run-old",
        asset="ETH",
        lot_id=1,
        actual_pnl_usd="0.01",
        observed_at="2026-08-31T01:00:00+00:00",
        four_leg_volume_usd="80",
        closed_child_lots=2,
    )

    baseline = load_pnl_baseline(baseline_path)
    assert baseline is not None
    assert baseline["confirmed_pnl_usd"] == "0.01"
    assert baseline["tracked_completed_cycles"] == 1
    assert baseline["confirmed_four_leg_volume_usd"] == "80"
    assert baseline["tracked_closed_child_lots"] == 2


def test_parse_since_date_uses_beijing_midnight() -> None:
    assert parse_since("2026-08-28") == datetime(
        2026,
        8,
        27,
        16,
        0,
        tzinfo=timezone.utc,
    )


def test_reporting_period_uses_30_or_90_beijing_days() -> None:
    now = datetime(2026, 8, 28, 4, 0, tzinfo=timezone.utc)

    assert reporting_period_since("1m", now=now) == datetime(
        2026,
        7,
        29,
        16,
        0,
        tzinfo=timezone.utc,
    )
    assert reporting_period_since("3m", now=now) == datetime(
        2026,
        5,
        30,
        16,
        0,
        tzinfo=timezone.utc,
    )


def test_daily_breakdown_includes_zero_trade_beijing_days() -> None:
    rows = [
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.03",
            "logged_at": "2026-08-26T15:30:00+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 2,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "-0.01",
            "logged_at": "2026-08-28T01:00:00+00:00",
        },
    ]
    report = build_report(
        rows,
        since=datetime(2026, 8, 25, 16, 0, tzinfo=timezone.utc),
        capital_usd=Decimal("100"),
    )

    daily = daily_pnl_breakdown(report)

    assert [item["beijing_day"] for item in daily] == [
        "2026-08-26",
        "2026-08-27",
        "2026-08-28",
    ]
    assert [item["actual_pnl_usd"] for item in daily] == [
        Decimal("0.03"),
        Decimal("0"),
        Decimal("-0.01"),
    ]


def test_pnl_baseline_records_external_cashflow_separately(tmp_path) -> None:
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "open_lots": [],
                "pending_actions": [],
                "realized_pnl_usd": "0",
                "completed_cycles": 0,
            }
        ),
        encoding="utf-8",
    )
    reset_pnl_reporting_baseline(
        asset="ETH",
        state_path=state_path,
        baseline_path=baseline_path,
        started_at="2026-08-26T00:00:00+00:00",
    )

    record_external_cashflow(
        baseline_path,
        amount_usd="170",
        observed_at="2026-08-27T00:00:00+00:00",
        reason="test_deposit",
    )

    baseline = load_pnl_baseline(baseline_path)
    assert baseline is not None
    assert baseline["external_cashflow_usd"] == "170"
    assert baseline["external_cashflow_events"][-1]["reason"] == "test_deposit"


def test_build_report_falls_back_to_first_complete_snapshot_capital() -> None:
    rows = [
        {
            "event": "live_inventory_run_config",
            "run_id": "run-1",
            "asset": "ETH",
            "logged_at": "2026-08-25T00:00:00+00:00",
        },
        {
            "event": "live_inventory_account_snapshot",
            "run_id": "run-1",
            "asset": "ETH",
            "snapshot_status": "complete",
            "snapshot_stage": "startup_flat",
            "combined_equity_usd": "35.00",
            "snapshot_captured_at": "2026-08-25T00:01:00+00:00",
            "logged_at": "2026-08-25T00:01:00+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.05",
            "logged_at": "2026-08-26T00:00:00+00:00",
        },
    ]

    report = build_report(rows, since=None, capital_usd=None)

    assert report.capital_usd == Decimal("35.00")
    assert report.capital_source == "first_complete_account_snapshot"
    assert report.period_start == datetime(
        2026, 8, 25, tzinfo=timezone.utc
    )
    assert len(report.cycles) == 1


def test_print_report_marks_annualized_unavailable_without_capital(
    capsys,
) -> None:
    report = build_report(
        [
            {
                "event": "live_inventory_actual_pnl",
                "run_id": "run-1",
                "asset": "ETH",
                "lot_id": 1,
                "actual_pnl_status": "lighter_final_fill_confirmed",
                "actual_pnl_usd": "0.01",
                "logged_at": "2026-08-25T01:00:00+00:00",
            }
        ],
        since=None,
        capital_usd=None,
    )

    print_report(report, last_cycles=1)

    output = capsys.readouterr().out
    assert "简单年化=-" in output
    assert "年化可信度=无法计算_缺少有效本金或统计时长" in output


def test_previous_flat_snapshot_ignores_entry_snapshot() -> None:
    snapshots = [
        {
            "snapshot_stage": "startup_flat",
            "snapshot_captured_at": "2026-08-25T00:00:00+00:00",
        },
        {
            "snapshot_stage": "entry_confirmed",
            "snapshot_captured_at": "2026-08-25T01:00:00+00:00",
        },
        {
            "snapshot_stage": "exit_confirmed_flat",
            "snapshot_captured_at": "2026-08-25T02:00:00+00:00",
        },
    ]

    baseline = previous_flat_snapshot(snapshots, snapshots[-1])

    assert baseline is snapshots[0]


def test_latest_pnl_telegram_payload_uses_latest_confirmed_cycle() -> None:
    rows = [
        {
            "event": "live_inventory_run_config",
            "run_id": "run-1",
            "asset": "ETH",
            "logged_at": "2026-08-25T00:00:00+00:00",
        },
        {
            "event": "live_inventory_account_snapshot",
            "run_id": "run-1",
            "asset": "ETH",
            "snapshot_stage": "startup_flat",
            "snapshot_status": "complete",
            "combined_equity_usd": "35.00",
            "snapshot_captured_at": "2026-08-25T00:00:01+00:00",
            "logged_at": "2026-08-25T00:00:01+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.04",
            "logged_at": "2026-08-25T01:00:00+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 2,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.06",
            "logged_at": "2026-08-25T02:00:00+00:00",
        },
        {
            "event": "live_inventory_account_snapshot",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 2,
            "snapshot_stage": "exit_confirmed_flat",
            "snapshot_status": "complete",
            "account_snapshot_flat": True,
            "variational_equity_usd": "14.90",
            "lighter_equity_usd": "20.20",
            "combined_equity_usd": "35.10",
            "snapshot_captured_at": "2026-08-25T02:00:01+00:00",
            "logged_at": "2026-08-25T02:00:01+00:00",
        },
    ]
    report = build_report(rows, since=None, capital_usd=None)

    payload = latest_pnl_telegram_payload(rows, report, asset="ETH")

    assert payload is not None
    assert payload["lot_id"] == 2
    assert payload["summary_status"] == "complete"
    assert payload["cycle_actual_pnl_usd"] == "0.06"
    assert payload["run_actual_pnl_usd"] == "0.10"
    assert payload["beijing_day"] == "2026-08-25"
    assert payload["beijing_day_actual_pnl_usd"] == "0.10"
    assert payload["beijing_day_completed_cycles"] == 2
    assert payload["account_net_change_usd"] == "0.10"
    assert payload["combined_equity_usd"] == "35.10"
    assert payload["completed_cycles"] == 2
    assert payload["return_pnl_source"] == "account_equity_delta"


def test_latest_pnl_telegram_payload_accumulates_tracking_period() -> None:
    rows = [
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-1",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.01",
            "logged_at": "2026-08-26T01:00:00+00:00",
        },
        {
            "event": "live_inventory_actual_pnl",
            "run_id": "run-2",
            "asset": "ETH",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "-0.004",
            "logged_at": "2026-08-27T01:00:00+00:00",
        },
    ]
    tracking_baseline = {
        "asset": "ETH",
        "started_at": "2026-08-26T00:00:00+00:00",
        "account_baseline_equity_usd": None,
        "account_baseline_at": None,
    }
    report = build_report(
        rows,
        since=datetime(2026, 8, 26, tzinfo=timezone.utc),
        capital_usd=Decimal("35"),
    )

    payload = latest_pnl_telegram_payload(
        rows,
        report,
        asset="ETH",
        tracking_baseline=tracking_baseline,
    )

    assert payload is not None
    assert payload["cycle_actual_pnl_usd"] == "-0.004"
    assert payload["run_actual_pnl_usd"] == "0.006"
    assert payload["beijing_day_actual_pnl_usd"] == "-0.004"
    assert payload["beijing_day_completed_cycles"] == 1
    assert payload["completed_cycles"] == 2
    assert payload["return_pnl_source"] == "confirmed_pair_fills"
    assert Decimal(payload["return_pct"]) > 0


def test_account_snapshot_logs_normalized_equity_without_raw_account(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(
            VariationalToLighterRuntime
        )
        runtime.mode = "live"
        runtime.live_inventory_dry_decisions = False
        runtime.live_inventory_run_id = "run-1"
        runtime.live_inventory_schema_version = "2"
        runtime.live_inventory_strategy_version = "basis-v4-live-test"
        runtime.live_inventory_strategy_variant = "test"
        runtime.live_inventory_config_hash = "hash"
        runtime.live_inventory_open_lots = []
        runtime.orders_file = tmp_path / "order_metrics.jsonl"
        runtime._order_write_lock = asyncio.Lock()
        runtime.telegram_notifier = None
        runtime.runtime = SimpleNamespace(
            monitor=SimpleNamespace(
                _lock=asyncio.Lock(),
                portfolio_summary={
                    "balance": "14.80",
                    "upnl": "0.05",
                    "published_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        )

        await runtime.capture_live_inventory_account_snapshot(
            stage="startup_flat",
            asset="ETH",
            lighter_account_result={
                "accounts": [
                    {
                        "collateral": "20.25",
                        "available_balance": "19.00",
                        "l1_address": "must-not-be-logged",
                    }
                ]
            },
        )

        row = json.loads(runtime.orders_file.read_text(encoding="utf-8"))
        assert row["snapshot_status"] == "complete"
        assert row["variational_snapshot_fresh"] is True
        assert row["variational_equity_usd"] == "14.80"
        assert row["lighter_equity_usd"] == "20.25"
        assert row["combined_equity_usd"] == "35.05"
        assert "must-not-be-logged" not in json.dumps(row)

    asyncio.run(run())


def test_exit_flat_snapshot_detects_runtime_deposit_after_cycle_pnl(
    tmp_path,
) -> None:
    async def run() -> None:
        baseline_path = tmp_path / "pnl_reporting_baseline.json"
        baseline_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "asset": "ETH",
                    "started_at": "2026-08-27T00:00:00+00:00",
                    "realized_pnl_baseline_usd": "0",
                    "completed_cycles_baseline": 0,
                    "confirmed_pnl_usd": "0",
                    "tracked_completed_cycles": 0,
                    "counted_cycle_keys": [],
                    "account_baseline_equity_usd": "100",
                    "account_baseline_at": "2026-08-27T00:00:00+00:00",
                    "external_cashflow_usd": "0",
                    "external_cashflow_events": [],
                }
            ),
            encoding="utf-8",
        )
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.live_inventory_dry_decisions = False
        runtime.live_inventory_run_id = "run-deposit"
        runtime.live_inventory_schema_version = "2"
        runtime.live_inventory_strategy_version = "test"
        runtime.live_inventory_strategy_variant = "test"
        runtime.live_inventory_config_hash = "test"
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_realized_pnl_usd = Decimal("2")
        runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_pnl_baseline_file = baseline_path
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_last_actual_pnl_payload = {
            "lot_id": 7,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "2",
        }
        runtime.orders_file = tmp_path / "order_metrics.jsonl"
        runtime._order_write_lock = asyncio.Lock()
        runtime.telegram_notifier = None
        runtime.runtime = SimpleNamespace(
            monitor=SimpleNamespace(
                _lock=asyncio.Lock(),
                portfolio_summary={
                    "balance": "50",
                    "upnl": "0",
                    "published_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        )
        runtime.sync_live_inventory_pnl_tracking_baseline()

        await runtime.capture_live_inventory_account_snapshot(
            stage="exit_confirmed_flat",
            asset="ETH",
            lot_id=7,
            lighter_account_result={"accounts": [{"collateral": "72"}]},
        )

        baseline = load_pnl_baseline(baseline_path)
        assert baseline["confirmed_pnl_usd"] == "2"
        assert baseline["external_cashflow_usd"] == "20"
        assert baseline["external_cashflow_events"][-1]["reason"] == (
            "runtime_flat_unexplained_equity_change"
        )

    asyncio.run(run())


def test_live_pnl_summary_uses_account_change_for_annualized_return(
    monkeypatch,
) -> None:
    monkeypatch.delenv("PNL_REPORT_CAPITAL_USD", raising=False)
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_last_actual_pnl_payload = {
        "lot_id": 3,
        "actual_pnl_status": "lighter_final_fill_confirmed",
        "actual_pnl_usd": "0.05",
    }
    runtime.live_inventory_realized_pnl_usd = Decimal("1.05")
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_completed_cycles = 1
    runtime.live_inventory_pnl_account_baseline_equity_usd = Decimal("35")
    runtime.live_inventory_pnl_account_baseline_at = (
        "2026-08-25T00:00:00+00:00"
    )

    payload = runtime.live_inventory_pnl_summary_payload(
        asset="ETH",
        lot_id=3,
        variational_equity_usd=Decimal("14.90"),
        lighter_equity_usd=Decimal("20.15"),
        combined_equity_usd=Decimal("35.05"),
        captured_at="2026-08-26T00:00:00+00:00",
    )

    assert payload["summary_status"] == "complete"
    assert payload["cycle_actual_pnl_usd"] == "0.05"
    assert payload["run_actual_pnl_usd"] == "0.05"
    assert payload["account_net_change_usd"] == "0.05"
    assert payload["capital_usd"] == "35"
    assert payload["return_pnl_source"] == "account_equity_delta"
    assert Decimal(payload["return_pct"]) > 0
    assert Decimal(payload["annualized_simple_pct"]) > 0
    assert payload["annualized_reliability"] == "sample_under_30_days"

    non_flat_payload = runtime.live_inventory_pnl_summary_payload(
        asset="ETH",
        lot_id=3,
        variational_equity_usd=Decimal("14.90"),
        lighter_equity_usd=Decimal("20.15"),
        combined_equity_usd=Decimal("35.05"),
        captured_at="2026-08-26T00:00:00+00:00",
        account_snapshot_flat=False,
    )

    assert non_flat_payload["summary_status"] == "partial"
    assert non_flat_payload["account_net_change_usd"] is None
    assert non_flat_payload["return_pnl_source"] == "confirmed_pair_fills"


def test_live_pnl_summary_persists_tracking_across_runs(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("PNL_REPORT_CAPITAL_USD", raising=False)
    state_path = tmp_path / "live_inventory_state.json"
    baseline_path = tmp_path / "pnl_reporting_baseline.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "open_lots": [],
                "pending_actions": [],
                "realized_pnl_usd": "0",
                "completed_cycles": 0,
            }
        ),
        encoding="utf-8",
    )
    reset_pnl_reporting_baseline(
        asset="ETH",
        state_path=state_path,
        baseline_path=baseline_path,
        started_at="2026-08-25T00:00:00+00:00",
    )
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_pnl_baseline_file = baseline_path
    runtime.live_allowed_assets = {"ETH"}
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_completed_cycles = 0
    runtime.live_inventory_pnl_account_baseline_equity_usd = None
    runtime.live_inventory_pnl_account_baseline_at = None
    runtime.live_inventory_pnl_tracking_confirmed_pnl_usd = None
    runtime.live_inventory_pnl_tracking_completed_cycles = None
    runtime.sync_live_inventory_pnl_tracking_baseline()

    runtime.live_inventory_run_id = "run-1"
    runtime.live_inventory_last_actual_pnl_payload = {
        "lot_id": 1,
        "actual_pnl_status": "lighter_final_fill_confirmed",
        "actual_pnl_usd": "0.01",
    }
    first = runtime.live_inventory_pnl_summary_payload(
        asset="ETH",
        lot_id=1,
        variational_equity_usd=None,
        lighter_equity_usd=None,
        combined_equity_usd=None,
        captured_at="2026-08-26T00:00:00+00:00",
    )

    runtime.live_inventory_run_id = "run-2"
    runtime.live_inventory_last_actual_pnl_payload = {
        "lot_id": 1,
        "actual_pnl_status": "lighter_final_fill_confirmed",
        "actual_pnl_usd": "-0.004",
    }
    second = runtime.live_inventory_pnl_summary_payload(
        asset="ETH",
        lot_id=1,
        variational_equity_usd=None,
        lighter_equity_usd=None,
        combined_equity_usd=None,
        captured_at="2026-08-27T00:00:00+00:00",
    )

    assert first["run_actual_pnl_usd"] == "0.01"
    assert first["completed_cycles"] == 1
    assert second["run_actual_pnl_usd"] == "0.006"
    assert second["completed_cycles"] == 2
