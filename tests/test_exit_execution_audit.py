import json
from datetime import datetime, timezone
from decimal import Decimal

from tools.exit_execution_audit import audit_files


def test_audit_groups_final_fills_and_normalizes_adverse_drift(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    common = {
        "asset": "ETH",
        "strategy_version": "basis-v4-live-v16",
        "run_id": "run-a",
        "logged_at": "2026-10-05T00:00:00+00:00",
    }
    rows = [
        {
            **common,
            "event": "live_inventory_actual_pnl",
            "lot_id": 15,
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.002146",
            "actual_pnl_bps": "1.07",
            "estimated_pnl_bps": "5.25",
            "estimated_vs_actual_pnl_shortfall_bps": "4.18",
            "closed_child_lots": 1,
            "exit_var_fill_to_lighter_fill_ms": "368.387",
        },
        {
            **common,
            "event": "live_inventory_final_pnl",
            "lot_id": 15,
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "exit_var_fill_drift_bps": "0",
            "exit_lighter_fill_drift_bps": "-4.176",
        },
        {
            **common,
            "event": "live_inventory_actual_pnl",
            "lot_id": 16,
            "direction": "long_var_short_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.004",
            "estimated_vs_actual_pnl_shortfall_bps": "2",
            "closed_child_lots": 2,
        },
        {
            **common,
            "event": "live_inventory_final_pnl",
            "lot_id": 16,
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "exit_var_fill_drift_bps": "-1",
            "exit_lighter_fill_drift_bps": "3",
        },
        {
            **common,
            "event": "live_inventory_actual_pnl",
            "run_id": "run-b",
            "lot_id": 15,
            "actual_pnl_status": "pending",
            "actual_pnl_usd": "99",
        },
        {
            **common,
            "event": "live_inventory_actual_pnl",
            "run_id": "run-c",
            "lot_id": 17,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "not-a-number",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    result = audit_files(
        [path], since=datetime(2026, 10, 4, 16, tzinfo=timezone.utc), asset="ETH"
    )

    assert len(result["groups"]) == 2
    assert result["closed_child_lots"] == 3
    assert result["price_pnl_usd"] == Decimal("0.006146")
    assert result["shortfall_median_bps"] == Decimal("3.09")
    assert result["positive_shortfall_groups"] == 2
    assert result["rh_adverse_median_bps"] == Decimal("3.588")
    assert result["groups"][0]["var_adverse_bps"] == Decimal("0")
    assert result["groups"][0]["rh_adverse_bps"] == Decimal("4.176")
    assert result["groups"][1]["var_adverse_bps"] == Decimal("1")
    assert result["groups"][1]["rh_adverse_bps"] == Decimal("3")


def test_refresh_ceiling_is_price_only_and_keeps_runs_distinct(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    common = {
        "asset": "ETH",
        "strategy_version": "basis-v4-live-v16",
        "run_id": "run-a",
        "lot_id": 16,
        "logged_at": "2026-10-05T00:00:00+00:00",
        "event": "live_inventory_exit_blocked",
    }
    rows = [
        {
            **common,
            "reason": "basis_exit_refresh_pnl_below_threshold",
            "effective_min_exit_pnl_bps": "4.5",
            "fast_refresh_observations": [
                {"refreshed_pnl_bps": "4.1", "executable_pnl_bps": "3.8"}
            ],
        },
        {
            **common,
            "run_id": "run-b",
            "reason": "v4_portfolio_exit_refresh_below_threshold",
            "refreshed_pnl_bps": "3.6",
            "effective_min_exit_pnl_bps": "4.0",
        },
        {
            **common,
            "reason": "basis_exit_refresh_quote_unavailable",
        },
        {
            **common,
            "logged_at": "2026-10-04T15:59:59+00:00",
            "reason": "v4_portfolio_exit_refresh_below_threshold",
            "refreshed_pnl_bps": "99",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    result = audit_files(
        [path], since=datetime(2026, 10, 4, 16, tzinfo=timezone.utc), asset="ETH"
    )

    assert result["refresh_counts"]["basis_exit_refresh_pnl_below_threshold"] == 1
    assert result["refresh_lots"]["v4_portfolio_exit_refresh_below_threshold"] == 1
    assert result["refresh_numeric"]["with_executable_quote"] == 2
    assert result["refresh_numeric"]["without_executable_quote"] == 1
    assert result["shadow_attempts"][Decimal("3.5")] == 2
    assert result["shadow_lots"][Decimal("3.5")] == 2
    assert result["shadow_attempts"][Decimal("4.0")] == 0
