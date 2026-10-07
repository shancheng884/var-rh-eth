import json
from datetime import datetime, timezone
from decimal import Decimal

from tools.profitability_audit import audit_files


def test_audit_separates_runs_and_counts_only_confirmed_price_pnl(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    common = {"strategy_version": "basis-v4-live-v16", "asset": "ETH"}
    rows = [
        {
            **common,
            "logged_at": "2026-10-04T15:59:59+00:00",
            "run_id": "old",
            "event": "live_inventory_actual_pnl",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "10",
        },
        {
            **common,
            "logged_at": "2026-10-05T00:00:00+00:00",
            "run_id": "run-1",
            "event": "live_inventory_basis_state",
            "sample_index": 1,
            "sample_pair_valid": True,
            "v4_direction_edges_bps": {"short_var_long_lighter": "2"},
            "v4_direction_thresholds_bps": {"short_var_long_lighter": "3"},
        },
        {
            **common,
            "logged_at": "2026-10-05T00:01:00+00:00",
            "run_id": "run-1",
            "event": "live_inventory_v4_exit_observation",
            "lots": [{"lot_id": 1, "executable_mfe_pnl_bps": "4.2"}],
        },
        {
            **common,
            "logged_at": "2026-10-05T00:02:00+00:00",
            "run_id": "run-1",
            "event": "live_inventory_exited",
            "lot_id": 1,
        },
        {
            **common,
            "logged_at": "2026-10-05T00:03:00+00:00",
            "run_id": "run-1",
            "event": "live_inventory_actual_pnl",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_usd": "0.01",
            "actual_pnl_bps": "2.5",
        },
        {
            **common,
            "logged_at": "2026-10-05T00:04:00+00:00",
            "run_id": "run-2",
            "event": "live_inventory_v4_exit_observation",
            "lots": [{"lot_id": 1, "executable_mfe_pnl_bps": "3.6"}],
        },
        {
            **common,
            "logged_at": "2026-10-05T00:05:00+00:00",
            "run_id": "run-2",
            "event": "live_inventory_actual_pnl",
            "lot_id": 1,
            "actual_pnl_status": "pending",
            "actual_pnl_usd": "99",
        },
        {
            **common,
            "logged_at": "2026-10-05T00:06:00+00:00",
            "run_id": "run-2",
            "event": "live_inventory_exit_blocked",
            "lot_id": 1,
            "reason": "v4_executable_pnl_below_threshold",
            "pnl_bps": "3.8",
        },
    ]
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
    )

    result = audit_files(
        [path],
        since=datetime(2026, 10, 4, 16, tzinfo=timezone.utc),
        asset="ETH",
    )

    assert result["confirmed_close_groups"] == 1
    assert result["confirmed_price_pnl_usd"] == Decimal("0.01")
    assert result["entry_signals"][("short_var_long_lighter", Decimal("0"))] == 0
    assert result["entry_signals"][("short_var_long_lighter", Decimal("1"))] == 1
    assert result["target_counts"][0]["matched_quote_peaks"] == 2
    assert result["target_counts"][1]["matched_quote_peaks"] == 1
    assert result["target_counts"][0]["throttled_block_log_peaks"] == 1
