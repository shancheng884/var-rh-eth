import json

import pytest

from main import parse_args
from tools import live
from tools.live import (
    LiveConfig,
    build_main_command,
    build_multi_asset_collector_command,
    request_maintenance_drain,
    validate_state,
)


def test_multi_asset_collector_command_has_no_live_submit_path() -> None:
    command = build_multi_asset_collector_command(("SOL", "BTC", "ETH"))

    assert command[1:] == ["tools/basis_collector.py", "--assets", "SOL,BTC,ETH"]
    assert "main.py" not in command
    assert "--confirm-live" not in command
    assert "--lighter-prewarm-submit-ws" not in command


def test_live_tool_disk_start_guard_requires_three_gb(monkeypatch) -> None:
    class Usage:
        free = 2 * 1024**3

    monkeypatch.setattr(live.shutil, "disk_usage", lambda _path: Usage())

    assert live.disk_start_allowed() is False


def test_live_tool_maintenance_drain_targets_current_process(
    tmp_path,
) -> None:
    control_path = tmp_path / "live_inventory_control.json"

    request = request_maintenance_drain(
        asset="ETH",
        process_line="54217 /home/ubuntu/project/.venv/bin/python main.py --mode live",
        state={"asset": "ETH", "run_id": "liveinv-1"},
        control_path=control_path,
    )

    assert request["action"] == "drain_after_flat"
    assert request["target_pid"] == 54217
    assert request["target_run_id"] == "liveinv-1"
    assert json.loads(control_path.read_text(encoding="utf-8")) == request


def test_live_tool_adds_basis_addon_flags_for_three_lots() -> None:
    command = build_main_command(
        "SOL",
        LiveConfig(max_total_lots=3, addon_min_basis_improvement_bps="2.0"),
    )

    assert "--live-inventory-i-accept-basis-addon-diagnostic" in command
    assert "--live-inventory-basis-dynamic-entry-threshold" in command
    assert "--live-inventory-basis-addon-min-basis-improvement-bps" in command
    assert command[command.index("--live-inventory-max-total-lots") + 1] == "3"
    assert command[command.index("--live-inventory-max-total-notional-usd") + 1] == "60"
    assert command[command.index("--live-inventory-basis-min-entry-edge-bps") + 1] == "7"
    assert command[command.index("--live-inventory-basis-min-abs-entry-bps") + 1] == "7"
    assert command[command.index("--live-inventory-basis-dynamic-entry-noise-buffer-bps") + 1] == "2.0"
    assert command[command.index("--live-inventory-snapshot-timeout-seconds") + 1] == "10"
    assert command[command.index("--live-inventory-basis-addon-min-basis-improvement-bps") + 1] == "2.0"
    assert "--live-inventory-basis-reversion-signal-exit-min-pnl-bps" not in command


def test_live_tool_omits_basis_addon_flags_for_single_lot() -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=1))

    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command
    assert "--live-inventory-basis-addon-min-basis-improvement-bps" not in command


def test_live_tool_can_disable_dynamic_entry_threshold() -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=1, dynamic_entry_threshold=False))

    assert "--live-inventory-basis-dynamic-entry-threshold" not in command


def test_live_tool_reversion_forces_one_small_lot_and_omits_normalized_primary() -> None:
    command = build_main_command(
        "SOL",
        LiveConfig(
            reversion_mode=True,
            max_cycles=3,
            max_lots=3,
            max_total_lots=3,
            max_total_inventory_notional_usd="60",
            reversion_min_deviation_bps="1.2",
            reversion_long_execution_reserve_bps="4.5",
            reversion_short_execution_reserve_bps="3.5",
            reversion_min_net_expected_pnl_bps="1.5",
            reversion_signal_exit_min_pnl_bps="-0.8",
            reversion_min_normalized_edge_bps="2.5",
            reversion_max_stablecoin_edge_share="0.6",
        ),
    )

    assert "--live-inventory-basis-reversion" in command
    assert command[command.index("--live-inventory-max-total-notional-usd") + 1] == "25"
    assert command[command.index("--live-inventory-max-cycles") + 1] == "1"
    assert command[command.index("--live-inventory-max-lots") + 1] == "1"
    assert command[command.index("--live-inventory-max-total-lots") + 1] == "1"
    assert "--live-inventory-basis-use-normalized-edge-for-entry" not in command
    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command
    assert "--live-inventory-basis-refresh-exit-quote-before-submit" in command
    assert command[command.index("--live-inventory-basis-max-var-quote-age-ms") + 1] == "1500"
    assert command[command.index("--live-inventory-max-lighter-book-age-seconds") + 1] == "2"
    assert command[command.index("--live-inventory-basis-reversion-min-deviation-bps") + 1] == "1.2"
    assert command[command.index("--live-inventory-basis-reversion-long-execution-reserve-bps") + 1] == "4.5"
    assert command[command.index("--live-inventory-basis-reversion-short-execution-reserve-bps") + 1] == "3.5"
    assert command[command.index("--live-inventory-basis-reversion-min-net-expected-pnl-bps") + 1] == "1.5"
    assert command[command.index("--live-inventory-basis-reversion-signal-exit-min-pnl-bps") + 1] == "-0.8"
    assert command[command.index("--live-inventory-basis-min-normalized-filter-edge-bps") + 1] == "2.5"
    assert command[command.index("--live-inventory-basis-max-stablecoin-edge-share") + 1] == "0.6"


def test_live_tool_cost_calibrated_reversion_command_passes_main_cli_guards(monkeypatch) -> None:
    command = build_main_command("SOL", LiveConfig(reversion_mode=True))
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_reversion is True
    assert args.live_inventory_basis_reversion_long_execution_reserve_bps == 4.5
    assert args.live_inventory_basis_reversion_short_execution_reserve_bps == 3.5
    assert args.live_inventory_basis_reversion_min_net_expected_pnl_bps == 1.5


def test_live_tool_calibration_is_bounded_and_bypasses_strategy_filters() -> None:
    command = build_main_command(
        "SOL",
        LiveConfig(
            calibration_mode=True,
            calibration_direction="short_var_long_lighter",
            calibration_weekdays_only=True,
            calibration_max_cycles=5,
            calibration_hold_samples=5,
            calibration_warmup_samples=30,
            calibration_entry_cooldown_samples=180,
            calibration_max_run_loss_usd="0.25",
            calibration_max_cycle_loss_usd="0.10",
            calibration_max_roundtrip_cost_bps="6.0",
            max_total_lots=3,
        ),
    )

    assert "--live-inventory-execution-calibration" in command
    assert "--live-inventory-i-accept-execution-calibration-loss" in command
    assert command[command.index("--live-inventory-calibration-direction") + 1] == "short_var_long_lighter"
    assert "--live-inventory-calibration-weekdays-only" in command
    assert "--live-inventory-basis-reversion" not in command
    assert "--live-inventory-basis-min-normalized-filter-edge-bps" not in command
    assert "--live-inventory-basis-max-stablecoin-edge-share" not in command
    assert command[command.index("--live-inventory-max-cycles") + 1] == "5"
    assert command[command.index("--live-inventory-max-lots") + 1] == "1"
    assert command[command.index("--live-inventory-max-total-lots") + 1] == "1"
    assert command[command.index("--live-inventory-max-total-notional-usd") + 1] == "25"
    assert command[command.index("--live-inventory-basis-max-entry-roundtrip-cost-bps") + 1] == "6.0"
    assert command[command.index("--live-inventory-min-hold-samples") + 1] == "5"
    assert command[command.index("--live-inventory-max-hold-samples") + 1] == "5"
    assert command[command.index("--live-inventory-basis-max-hold-action") + 1] == "exit"
    assert command[command.index("--live-inventory-calibration-max-run-loss-usd") + 1] == "0.25"


def test_live_tool_calibration_command_passes_main_cli_guards(monkeypatch) -> None:
    command = build_main_command("SOL", LiveConfig(calibration_mode=True))
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_execution_calibration is True
    assert args.live_inventory_i_accept_execution_calibration_loss is True
    assert args.live_inventory_calibration_direction == "alternate"
    assert args.live_inventory_calibration_weekdays_only is False
    assert args.live_inventory_max_cycles == 5
    assert args.live_inventory_min_hold_samples == args.live_inventory_max_hold_samples == 5


def test_live_tool_v4_profile_is_bounded_and_passes_main_cli_guards(monkeypatch) -> None:
    command = build_main_command("ETH", LiveConfig(v4_live_mode=True))
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_profile == "eth_short_execution_calibrated_20260724_n10"
    assert args.live_inventory_i_accept_basis_v4_live is True
    assert args.live_allowed_assets == "ETH"
    assert args.live_inventory_max_cycles == 1
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_lot_notional_usd == 20
    assert args.live_inventory_max_total_notional_usd == 25
    assert args.live_inventory_basis_profit_take_pnl_bps == 0
    assert args.live_inventory_basis_max_hold_action == "exit"
    assert args.live_inventory_basis_refresh_exit_quote_before_submit is True
    assert args.live_inventory_basis_dynamic_exit_buffer is True
    assert args.live_inventory_exit_blocked_log_throttle_seconds == 300
    assert args.live_inventory_basis_use_normalized_edge_for_entry is False
    assert args.live_inventory_basis_size_ladder_notionals_usd == "20,40,60"
    assert "--live-inventory-basis-reversion" not in command
    assert "--live-inventory-execution-calibration" not in command
    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command


def test_live_tool_v4_test_health_bypass_is_explicit_and_temporary(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_test_skip_recent_health=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_test_skip_recent_health is True
    assert "--live-inventory-basis-v4-test-skip-recent-health" in command


def test_live_tool_v4_weekend_bypass_is_explicit_and_bounded(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_test_skip_recent_health=True,
        v4_test_allow_weekend=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_test_skip_recent_health is True
    assert args.live_inventory_basis_v4_test_allow_weekend is True
    assert "--live-inventory-basis-v4-test-allow-weekend" in command
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1


def test_live_tool_v4_weekend_bypass_requires_health_test_mode(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_test_allow_weekend=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_tool_v4_shadow_gradient_remains_one_real_lot(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_shadow_gradient=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_shadow_gradient is True
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_max_total_notional_usd == 25
    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command


def test_live_tool_v4_real_gradient_enables_five_dynamic_tiers(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_real_gradient=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_real_gradient is True
    assert args.live_inventory_max_lots == 0
    assert args.live_inventory_max_total_lots == 0
    assert args.live_inventory_max_total_notional_usd == 0
    assert args.live_inventory_max_venue_leverage == 5
    assert args.live_inventory_margin_block_entry_pct == 50
    assert args.live_inventory_max_unrealized_loss_bps == 50
    assert "--live-inventory-i-accept-basis-addon-diagnostic" in command
    assert args.live_inventory_equity_balance_warning_ratio == 0.82
    assert args.live_inventory_equity_balance_block_ratio == 0.74


def test_live_tool_v4_elastic_capacity_is_explicit(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_real_gradient=True,
        v4_elastic_capacity=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_elastic_capacity is True
    assert "--live-inventory-basis-v4-elastic-capacity" in command


def test_live_tool_v4_reverse_test_is_one_bounded_lot(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_reverse_test=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_reverse_test is True
    assert args.live_inventory_basis_v4_real_gradient is False
    assert args.live_inventory_basis_v4_continuous is False
    assert args.live_inventory_max_cycles == 1
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_lot_notional_usd == 20


def test_live_tool_v4_bidirectional_requires_continuous_real_gradient(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_real_gradient=True,
        v4_bidirectional=True,
        v4_continuous=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_basis_v4_bidirectional is True
    assert args.live_inventory_basis_v4_real_gradient is True
    assert args.live_inventory_basis_v4_continuous is True
    assert args.live_inventory_max_cycles == 0
    assert args.live_inventory_max_lots == 0
    assert args.live_inventory_max_total_lots == 0
    assert args.live_inventory_basis_disable_negative_direction is True


def test_live_tool_close_open_position_uses_reconciled_one_shot_mode(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        close_open_position=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_force_close_open_state is True
    assert "--live-inventory-force-close-open-state" in command
    assert "--live-inventory-i-confirm-flat-start" not in command
    assert "--live-inventory-reset-state-after-manual-flat" not in command


def test_live_tool_close_open_position_requires_clean_open_state(
    tmp_path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    missing_ok, missing_message = validate_state(
        LiveConfig(v4_live_mode=True),
        close_open_position=True,
    )
    state_path.write_text(
        json.dumps(
            {
                "status": "open",
                "asset": "ETH",
                "open_lots": [{"lot_id": 1, "qty": "0.008"}],
                "pending_actions": [],
                "completed_cycles": 0,
            }
        ),
        encoding="utf-8",
    )
    open_ok, open_message = validate_state(
        LiveConfig(v4_live_mode=True),
        close_open_position=True,
    )
    state_path.write_text(
        json.dumps(
            {
                "status": "open",
                "asset": "ETH",
                "open_lots": [{"lot_id": 1, "qty": "0.008"}],
                "pending_actions": [{"role": "live_inventory_exit"}],
            }
        ),
        encoding="utf-8",
    )
    pending_ok, pending_message = validate_state(
        LiveConfig(v4_live_mode=True),
        close_open_position=True,
    )

    assert missing_ok is False
    assert missing_message == "close_requires_existing_open_state"
    assert open_ok is True
    assert "operator_exit_requested" in open_message
    assert pending_ok is False
    assert "close_refuses_pending_actions" in pending_message


def test_live_tool_resume_open_position_uses_strict_reconcile_mode(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_real_gradient=True,
        resume_open_position=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_i_accept_open_state_resume is True
    assert "--live-inventory-i-accept-open-state-resume" in command
    assert "--live-inventory-i-confirm-flat-start" not in command
    assert "--live-inventory-force-close-open-state" not in command


def test_live_tool_resume_and_drain_blocks_entries_from_start(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        v4_real_gradient=True,
        resume_open_position=True,
        maintenance_drain_after_start=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_i_accept_open_state_resume is True
    assert args.live_inventory_maintenance_drain_after_start is True
    assert "--live-inventory-maintenance-drain-after-start" in command


def test_live_tool_resume_accepts_only_clean_recoverable_open_state(
    tmp_path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    monkeypatch.setattr(live, "LIVE_STATE", state_path)
    base_state = {
        "status": "manual_review_required",
        "asset": "ETH",
        "open_lots": [{"lot_id": 1, "qty": "0.008"}],
        "pending_actions": [],
        "completed_cycles": 0,
    }

    state_path.write_text(
        json.dumps({**base_state, "manual_review_reason": "variational_html_response"}),
        encoding="utf-8",
    )
    ok, message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )

    state_path.write_text(
        json.dumps({**base_state, "manual_review_reason": "basis_exit_submit_exception"}),
        encoding="utf-8",
    )
    unsafe_ok, unsafe_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )

    state_path.write_text(
        json.dumps(
            {
                **base_state,
                "manual_review_reason": "variational_html_response",
                "pending_actions": [{"role": "live_inventory_exit"}],
            }
        ),
        encoding="utf-8",
    )
    pending_ok, pending_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )

    assert ok is True
    assert "strict_exchange_reconcile_required=true" in message
    assert unsafe_ok is False
    assert "resume_refuses_state" in unsafe_message

    state_path.write_text(
        json.dumps(
            {
                **base_state,
                "manual_review_reason": "basis_exit_lighter_final_fill_not_confirmed",
            }
        ),
        encoding="utf-8",
    )
    exit_fill_ok, exit_fill_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )

    assert exit_fill_ok is True
    assert "strict_exchange_reconcile_required=true" in exit_fill_message

    state_path.write_text(
        json.dumps(
            {
                **base_state,
                "manual_review_reason": "basis_entry_lighter_submit_after_var_fill_failed",
            }
        ),
        encoding="utf-8",
    )
    balanced_ok, _ = validate_state(
        LiveConfig(v4_live_mode=True), resume_open_position=True
    )
    state_path.write_text(
        json.dumps(
            {
                **base_state,
                "manual_review_reason": "basis_entry_lighter_submit_after_var_fill_failed",
                "pending_actions": [{"role": "unresolved_entry"}],
            }
        ),
        encoding="utf-8",
    )
    unresolved_ok, _ = validate_state(
        LiveConfig(v4_live_mode=True), resume_open_position=True
    )
    assert balanced_ok is True
    assert unresolved_ok is False

    assert pending_ok is True
    assert "pending_actions=1" in pending_message
    assert "strict_exchange_reconcile_required=true" in pending_message

    state_path.write_text(
        json.dumps(
            {
                **base_state,
                "manual_review_reason": "runtime_stopped_with_unresolved_entry_submission",
                "pending_actions": [
                    {"role": "live_inventory_entry_pending_var_fill"}
                ],
            }
        ),
        encoding="utf-8",
    )
    interrupted_ok, interrupted_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )
    assert interrupted_ok is True
    assert "pending_actions=1" in interrupted_message
    assert "strict_exchange_reconcile_required=true" in interrupted_message

    state_path.write_text(
        json.dumps(
            {
                **base_state,
                "manual_review_reason": "startup_reconcile_exchange_position_check_failed",
            }
        ),
        encoding="utf-8",
    )
    transient_check_ok, transient_check_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )
    assert transient_check_ok is True
    assert "strict_exchange_reconcile_required=true" in transient_check_message

    state_path.write_text(
        json.dumps(
            {
                "status": "pending",
                "asset": "ETH",
                "open_lots": [],
                "pending_actions": [{"role": "live_inventory_entry"}],
            }
        ),
        encoding="utf-8",
    )
    pending_only_ok, pending_only_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )
    assert pending_only_ok is True
    assert "state=pending" in pending_only_message
    assert "strict_exchange_reconcile_required=true" in pending_only_message


def test_live_tool_v4_batch_is_sequential_bounded_and_guarded(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(
            v4_live_mode=True,
            max_cycles=3,
            v4_test_max_run_loss_usd="0.025",
            v4_test_cycle_cooldown_seconds=0,
        ),
        v4_test_skip_recent_health=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_max_cycles == 3
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1
    assert args.live_inventory_basis_v4_test_skip_recent_health is True
    assert args.live_inventory_basis_v4_max_run_loss_usd == 0.025
    assert args.live_inventory_basis_v4_cycle_cooldown_seconds == 0


def test_live_tool_v4_continuous_is_explicit_unbounded_and_guarded(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(
            v4_live_mode=True,
            max_cycles=0,
            v4_test_max_run_loss_usd="0.025",
        ),
        v4_real_gradient=True,
        v4_continuous=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_max_cycles == 0
    assert args.live_inventory_basis_v4_real_gradient is True
    assert args.live_inventory_basis_v4_continuous is True
    assert args.live_inventory_basis_v4_max_run_loss_usd == 0.025


def test_live_tool_continuous_state_has_no_completed_cycle_cap(
    tmp_path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "ETH",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 999,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(
        LiveConfig(v4_live_mode=True, max_cycles=0)
    )

    assert ok is True
    assert "max_cycles=0" in message


def test_live_tool_v4_batch_requires_explicit_test_health_bypass(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True, max_cycles=3),
    )
    monkeypatch.setattr("sys.argv", command[1:])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_tool_v4_batch_requires_positive_loss_limit(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(
            v4_live_mode=True,
            max_cycles=3,
            v4_test_max_run_loss_usd="0",
        ),
        v4_test_skip_recent_health=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_tool_v4_batch_rejects_more_than_three_cycles(monkeypatch) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True, max_cycles=4),
        v4_test_skip_recent_health=True,
    )
    monkeypatch.setattr("sys.argv", command[1:])

    with pytest.raises(SystemExit):
        parse_args()


def test_live_tool_collect_only_disables_all_inventory_entries(monkeypatch) -> None:
    command = build_main_command("SOL", LiveConfig(max_total_lots=3), collect_only=True)
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_dry_decisions is True
    assert args.live_inventory_collect_only is True
    assert args.live_inventory_signal_mode == "basis"
    assert args.live_inventory_max_lots == 1
    assert args.live_inventory_max_total_lots == 1
    assert "--lighter-prewarm-submit-ws" not in command
    assert "--live-inventory-i-accept-basis-real-diagnostic" not in command
    assert "--live-inventory-i-accept-basis-addon-diagnostic" not in command


def test_live_tool_collect_only_ignores_completed_cycle_cap_but_requires_flat_state(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(max_cycles=1), collect_only=True)

    assert ok is True
    assert "collect_only=true" in message


def test_live_tool_calibration_state_uses_calibration_cycle_cap(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 4,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(calibration_mode=True, calibration_max_cycles=5))

    assert ok is True
    assert "max_cycles=5" in message


def test_live_tool_reversion_state_uses_one_cycle_cap(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(reversion_mode=True, max_cycles=3))

    assert ok is False
    assert "state_cycle_cap_reached" in message
    assert "max_cycles=1" in message


def test_live_tool_reset_state_allows_next_reversion_cycle(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(
        LiveConfig(reversion_mode=True),
        reset_state_after_manual_flat=True,
    )
    command = build_main_command(
        "SOL",
        LiveConfig(reversion_mode=True),
        reset_state_after_manual_flat=True,
    )

    assert ok is True
    assert "reset_after_manual_flat_requested" in message
    assert "--live-inventory-reset-state-after-manual-flat" in command


def test_live_tool_explicit_reset_allows_confirmed_flat_cleanup_of_old_state(
    tmp_path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "manual_review_required",
                "asset": "ETH",
                "open_lots": [{"lot_id": 2, "qty": "0.0105"}],
                "pending_actions": [{"lot_id": 2, "role": "live_inventory_entry_pending_var_fill"}],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(
        LiveConfig(v4_live_mode=True),
        reset_state_after_manual_flat=True,
    )

    assert ok is True
    assert "state=manual_review_required" in message
    assert "open_lots=1" in message
    assert "pending_actions=1" in message
    assert "local_state_backup_required=true" in message


def test_live_tool_state_only_reset_uses_no_strategy_or_submit_flags(
    monkeypatch,
) -> None:
    command = build_main_command(
        "ETH",
        LiveConfig(v4_live_mode=True),
        reset_state_only_after_manual_flat=True,
        flat_reset_confirmation="FLAT:ETH",
    )
    monkeypatch.setattr("sys.argv", command[1:])

    args = parse_args()

    assert args.live_inventory_reset_state_only_after_manual_flat is True
    assert args.live_inventory_flat_reset_confirmation == "FLAT:ETH"
    assert "--live-inventory-i-confirm-flat-start" in command
    assert "--live-inventory-reset-state-after-manual-flat" not in command
    assert "--lighter-prewarm-submit-ws" not in command
    assert "--live-inventory-i-accept-basis-real-diagnostic" not in command
    assert "--live-inventory-basis-v4-real-gradient" not in command


def test_live_tool_state_only_reset_accepts_stale_known_state_but_not_unknown(
    tmp_path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    monkeypatch.setattr(live, "LIVE_STATE", state_path)
    state_path.write_text(
        json.dumps(
            {
                "status": "open",
                "asset": "ETH",
                "open_lots": [{"lot_id": 1, "qty": "0.01"}],
                "pending_actions": [],
            }
        ),
        encoding="utf-8",
    )

    allowed, message = validate_state(
        LiveConfig(v4_live_mode=True),
        reset_state_only_after_manual_flat=True,
    )
    state_path.write_text(json.dumps({"status": "corrupt"}), encoding="utf-8")
    refused, refusal = validate_state(
        LiveConfig(v4_live_mode=True),
        reset_state_only_after_manual_flat=True,
    )

    assert allowed is True
    assert "exchange_position_and_order_verification_required=true" in message
    assert refused is False
    assert "reset_only_refuses_state" in refusal


def test_live_tool_corrects_flat_status_when_open_lots_exist(
    tmp_path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "ETH",
                "open_lots": [{"lot_id": 1, "qty": "0.008"}],
                "pending_actions": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    reset_ok, reset_message = validate_state(
        LiveConfig(v4_live_mode=True),
        reset_state_after_manual_flat=True,
    )
    resume_ok, resume_message = validate_state(
        LiveConfig(v4_live_mode=True),
        resume_open_position=True,
    )

    assert reset_ok is True
    assert "local_state_backup_required=true" in reset_message
    assert resume_ok is True
    assert "state=open" in resume_message


def test_live_tool_refuses_flat_state_when_cycle_cap_reached(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "live_inventory_state.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "flat",
                "asset": "SOL",
                "open_lots": [],
                "pending_actions": [],
                "completed_cycles": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "LIVE_STATE", state_path)

    ok, message = validate_state(LiveConfig(max_cycles=1))

    assert ok is False
    assert "state_cycle_cap_reached" in message
    assert "completed_cycles=1" in message
