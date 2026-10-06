from decimal import Decimal
from datetime import datetime, timedelta, timezone

from tools.analyze import (
    _basis_v4_formal_profile,
    _basis_v3_simulate_episode,
    _basis_v4_candidate_verdict,
    _basis_v4_simulate_episode,
    _deduplicate_sample_rows,
    basis_state_rows,
    bounded_diagnostic_line,
    build_basis_v4_stratification,
    build_v4_live_funnel,
    build_basis_v3_replay,
    build_basis_v4_replay,
    build_basis_regimes,
    build_basis_v2_replay,
    build_entry_semantics,
    dynamic_cost_summary,
    build_v4_exit_observation_summary,
    build_v4_exit_target_sensitivity,
    summarize_basis_v2_sweep_events,
    print_execution_calibration,
    print_v4_live_funnel,
)


def test_v4_exit_target_sensitivity_separates_open_unconfirmed_and_confirmed() -> None:
    common = {
        "run_id": "live-v4-exit-sensitivity",
        "strategy_version": "basis-v4-live-v16-test",
        "asset": "ETH",
    }
    rows = [
        {
            **common,
            "event": "live_inventory_v4_exit_observation",
            "observed_at": "2026-10-06T00:01:00Z",
            "lots": [
                {
                    "lot_id": 1,
                    "direction": "short_var_long_lighter",
                    "executable_mfe_pnl_bps": "4.2",
                    "effective_exit_target_bps": "4.5",
                },
                {
                    "lot_id": 2,
                    "direction": "short_var_long_lighter",
                    "executable_mfe_pnl_bps": "3.7",
                    "effective_exit_target_bps": "4.5",
                },
                {
                    "lot_id": 3,
                    "direction": "short_var_long_lighter",
                    "executable_mfe_pnl_bps": "3.9",
                    "effective_exit_target_bps": "4.5",
                },
            ],
        },
        {
            **common,
            "event": "live_inventory_exited",
            "logged_at": "2026-10-06T00:02:00Z",
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "effective_min_exit_pnl_bps": "4.5",
            "executable_exit_mfe_pnl_bps": "4.2",
        },
        {
            **common,
            "event": "live_inventory_exited",
            "logged_at": "2026-10-06T00:02:30Z",
            "lot_id": 3,
            "direction": "short_var_long_lighter",
            "effective_min_exit_pnl_bps": "4.5",
            "executable_exit_mfe_pnl_bps": "3.9",
        },
        {
            **common,
            "event": "live_inventory_actual_pnl",
            "logged_at": "2026-10-06T00:03:00Z",
            "lot_id": 1,
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "actual_pnl_bps": "2.2",
            "closed_child_lots": 1,
        },
    ]

    lots = build_v4_exit_observation_summary(rows)
    results = build_v4_exit_target_sensitivity(lots)
    by_target = {str(row["target_bps"]): row for row in results}

    assert by_target["3.5"]["peak_crossed_lots"] == 3
    assert by_target["3.5"]["open_crossed_lots"] == 1
    assert by_target["3.5"]["exited_without_confirmed_pnl"] == 1
    assert by_target["3.5"]["confirmed_multi_lot_exits"] == 0
    assert by_target["3.5"]["confirmed_single_lot_closes_crossed"] == 1
    assert by_target["3.5"]["closed_crossed_below_recorded_target"] == 1
    assert by_target["3.5"]["confirmed_actual_avg_pnl_bps"] == Decimal("2.2")
    assert by_target["4.0"]["peak_crossed_lots"] == 1
    assert by_target["4.5"]["peak_crossed_lots"] == 0


def test_v4_live_funnel_flags_incompatible_immediate_arb_floor() -> None:
    common = {
        "run_id": "live-v4-test",
        "strategy_version": "basis-v4-live-v1",
        "asset": "ETH",
    }
    rows = [
        {
            **common,
            "event": "live_inventory_basis_state",
            "basis_v4_profile": "eth_short_execution_calibrated_20260724_n10",
            "short_edge_bps": "-5",
            "v4_entry_threshold_bps": "-6",
            "v4_entry_execution_reserve_bps": "0",
            "v4_entry_capture_sample_count": 2,
            "v4_entry_capture_min_samples": 10,
            "v4_entry_capture_calibration_ready": False,
            "v4_entry_capture_raw_p80_bps": "3.19",
            "v4_entry_capture_cap_bps": "3.00",
            "v4_entry_capture_applied_bps": "0",
            "v4_baseline_window_seconds": 21600,
            "v4_baseline_max_sample_gap_seconds": "30.000",
            "v4_anchor_ready": True,
            "v4_health_ready": True,
            "v4_health_coverage_seconds": "3000.000",
            "v4_health_max_sample_gap_seconds": "30.000",
        },
        {
            **common,
            "event": "live_inventory_entry_shadow_candidate",
            "shadow_status": "blocked",
            "shadow_block_reason": "edge_bps_below_dynamic_live_inventory_entry",
        },
        {
            **common,
            "event": "live_inventory_entry_blocked",
            "reason": "edge_bps_below_dynamic_live_inventory_entry",
        },
    ]

    funnel = build_v4_live_funnel(rows)

    assert funnel is not None
    assert funnel["threshold_crossings"] == 1
    assert funnel["preflight_reached"] == 1
    assert funnel["preflight_blocked"] == 1
    assert funnel["dynamic_floor_blocks"] == 1
    assert funnel["latest_window_max_gap_seconds"] == "30.000"
    assert funnel["anchor_ready"] is True
    assert funnel["health_ready"] is True
    assert funnel["entry_capture_sample_count"] == 2
    assert funnel["entry_capture_calibration_ready"] is False
    assert funnel["entry_capture_raw_p80_bps"] == "3.19"
    assert funnel["entry_capture_applied_bps"] == "0"
    assert funnel["status"] == "ERROR_V4_IMMEDIATE_ARB_FLOOR_APPLIED"


def test_v4_live_funnel_uses_reverse_test_signal_edge() -> None:
    funnel = build_v4_live_funnel(
        [
            {
                "run_id": "live-v4-reverse",
                "strategy_version": "basis-v4-live-test-v14",
                "event": "live_inventory_basis_state",
                "asset": "ETH",
                "short_edge_bps": "-6",
                "long_edge_bps": "5",
                "v4_entry_direction": "long_var_short_lighter",
                "v4_signal_edge_bps": "5",
                "v4_entry_threshold_bps": "4",
            }
        ]
    )

    assert funnel is not None
    assert funnel["entry_direction"] == "long_var_short_lighter"
    assert funnel["latest_edge_bps"] == "5"
    assert funnel["threshold_crossings"] == 1


def test_v4_live_funnel_reports_shadow_gradient_result() -> None:
    common = {
        "run_id": "live-v4-shadow-gradient",
        "strategy_version": "basis-v4-live-test-v9",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {
                **common,
                "event": "live_inventory_strategy_snapshot",
                "profile": "eth_short_execution_calibrated_20260724_n10",
                "shadow_gradient_enabled": True,
            },
            {
                **common,
                "event": "live_inventory_basis_state",
                "v4_exit_confirmation_policy": "latest_and_2_of_3",
                "v4_strong_single_enabled": False,
                "v4_strong_single_disabled_reason": (
                    "estimated_profitable_actual_loss"
                ),
                "v4_strong_single_shortfall_sample_count": 4,
                "v4_strong_single_shortfall_raw_p80_bps": "11.68",
                "v4_strong_single_shortfall_reserve_bps": "11.68",
                "v4_strong_single_threshold_bps": "13.68",
            },
            {
                **common,
                "event": "live_inventory_v4_strong_single_auto_disabled",
                "reason": "estimated_profitable_actual_loss",
            },
            {
                **common,
                "event": "live_inventory_v4_shadow_tranche_entered",
                "episode_id": "episode-1",
                "basis_improvement_trigger_bps": "1.00",
            },
            {
                **common,
                "event": "live_inventory_v4_shadow_tranche_exited",
                "episode_id": "episode-1",
                "basis_improvement_trigger_bps": "1.00",
                "reason": "shadow_v4_executable_net_target_reached",
                "shadow_pnl_bps": "3.80",
                "holding_seconds": 90.0,
                "shadow_mfe_pnl_bps": "4.10",
                "shadow_mae_pnl_bps": "-1.20",
            },
        ]
    )

    assert funnel is not None
    assert funnel["shadow_gradient_enabled"] is True
    assert funnel["shadow_gradient_entries"] == 1
    assert funnel["shadow_gradient_exits"] == 1
    assert funnel["shadow_gradient_active"] is False
    assert funnel["shadow_gradient_last_pnl_bps"] == "3.80"
    assert funnel["shadow_gradient_levels"] == {
        "1.00": {
            "entries": 1,
            "exits": 1,
            "wins": 1,
            "active": False,
            "win_rate": 1.0,
            "avg_pnl_bps": "3.80",
        }
    }
    assert funnel["exit_confirmation_policy"] == "latest_and_2_of_3"
    assert funnel["strong_single_enabled"] is False
    assert funnel["strong_single_auto_disables"] == 1
    assert funnel["strong_single_shortfall_raw_p80_bps"] == "11.68"


def test_v4_live_funnel_prefers_latest_account_risk_freshness() -> None:
    common = {
        "run_id": "live-v4-account-risk",
        "strategy_version": "basis-v4-live-test-v14",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {**common, "event": "live_inventory_strategy_snapshot"},
            {**common, "event": "live_inventory_basis_state"},
            {
                **common,
                "event": "live_inventory_account_snapshot",
                "variational_snapshot_fresh": True,
                "variational_snapshot_age_seconds": 0.15,
            },
            {
                **common,
                "event": "live_inventory_account_risk_alert",
                "risk_action": "block_entry",
                "risk_reason": "variational_account_snapshot_stale",
                "variational_account_snapshot_fresh": False,
                "variational_account_snapshot_usable": False,
                "variational_account_snapshot_degraded": False,
                "variational_account_snapshot_age_seconds": 362.6,
            },
        ]
    )

    assert funnel is not None
    assert funnel["variational_snapshot_fresh"] is False
    assert funnel["variational_snapshot_usable"] is False
    assert funnel["variational_snapshot_degraded"] is False
    assert funnel["variational_snapshot_age_seconds"] == 362.6
    assert funnel["variational_snapshot_status_source"] == "account_risk"


def test_v4_live_funnel_distinguishes_anchor_and_recent_health_waits() -> None:
    common = {
        "run_id": "live-v4-wait-test",
        "strategy_version": "basis-v4-live-v1",
        "event": "live_inventory_basis_state",
        "asset": "ETH",
        "basis_v4_profile": "eth_short_execution_calibrated_20260724_n10",
    }
    anchor_wait = build_v4_live_funnel(
        [
            {
                **common,
                "v4_anchor_ready": False,
                "v4_health_ready": True,
            }
        ]
    )
    health_wait = build_v4_live_funnel(
        [
            {
                **common,
                "v4_anchor_ready": True,
                "v4_health_ready": False,
            }
        ]
    )

    assert anchor_wait is not None
    assert anchor_wait["status"] == "WAITING_FOR_ROLLING_7D_ANCHOR"
    assert health_wait is not None
    assert health_wait["status"] == "WAITING_FOR_RECENT_HEALTH_WINDOW"


def test_v4_live_funnel_reports_multi_window_and_episode_rearm() -> None:
    funnel = build_v4_live_funnel(
        [
            {
                "run_id": "live-v4-rearm",
                "strategy_version": "basis-v4-live-v4",
                "event": "live_inventory_basis_state",
                "logged_at": "2026-08-08T01:00:00+00:00",
                "asset": "ETH",
                "basis_v4_profile": "eth_short_execution_calibrated_20260724_n10",
                "short_edge_bps": "-8.2",
                "v4_entry_threshold_bps": "-7.5",
                "v4_7d_entry_threshold_bps": "-8.0",
                "v4_fast_ready": True,
                "v4_fast_threshold_bps": "-7.5",
                "v4_fast_threshold_applied": True,
                "v4_mid_ready": False,
                "v4_mid_threshold_bps": "-8.3",
                "v4_long_ready": False,
                "v4_long_threshold_bps": "-8.4",
                "v4_anchor_ready": True,
                "v4_health_ready": True,
                "v4_rearm_required": True,
                "v4_episode_state": "disarmed",
                "v4_episode_id": "episode-1",
                "v4_rearm_confirmation_count": 2,
                "v4_rearm_confirm_samples": 3,
                "v4_rearm_reset_threshold_bps": "-8.0",
            }
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "WAITING_FOR_EPISODE_REARM"
    assert funnel["seven_day_threshold_bps"] == "-8.0"
    assert funnel["fast_threshold_bps"] == "-7.5"
    assert funnel["fast_threshold_applied"] is True
    assert funnel["episode_state"] == "disarmed"
    assert funnel["rearm_confirmation_count"] == 2


def test_v4_live_funnel_ignores_stale_batch_wait_after_sampling_resumes() -> None:
    common = {
        "run_id": "live-v4-resumed",
        "strategy_version": "basis-v4-live-v4",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {
                **common,
                "event": "live_inventory_v4_batch_waiting",
                "logged_at": "2026-08-08T00:00:00+00:00",
                "reason": "v4_batch_cycle_cooldown",
            },
            {
                **common,
                "event": "live_inventory_basis_state",
                "logged_at": "2026-08-08T00:31:00+00:00",
                "v4_anchor_ready": True,
                "v4_health_ready": True,
                "short_edge_bps": "-9",
                "v4_entry_threshold_bps": "-7",
            },
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "WAITING_FOR_THRESHOLD_CROSSING"


def test_v4_live_funnel_reports_anchor_progress_and_runtime_fuse() -> None:
    common = {
        "run_id": "live-v4-fuse",
        "strategy_version": "basis-v4-live-v1",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {
                **common,
                "event": "live_inventory_basis_state",
                "v4_anchor_ready": False,
                "v4_health_ready": True,
                "v4_anchor_count": 4000,
                "v4_anchor_effective_seconds": 120000,
                "v4_anchor_min_effective_seconds": 129600,
                "v4_anchor_missing_effective_seconds": 9600,
                "v4_anchor_progress_pct": "92.59",
                "v4_anchor_projected_ready_seconds": 10800,
                "v4_anchor_projected_ready_at": "2026-07-30T09:00:00+00:00",
            },
            {
                **common,
                "event": "live_inventory_runtime_fuse_triggered",
                "reason": "variational_extension_disconnected",
            },
            {
                **common,
                "event": "live_inventory_runtime_stopped",
                "logged_at": "2026-07-30T06:00:00+00:00",
                "reason": "variational_extension_disconnected",
            },
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "STOPPED_BY_RUNTIME_FUSE"
    assert funnel["anchor_count"] == 4000
    assert funnel["anchor_missing_effective_seconds"] == 9600
    assert funnel["anchor_projected_ready_seconds"] == 10800
    assert funnel["runtime_fuses"] == 1
    assert funnel["runtime_stop_reason"] == "variational_extension_disconnected"
    assert funnel["runtime_stopped_at"] == "2026-07-30T06:00:00+00:00"


def test_v4_live_funnel_reports_concurrent_exit_and_cycle_summary() -> None:
    common = {
        "run_id": "live-v4-cycle",
        "strategy_version": "basis-v4-live-v2",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {
                **common,
                "event": "live_inventory_execution_ledger",
                "phase": "exit",
                "execution_stage": "submit_returned",
                "submit_mode": "concurrent",
                "pair_submit_elapsed_ms": "170.2",
                "var_submit_ms": "160.1",
                "lighter_submit_ms": "45.8",
            },
            {
                **common,
                "event": "live_inventory_exit_blocked",
                "reason": "entry_final_fill_cost_pending",
            },
            {
                **common,
                "event": "live_inventory_exit_blocked",
                "reason": "v4_exit_confirmation_pending",
            },
            {
                **common,
                "event": "live_inventory_cycle_report",
                "report_status": "completed",
                "final_pnl_bps": "1.2",
                "final_pnl_usd": "0.0024",
                "exit_reason": "v4_executable_net_target_reached",
                "v4_exit_shortfall_reserve_bps": "6.65",
                "v4_exit_shortfall_sample_count": 10,
                "v4_exit_shortfall_min_samples": 10,
                "v4_exit_shortfall_calibration_ready": True,
                "v4_exit_shortfall_raw_p80_bps": "6.65",
                "v4_exit_shortfall_cap_bps": "3.00",
                "v4_exit_shortfall_applied_dynamic_bps": "3.00",
                "v4_exit_shortfall_stage_floor_bps": "1.50",
                "effective_min_exit_pnl_bps": "7.65",
                "holding_seconds": 900,
                "shadow_mfe_pnl_bps": "1.4",
                "executable_exit_mfe_pnl_bps": "1.1",
                "shadow_mae_pnl_bps": "-0.8",
            },
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "CYCLE_COMPLETE"
    assert funnel["exit_submit_mode"] == "concurrent"
    assert funnel["exit_pair_submit_elapsed_ms"] == "170.2"
    assert funnel["cycle_report_status"] == "completed"
    assert funnel["cycle_final_pnl_bps"] == "1.2"
    assert funnel["cycle_exit_shortfall_reserve_bps"] == "6.65"
    assert funnel["cycle_effective_min_exit_pnl_bps"] == "7.65"
    assert funnel["cycle_mfe_pnl_bps"] == "1.4"
    assert funnel["cycle_executable_mfe_pnl_bps"] == "1.1"
    assert funnel["exit_shortfall_sample_count"] == 10
    assert funnel["exit_shortfall_calibration_ready"] is True
    assert funnel["exit_shortfall_applied_dynamic_bps"] == "3.00"
    assert funnel["entry_cost_pending_exit_blocks"] == 1
    assert funnel["exit_confirmation_pending_blocks"] == 1
    assert funnel["exit_block_reasons"] == {
        "entry_final_fill_cost_pending": 1,
        "v4_exit_confirmation_pending": 1,
    }


def test_v4_live_funnel_keeps_partial_tier_close_as_position_open() -> None:
    common = {
        "run_id": "live-v4-partial-close",
        "strategy_version": "basis-v4-live-v2",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {
                **common,
                "event": "live_inventory_entered",
                "lot_id": 1,
            },
            {
                **common,
                "event": "live_inventory_entered",
                "lot_id": 2,
            },
            {
                **common,
                "event": "live_inventory_exited",
                "lot_id": 1,
            },
            {
                **common,
                "event": "live_inventory_final_pnl",
                "logged_at": "2026-08-31T12:00:00+00:00",
                "lot_id": 1,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "remaining_child_lots": 1,
            },
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "POSITION_OPEN"


def test_v4_live_funnel_does_not_call_reconciliation_cycle_complete() -> None:
    funnel = build_v4_live_funnel(
        [
            {
                "event": "live_inventory_cycle_report",
                "run_id": "live-v4-reconcile",
                "strategy_version": "basis-v4-live-v1",
                "asset": "ETH",
                "report_status": "requires_reconciliation",
            }
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "ERROR_RECONCILIATION_REQUIRED"


def test_v4_live_funnel_reports_intermediate_batch_checkpoint() -> None:
    common = {
        "run_id": "live-v4-batch",
        "strategy_version": "basis-v4-live-test-v3",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {**common, "event": "live_inventory_entered", "lot_id": 1},
            {**common, "event": "live_inventory_exited", "lot_id": 1},
            {
                **common,
                "event": "live_inventory_v4_cycle_checkpoint",
                "lot_id": 1,
                "completed_cycles": 1,
                "max_cycles": 5,
                "cumulative_run_pnl_usd": "0.003",
            },
            {
                **common,
                "event": "live_inventory_v4_batch_waiting",
                "reason": "v4_batch_cycle_cooldown",
                "cooldown_remaining_seconds": 120,
                "batch_run_pnl_usd": "0.003",
            },
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "WAITING_FOR_NEXT_BATCH_CYCLE"
    assert funnel["cycle_checkpoints"] == 1
    assert funnel["completed_cycles"] == 1
    assert funnel["max_cycles"] == 5
    assert funnel["batch_wait_reason"] == "v4_batch_cycle_cooldown"


def test_v4_live_funnel_reports_continuous_cycles_as_unlimited(capsys) -> None:
    common = {
        "run_id": "live-v4-continuous",
        "strategy_version": "basis-v4-live-v14",
        "asset": "ETH",
    }
    rows = [
        {
            **common,
            "event": "live_inventory_strategy_snapshot",
            "max_cycles": 0,
            "continuous_mode": True,
        },
        {
            **common,
            "event": "live_inventory_v4_cycle_checkpoint",
            "completed_cycles": 27,
            "max_cycles": 0,
        },
    ]

    funnel = build_v4_live_funnel(rows)

    assert funnel is not None
    assert funnel["completed_cycles"] == 27
    assert funnel["max_cycles"] == 0
    print_v4_live_funnel(rows)
    assert "completed_cycles=27/unlimited" in capsys.readouterr().out


def test_v4_live_funnel_checkpoint_resolves_final_fill_wait_status() -> None:
    common = {
        "run_id": "live-v4-resolved-final-fill",
        "strategy_version": "basis-v4-live-test-v5",
        "asset": "ETH",
    }
    funnel = build_v4_live_funnel(
        [
            {**common, "event": "live_inventory_entered", "logged_at": "2026-08-12T00:00:00+00:00"},
            {
                **common,
                "event": "live_inventory_final_pnl",
                "logged_at": "2026-08-12T00:10:00+00:00",
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            },
            {**common, "event": "live_inventory_exited", "logged_at": "2026-08-12T00:10:01+00:00"},
            {
                **common,
                "event": "live_inventory_v4_cycle_checkpoint",
                "logged_at": "2026-08-12T00:10:02+00:00",
                "completed_cycles": 1,
                "max_cycles": 3,
            },
            {
                **common,
                "event": "live_inventory_basis_state",
                "logged_at": "2026-08-12T00:10:03+00:00",
                "v4_rearm_required": True,
            },
        ]
    )

    assert funnel is not None
    assert funnel["status"] == "WAITING_FOR_EPISODE_REARM"


def test_basis_v4_formal_profile_accepts_only_versioned_calibrated_eth_short_config() -> None:
    common = {
        "evaluation_interval_seconds": 1,
        "history_sample_seconds": 30,
        "episode_cooldown_seconds": 180,
        "max_hold_seconds": 21600,
        "max_sample_gap_seconds": 60,
        "min_window_coverage": Decimal("0.80"),
        "min_history_samples": 100,
        "long_shortfall_reserve_bps": Decimal("2"),
        "net_exit_target_bps": Decimal("1"),
    }

    assert (
        _basis_v4_formal_profile(
            asset="ETH",
            short_shortfall_reserve_bps=Decimal("0.50"),
            **common,
        )
        == "eth_short_execution_calibrated_20260724_n10"
    )
    assert (
        _basis_v4_formal_profile(
            asset="ETH",
            short_shortfall_reserve_bps=Decimal("2"),
            **common,
        )
        == "fixed_2bps_v1"
    )
    assert (
        _basis_v4_formal_profile(
            asset="SOL",
            short_shortfall_reserve_bps=Decimal("0.50"),
            **common,
        )
        is None
    )
    assert (
        _basis_v4_formal_profile(
            asset="ETH",
            short_shortfall_reserve_bps=Decimal("0.75"),
            **common,
        )
        is None
    )


def test_diagnostic_log_lines_are_bounded() -> None:
    assert bounded_diagnostic_line("short") == "short"
    rendered = bounded_diagnostic_line("x" * 10000)
    assert rendered.startswith("x" * 500)
    assert rendered.endswith("[truncated original_chars=10000]")
    assert len(rendered) < 550


def test_basis_v4_stratification_reports_balanced_all_week_candidate() -> None:
    started_at = datetime(2026, 7, 6, tzinfo=timezone.utc)
    episodes = []
    for index in range(30):
        timestamp = started_at + timedelta(hours=index * 6)
        episodes.append(
            {
                "timestamp": timestamp.timestamp(),
                "net_pnl_bps": Decimal("2"),
                "holding_seconds": Decimal("900"),
                "exit_reason": "executable_net_target_reached",
                "entry_var_spread_bps": Decimal(str(1 + index % 4)),
                "entry_lighter_spread_bps": Decimal("0.5"),
            }
        )
    baseline_rows = [
        {
            "var_spread_bps": str(1 + index % 4),
            "lighter_spread_bps": "0.5",
        }
        for index in range(90)
    ]

    result = build_basis_v4_stratification(
        episodes,
        baseline_rows,
        holdout_fraction=Decimal("0.30"),
        min_independent_samples=5,
    )
    verdict, reasons = _basis_v4_candidate_verdict(
        result,
        effective_coverage_hours=Decimal("168"),
    )

    assert result["overall"]["n"] == 30
    assert result["periods"]["weekday"]["n"] > 5
    assert result["periods"]["weekend"]["n"] > 5
    assert set(result["liquidity"]) == {"tight", "normal", "wide"}
    assert result["max_utc_share_pct"] < Decimal("50")
    assert verdict == "bounded_all_week_real_calibration_candidate"
    assert reasons == []


def test_sample_dedup_prefers_baseline_over_burst_copy() -> None:
    rows = [
        {"sample_id": "same", "sample_kind": "burst", "logged_at": "2026-07-10T00:00:00+00:00"},
        {"sample_id": "same", "sample_kind": "baseline", "logged_at": "2026-07-10T00:00:00+00:00"},
    ]
    assert _deduplicate_sample_rows(rows) == [rows[1]]


def test_basis_v4_drops_right_censored_timeout() -> None:
    rows = [
        {"var_ask": "100", "lighter_sell_price": "100.10"},
        {
            "var_bid": "99.99",
            "lighter_buy_price": "100.10",
            "var_quote_age_seconds": "0.1",
            "lighter_book_age_seconds": "0.1",
        },
    ]
    result = _basis_v4_simulate_episode(
        rows=rows,
        times=[0.0, 60.0],
        entry_index=0,
        direction="long_var_short_lighter",
        max_hold_seconds=300,
        shortfall_reserve_bps=Decimal("1"),
        net_exit_target_bps=Decimal("1"),
        max_quote_age_ms=Decimal("1500"),
        max_lighter_book_age_seconds=Decimal("2"),
    )
    assert result is None


def test_basis_v4_rejects_path_crossing_sample_gap() -> None:
    rows = [
        {"var_ask": "100", "lighter_sell_price": "100.10"},
        {
            "var_bid": "100.10",
            "lighter_buy_price": "100.00",
            "var_quote_age_seconds": "0.1",
            "lighter_book_age_seconds": "0.1",
        },
    ]
    result = _basis_v4_simulate_episode(
        rows=rows,
        times=[0.0, 61.0],
        entry_index=0,
        direction="long_var_short_lighter",
        max_hold_seconds=300,
        shortfall_reserve_bps=Decimal("1"),
        net_exit_target_bps=Decimal("1"),
        max_quote_age_ms=Decimal("1500"),
        max_lighter_book_age_seconds=Decimal("2"),
        max_sample_gap_seconds=60,
    )
    assert result == {"blocked_reason": "sample_gap"}


def test_basis_v3_drops_right_censored_timeout() -> None:
    rows = [
        {"var_ask": "100", "lighter_sell_price": "100.10"},
        {
            "var_bid": "99.99",
            "lighter_buy_price": "100.10",
            "long_edge_bps": "5",
            "var_quote_age_seconds": "0.1",
            "lighter_book_age_seconds": "0.1",
        },
    ]
    result = _basis_v3_simulate_episode(
        rows=rows,
        times=[0.0, 60.0],
        entry_index=0,
        direction="long_var_short_lighter",
        target_exit_edge_bps=Decimal("1"),
        max_hold_seconds=300,
        shortfall_reserve_bps=Decimal("1"),
        max_quote_age_ms=Decimal("1500"),
        max_lighter_book_age_seconds=Decimal("2"),
    )
    assert result is None


def test_basis_state_metrics_do_not_double_count_matching_block_event() -> None:
    state = {
        "event": "live_inventory_basis_state",
        "asset": "SOL",
        "basis_bps": "-8",
        "normalized_basis_bps": "-2",
        "long_edge_bps": "8",
        "short_edge_bps": "-9",
        "normalized_long_edge_bps": "2",
        "normalized_short_edge_bps": "-3",
        "basis_sample_move_bps": "0.5",
        "var_bid": "100",
        "var_ask": "100.01",
        "lighter_bid": "100",
        "lighter_ask": "100.01",
        "z": "2",
    }
    blocked = {**state, "event": "live_inventory_entry_blocked", "reason": "basis_sample_move_too_large"}
    rows = [state, blocked]

    assert basis_state_rows(rows) == [state]
    regime = build_basis_regimes(rows)["SOL"]
    assert regime["rows"] == 1
    assert regime["blocked_count"] == 1
    assert regime["raw_edge_p95"] == Decimal("8")
    assert dynamic_cost_summary(rows, asset_filter="SOL")["rows"] == 1


def test_entry_semantics_distinguishes_normalized_primary_from_raw_primary_filter() -> None:
    rows = [
        {
            "event": "live_inventory_basis_state",
            "run_id": "run-1",
            "logged_at": "2026-07-10T00:00:00+00:00",
            "asset": "SOL",
            "basis_bps": "-8",
            "long_edge_bps": "8",
            "short_edge_bps": "-9",
            "normalized_long_edge_bps": "2",
            "normalized_short_edge_bps": "-3",
            "var_bid": "99.9",
            "var_ask": "100",
            "lighter_buy_price": "100.1",
            "lighter_sell_price": "100.2",
            "long_stablecoin_filter_ok": True,
            "short_stablecoin_filter_ok": False,
            "basis_sample_move_ok": True,
        },
        {
            "event": "live_inventory_basis_state",
            "run_id": "run-1",
            "logged_at": "2026-07-10T00:00:01+00:00",
            "asset": "SOL",
            "basis_bps": "-8.1",
            "long_edge_bps": "8.5",
            "short_edge_bps": "-9",
            "normalized_long_edge_bps": "2.1",
            "normalized_short_edge_bps": "-3",
            "var_bid": "100",
            "var_ask": "100.1",
            "lighter_buy_price": "100",
            "lighter_sell_price": "100.2",
            "long_stablecoin_filter_ok": True,
            "short_stablecoin_filter_ok": False,
            "basis_sample_move_ok": True,
        },
    ]

    result = build_entry_semantics(
        rows,
        primary_threshold=Decimal("7"),
        min_abs_entry=Decimal("7"),
        min_normalized_filter=Decimal("1"),
    )["SOL"]

    assert result["normalized_primary_candidates"] == 0
    assert result["raw_primary_candidates"] == 2
    assert result["forward"][1]["attempts"] == 1
    assert result["forward"][1]["retained"] == 1
    assert result["forward"][1]["pnl_bps"] == [Decimal("20")]


def test_basis_v2_uses_real_time_and_does_not_require_normalized_edge() -> None:
    rows = []
    for timestamp, edge, age in (
        ("2026-07-10T00:00:00+00:00", "8", "0.1"),
        ("2026-07-10T00:00:00.100000+00:00", "8.5", "0.1"),
        ("2026-07-10T00:00:01.200000+00:00", "9", "0.1"),
    ):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-v2",
                "logged_at": timestamp,
                "asset": "SOL",
                "long_edge_bps": edge,
                "short_edge_bps": "-2",
                "var_bid": "99.9",
                "var_ask": "100",
                "lighter_bid": "100.08",
                "lighter_ask": "100.1",
                "lighter_sell_price": "100.08",
                "lighter_buy_price": "100.1",
                "var_quote_age_seconds": age,
            }
        )

    result = build_basis_v2_replay(rows, min_raw_edge_bps=Decimal("7"))["SOL"]

    assert result["candidate_count"] == 3
    assert result["direction_counts"]["long_var_short_lighter"] == 3
    assert result["horizons"][1]["attempts"] == 2
    assert result["horizons"][1]["pnl_bps"]
    assert not result["contexts"]["short_vs_long"]["insufficient_history"][1]["pnl_bps"] == []


def test_basis_v2_filter_sweep_inputs_filter_normalized_edge_and_stablecoin_share() -> None:
    rows = []
    for index, (edge, normalized, share) in enumerate(
        (("8", "1", "0.5"), ("8.5", "2.5", "0.5"), ("9", "3", "0.8"))
    ):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-filter",
                "logged_at": f"2026-07-10T00:00:0{index}+00:00",
                "asset": "SOL",
                "long_edge_bps": edge,
                "short_edge_bps": "-2",
                "normalized_long_edge_bps": normalized,
                "normalized_short_edge_bps": "-3",
                "long_stablecoin_edge_share": share,
                "short_stablecoin_edge_share": "0.2",
            }
        )

    unfiltered = build_basis_v2_replay(rows, min_raw_edge_bps=Decimal("7"))["SOL"]
    filtered = build_basis_v2_replay(
        rows,
        min_raw_edge_bps=Decimal("7"),
        min_normalized_edge_bps=Decimal("2.5"),
        max_stablecoin_edge_share=Decimal("0.6"),
    )["SOL"]

    assert unfiltered["candidate_count"] == 3
    assert filtered["candidate_count"] == 1


def test_execution_calibration_summary_only_uses_versioned_final_actual_rows(capsys) -> None:
    rows = [
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "execution-calibration-v1",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "asset": "SOL",
            "direction": "long_var_short_lighter",
            "actual_pnl_bps": "-2",
            "estimated_vs_actual_pnl_shortfall_bps": "1.5",
            "entry_lighter_slippage_bps": "0.2",
            "exit_lighter_slippage_bps": "0.3",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "legacy",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "asset": "SOL",
            "direction": "long_var_short_lighter",
            "actual_pnl_bps": "99",
        },
    ]

    print_execution_calibration(rows)

    output = capsys.readouterr().out
    assert "completed_actual_cycles=1" in output
    assert "actual_pnl_p50=-2.00" in output
    assert "shortfall_p80=1.50" in output
    assert "99" not in output


def test_execution_calibration_recommends_reserve_after_ten_cycles(capsys) -> None:
    rows = [
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "execution-calibration-v1",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "asset": "SOL",
            "direction": "long_var_short_lighter" if index % 2 == 0 else "short_var_long_lighter",
            "actual_pnl_bps": "-3" if index % 2 == 0 else "-2",
        }
        for index in range(10)
    ]

    print_execution_calibration(rows)

    output = capsys.readouterr().out
    assert "completed_actual_cycles=10" in output
    assert output.count("suggested_shortfall_reserve=0.50") == 2
    assert "observed_total_roundtrip_floor=4.00" in output
    assert "observed_total_roundtrip_floor=3.00" in output
    assert "recommendation=shortfall_reserve_ready_for_executable_price_replay" in output


def test_basis_v3_uses_prior_quantiles_across_runs_and_does_not_double_count_spread() -> None:
    started_at = datetime(2026, 7, 10, tzinfo=timezone.utc)
    rows = []
    for index in range(11):
        edge = Decimal("10") if index == 8 else Decimal("5")
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-a" if index < 5 else "run-b",
                "logged_at": (started_at + timedelta(minutes=index)).isoformat(),
                "asset": "SOL",
                "long_edge_bps": str(edge),
                "short_edge_bps": "-2",
                "var_bid": "99.99",
                "var_ask": "100",
                "lighter_sell_price": str(Decimal("100") + edge / Decimal("100")),
                "lighter_buy_price": str(Decimal("100.01") + edge / Decimal("100")),
                "var_quote_age_seconds": "5" if index == 9 else "0.1",
                "lighter_book_age_seconds": "0.1",
                "basis_sample_move_ok": True,
                "long_stablecoin_alignment": "opposed",
            }
        )

    result = build_basis_v3_replay(
        rows,
        asset_filter="SOL",
        evaluation_interval_seconds=60,
        history_sample_seconds=60,
        episode_cooldown_seconds=0,
        max_hold_seconds=300,
        min_window_coverage=Decimal("0.10"),
        min_history_samples=5,
        long_shortfall_reserve_bps=Decimal("1"),
        short_shortfall_reserve_bps=Decimal("1"),
        min_net_expected_bps=Decimal("1"),
        max_sample_gap_seconds=180,
    )["SOL"]

    episodes = [
        episode
        for episode in result["episodes"]
        if episode["variant"] == "main_p90_to_p55"
        and episode["direction"] == "long_var_short_lighter"
    ]
    assert len(episodes) == 1
    episode = episodes[0]
    assert episode["baseline_window_seconds"] == 3600
    assert episode["exit_reason"] == "target_quantile_reached"
    assert episode["holding_seconds"] == Decimal("120.0")
    assert episode["stablecoin_alignment"] == "opposed"
    assert episode["executable_pnl_bps"] == Decimal("3")
    assert episode["net_pnl_bps"] == Decimal("2")


def test_basis_v4_exits_only_after_executable_net_target() -> None:
    started_at = datetime(2026, 7, 10, tzinfo=timezone.utc)
    rows = []
    for index in range(11):
        edge = Decimal("10") if index == 8 else Decimal("5")
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "logged_at": (started_at + timedelta(minutes=index)).isoformat(),
                "asset": "SOL",
                "long_edge_bps": str(edge),
                "short_edge_bps": "-2",
                "var_bid": "100" if index == 9 else "99.99",
                "var_ask": "100",
                "lighter_sell_price": str(Decimal("100") + edge / Decimal("100")),
                "lighter_buy_price": "100" if index == 9 else "100.01",
                "var_quote_age_seconds": "0.1",
                "lighter_book_age_seconds": "0.1",
                "basis_sample_move_ok": True,
            }
        )

    result = build_basis_v4_replay(
        rows,
        asset_filter="SOL",
        evaluation_interval_seconds=60,
        history_sample_seconds=60,
        episode_cooldown_seconds=0,
        max_hold_seconds=300,
        min_window_coverage=Decimal("0.10"),
        min_history_samples=5,
        long_shortfall_reserve_bps=Decimal("1"),
        short_shortfall_reserve_bps=Decimal("1"),
        net_exit_target_bps=Decimal("1"),
    )["SOL"]

    episodes = [
        episode
        for episode in result["episodes"]
        if episode["entry_percentile"] == Decimal("90")
        and episode["direction"] == "long_var_short_lighter"
    ]
    assert len(episodes) == 1
    assert episodes[0]["exit_reason"] == "executable_net_target_reached"
    assert episodes[0]["holding_seconds"] == Decimal("60.0")
    assert episodes[0]["net_pnl_bps"] == Decimal("9")


def test_basis_v2_event_cooldown_collapses_repeated_same_direction_candidates() -> None:
    rows = []
    for index in range(3):
        rows.append(
            {
                "event": "live_inventory_basis_state",
                "run_id": "run-cooldown",
                "logged_at": f"2026-07-10T00:00:0{index}+00:00",
                "asset": "SOL",
                "long_edge_bps": "8",
                "short_edge_bps": "-2",
                "var_bid": "99.9",
                "var_ask": "100",
                "lighter_bid": "100.08",
                "lighter_ask": "100.1",
                "lighter_sell_price": "100.08",
                "lighter_buy_price": "100.1",
            }
        )

    result = build_basis_v2_replay(
        rows,
        min_raw_edge_bps=Decimal("7"),
        horizons=(1,),
        event_cooldown_seconds=300,
    )["SOL"]

    assert result["raw_candidate_count"] == 3
    assert result["candidate_count"] == 1
    assert len(result["candidate_events"]) == 1


def test_basis_v2_sweep_requires_train_and_holdout_positive_p20() -> None:
    events = [
        {"timestamp": float(index), "direction": "long_var_short_lighter", "pnl_bps": {5: Decimal(value)}}
        for index, value in enumerate(("3", "4", "5", "4", "5", "6"))
    ]

    summary = summarize_basis_v2_sweep_events(
        events,
        horizons=(5,),
        holdout_fraction=Decimal("0.5"),
        min_independent_samples=3,
        execution_reserve_bps=Decimal("1"),
    )[5]

    assert summary["train"]["n"] == 3
    assert summary["holdout"]["n"] == 3
    assert summary["train"]["p20"] == Decimal("2")
    assert summary["holdout"]["p20"] == Decimal("3")
    assert summary["verdict"] == "manual_review_candidate"
