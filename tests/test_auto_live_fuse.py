import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.lib.rolling_rate_limiter import RollingWindowRateLimiter

from main import (
    AutoLivePositionState,
    CrossSpreadSnapshot,
    LiveInventoryBasisState,
    OrderLifecycle,
    PendingAutoLiveMatch,
    PendingLiveInventoryVarFillMatch,
    VariationalMonitor,
    VariationalToLighterRuntime,
    account_risk_context,
    account_risk_notification_transition,
    adaptive_margin_thresholds,
    account_snapshot_freshness,
    live_inventory_state_status,
    maintenance_drain_uses_individual_legacy_exits,
    maintenance_control_targets_runtime,
    variational_api_amount_to_str,
    v4_real_gradient_capacity_notional_usd,
    v4_real_gradient_confirmed_tier,
    v4_real_gradient_eligible_tier,
    v4_real_gradient_entry_activation,
    v4_real_gradient_elastic_capacity_child_lots,
    v4_real_gradient_sample_move_decision,
    v4_real_gradient_lot_groups,
    v4_real_gradient_slot_caps,
    v4_real_gradient_thresholds,
    v4_exact_rfq_entry_threshold,
    v4_exact_rfq_gradient_thresholds,
    v4_partial_detier_selection,
    v4_weekend_regime_context,
)


def _runtime_for_fuse_test() -> VariationalToLighterRuntime:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.auto_live_manual_review_required = False
    runtime.auto_live_manual_review_reason = None
    runtime.auto_live_max_cycles = 1
    runtime.auto_live_completed_cycles = 0
    runtime.auto_live_next_cycle_id = 1
    runtime.auto_live_last_closed_monotonic = None
    runtime.auto_live_cooldown_seconds = 60.0
    runtime.auto_live_position = None
    runtime.auto_live_state_file = None
    runtime._last_auto_live_guard_log = None
    runtime._last_auto_live_precheck_failure_log = {}
    runtime.logger = logging.getLogger("test_auto_live_fuse")
    return runtime


def _position() -> AutoLivePositionState:
    return AutoLivePositionState(
        cycle_id=7,
        asset="BTC",
        direction="long_var_short_lighter",
        entered_at_iso="2026-06-01T00:00:00Z",
        entered_at_monotonic=1.0,
        entry_spread_pct=Decimal("0.01"),
        entry_median_pct=Decimal("0"),
        entry_deviation_bps=Decimal("1"),
        entry_var_mid=Decimal("100000"),
        entry_lighter_mid=Decimal("100000"),
        entry_var_execution_price=Decimal("100001"),
        entry_lighter_execution_price=Decimal("100000"),
        planned_notional_usd=Decimal("25"),
        planned_qty=Decimal("0.00025"),
    )


def test_variational_account_snapshot_freshness_rejects_stale_data() -> None:
    now = datetime(2026, 8, 27, 0, 2, tzinfo=timezone.utc)

    fresh = account_snapshot_freshness(
        "2026-08-27T00:01:30+00:00",
        now=now,
        max_age_seconds=60,
    )
    stale = account_snapshot_freshness(
        "2026-08-26T20:00:00+00:00",
        now=now,
        max_age_seconds=60,
    )

    assert fresh["fresh"] is True
    assert fresh["age_seconds"] == 30
    assert stale["fresh"] is False
    assert stale["reason"] == "snapshot_stale"


def test_account_recovery_requires_three_complete_checks_and_clears_signals() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_account_recovery_required = True
    runtime.live_inventory_account_recovery_confirm_count = 0
    runtime.live_inventory_account_recovery_confirm_samples = 3
    runtime.live_inventory_account_recovery_reason = "startup_confirmation_required"
    runtime.live_inventory_basis_entry_confirm_counts = {
        "short_var_long_lighter": 1
    }
    runtime.live_inventory_v4_gradient_entry_tier_window = deque([2, 2], maxlen=3)
    fresh = {
        "risk_action": "normal",
        "risk_reason": "account_risk_normal",
        "variational_account_snapshot_fresh": True,
        "variational_account_snapshot_freshness_reason": "fresh",
        "variational_equity_usd": "100",
        "lighter_equity_usd": "100",
    }

    first = runtime.apply_live_inventory_account_recovery_gate(
        fresh,
        advance_confirmation=True,
    )
    second = runtime.apply_live_inventory_account_recovery_gate(
        fresh,
        advance_confirmation=True,
    )
    third = runtime.apply_live_inventory_account_recovery_gate(
        fresh,
        advance_confirmation=True,
    )

    assert first["risk_action"] == "block_entry"
    assert first["account_recovery_confirm_count"] == 1
    assert second["risk_action"] == "block_entry"
    assert second["account_recovery_confirm_count"] == 2
    assert third["risk_action"] == "normal"
    assert third["account_recovery_required"] is False
    assert runtime.live_inventory_basis_entry_confirm_counts == {}
    assert list(runtime.live_inventory_v4_gradient_entry_tier_window) == []


def test_account_recovery_resets_after_an_incomplete_check() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_account_recovery_required = True
    runtime.live_inventory_account_recovery_confirm_count = 2
    runtime.live_inventory_account_recovery_confirm_samples = 3
    runtime.live_inventory_account_recovery_reason = "startup_confirmation_required"
    runtime.live_inventory_basis_entry_confirm_counts = {}
    runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
    incomplete = {
        "risk_action": "block_entry",
        "risk_reason": "account_equity_unavailable",
        "variational_account_snapshot_fresh": True,
        "variational_account_snapshot_freshness_reason": "fresh",
        "variational_equity_usd": "100",
        "lighter_equity_usd": None,
    }

    result = runtime.apply_live_inventory_account_recovery_gate(
        incomplete,
        advance_confirmation=True,
    )

    assert result["risk_action"] == "block_entry"
    assert result["account_recovery_confirm_count"] == 0
    assert result["account_recovery_required"] is True


def test_lighter_account_failure_reason_stays_stable_until_recovery() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_account_recovery_required = False
    runtime.live_inventory_account_recovery_confirm_count = 0
    runtime.live_inventory_account_recovery_confirm_samples = 3
    runtime.live_inventory_account_recovery_reason = None
    runtime.live_inventory_basis_entry_confirm_counts = {}
    runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
    unavailable = {
        "risk_action": "block_entry",
        "risk_reason": "account_equity_unavailable",
        "variational_account_snapshot_fresh": True,
        "variational_account_snapshot_usable": True,
        "variational_equity_usd": "100",
        "lighter_equity_usd": None,
        "lighter_risk_fetch_error": "ServiceException:HTTP 502",
    }

    failed = runtime.apply_live_inventory_account_recovery_gate(unavailable)

    assert failed["risk_reason"] == "lighter_account_equity_unavailable"
    assert failed["account_recovery_reason"] == "lighter_account_equity_unavailable"

    healthy = {
        "risk_action": "normal",
        "risk_reason": "account_risk_normal",
        "variational_account_snapshot_fresh": True,
        "variational_account_snapshot_usable": True,
        "variational_equity_usd": "100",
        "lighter_equity_usd": "100",
    }
    first = runtime.apply_live_inventory_account_recovery_gate(
        healthy,
        advance_confirmation=True,
    )
    second = runtime.apply_live_inventory_account_recovery_gate(
        healthy,
        advance_confirmation=True,
    )
    third = runtime.apply_live_inventory_account_recovery_gate(
        healthy,
        advance_confirmation=True,
    )

    assert first["risk_action"] == second["risk_action"] == "block_entry"
    assert first["risk_reason"] == second["risk_reason"] == (
        "lighter_account_equity_unavailable"
    )
    assert third["risk_action"] == "normal"
    assert third["account_recovery_required"] is False


def test_lighter_account_read_retries_transient_server_error_only(monkeypatch) -> None:
    import asyncio
    import sys

    import main as main_module

    async def run() -> None:
        nonlocal_calls = {"count": 0}

        class TransientServerError(Exception):
            status_code = 502

        class FakeAccountApi:
            def __init__(self, _client):
                pass

            async def account(self, **_kwargs):
                nonlocal_calls["count"] += 1
                if nonlocal_calls["count"] == 1:
                    raise TransientServerError("upstream unavailable")
                return {"code": 0, "accounts": []}

        monkeypatch.setitem(
            sys.modules,
            "lighter",
            type("FakeLighterModule", (), {"AccountApi": FakeAccountApi}),
        )
        monkeypatch.setattr(
            main_module,
            "LIGHTER_ACCOUNT_READ_RETRY_DELAYS_SECONDS",
            (0.0, 0.0),
        )
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.account_index = 7
        runtime.initialize_lighter_client = lambda: SimpleNamespace(api_client=object())

        result = await runtime.fetch_lighter_account()

        assert result == {"code": 0, "accounts": []}
        assert nonlocal_calls["count"] == 2

    asyncio.run(run())


def test_lighter_account_read_does_not_retry_auth_error(monkeypatch) -> None:
    import asyncio
    import sys

    async def run() -> None:
        calls = {"count": 0}

        class AuthenticationError(Exception):
            status_code = 401

        class FakeAccountApi:
            def __init__(self, _client):
                pass

            async def account(self, **_kwargs):
                calls["count"] += 1
                raise AuthenticationError("unauthorized")

        monkeypatch.setitem(
            sys.modules,
            "lighter",
            type("FakeLighterModule", (), {"AccountApi": FakeAccountApi}),
        )
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.account_index = 7
        runtime.initialize_lighter_client = lambda: SimpleNamespace(api_client=object())

        with pytest.raises(AuthenticationError):
            await runtime.fetch_lighter_account()

        assert calls["count"] == 1

    asyncio.run(run())


def test_startup_recovery_requires_strictly_fresh_snapshot() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_account_recovery_required = True
    runtime.live_inventory_account_recovery_confirm_count = 0
    runtime.live_inventory_account_recovery_confirm_samples = 3
    runtime.live_inventory_account_recovery_reason = "startup_confirmation_required"
    runtime.live_inventory_basis_entry_confirm_counts = {}
    runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
    degraded = {
        "risk_action": "normal",
        "risk_reason": "account_risk_normal",
        "variational_account_snapshot_fresh": False,
        "variational_account_snapshot_usable": True,
        "variational_equity_usd": "100",
        "lighter_equity_usd": "100",
    }

    result = runtime.apply_live_inventory_account_recovery_gate(
        degraded,
        advance_confirmation=True,
    )

    assert result["risk_action"] == "block_entry"
    assert result["risk_reason"] == "variational_account_recovery_confirmation_pending"
    assert result["account_recovery_confirm_count"] == 0
    assert result["account_recovery_required"] is True


def test_stale_snapshot_recovery_releases_on_first_strictly_fresh_update() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_account_recovery_required = True
    runtime.live_inventory_account_recovery_confirm_count = 0
    runtime.live_inventory_account_recovery_confirm_samples = 3
    runtime.live_inventory_account_recovery_reason = (
        "variational_account_snapshot_stale"
    )
    runtime.live_inventory_basis_entry_confirm_counts = {}
    runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
    fresh = {
        "risk_action": "normal",
        "risk_reason": "account_risk_normal",
        "variational_account_snapshot_fresh": True,
        "variational_account_snapshot_usable": True,
        "variational_equity_usd": "100",
        "lighter_equity_usd": "100",
    }

    result = runtime.apply_live_inventory_account_recovery_gate(fresh)

    assert result["risk_action"] == "normal"
    assert result["account_recovery_required"] is False
    assert result["account_recovery_confirm_count"] == 3


def test_live_inventory_state_status_follows_actual_positions_and_actions() -> None:
    assert live_inventory_state_status(open_lots=[{"lot_id": 1}], pending_actions=[]) == "open"
    assert live_inventory_state_status(open_lots=[], pending_actions=[{"role": "entry"}]) == "pending"
    assert live_inventory_state_status(open_lots=[], pending_actions=[]) == "flat"


def test_maintenance_control_is_bound_to_exact_runtime() -> None:
    control = {
        "action": "drain_after_flat",
        "status": "requested",
        "asset": "ETH",
        "target_pid": 54217,
        "target_run_id": "liveinv-1",
    }

    assert maintenance_control_targets_runtime(
        control,
        process_id=54217,
        run_id="liveinv-1",
        asset="ETH",
    )
    assert not maintenance_control_targets_runtime(
        control,
        process_id=54218,
        run_id="liveinv-1",
        asset="ETH",
    )
    assert not maintenance_control_targets_runtime(
        control,
        process_id=54217,
        run_id="liveinv-2",
        asset="ETH",
    )


def test_v4_real_gradient_thresholds_are_dynamic_and_strictly_ordered() -> None:
    history = [Decimal(index) / Decimal("10") for index in range(1000)]

    thresholds = v4_real_gradient_thresholds(
        history_values=history,
        base_threshold_bps=Decimal("97.4"),
        entry_execution_reserve_bps=Decimal("0.5"),
    )

    assert len(thresholds) == 5
    assert thresholds[0] == Decimal("97.4")
    assert all(right > left for left, right in zip(thresholds, thresholds[1:]))
    assert v4_real_gradient_eligible_tier(thresholds[2], thresholds) == 3


def test_v4_real_gradient_thresholds_respect_noise_and_depth_spacing() -> None:
    thresholds = v4_real_gradient_thresholds(
        history_values=[Decimal(index) / Decimal("10") for index in range(1000)],
        base_threshold_bps=Decimal("97.4"),
        entry_execution_reserve_bps=Decimal("0.5"),
        actual_market_noise_bps=Decimal("0.8"),
        incremental_depth_cost_bps=Decimal("0.6"),
        recent_pair_execution_error_bps=Decimal("0.7"),
    )

    assert all(
        right - left >= Decimal("0.8")
        for left, right in zip(thresholds, thresholds[1:])
    )


def test_v4_real_gradient_thresholds_do_not_repeat_total_execution_error() -> None:
    kwargs = {
        "history_values": [Decimal(index) / Decimal("10") for index in range(1000)],
        "base_threshold_bps": Decimal("97.4"),
        "entry_execution_reserve_bps": Decimal("0.5"),
        "actual_market_noise_bps": Decimal("0.3"),
        "incremental_depth_cost_bps": Decimal("0.4"),
    }

    without_execution_error = v4_real_gradient_thresholds(
        **kwargs,
        recent_pair_execution_error_bps=None,
    )
    with_execution_error = v4_real_gradient_thresholds(
        **kwargs,
        recent_pair_execution_error_bps=Decimal("4.0"),
    )

    assert with_execution_error == without_execution_error
    assert all(
        right - left >= Decimal("0.4")
        for left, right in zip(with_execution_error, with_execution_error[1:])
    )


def test_v4_real_gradient_entry_uses_latest_and_two_of_three() -> None:
    assert v4_real_gradient_confirmed_tier([0, 3], latest_tier=3) == 0
    assert v4_real_gradient_confirmed_tier([3, 0, 3], latest_tier=3) == 3
    assert v4_real_gradient_confirmed_tier([3, 3, 0], latest_tier=0) == 0


def test_v4_real_gradient_high_tier_single_sample_is_one_layer_probe() -> None:
    assert v4_real_gradient_entry_activation([3], latest_tier=3) == (
        1,
        "strong_single_probe",
    )
    assert v4_real_gradient_entry_activation([0, 3], latest_tier=3) == (
        1,
        "strong_single_probe",
    )
    assert v4_real_gradient_entry_activation([3, 0, 3], latest_tier=3) == (
        3,
        "two_of_three",
    )
    assert v4_real_gradient_entry_activation([2], latest_tier=2) == (
        0,
        "pending_two_of_three",
    )


def test_v4_real_gradient_sample_move_only_rechecks_high_tier_single_probe() -> None:
    assert v4_real_gradient_sample_move_decision(
        sample_move_ok=True,
        strong_single_probe=False,
        refresh_entry_quote_enabled=False,
    ) == "normal"
    assert v4_real_gradient_sample_move_decision(
        sample_move_ok=False,
        strong_single_probe=False,
        refresh_entry_quote_enabled=True,
    ) == "block"
    assert v4_real_gradient_sample_move_decision(
        sample_move_ok=False,
        strong_single_probe=True,
        refresh_entry_quote_enabled=True,
    ) == "shock_recheck"
    assert v4_real_gradient_sample_move_decision(
        sample_move_ok=False,
        strong_single_probe=True,
        refresh_entry_quote_enabled=False,
    ) == "block"


def test_v4_real_gradient_active_tier_promotes_probe_only_after_confirmation() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_bidirectional = False
    runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
    runtime.live_inventory_v4_gradient_tier_states = {}
    thresholds = [
        Decimal("1"),
        Decimal("2"),
        Decimal("3"),
        Decimal("4"),
        Decimal("5"),
    ]

    assert runtime.live_inventory_basis_v4_active_gradient_tier(
        raw_tier=3,
        edge_bps=Decimal("3"),
        thresholds_bps=thresholds,
    ) == 1
    assert runtime.live_inventory_basis_v4_active_gradient_tier(
        raw_tier=3,
        edge_bps=Decimal("3"),
        thresholds_bps=thresholds,
    ) == 3


def test_v4_real_gradient_closed_tier_requires_reset_before_rearm() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_v4_gradient_entry_tier_window = deque(
        [3, 3],
        maxlen=3,
    )
    runtime.live_inventory_v4_gradient_tier_states = {
        tier: {"armed": True, "reset_seen": False}
        for tier in range(1, 6)
    }
    thresholds = [
        Decimal("1"),
        Decimal("2"),
        Decimal("3"),
        Decimal("4"),
        Decimal("5"),
    ]

    runtime.mark_live_inventory_gradient_tier_closed(
        tier=3,
        edge_bps=Decimal("4"),
        thresholds_bps=thresholds,
    )

    assert runtime.live_inventory_basis_v4_active_gradient_tier(
        raw_tier=3,
        edge_bps=Decimal("4"),
        thresholds_bps=thresholds,
    ) == 2
    assert runtime.live_inventory_basis_v4_active_gradient_tier(
        raw_tier=0,
        edge_bps=Decimal("0"),
        thresholds_bps=thresholds,
    ) == 0
    assert runtime.live_inventory_basis_v4_active_gradient_tier(
        raw_tier=3,
        edge_bps=Decimal("4"),
        thresholds_bps=thresholds,
    ) == 3


def test_v4_real_gradient_capacity_uses_smaller_venue_equity() -> None:
    assert v4_real_gradient_capacity_notional_usd(
        tier=3,
        variational_equity_usd=Decimal("100"),
        lighter_equity_usd=Decimal("120"),
        max_venue_leverage=Decimal("5"),
    ) == Decimal("300")
    assert v4_real_gradient_capacity_notional_usd(
        tier=9,
        variational_equity_usd=Decimal("100"),
        lighter_equity_usd=Decimal("120"),
        max_venue_leverage=Decimal("5"),
    ) == Decimal("500")


def test_v4_real_gradient_slot_caps_follow_dynamic_smaller_equity() -> None:
    assert v4_real_gradient_slot_caps(
        variational_equity_usd=Decimal("102.97"),
        lighter_equity_usd=Decimal("98.64"),
        child_notional_usd=Decimal("20"),
        max_venue_leverage=Decimal("5"),
    ) == [5, 10, 15, 20, 24]
    assert v4_real_gradient_slot_caps(
        variational_equity_usd=Decimal("100"),
        lighter_equity_usd=Decimal("100"),
        child_notional_usd=Decimal("20"),
        max_venue_leverage=Decimal("5"),
    ) == [5, 10, 15, 20, 25]
    assert v4_real_gradient_slot_caps(
        variational_equity_usd=Decimal("120"),
        lighter_equity_usd=Decimal("120"),
        child_notional_usd=Decimal("20"),
        max_venue_leverage=Decimal("5"),
    ) == [6, 12, 18, 24, 30]


def test_v4_real_gradient_elastic_capacity_borrows_one_guarded_slot() -> None:
    slot_caps = [5, 10, 15, 20, 25]

    assert v4_real_gradient_elastic_capacity_child_lots(
        tier=1,
        slot_caps=slot_caps,
        open_child_lots=4,
        enabled=True,
        addon_eligible=True,
    ) == 5
    assert v4_real_gradient_elastic_capacity_child_lots(
        tier=1,
        slot_caps=slot_caps,
        open_child_lots=5,
        enabled=True,
        addon_eligible=True,
    ) == 6
    assert v4_real_gradient_elastic_capacity_child_lots(
        tier=1,
        slot_caps=slot_caps,
        open_child_lots=5,
        enabled=True,
        addon_eligible=False,
    ) == 5
    assert v4_real_gradient_elastic_capacity_child_lots(
        tier=2,
        slot_caps=slot_caps,
        open_child_lots=10,
        enabled=True,
        addon_eligible=True,
    ) == 10


def test_v4_partial_detier_selects_entire_highest_tier_only() -> None:
    lots = [
        {
            "lot_id": 1,
            "qty": "1",
            "entry_var_fill_price": "20",
            "entry_gradient_tier": 1,
            "tranche_index": 1,
        },
        {
            "lot_id": 2,
            "qty": "1",
            "entry_var_fill_price": "20",
            "entry_gradient_tier": 3,
            "tranche_index": 2,
        },
        {
            "lot_id": 3,
            "qty": "1",
            "entry_var_fill_price": "20",
            "entry_gradient_tier": 3,
            "tranche_index": 3,
        },
    ]

    plan = v4_partial_detier_selection(
        lots=lots,
        current_tier=2,
        variational_equity_usd=Decimal("20"),
        lighter_equity_usd=Decimal("25"),
        max_venue_leverage=Decimal("5"),
    )

    assert plan["ready"] is True
    assert plan["selected_lot_ids"] == ["2", "3"]
    assert plan["selected_gradient_tier"] == 3
    assert plan["remaining_notional_usd"] == "20"
    assert "combined_projected_pnl_usd" not in plan


def test_v4_real_gradient_lot_groups_never_mix_tier_pnl() -> None:
    lots = [
        {"lot_id": 1, "entry_gradient_tier": 1},
        {"lot_id": 2, "entry_gradient_tier": 3},
        {"lot_id": 3, "entry_gradient_tier": 1},
        {"lot_id": 4, "entry_gradient_tier": 3},
    ]

    groups = v4_real_gradient_lot_groups(lots)

    assert [(tier, [lot["lot_id"] for lot in group]) for tier, group in groups] == [
        (3, [2, 4]),
        (1, [1, 3]),
    ]


def test_maintenance_drain_keeps_unlabeled_legacy_lots_independent() -> None:
    assert maintenance_drain_uses_individual_legacy_exits(
        requested=True,
        lots=[
            {"lot_id": 1, "entry_gradient_tier": None},
            {"lot_id": 2, "entry_gradient_tier": None},
        ],
    ) is True
    assert maintenance_drain_uses_individual_legacy_exits(
        requested=False,
        lots=[{"lot_id": 1, "entry_gradient_tier": None}],
    ) is False
    assert maintenance_drain_uses_individual_legacy_exits(
        requested=True,
        lots=[{"lot_id": 1, "entry_gradient_tier": 3}],
    ) is False


def test_adaptive_margin_thresholds_allow_normal_five_x_baseline() -> None:
    thresholds = adaptive_margin_thresholds(
        maintenance_requirement_usd=Decimal("10"),
        current_notional_usd=Decimal("100"),
        equity_usd=Decimal("100"),
        target_notional_usd=Decimal("500"),
        max_venue_leverage=Decimal("5"),
        warning_pct=Decimal("40"),
        block_entry_pct=Decimal("50"),
        reduce_pct=Decimal("60"),
        emergency_pct=Decimal("75"),
    )

    assert thresholds["projected_usage_pct"] == Decimal("50")
    assert thresholds["warning_pct"] == Decimal("60")
    assert thresholds["block_entry_pct"] == Decimal("70")
    assert thresholds["reduce_pct"] == Decimal("80")
    assert thresholds["emergency_pct"] == Decimal("90")


def test_adaptive_margin_thresholds_use_conservative_fallback() -> None:
    thresholds = adaptive_margin_thresholds(
        maintenance_requirement_usd=None,
        current_notional_usd=Decimal("500"),
        equity_usd=Decimal("100"),
        target_notional_usd=Decimal("500"),
        max_venue_leverage=Decimal("5"),
        warning_pct=Decimal("40"),
        block_entry_pct=Decimal("50"),
        reduce_pct=Decimal("60"),
        emergency_pct=Decimal("75"),
        fallback_maintenance_rate=Decimal("0.10"),
    )

    assert thresholds["maintenance_rate"] == Decimal("0.10")
    assert thresholds["maintenance_rate_source"] == "conservative_fallback"
    assert thresholds["projected_usage_pct"] == Decimal("50.00")


def test_account_risk_uses_venue_specific_adaptive_margin_thresholds() -> None:
    context = account_risk_context(
        variational_metrics={
            "equity_usd": Decimal("100"),
            "maintenance_margin_requirement_usd": Decimal("50"),
            "maintenance_margin_usage_pct": Decimal("50"),
        },
        lighter_metrics={
            "equity_usd": Decimal("100"),
            "maintenance_margin_requirement_usd": Decimal("6"),
            "maintenance_margin_usage_pct": Decimal("6"),
        },
        current_notional_usd=Decimal("500"),
        proposed_notional_usd=None,
        max_venue_leverage=Decimal("5"),
        margin_warning_pct=Decimal("40"),
        margin_block_entry_pct=Decimal("50"),
        margin_reduce_pct=Decimal("60"),
        margin_emergency_pct=Decimal("75"),
        balance_warning_ratio=Decimal("0.82"),
        balance_block_ratio=Decimal("0.74"),
    )

    assert context["risk_action"] == "normal"
    assert context["variational_effective_margin_warning_pct"] == "60.0"
    assert context["lighter_effective_margin_warning_pct"] == "40"


def test_account_risk_recommends_rebalance_at_warning_threshold() -> None:
    context = account_risk_context(
        variational_metrics={
            "equity_usd": Decimal("110"),
            "maintenance_margin_usage_pct": Decimal("0"),
        },
        lighter_metrics={
            "equity_usd": Decimal("90"),
            "maintenance_margin_usage_pct": Decimal("0"),
        },
        current_notional_usd=Decimal("100"),
        proposed_notional_usd=None,
        max_venue_leverage=Decimal("5"),
        margin_warning_pct=Decimal("40"),
        margin_block_entry_pct=Decimal("50"),
        margin_reduce_pct=Decimal("60"),
        margin_emergency_pct=Decimal("75"),
        balance_warning_ratio=Decimal("0.82"),
        balance_block_ratio=Decimal("0.74"),
    )

    assert context["risk_action"] == "warning"
    assert context["risk_reason"] == "venue_equity_imbalance_warning"
    assert context["rebalance_recommended"] is True
    assert context["rebalance_from_venue"] == "variational"
    assert context["rebalance_to_venue"] == "lighter"
    assert Decimal(context["rebalance_suggested_amount_usd"]) == Decimal("6.75")


def test_account_risk_notification_debounces_and_uses_imbalance_hysteresis() -> None:
    warning_context = {
        "equity_balance_ratio": "0.8201",
        "balance_warning_ratio": "0.82",
    }
    first = account_risk_notification_transition(
        current_action="normal",
        current_reason="account_risk_normal",
        pending_reason=None,
        warning_confirm_count=0,
        recovery_confirm_count=0,
        risk_action="warning",
        risk_reason="venue_equity_imbalance_warning",
        context=warning_context,
    )
    assert first["notification_event"] is None
    assert first["notification_action"] == "normal"
    assert first["notification_warning_confirm_count"] == 1

    second = account_risk_notification_transition(
        current_action=first["notification_action"],
        current_reason=first["notification_reason"],
        pending_reason=first["notification_pending_reason"],
        warning_confirm_count=first["notification_warning_confirm_count"],
        recovery_confirm_count=first["notification_recovery_confirm_count"],
        risk_action="warning",
        risk_reason="venue_equity_imbalance_warning",
        context=warning_context,
    )
    assert second["notification_event"] == "alert"
    assert second["notification_action"] == "warning"

    not_clear = account_risk_notification_transition(
        current_action=second["notification_action"],
        current_reason=second["notification_reason"],
        pending_reason=None,
        warning_confirm_count=second["notification_warning_confirm_count"],
        recovery_confirm_count=0,
        risk_action="normal",
        risk_reason="account_risk_normal",
        context={**warning_context, "equity_balance_ratio": "0.83"},
    )
    assert not_clear["notification_action"] == "warning"
    assert not_clear["notification_event"] is None

    recovery = {**warning_context, "equity_balance_ratio": "0.84"}
    current = not_clear
    for expected_event in (None, None, "recovered"):
        current = account_risk_notification_transition(
            current_action=current["notification_action"],
            current_reason=current["notification_reason"],
            pending_reason=current["notification_pending_reason"],
            warning_confirm_count=current["notification_warning_confirm_count"],
            recovery_confirm_count=current["notification_recovery_confirm_count"],
            risk_action="normal",
            risk_reason="account_risk_normal",
            context=recovery,
        )
        assert current["notification_event"] == expected_event
    assert current["notification_action"] == "normal"


def test_account_risk_blocks_new_notional_above_five_x_but_does_not_immediately_reduce() -> None:
    metrics = {
        "equity_usd": Decimal("99"),
        "maintenance_margin_requirement_usd": Decimal("50"),
        "maintenance_margin_usage_pct": Decimal("50.505050505"),
    }
    existing = account_risk_context(
        variational_metrics=metrics,
        lighter_metrics=metrics,
        current_notional_usd=Decimal("500"),
        proposed_notional_usd=None,
        max_venue_leverage=Decimal("5"),
        margin_warning_pct=Decimal("40"),
        margin_block_entry_pct=Decimal("50"),
        margin_reduce_pct=Decimal("60"),
        margin_emergency_pct=Decimal("75"),
        balance_warning_ratio=Decimal("0.82"),
        balance_block_ratio=Decimal("0.74"),
    )
    proposed = account_risk_context(
        variational_metrics=metrics,
        lighter_metrics=metrics,
        current_notional_usd=Decimal("480"),
        proposed_notional_usd=Decimal("500"),
        max_venue_leverage=Decimal("5"),
        margin_warning_pct=Decimal("40"),
        margin_block_entry_pct=Decimal("50"),
        margin_reduce_pct=Decimal("60"),
        margin_emergency_pct=Decimal("75"),
        balance_warning_ratio=Decimal("0.82"),
        balance_block_ratio=Decimal("0.74"),
    )

    assert existing["risk_action"] == "warning"
    assert existing["risk_reason"] == "venue_leverage_above_entry_cap_monitoring_margin"
    assert proposed["risk_action"] == "block_entry"
    assert proposed["risk_reason"] == "venue_leverage_exceeds_hard_entry_limit"


def test_weekend_regime_marks_six_hour_boundaries() -> None:
    saturday = datetime(2026, 8, 22, 2, 0, tzinfo=timezone.utc).timestamp()
    sunday = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc).timestamp()

    transition = v4_weekend_regime_context(saturday)
    settled = v4_weekend_regime_context(sunday)

    assert transition["v4_market_regime"] == "weekend"
    assert transition["v4_weekend_transition"] == "weekend_start"
    assert transition["v4_weekend_transition_active"] is True
    assert settled["v4_weekend_transition_active"] is False


def test_v4_aggregate_exit_uses_all_confirmed_lots(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    lots = [
        {
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "qty": "1",
            "entry_var_fill_price": "110",
            "entry_lighter_fill_price": "100",
            "entry_cost_status": "final_fills_confirmed",
        },
        {
            "lot_id": 2,
            "direction": "short_var_long_lighter",
            "qty": "1",
            "entry_var_fill_price": "108",
            "entry_lighter_fill_price": "101",
            "entry_cost_status": "final_fills_confirmed",
        },
    ]

    context = runtime.live_inventory_v4_aggregate_exit_context(
        lots=lots,
        var_exit_price=Decimal("105"),
        lighter_exit_price=Decimal("104"),
    )

    assert context["ready"] is True
    assert context["lot_ids"] == ["1", "2"]
    assert context["aggregate_executable_pnl_usd"] == "15"
    assert Decimal(context["aggregate_executable_pnl_bps"]) == (
        Decimal("15") / Decimal("218") * Decimal("10000")
    )

    merged = runtime.live_inventory_v4_merge_lots_for_atomic_exit(lots)
    assert merged["qty"] == "2"
    assert merged["entry_var_fill_price"] == "109"
    assert merged["entry_lighter_fill_price"] == "100.5"
    assert merged["portfolio_component_lot_ids"] == ["1", "2"]
    _, _, merged_pnl = runtime.live_inventory_pair_pnl(
        direction=merged["direction"],
        qty=Decimal(merged["qty"]),
        entry_var_price=Decimal(merged["entry_var_fill_price"]),
        entry_lighter_price=Decimal(merged["entry_lighter_fill_price"]),
        exit_var_price=Decimal("105"),
        exit_lighter_price=Decimal("104"),
    )
    assert merged_pnl == Decimal("15")


def test_state_restore_clears_stale_portfolio_exit_lock(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_v4_shadow_tranche = None
    runtime.live_inventory_v4_shadow_tranches = {}
    runtime.live_inventory_v4_shadow_completed_keys = set()
    runtime.live_inventory_state_file.write_text(
        json.dumps(
            {
                "status": "open",
                "open_lots": [{"lot_id": 2, "asset": "ETH"}],
                "v4_portfolio_exit_lot_ids": [1, 2],
                "v4_portfolio_exit_context": {"locked_at": "old"},
            }
        ),
        encoding="utf-8",
    )

    runtime.sync_live_inventory_memory_from_state()

    assert runtime.live_inventory_v4_portfolio_exit_lot_ids == set()
    assert runtime.live_inventory_v4_stale_portfolio_exit_lot_ids == ["1"]
    assert runtime.live_inventory_v4_portfolio_exit_context == {}


def test_state_restore_rehydrates_pending_submission_for_reconciliation(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.auto_live_match_window_seconds = 30.0
    runtime.live_inventory_state_file.write_text(
        json.dumps(
            {
                "pending_actions": [
                    {
                        "asset": "ETH",
                        "side": "sell",
                        "qty": "0.00820",
                        "lot_id": 2,
                        "role": "live_inventory_entry_pending_var_fill",
                        "direction": "short_var_long_lighter",
                        "submitted_at": "2026-09-02T18:00:00+00:00",
                        "rfq_id": "rfq-restart",
                        "submitted_order_id": "order-restart",
                        "lighter_started": True,
                        "context": {
                            "entry_kind": "basis_v4_eth_short_p97_5",
                            "lighter_submitted_before_var_fill": True,
                            "orders_v2_last_check_monotonic": 999999999.0,
                        },
                        "execution_unknown": True,
                        "reconciliation_required": True,
                        "execution_unknown_reason": "process_restart_before_fill_confirmation",
                    }
                ],
                "open_lots": [],
            }
        ),
        encoding="utf-8",
    )

    runtime.sync_live_inventory_memory_from_state()

    assert len(runtime.pending_live_inventory_var_fill_matches) == 1
    match = runtime.pending_live_inventory_var_fill_matches[0]
    assert match.asset == "ETH"
    assert match.side == "sell"
    assert match.qty == Decimal("0.00820")
    assert match.lot_id == 2
    assert match.role == "live_inventory_entry_pending_var_fill"
    assert match.context["rfq_id"] == "rfq-restart"
    assert match.context["entry_kind"] == "basis_v4_eth_short_p97_5"
    assert match.context["lighter_submitted_before_var_fill"] is True
    assert "orders_v2_last_check_monotonic" not in match.context
    assert match.context["execution_unknown"] is True
    assert match.context["reconciliation_required"] is True
    assert match.context["restored_from_state"] is True


def test_robinhood_regime_is_independent_observation_only(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.output_dir = tmp_path
    now = datetime.now(timezone.utc)
    sample_dir = tmp_path / "robinhood_basis_samples" / "ETH"
    sample_dir.mkdir(parents=True)
    (tmp_path / "robinhood_basis_health.json").write_text(
        json.dumps(
            {
                "status": "running",
                "book_ready": True,
                "errors": {},
                "last_sample_at": now.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    (sample_dir / f"{now.date().isoformat()}.jsonl").write_text(
        json.dumps(
            {
                "logged_at": now.isoformat(),
                "short_edge_bps": "-10",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    context = runtime.live_inventory_robinhood_regime_context(
        asset="ETH",
        primary_short_edge_bps=Decimal("-4"),
    )

    assert context["v4_robinhood_fresh"] is True
    assert context["v4_robinhood_threshold_penalty_bps"] == "0"
    assert context["v4_robinhood_policy"] == "independent_observation_only"


def test_account_risk_blocks_entries_when_variational_snapshot_is_stale() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_max_venue_leverage = Decimal("5")
        runtime.live_inventory_margin_warning_pct = Decimal("40")
        runtime.live_inventory_margin_block_entry_pct = Decimal("50")
        runtime.live_inventory_margin_reduce_pct = Decimal("60")
        runtime.live_inventory_margin_emergency_pct = Decimal("75")
        runtime.live_inventory_equity_balance_warning_ratio = Decimal("0.82")
        runtime.live_inventory_equity_balance_block_ratio = Decimal("0.74")
        runtime.runtime = SimpleNamespace(
            monitor=SimpleNamespace(
                _lock=asyncio.Lock(),
                portfolio_summary={
                    "balance": "100",
                    "upnl": "0",
                    "published_at": "2026-01-01T00:00:00+00:00",
                },
            )
        )

        async def fetch_lighter_account():
            return {"accounts": [{"collateral": "100"}]}

        runtime.fetch_lighter_account = fetch_lighter_account

        context = await runtime.live_inventory_account_risk_context(
            proposed_notional_usd=Decimal("20")
        )

        assert context["risk_action"] == "block_entry"
        assert context["risk_reason"] == "variational_account_snapshot_stale"
        assert context["variational_account_snapshot_fresh"] is False
        assert context["variational_equity_usd"] is None
        assert context["variational_raw_equity_usd"] == "100"

    asyncio.run(run())


def test_account_risk_uses_recent_cached_snapshot_without_blocking_entry() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_max_venue_leverage = Decimal("5")
        runtime.live_inventory_margin_warning_pct = Decimal("40")
        runtime.live_inventory_margin_block_entry_pct = Decimal("50")
        runtime.live_inventory_margin_reduce_pct = Decimal("60")
        runtime.live_inventory_margin_emergency_pct = Decimal("75")
        runtime.live_inventory_equity_balance_warning_ratio = Decimal("0.82")
        runtime.live_inventory_equity_balance_block_ratio = Decimal("0.74")
        runtime.live_inventory_account_recovery_required = False
        runtime.live_inventory_account_recovery_confirm_count = 0
        runtime.live_inventory_account_recovery_confirm_samples = 3
        runtime.live_inventory_account_recovery_reason = None
        runtime.live_inventory_basis_entry_confirm_counts = {}
        runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
        published_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        runtime.runtime = SimpleNamespace(
            monitor=SimpleNamespace(
                _lock=asyncio.Lock(),
                portfolio_summary={
                    "balance": "100",
                    "upnl": "0",
                    "published_at": published_at.isoformat(),
                },
            )
        )

        async def fetch_variational_portfolio():
            return {"ok": False, "error": "temporary_http_503"}

        async def fetch_lighter_account():
            return {"accounts": [{"collateral": "100"}]}

        runtime.fetch_variational_portfolio = fetch_variational_portfolio
        runtime.fetch_lighter_account = fetch_lighter_account

        context = await runtime.live_inventory_account_risk_context(
            proposed_notional_usd=Decimal("20")
        )

        assert context["risk_action"] == "normal"
        assert context["variational_account_snapshot_fresh"] is False
        assert context["variational_account_snapshot_usable"] is True
        assert context["variational_account_snapshot_degraded"] is True
        assert context["variational_equity_usd"] == "100"
        assert context["account_recovery_required"] is False

    asyncio.run(run())


def test_account_risk_refreshes_stale_variational_snapshot_via_api() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_max_venue_leverage = Decimal("5")
        runtime.live_inventory_margin_warning_pct = Decimal("40")
        runtime.live_inventory_margin_block_entry_pct = Decimal("50")
        runtime.live_inventory_margin_reduce_pct = Decimal("60")
        runtime.live_inventory_margin_emergency_pct = Decimal("75")
        runtime.live_inventory_equity_balance_warning_ratio = Decimal("0.82")
        runtime.live_inventory_equity_balance_block_ratio = Decimal("0.74")
        runtime.live_inventory_account_recovery_required = False
        runtime.live_inventory_account_recovery_confirm_count = 0
        runtime.live_inventory_account_recovery_confirm_samples = 3
        runtime.live_inventory_account_recovery_reason = None
        monitor = VariationalMonitor()
        monitor.portfolio_summary = {
            "balance": "100",
            "upnl": "0",
            "published_at": "2026-01-01T00:00:00+00:00",
        }
        runtime.runtime = SimpleNamespace(monitor=monitor)

        async def fetch_variational_portfolio():
            return {
                "ok": True,
                "result": {
                    "ok": True,
                    "httpStatus": 200,
                    "portfolio": {
                        "balance": "101",
                        "upnl": "1",
                        "margin_usage": {},
                    },
                },
            }

        async def fetch_lighter_account():
            return {"accounts": [{"collateral": "102"}]}

        runtime.fetch_variational_portfolio = fetch_variational_portfolio
        runtime.fetch_lighter_account = fetch_lighter_account

        context = await runtime.live_inventory_account_risk_context(
            proposed_notional_usd=Decimal("20")
        )

        assert context["risk_action"] == "normal"
        assert context["variational_account_snapshot_fresh"] is True
        assert context["variational_equity_usd"] == "102"
        assert context["variational_portfolio_refresh_attempted"] is True
        assert context["variational_portfolio_refresh_ok"] is True
        assert context["variational_portfolio_refresh_reason"] == "api_fallback"

    asyncio.run(run())


def test_account_risk_nested_api_refresh_clears_stale_recovery_lock() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_max_venue_leverage = Decimal("5")
        runtime.live_inventory_margin_warning_pct = Decimal("40")
        runtime.live_inventory_margin_block_entry_pct = Decimal("50")
        runtime.live_inventory_margin_reduce_pct = Decimal("60")
        runtime.live_inventory_margin_emergency_pct = Decimal("75")
        runtime.live_inventory_equity_balance_warning_ratio = Decimal("0.82")
        runtime.live_inventory_equity_balance_block_ratio = Decimal("0.74")
        runtime.live_inventory_account_recovery_required = True
        runtime.live_inventory_account_recovery_confirm_count = 0
        runtime.live_inventory_account_recovery_confirm_samples = 3
        runtime.live_inventory_account_recovery_reason = (
            "variational_account_snapshot_stale"
        )
        runtime.live_inventory_basis_entry_confirm_counts = {}
        runtime.live_inventory_v4_gradient_entry_tier_window = deque(maxlen=3)
        monitor = VariationalMonitor()
        monitor.portfolio_summary = {
            "balance": "100",
            "upnl": "0",
            "published_at": "2026-01-01T00:00:00+00:00",
        }
        runtime.runtime = SimpleNamespace(monitor=monitor)

        async def fetch_variational_portfolio():
            return {
                "ok": True,
                "result": {
                    "ok": True,
                    "httpStatus": 200,
                    "portfolio": {
                        "data": {
                            "pool_portfolio_result": {
                                "balance": "101",
                                "upnl": "1",
                                "margin_usage": {},
                            }
                        }
                    },
                },
            }

        async def fetch_lighter_account():
            return {"accounts": [{"collateral": "102"}]}

        runtime.fetch_variational_portfolio = fetch_variational_portfolio
        runtime.fetch_lighter_account = fetch_lighter_account

        context = await runtime.live_inventory_account_risk_context(
            proposed_notional_usd=Decimal("20")
        )

        assert context["risk_action"] == "normal"
        assert context["variational_portfolio_refresh_ok"] is True
        assert context["variational_account_snapshot_fresh"] is True
        assert context["account_recovery_required"] is False
        assert context["account_recovery_confirm_count"] == 3

    asyncio.run(run())


def test_v4_history_filter_rejects_stale_and_duplicate_passive_pairs() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_max_lighter_book_age_seconds = 2.0
    runtime.live_inventory_basis_max_var_quote_age_ms = 1500
    base = {
        "logged_at": "2026-09-11T00:00:00+00:00",
        "quote_source": "passive_browser_stream",
        "quote_received_at": "2026-09-11T00:00:00+00:00",
        "reference_price": "2400",
        "var_quote_age_seconds": "0.2",
        "lighter_book_age_seconds": "0.1",
    }

    accepted, counts = runtime.filter_live_inventory_basis_v4_history_rows(
        [
            base,
            {**base, "logged_at": "2026-09-11T00:00:30+00:00"},
            {
                **base,
                "logged_at": "2026-09-11T00:01:00+00:00",
                "quote_received_at": "2026-09-11T00:01:00+00:00",
                "var_quote_age_seconds": "6.0",
            },
            {
                **base,
                "logged_at": "2026-09-11T00:01:30+00:00",
                "quote_received_at": "2026-09-11T00:01:30+00:00",
                "lighter_book_age_seconds": "3.0",
            },
        ]
    )

    assert accepted == [base]
    assert counts["duplicate_passive_reference"] == 1
    assert counts["var_quote_too_old"] == 1
    assert counts["lighter_book_too_old"] == 1
    assert counts["accepted"] == 1
    assert counts["rejected"] == 3


def test_v4_history_filter_accepts_only_fresh_matching_robinhood_baselines() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_max_lighter_book_age_seconds = 2.0
    runtime.live_inventory_basis_max_var_quote_age_ms = 1500
    runtime.live_inventory_lot_notional_usd = Decimal("20")
    runtime.lighter_market_index = 0
    base = {
        "event": "robinhood_lighter_basis_state",
        "sample_id": "rh-1",
        "asset": "ETH",
        "logged_at": "2026-09-20T00:00:00+00:00",
        "sample_kind": "baseline",
        "sample_quality": "valid",
        "venue": "robinhood_chain_lighter",
        "execution_mode": "collect_only",
        "basis_collect_only": True,
        "source_age_seconds": "0.2",
        "source_var_quote_age_seconds": "0.1",
        "source_sample_quality_version": 2,
        "source_sample_pair_valid": True,
        "source_quote_source": "websocket",
        "robinhood_lighter_book_age_seconds": "0.1",
        "robinhood_lighter_continuity_ok": True,
        "robinhood_lighter_sequence_gaps": 0,
        "robinhood_lighter_market_id": 0,
        "robinhood_lighter_primary_notional_usd": "20",
        "depth_ladder": [{"notional_usd": "20"}],
        "short_edge_bps": "8",
        "long_edge_bps": "-8",
    }
    legacy = {
        key: value
        for key, value in base.items()
        if key
        not in {
            "source_quote_source",
            "source_quote_received_at",
            "source_reference_price",
            "source_sample_quality_version",
            "source_sample_pair_valid",
            "source_quote_size_mode",
            "robinhood_lighter_primary_notional_usd",
        }
    }
    legacy["sample_id"] = "rh-legacy"

    accepted, counts = runtime.filter_live_inventory_basis_v4_history_rows(
        [
            base,
            legacy,
            {**base, "sample_id": "rh-stale", "source_age_seconds": "91"},
            {**base, "sample_id": "rh-wrong-market", "robinhood_lighter_market_id": 1},
            {**base, "sample_id": "rh-wrong-size", "robinhood_lighter_primary_notional_usd": "40"},
            {**base, "sample_id": "rh-gap", "robinhood_lighter_sequence_gaps": 1},
        ]
    )

    assert accepted == [base, legacy]
    assert counts["robinhood_var_source_stale"] == 1
    assert counts["robinhood_market_mismatch"] == 1
    assert counts["robinhood_depth_notional_mismatch"] == 1
    assert counts["robinhood_book_continuity_invalid"] == 1
    assert counts["rejected"] == 4


def test_manual_review_sets_runtime_level_auto_live_fuse() -> None:
    runtime = _runtime_for_fuse_test()
    position = _position()
    runtime.auto_live_position = position

    runtime.require_auto_live_manual_review(position, "exit_precheck_failed:test")

    assert runtime.auto_live_guard_reason() == "manual_review_required"
    assert runtime.auto_live_manual_review_required is True
    assert runtime.auto_live_manual_review_reason == "exit_precheck_failed:test"
    assert position.manual_review_required is True
    assert position.manual_review_reason == "exit_precheck_failed:test"


def test_manual_review_guard_takes_priority_over_max_cycles() -> None:
    runtime = _runtime_for_fuse_test()
    runtime.auto_live_completed_cycles = 1

    runtime.require_auto_live_manual_review(None, "exit_already_submitted")

    assert runtime.auto_live_guard_reason() == "manual_review_required"


def test_auto_live_precheck_failure_logging_is_throttled() -> None:
    runtime = _runtime_for_fuse_test()

    assert runtime.should_log_auto_live_precheck_failure(
        "entry",
        1,
        "BTC",
        "SELL",
        "hedge_price_deviation_exceeds_risk_limit",
        interval_seconds=10.0,
    ) is True
    assert runtime.should_log_auto_live_precheck_failure(
        "entry",
        1,
        "BTC",
        "SELL",
        "hedge_price_deviation_exceeds_risk_limit",
        interval_seconds=10.0,
    ) is False

    assert runtime.should_log_auto_live_precheck_failure(
        "entry",
        1,
        "BTC",
        "BUY",
        "hedge_price_deviation_exceeds_risk_limit",
        interval_seconds=10.0,
    ) is True


def test_auto_live_entry_actionable_edge_uses_taker_prices() -> None:
    long_edge = VariationalToLighterRuntime.auto_live_entry_actionable_edge_bps(
        "long_var_short_lighter",
        Decimal("100000"),
        Decimal("100080"),
        Decimal("100100"),
    )
    short_edge = VariationalToLighterRuntime.auto_live_entry_actionable_edge_bps(
        "short_var_long_lighter",
        Decimal("100000"),
        Decimal("99900"),
        Decimal("99920"),
    )
    bad_short_edge = VariationalToLighterRuntime.auto_live_entry_actionable_edge_bps(
        "short_var_long_lighter",
        Decimal("100000"),
        Decimal("100050"),
        Decimal("100080"),
    )

    assert f"{long_edge:.3f}" == "8.000"
    assert f"{short_edge:.3f}" == "8.000"
    assert f"{bad_short_edge:.3f}" == "-8.000"


def test_variational_api_quote_execution_price_uses_side() -> None:
    quote = {"bid": "99990", "ask": "100010"}
    nested_quote = {"result": {"bid": "99980", "ask": "100020"}}

    buy_price = VariationalToLighterRuntime.variational_api_quote_execution_price("BUY", quote)
    sell_price = VariationalToLighterRuntime.variational_api_quote_execution_price("SELL", quote)
    nested_buy_price = VariationalToLighterRuntime.variational_api_quote_execution_price("BUY", nested_quote)

    assert buy_price == Decimal("100010")
    assert sell_price == Decimal("99990")
    assert nested_buy_price == Decimal("100020")


def test_variational_api_amount_is_quantized_to_min_qty_tick() -> None:
    assert variational_api_amount_to_str(Decimal("0.0002443343566137633103278690968")) == "0.000244"
    assert variational_api_amount_to_str(Decimal("0.0000019")) == "0.000001"
    assert variational_api_amount_to_str(Decimal("0.0000009")) == "0.000000"


def test_live_inventory_common_order_qty_uses_coarser_lighter_step() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.base_amount_multiplier = 10_000

    qty = runtime.live_inventory_common_order_qty(
        asset="ETH",
        qty=Decimal("0.01075"),
    )

    assert qty == Decimal("0.0107")
    assert variational_api_amount_to_str(qty, asset="ETH") == "0.01070"


def test_live_inventory_log_serializes_nested_decimal_diagnostics(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.live_inventory_dry_decisions = False
        runtime.live_inventory_run_id = "live-test"
        runtime.orders_file = tmp_path / "order_metrics.jsonl"
        runtime._order_write_lock = asyncio.Lock()

        await runtime.append_live_inventory_log(
            "live_inventory_exit_blocked",
            {
                "asset": "ETH",
                "strong_single_context": {
                    "raw_p80_bps": Decimal("11.685"),
                    "samples": [Decimal("3.1"), Decimal("4.2")],
                },
            },
        )

        row = json.loads(runtime.orders_file.read_text(encoding="utf-8"))
        context = row["strong_single_context"]
        assert context["raw_p80_bps"] == "11.685"
        assert context["samples"] == ["3.1", "4.2"]

    asyncio.run(run())


def test_live_inventory_final_pnl_waits_for_var_and_lighter_final_fills(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.live_inventory_dry_decisions = False
        runtime.records = {}
        runtime.record_order = deque()
        runtime.pending_auto_live_matches = []
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="BTC",
                side="buy",
                qty=Decimal("0.0003"),
                lot_id=1,
                role="live_inventory_entry",
                created_at_monotonic=time.monotonic(),
            ),
            PendingLiveInventoryVarFillMatch(
                asset="BTC",
                side="sell",
                qty=Decimal("0.0003"),
                lot_id=1,
                role="live_inventory_exit",
                created_at_monotonic=time.monotonic(),
            ),
        ]
        runtime.pending_live_inventory_actual_pnl = {}
        runtime.pending_live_inventory_final_pnl = {}
        runtime.auto_live_match_window_seconds = 10.0
        runtime.trade_event_min_timestamp = None
        runtime.last_variational_trade_event_at = None
        runtime.variational_ticker = "BTC"
        runtime.accepted_assets = {"BTC"}
        runtime._record_lock = asyncio.Lock()
        runtime.logger = logging.getLogger("test_auto_live_fuse")
        runtime.lighter_client_order_to_trade_key = {}
        runtime.orders_file = tmp_path / "order_metrics.jsonl"
        runtime._order_write_lock = asyncio.Lock()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.0003",
                "entry_var_fill_price": "100",
                "entry_lighter_fill_price": "110",
                "entry_var_price_source": "estimated_snapshot",
                "entry_lighter_price_source": "estimated_snapshot",
                "entry_cost_status": "final_fills_pending",
            }
        ]
        persist_reasons: list[str] = []

        async def fake_persist_live_inventory_memory(*, reason: str) -> None:
            persist_reasons.append(reason)

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory

        runtime.remember_live_inventory_final_pnl_lot(
            asset="BTC",
            lot={
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.0003",
                "entry_var_fill_price": "100",
                "entry_lighter_fill_price": "110",
                "entry_edge_bps": "1000",
                "entry_snapshot_var_bid": "99",
                "entry_snapshot_var_ask": "101",
                "entry_snapshot_var_mid": "100",
                "entry_snapshot_var_buy_price": "100",
                "entry_snapshot_var_sell_price": "99",
                "entry_snapshot_var_full_spread_bps": "200",
                "entry_snapshot_var_spread_source": "test",
                "entry_var_order_quote_id": "entry-quote",
                "entry_var_order_quote_bid": "119",
                "entry_var_order_quote_ask": "120",
                "entry_var_order_quote_timestamp": "2026-06-15T00:00:00.050000Z",
                "entry_var_order_quote_execution_price": "120",
                "entered_at": "2026-06-15T00:00:00Z",
            },
        )
        key = runtime.live_inventory_final_pnl_key("BTC", 1)
        runtime.pending_live_inventory_final_pnl[key].update(
            {
                "exit_var_price": "111",
                "exit_estimated_var_price": "111",
                "exit_lighter_estimated_price": "112",
                "exit_var_order_quote_execution_price": "111",
                "estimated_pnl_usd": "0.003",
            }
        )

        await runtime.maybe_append_live_inventory_final_pnl_from_fill(
            {
                "asset": "BTC",
                "qty": "0.0003",
                "auto_live_cycle_id": 1,
                "auto_live_role": "live_inventory_entry",
                "lighter_filled_price": "110",
                "lighter_filled_at": "2026-06-15T00:00:00.100000Z",
                "lighter_filled_base_amount": "0.0002",
            }
        )
        await runtime.process_variational_trade_event(
            {
                "asset": "BTC",
                "side": "buy",
                "qty": "0.0003",
                "status": "filled",
                "trade_id": "entry-var",
                "timestamp": "2026-06-15T00:00:00.200000Z",
                "price": "130",
            }
        )
        assert runtime.live_inventory_open_lots[0]["entry_var_fill_price"] == "130"
        assert runtime.live_inventory_open_lots[0]["entry_lighter_fill_price"] == "110"
        assert runtime.live_inventory_open_lots[0]["entry_cost_status"] == "final_fills_confirmed"
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        assert runtime.live_inventory_open_lots[0]["entry_lighter_price_source"] == "final_fill"
        assert "entry_final_fill_cost_update" in persist_reasons
        await runtime.maybe_append_live_inventory_final_pnl_from_fill(
            {
                "asset": "BTC",
                "qty": "0.0003",
                "auto_live_cycle_id": 1,
                "auto_live_role": "live_inventory_exit",
                "lighter_filled_price": "112",
                "lighter_filled_at": "2026-06-15T00:00:10.100000Z",
                "lighter_filled_base_amount": "0.0002",
            }
        )
        await runtime.process_variational_trade_event(
            {
                "asset": "BTC",
                "side": "sell",
                "qty": "0.0003",
                "status": "filled",
                "trade_id": "exit-var",
                "timestamp": "2026-06-15T00:00:10.200000Z",
                "price": "111",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        final_rows = [row for row in rows if row["event"] == "live_inventory_final_pnl"]
        assert len(final_rows) == 1
        assert final_rows[0]["final_var_leg_pnl_usd"] == "-0.0057"
        assert final_rows[0]["final_lighter_leg_pnl_usd"] == "-0.0004"
        assert final_rows[0]["final_pnl_usd"] == "-0.0061"
        assert final_rows[0]["final_var_pnl_qty"] == "0.0003"
        assert final_rows[0]["final_lighter_pnl_qty"] == "0.0002"
        assert final_rows[0]["cross_venue_entry_qty_delta"] == "0.0001"
        assert final_rows[0]["cross_venue_exit_qty_delta"] == "0.0001"
        assert Decimal(final_rows[0]["entry_var_fill_drift_bps"]) == Decimal("3000")
        assert Decimal(final_rows[0]["exit_var_fill_drift_bps"]) == Decimal("0")
        assert Decimal(final_rows[0]["entry_estimated_edge_bps"]) == Decimal("1000")
        assert Decimal(final_rows[0]["entry_final_edge_bps"]) < Decimal("0")
        assert Decimal(final_rows[0]["entry_edge_capture_loss_bps"]) > Decimal("2500")
        assert Decimal(final_rows[0]["entry_var_final_vs_snapshot_buy_bps"]) == Decimal("3000")
        assert Decimal(final_rows[0]["entry_var_final_vs_snapshot_ask_bps"]) > Decimal("2800")
        assert final_rows[0]["entry_var_order_quote_id"] == "entry-quote"
        assert Decimal(final_rows[0]["entry_var_order_quote_vs_snapshot_buy_bps"]) == Decimal("2000")
        assert Decimal(final_rows[0]["entry_var_final_vs_order_quote_bps"]) == Decimal("833.3333333333333333333333333")
        assert Decimal(final_rows[0]["exit_var_final_vs_order_quote_bps"]) == Decimal("0")
        assert runtime.live_inventory_execution_loss_bps_samples

    asyncio.run(run())


def test_variational_api_order_quote_fields_uses_side_execution_price() -> None:
    buy_fields = VariationalToLighterRuntime.variational_api_order_quote_fields(
        "BUY",
        {
            "result": {
                "quoteId": "q1",
                "bid": "99",
                "ask": "101",
                "markPrice": "100",
                "quoteTimestamp": "2026-06-15T00:00:00Z",
            }
        },
    )
    sell_fields = VariationalToLighterRuntime.variational_api_order_quote_fields(
        "SELL",
        {"result": {"quote_id": "q2", "bid": "98", "ask": "102"}},
    )

    assert buy_fields["quote_id"] == "q1"
    assert buy_fields["quote_execution_price"] == "101"
    assert buy_fields["quote_mark_price"] == "100"
    assert sell_fields["quote_id"] == "q2"
    assert sell_fields["quote_execution_price"] == "98"


def test_extract_variational_position_qty_from_positions_result() -> None:
    result = {
        "ok": True,
        "result": {
            "positions": [
                {"instrument": {"underlying": "BTC"}, "qty": "0"},
                {"instrument": {"underlying": "ETH"}, "position_size": "0.011441"},
            ]
        },
    }

    assert VariationalToLighterRuntime.extract_variational_position_qty(result, asset="ETH") == Decimal("0.011441")
    assert VariationalToLighterRuntime.extract_variational_position_qty(result, asset="SOL") == Decimal("0")


def test_extract_lighter_position_qty_uses_position_sign() -> None:
    result = {
        "code": 200,
        "accounts": [
            {
                "positions": [
                    {"symbol": "BTC", "sign": -1, "position": "0.001"},
                    {"symbol": "ETH", "sign": 1, "position": "0.0210"},
                ]
            }
        ],
    }

    assert VariationalToLighterRuntime.extract_lighter_position_qty(result, asset="ETH") == Decimal("0.0210")
    assert VariationalToLighterRuntime.extract_lighter_position_qty(result, asset="BTC") == Decimal("-0.001")
    assert VariationalToLighterRuntime.extract_lighter_position_qty(result, asset="SOL") == Decimal("0")


def test_live_inventory_persists_pending_entry_submission(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="sell",
                qty=Decimal("0.0105"),
                lot_id=2,
                role="live_inventory_entry_pending_var_fill",
                created_at_monotonic=time.monotonic(),
                context={
                    "direction": "short_var_long_lighter",
                    "submitted_at": "2026-08-06T12:03:09Z",
                    "rfq_id": "rfq-2",
                    "lighter_started": True,
                },
            )
        ]

        await runtime.persist_live_inventory_memory(reason="basis_entry_submission_started")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["status"] == "pending"
        assert state["open_lots"] == []
        assert state["pending_actions"] == [
            {
                "asset": "ETH",
                "side": "sell",
                "qty": "0.0105",
                "lot_id": 2,
                "role": "live_inventory_entry_pending_var_fill",
                "direction": "short_var_long_lighter",
                "submitted_at": "2026-08-06T12:03:09Z",
                "rfq_id": "rfq-2",
                    "submitted_order_id": None,
                    "lighter_started": True,
                    "lighter_record_key": None,
                    "execution_unknown": False,
                    "reconciliation_required": False,
                    "execution_unknown_reason": None,
                    "context": {
                        "direction": "short_var_long_lighter",
                        "submitted_at": "2026-08-06T12:03:09Z",
                        "rfq_id": "rfq-2",
                        "lighter_started": True,
                    },
                }
            ]

    asyncio.run(run())


def test_live_inventory_startup_reconcile_rejects_hidden_exchange_position(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {
                    "positions": [
                        {"instrument": {"underlying": "ETH"}, "qty": "-0.0105"}
                    ]
                },
            }

        async def fake_fetch_lighter_account():
            return {"code": 200, "accounts": [{"positions": []}]}

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        try:
            await runtime.reconcile_live_inventory_startup_state()
        except RuntimeError as exc:
            assert "exchange position exists" in str(exc)
        else:
            raise AssertionError("startup reconcile should reject hidden exchange exposure")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "startup_reconcile_local_flat_but_exchange_position_open"
        assert state["manual_review_context"]["variational_position_qty"] == "-0.0105"
        assert runtime.stop_flag is True

    asyncio.run(run())


def test_live_inventory_startup_reconcile_requires_both_exchange_qtys_to_match(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 3,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0105",
            }
        ]

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {
                    "positions": [
                        {"instrument": {"underlying": "ETH"}, "qty": "-0.0210"}
                    ]
                },
            }

        async def fake_fetch_lighter_account():
            return {
                "code": 200,
                "accounts": [
                    {"positions": [{"symbol": "ETH", "sign": 1, "position": "0.0210"}]}
                ],
            }

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        try:
            await runtime.reconcile_live_inventory_startup_state()
        except RuntimeError as exc:
            assert "do not match local open lots" in str(exc)
        else:
            raise AssertionError("startup reconcile should reject doubled exchange exposure")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["manual_review_reason"] == "startup_reconcile_exchange_position_mismatch"
        assert state["manual_review_context"]["expected_open_qty"] == "0.0105"
        assert state["manual_review_context"]["variational_position_qty"] == "-0.0210"
        assert state["manual_review_context"]["lighter_position_qty"] == "0.0210"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_accepts_verified_flat_exchanges(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": []}}

        async def fake_fetch_lighter_account():
            return {"code": 200, "accounts": [{"positions": []}]}

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        await runtime.reconcile_live_inventory_startup_state()

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert rows[-1]["event"] == "live_inventory_startup_reconcile_ok"
        assert rows[-1]["status"] == "both_exchanges_flat"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_rejects_wrong_lighter_direction(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 4,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0105",
            }
        ]

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "-0.0105"}]},
            }

        async def fake_fetch_lighter_account():
            return {
                "code": 200,
                "accounts": [{"positions": [{"symbol": "ETH", "sign": -1, "position": "0.0105"}]}],
            }

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        try:
            await runtime.reconcile_live_inventory_startup_state()
        except RuntimeError:
            pass
        else:
            raise AssertionError("startup reconcile should reject an unhedged Lighter direction")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["manual_review_reason"] == "startup_reconcile_exchange_position_mismatch"
        assert state["manual_review_context"]["expected_lighter_sign"] == "1"
        assert state["manual_review_context"]["lighter_position_qty"] == "-0.0105"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_accepts_matching_open_pair(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 5,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0105",
            }
        ]

        async def fake_fetch_variational_positions():
            return {
                "ok": True,
                "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "-0.0105"}]},
            }

        async def fake_fetch_lighter_account():
            return {
                "code": 200,
                "accounts": [{"positions": [{"symbol": "ETH", "sign": 1, "position": "0.0105"}]}],
            }

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account

        await runtime.reconcile_live_inventory_startup_state()

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert rows[-1]["event"] == "live_inventory_startup_reconcile_ok"
        assert rows[-1]["status"] == "open_state_matches_both_exchanges"
        state = json.loads(
            runtime.live_inventory_state_file.read_text(encoding="utf-8")
        )
        assert state["status"] == "open"
        assert state["open_lots"] == runtime.live_inventory_open_lots
        assert state["pending_actions"] == []
        assert state["reason"] == "startup_open_state_reconciled"

    asyncio.run(run())


def test_live_inventory_startup_reconcile_rejects_wrong_variational_direction(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = [
            {"lot_id": 1, "asset": "ETH", "direction": "short_var_long_lighter", "qty": "0.0814"}
        ]

        async def var_positions():
            return {"ok": True, "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "0.0814"}]}}

        async def lighter_account():
            return {"accounts": [{"positions": [{"symbol": "ETH", "sign": 1, "position": "0.0814"}]}]}

        runtime.fetch_variational_positions = var_positions
        runtime.fetch_lighter_account = lighter_account
        with pytest.raises(RuntimeError, match="do not match local open lots"):
            await runtime.reconcile_live_inventory_startup_state()
        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["manual_review_reason"] == "startup_reconcile_exchange_position_mismatch"
        assert state["manual_review_context"]["expected_var_sign"] == "-1"

    asyncio.run(run())


def test_balanced_entry_manual_review_requires_no_orders_before_takeover(tmp_path) -> None:
    async def run(*, active_orders: bool) -> tuple[bool, str]:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_reconcile_on_start = True
        runtime.live_inventory_i_accept_open_state_resume = True
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = [
            {"lot_id": i, "asset": "ETH", "direction": "short_var_long_lighter", "qty": "0.0074"}
            for i in range(1, 12)
        ]
        runtime.live_inventory_state_file.write_text(
            json.dumps({
                "status": "manual_review_required",
                "asset": "ETH",
                "manual_review_reason": "basis_entry_lighter_submit_after_var_fill_failed",
                "open_lots": runtime.live_inventory_open_lots,
                "pending_actions": [],
            }),
            encoding="utf-8",
        )

        async def var_positions():
            return {"ok": True, "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "-0.0814"}]}}

        async def lighter_account():
            return {"accounts": [{"positions": [{"symbol": "ETH", "sign": 1, "position": "0.0814"}]}]}

        async def var_orders(*, asset):
            assert asset == "ETH"
            return [{"id": "pending"}] if active_orders else []

        async def lighter_orders():
            return []

        async def no_snapshot(**kwargs):
            return None

        runtime.fetch_variational_positions = var_positions
        runtime.fetch_lighter_account = lighter_account
        runtime.fetch_all_variational_pending_orders = var_orders
        runtime.fetch_lighter_active_orders = lighter_orders
        runtime.capture_live_inventory_account_snapshot = no_snapshot
        if active_orders:
            with pytest.raises(RuntimeError, match="active orders remain"):
                await runtime.reconcile_live_inventory_startup_state()
        else:
            await runtime.reconcile_live_inventory_startup_state()
        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        return active_orders, state["status"]

    assert asyncio.run(run(active_orders=True)) == (True, "manual_review_required")
    assert asyncio.run(run(active_orders=False)) == (False, "open")


def test_live_inventory_blocks_spread_reverted_exit_until_entry_cost_confirmed(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.0003",
                "entry_var_fill_price": "60000",
                "entry_lighter_fill_price": "60400",
                "entry_var_side": "BUY",
                "entry_cost_status": "final_fills_pending",
                "entered_sample_index": 1,
            }
        ]
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _inventory_entry_snapshot()
        snapshot.long_var_short_lighter_pct = Decimal("0.0001")

        await runtime.maybe_run_live_inventory(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert submit_calls == []
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "entry_final_fill_cost_pending"

    asyncio.run(run())


def test_confirmed_entry_fill_ledger_cannot_be_downgraded_to_pending(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    lot = {
        "lot_id": 1,
        "direction": "short_var_long_lighter",
        "qty": "0.0105",
        "entry_var_fill_price": "1900.25",
        "entry_lighter_fill_price": "1901.10",
        "entry_estimated_var_price": "1900.00",
        "entry_estimated_lighter_price": "1900.80",
        "entry_var_final_fill_qty": "0.0105",
        "entry_lighter_final_fill_qty": "0.0105",
        "entry_var_price_source": "final_fill",
        "entry_lighter_price_source": "final_fill",
        "entry_cost_status": "final_fills_confirmed",
    }
    runtime.live_inventory_open_lots = [lot]

    runtime.remember_live_inventory_final_pnl_lot(asset="ETH", lot=lot)
    updated = runtime.sync_live_inventory_open_lot_entry_cost(
        asset="ETH",
        lot_id=1,
    )

    pending = runtime.pending_live_inventory_final_pnl["ETH:1"]
    assert pending["entry_var_final_fill_price"] == "1900.25"
    assert pending["entry_lighter_final_fill_price"] == "1901.10"
    assert pending["entry_estimated_var_price"] == "1900.00"
    assert pending["entry_estimated_lighter_price"] == "1900.80"
    assert lot["entry_cost_status"] == "final_fills_confirmed"
    assert runtime.live_inventory_entry_cost_confirmed(lot) is True
    assert updated is False


def test_entry_cost_confirmation_accepts_consistent_final_fill_sources() -> None:
    assert VariationalToLighterRuntime.live_inventory_entry_cost_confirmed(
        {
            "entry_cost_status": "final_fills_pending",
            "entry_var_price_source": "final_fill",
            "entry_lighter_price_source": "final_fill",
            "entry_var_fill_price": "1900.25",
            "entry_lighter_fill_price": "1901.10",
        }
    ) is True


def test_reversion_signal_exit_floor_is_separate_from_normal_exit_floor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_min_signal_reverted_exit_pnl_bps = Decimal("3")
    runtime.live_inventory_basis_reversion_signal_exit_min_pnl_bps = Decimal("-1")

    runtime.live_inventory_basis_reversion_mode = False
    runtime.live_inventory_basis_reversion_min_deviation_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_exit_deviation_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_max_entry_roundtrip_cost_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_context_gap_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_long_execution_reserve_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_short_execution_reserve_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_min_net_expected_pnl_bps = Decimal("0")
    assert runtime.live_inventory_signal_reverted_exit_min_pnl_bps(
        time_decayed_min_exit_pnl_bps=Decimal("0.03")
    ) == Decimal("3")

    runtime.live_inventory_basis_reversion_mode = True
    assert runtime.live_inventory_signal_reverted_exit_min_pnl_bps(
        time_decayed_min_exit_pnl_bps=Decimal("0.03")
    ) == Decimal("-1")


def test_reversion_execution_reserve_is_directional() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_reversion_long_execution_reserve_bps = Decimal("4.5")
    runtime.live_inventory_basis_reversion_short_execution_reserve_bps = Decimal("3.5")

    assert runtime.live_inventory_basis_reversion_execution_reserve_bps(
        "long_var_short_lighter"
    ) == Decimal("4.5")
    assert runtime.live_inventory_basis_reversion_execution_reserve_bps(
        "short_var_long_lighter"
    ) == Decimal("3.5")


def _v4_rolling_anchor_rows(
    now: float,
    recent_rows: list[tuple[float, Decimal]],
    total_count: int = 5760,
) -> deque[tuple[float, Decimal]]:
    older_count = total_count - len(recent_rows)
    older_rows = [
        (
            now - 604_700 + index * 30,
            Decimal(index % 100),
        )
        for index in range(older_count)
    ]
    return deque([*older_rows, *recent_rows])


def test_v4_entry_threshold_uses_rolling_7d_anchor_with_recent_health() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold == Decimal("98.50")
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_baseline_window_seconds"] == 604800
    assert context["v4_baseline_count"] == 5760
    assert context["v4_anchor_effective_seconds"] == 172800
    assert Decimal("98") <= threshold < Decimal("99")


def test_v4_entry_threshold_adds_recent_entry_capture_loss_reserve() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_execution_loss_bps_samples = deque(
        [Decimal("0.64")] * 2 + [Decimal("3.19")] * 8,
        maxlen=20,
    )
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert context["v4_raw_entry_threshold_bps"] == "97"
    assert context["v4_entry_capture_sample_count"] == 10
    assert context["v4_entry_capture_calibration_ready"] is True
    assert context["v4_entry_capture_raw_p80_bps"] == "3.19"
    assert context["v4_entry_capture_calibration_weight"] == "0"
    assert context["v4_entry_execution_reserve_bps"] == "1.50"
    assert threshold == Decimal("98.50")
    assert context["v4_entry_threshold_bps"] == "98.50"


def test_v4_entry_threshold_uses_prior_for_immature_entry_capture_reserve() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_execution_loss_bps_samples = deque(
        [Decimal("3.19"), Decimal("0")],
        maxlen=20,
    )
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert context["v4_entry_capture_sample_count"] == 2
    assert context["v4_entry_capture_calibration_ready"] is False
    assert context["v4_entry_capture_raw_p80_bps"] == "3.19"
    assert context["v4_entry_capture_prior_bps"] == "1.50"
    assert context["v4_entry_execution_reserve_bps"] == "1.50"
    assert threshold == Decimal("98.50")


def test_v4_entry_capture_reserve_blends_prior_until_twenty_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_execution_loss_bps_samples = deque(
        [Decimal("3.00")] * 15,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_entry_calibration_context()

    assert context["ready"] is True
    assert context["fully_mature"] is False
    assert context["calibration_weight"] == Decimal("0.5")
    assert context["applied_bps"] == Decimal("2.250")


def test_v4_fast_1d_threshold_can_raise_rolling_7d_entry_gate() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (
            now - 21_600 + index * 30,
            Decimal("20") if index >= 677 else Decimal("0"),
        )
        for index in range(721)
    ]
    older_rows = [
        (
            now - 604_700 + index * 30,
            Decimal(index % 10),
        )
        for index in range(5760 - len(recent_rows))
    ]
    runtime.live_inventory_basis_v4_history = deque([*older_rows, *recent_rows])

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert context["v4_fast_ready"] is True
    assert context["v4_fast_threshold_applied"] is True
    assert Decimal(context["v4_fast_threshold_bps"]) > Decimal(
        context["v4_7d_entry_threshold_bps"]
    )
    assert threshold == Decimal(context["v4_fast_threshold_bps"]) + Decimal(
        "1.50"
    )


def test_v4_entry_threshold_cache_refreshes_on_baseline_cadence() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    first_threshold, first_context = (
        runtime.live_inventory_basis_v4_entry_threshold(now=now)
    )
    runtime.live_inventory_basis_v4_history.append(
        (now + 1, Decimal("1000"))
    )
    cached_threshold, cached_context = (
        runtime.live_inventory_basis_v4_entry_threshold(now=now + 1)
    )
    refreshed_threshold, refreshed_context = (
        runtime.live_inventory_basis_v4_entry_threshold(now=now + 31)
    )

    assert cached_threshold == first_threshold
    assert cached_context["v4_history_samples"] == first_context["v4_history_samples"]
    assert refreshed_context["v4_history_samples"] == (
        first_context["v4_history_samples"] + 1
    )
    assert refreshed_threshold is not None


def test_v4_exit_target_uses_conservative_prior_until_calibration_is_ready() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0.64"), Decimal("6.65")],
        maxlen=20,
    )
    lot: dict[str, object] = {}

    assert runtime.live_inventory_basis_v4_exit_shortfall_reserve_bps() == Decimal(
        "3.50"
    )
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "4.50"
    )
    context = runtime.live_inventory_basis_v4_exit_calibration_context()
    assert context["sample_count"] == 2
    assert context["ready"] is False
    assert context["fully_mature"] is False
    assert context["raw_p80_bps"] == Decimal("6.65")
    assert context["prior_bps"] == Decimal("3.50")
    assert context["calibration_weight"] == Decimal("0")
    assert runtime.live_inventory_basis_v4_confirm_exit_candidate(
        lot,
        eligible=True,
    ) == (False, 1)
    assert runtime.live_inventory_basis_v4_confirm_exit_candidate(
        lot,
        eligible=False,
    ) == (False, 1)
    assert runtime.live_inventory_basis_v4_confirm_exit_candidate(
        lot,
        eligible=True,
    ) == (True, 2)
    assert lot["v4_exit_confirmation_window"] == [True, False, True]


def test_v4_exit_target_uses_observed_shortfall_after_ten_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0.10")] * 10 + [Decimal("6.65")] * 5,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["sample_count"] == 15
    assert context["ready"] is True
    assert context["fully_mature"] is False
    assert context["raw_p80_bps"] == Decimal("6.65")
    assert context["calibration_weight"] == Decimal("1")
    assert context["stage_floor_bps"] == Decimal("1.50")
    assert context["applied_dynamic_bps"] == Decimal("3.00")
    assert runtime.live_inventory_basis_v4_exit_shortfall_reserve_bps() == Decimal(
        "3.00"
    )
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "4.00"
    )


def test_v4_exit_target_uses_early_ready_floor_for_low_shortfall() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0")] * 10,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["ready"] is True
    assert context["fully_mature"] is False
    assert context["stage_floor_bps"] == Decimal("1.50")
    assert context["applied_dynamic_bps"] == Decimal("1.50")
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "2.50"
    )


def test_v4_exit_target_caps_observed_shortfall_after_twenty_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0.10")] * 14 + [Decimal("6.65")] * 6,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["sample_count"] == 20
    assert context["ready"] is True
    assert context["fully_mature"] is True
    assert context["raw_p80_bps"] == Decimal("6.65")
    assert context["calibration_weight"] == Decimal("1")
    assert context["stage_floor_bps"] == Decimal("0.50")
    assert context["applied_dynamic_bps"] == Decimal("3.00")
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "4.00"
    )


def test_v4_exit_target_uses_mature_floor_for_low_shortfall() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_dynamic_exit_buffer = True
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
        [Decimal("0")] * 20,
        maxlen=20,
    )

    context = runtime.live_inventory_basis_v4_exit_calibration_context()

    assert context["fully_mature"] is True
    assert context["stage_floor_bps"] == Decimal("0.50")
    assert context["applied_dynamic_bps"] == Decimal("0")
    assert runtime.live_inventory_basis_v4_effective_exit_target_bps() == Decimal(
        "1.50"
    )


def test_v4_execution_reserve_loaders_ignore_other_strategies_and_assets(
    tmp_path,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.orders_file = Path(tmp_path) / "order_metrics.jsonl"
    runtime.logger = logging.getLogger("test_v4_execution_reserve_loader")
    runtime.live_inventory_execution_loss_bps_samples = deque(maxlen=20)
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(maxlen=20)
    runtime.live_inventory_strong_single_shortfall_bps_samples = deque(maxlen=20)
    rows = [
        {
            "event": "live_inventory_final_pnl",
            "strategy_version": "basis-v4-live-v3",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "entry_edge_capture_loss_bps": "-0.25",
        },
        {
            "event": "live_inventory_final_pnl",
            "strategy_version": "basis-v4-live-test-v1",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "entry_edge_capture_loss_bps": "3.19",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-test-v1",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "5.19",
            "actual_pnl_bps": "-1.46",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-test-v2",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "1.08",
            "actual_pnl_bps": "1.08",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-v3",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "2.50",
            "actual_pnl_bps": "0.50",
            "exit_confirmation_mode": "strong_single",
        },
        {
            "event": "live_inventory_final_pnl",
            "strategy_version": "execution-calibration-v1",
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "final_pnl_status": "var_and_lighter_final_fills_confirmed",
            "entry_edge_capture_loss_bps": "99",
        },
        {
            "event": "live_inventory_actual_pnl",
            "strategy_version": "basis-v4-live-v1",
            "asset": "BTC",
            "direction": "short_var_long_lighter",
            "actual_pnl_status": "lighter_final_fill_confirmed",
            "estimated_pnl_bps": "99",
            "actual_pnl_bps": "0",
        },
    ]
    runtime.orders_file.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    runtime.load_recent_live_inventory_execution_loss_bps()
    runtime.load_recent_live_inventory_exit_shortfall_bps()

    assert list(runtime.live_inventory_execution_loss_bps_samples) == [
        Decimal("0"),
        Decimal("3.19"),
    ]
    assert list(runtime.live_inventory_exit_estimate_shortfall_bps_samples) == [
        Decimal("0"),
        Decimal("2.00"),
    ]
    assert list(runtime.live_inventory_strong_single_shortfall_bps_samples) == [
        Decimal("2.00")
    ]


def test_v4_entry_threshold_accepts_36h_effective_7d_anchor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(100)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
        total_count=4320,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is not None
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_anchor_effective_seconds"] == 129600
    assert context["v4_anchor_min_effective_seconds"] == 129600
    assert context["v4_anchor_missing_effective_seconds"] == 0
    assert context["v4_anchor_progress_pct"] == "100.00"
    assert context["v4_anchor_projected_ready_seconds"] == 0


def test_v4_entry_threshold_keeps_7d_anchor_across_historical_gap() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    recent_rows = [
        (now - 21_570 + index * 30, Decimal(index))
        for index in range(720)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        recent_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is not None
    assert context["v4_mature_windows"] == [604800]
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_baseline_window_seconds"] == 604800
    assert Decimal(context["v4_anchor_max_sample_gap_seconds"]) > Decimal("60")
    assert context["v4_health_max_sample_gap_seconds"] == "30.000"
    assert threshold < Decimal("9999")


def test_v4_entry_threshold_rejects_recent_health_gap_without_dropping_anchor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    now = 1_000_000.0
    health_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(50)
    ] + [
        (now - 1400 + index * 30, Decimal(index + 50))
        for index in range(50)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        health_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(now=now)

    assert threshold is None
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is False
    assert Decimal(context["v4_health_max_sample_gap_seconds"]) > Decimal("60")
    assert len(runtime.live_inventory_basis_v4_history) == 5760


def test_v4_test_mode_can_bypass_recent_health_without_dropping_anchor() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_test_skip_recent_health = True
    now = 1_000_000.0
    health_rows = [
        (now - 3000 + index * 30, Decimal(index))
        for index in range(50)
    ] + [
        (now - 1400 + index * 30, Decimal(index + 50))
        for index in range(50)
    ]
    runtime.live_inventory_basis_v4_history = _v4_rolling_anchor_rows(
        now,
        health_rows,
    )

    threshold, context = runtime.live_inventory_basis_v4_entry_threshold(
        now=now
    )

    assert threshold is not None
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready_observed"] is False
    assert context["v4_health_gate_bypassed"] is True
    assert context["v4_health_ready"] is True


def test_v4_history_gap_preserves_rolling_anchor_before_recording() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_history = deque(
        [(100.0, Decimal("4.2"))]
    )
    runtime.live_inventory_basis_v4_next_history_sample_at = 130.0
    runtime.live_inventory_basis_v4_history_ready = True
    runtime.live_inventory_basis_v4_history_reason = "ready"

    recorded = runtime.record_live_inventory_basis_v4_edge(
        now=200.0,
        short_edge_bps=Decimal("5.1"),
    )

    assert list(runtime.live_inventory_basis_v4_history) == [
        (100.0, Decimal("4.2")),
        (200.0, Decimal("5.1"))
    ]
    assert recorded is True
    assert runtime.live_inventory_basis_v4_history_ready is True
    assert runtime.live_inventory_basis_v4_history_reason == "ready"


def test_v4_reverse_test_selects_long_direction_and_conservative_reserves() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_reverse_test = True
    runtime.live_inventory_basis_v4_entry_calibration_context = lambda: {
        "applied_bps": Decimal("0.25")
    }
    runtime.live_inventory_basis_v4_exit_calibration_context = lambda: {
        "reserve_bps": Decimal("0.50")
    }

    assert (
        runtime.live_inventory_basis_v4_entry_direction()
        == "long_var_short_lighter"
    )
    assert (
        runtime.live_inventory_basis_v4_entry_execution_reserve_bps()
        == Decimal("1.50")
    )
    assert (
        runtime.live_inventory_basis_v4_exit_shortfall_reserve_bps()
        == Decimal("3.50")
    )


def test_v4_bidirectional_selects_best_flat_direction_and_locks_open_episode() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_reverse_test = False
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_open_lots = []
    edges = {
        "long_var_short_lighter": Decimal("4.0"),
        "short_var_long_lighter": Decimal("3.0"),
    }
    thresholds = {
        "long_var_short_lighter": Decimal("1.5"),
        "short_var_long_lighter": Decimal("1.0"),
    }

    assert runtime.live_inventory_basis_v4_select_entry_direction(
        signal_edges=edges,
        thresholds=thresholds,
    ) == "long_var_short_lighter"

    runtime.live_inventory_open_lots = [
        {"direction": "short_var_long_lighter"}
    ]
    assert runtime.live_inventory_basis_v4_select_entry_direction(
        signal_edges=edges,
        thresholds=thresholds,
    ) == "short_var_long_lighter"


def test_v4_bidirectional_uses_conservative_long_execution_reserves() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_reverse_test = False
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_basis_v4_entry_calibration_context = lambda: {
        "applied_bps": Decimal("0.25")
    }
    runtime.live_inventory_basis_v4_exit_calibration_context = lambda: {
        "sample_count": 20,
        "min_samples": 10,
        "full_samples": 20,
        "ready": True,
        "fully_mature": True,
        "raw_p80_bps": Decimal("0.25"),
        "prior_bps": Decimal("3.50"),
        "calibration_weight": Decimal("1"),
        "stage_floor_bps": Decimal("0.50"),
        "cap_bps": Decimal("3.00"),
        "applied_dynamic_bps": Decimal("0.25"),
        "reserve_bps": Decimal("0.50"),
    }

    assert runtime.live_inventory_basis_v4_entry_execution_reserve_bps(
        direction="long_var_short_lighter"
    ) == Decimal("1.50")
    assert runtime.live_inventory_basis_v4_entry_execution_reserve_bps(
        direction="short_var_long_lighter"
    ) == Decimal("0.25")
    assert runtime.live_inventory_basis_v4_exit_calibration_payload(
        direction="long_var_short_lighter"
    )["v4_exit_shortfall_reserve_bps"] == "3.50"


def test_v4_bidirectional_gradient_confirmation_state_is_isolated() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_v4_gradient_entry_tier_windows_by_direction = {
        "long_var_short_lighter": deque(maxlen=3),
        "short_var_long_lighter": deque(maxlen=3),
    }
    runtime.live_inventory_v4_gradient_tier_states_by_direction = {
        direction: {
            tier: {"armed": True, "reset_seen": False}
            for tier in range(1, 6)
        }
        for direction in (
            "long_var_short_lighter",
            "short_var_long_lighter",
        )
    }
    thresholds = [Decimal(str(value)) for value in range(1, 6)]

    runtime.live_inventory_basis_v4_active_gradient_tier(
        raw_tier=3,
        edge_bps=Decimal("3.5"),
        thresholds_bps=thresholds,
        direction="long_var_short_lighter",
    )

    assert list(
        runtime.live_inventory_v4_gradient_entry_tier_windows_by_direction[
            "long_var_short_lighter"
        ]
    ) == [3]
    assert list(
        runtime.live_inventory_v4_gradient_entry_tier_windows_by_direction[
            "short_var_long_lighter"
        ]
    ) == []


def test_v4_bidirectional_records_each_direction_history_independently() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_basis_v4_reverse_test = False
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_histories = {
        "long_var_short_lighter": deque(),
        "short_var_long_lighter": deque(),
    }
    runtime.live_inventory_basis_v4_next_history_sample_at_by_direction = {
        "long_var_short_lighter": 0.0,
        "short_var_long_lighter": 0.0,
    }

    assert runtime.record_live_inventory_basis_v4_edge(
        now=100.0,
        direction="long_var_short_lighter",
        edge_bps=Decimal("2.5"),
    ) is True
    assert runtime.record_live_inventory_basis_v4_edge(
        now=100.0,
        direction="short_var_long_lighter",
        edge_bps=Decimal("1.5"),
    ) is True

    assert list(runtime.live_inventory_basis_v4_histories["long_var_short_lighter"]) == [
        (100.0, Decimal("2.5"))
    ]
    assert list(runtime.live_inventory_basis_v4_histories["short_var_long_lighter"]) == [
        (100.0, Decimal("1.5"))
    ]


def test_v4_bidirectional_thresholds_use_separate_direction_histories() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_bidirectional = True
    runtime.live_inventory_basis_v4_reverse_test = False
    runtime.live_inventory_basis_v4_test_skip_recent_health = False
    runtime.live_inventory_basis_v4_histories = {}
    runtime.live_inventory_basis_v4_threshold_cached_at_by_direction = {}
    runtime.live_inventory_basis_v4_threshold_cache_by_direction = {}
    runtime.live_inventory_basis_v4_shadow_cached_at_by_direction = {}
    runtime.live_inventory_basis_v4_shadow_cache_by_direction = {}
    runtime.live_inventory_basis_v4_projection_cached_at_by_direction = {}
    runtime.live_inventory_basis_v4_projection_cache_by_direction = {}
    runtime.live_inventory_execution_loss_bps_samples_by_direction = {
        "long_var_short_lighter": deque(maxlen=20),
        "short_var_long_lighter": deque(maxlen=20),
    }
    runtime.live_inventory_basis_sample_move_bps_samples = deque(maxlen=200)
    runtime.live_inventory_latest_basis_size_ladder = []
    now = 1_000_000.0
    short_rows = _v4_rolling_anchor_rows(
        now,
        [
            (now - 3000 + index * 30, Decimal(index))
            for index in range(100)
        ],
    )
    long_rows = deque(
        (timestamp, edge + Decimal("10"))
        for timestamp, edge in short_rows
    )
    runtime.live_inventory_basis_v4_histories = {
        "long_var_short_lighter": long_rows,
        "short_var_long_lighter": short_rows,
    }

    long_threshold, long_context = runtime.live_inventory_basis_v4_entry_threshold(
        now=now,
        direction="long_var_short_lighter",
    )
    short_threshold, short_context = runtime.live_inventory_basis_v4_entry_threshold(
        now=now,
        direction="short_var_long_lighter",
    )

    assert long_threshold is not None
    assert short_threshold is not None
    assert long_threshold - short_threshold == Decimal("10")
    assert long_context["v4_history_direction"] == "long_var_short_lighter"
    assert short_context["v4_history_direction"] == "short_var_long_lighter"


def test_v4_history_loader_requires_7d_anchor_and_recent_health(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    sample_rows = _v4_rolling_anchor_rows(
        now,
        [
            (now - 3001 + index * 30, Decimal(index))
            for index in range(101)
        ],
    )
    rows = [
        {
            "asset": "ETH",
            "logged_at": datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).isoformat(),
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "quote_size_mode": "exact_base_qty_v1",
            "short_edge_bps": str(edge_bps),
        }
        for timestamp, edge_bps in sample_rows
    ]
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["ready"] is True
    assert context["reason"] == "ready"
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready"] is True
    assert context["v4_baseline_window_seconds"] == 604800
    assert context["v4_anchor_effective_seconds"] == 172800
    assert context["v4_entry_threshold_bps"] == "98.50"
    assert context["compatible_source_rows"] == 5760
    assert context["incompatible_quote_size_rows"] == 0
    assert context["quote_size_mode"] == "exact_base_qty_v1"


def test_v4_history_loader_uses_external_robinhood_anchor_but_not_health(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = tmp_path / "live_project"
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_bidirectional = False
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    runtime.live_inventory_max_lighter_book_age_seconds = 2.0
    runtime.live_inventory_basis_max_var_quote_age_ms = 1500
    runtime.live_inventory_lot_notional_usd = Decimal("20")
    runtime.lighter_market_index = 0
    external_root = tmp_path / "old_robinhood_collector" / "robinhood_basis_samples"
    monkeypatch.setenv(
        "LIVE_INVENTORY_BASIS_V4_ROBINHOOD_HISTORY_DIR",
        str(external_root),
    )
    asset_dir = external_root / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    rows = []
    for index in range(5760):
        timestamp = now - 172_770 + index * 30
        rows.append(
            {
                "event": "robinhood_lighter_basis_state",
                "sample_id": f"rh-{index}",
                "asset": "ETH",
                "logged_at": datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).isoformat(),
                "sample_kind": "baseline",
                "sample_quality": "valid",
                "venue": "robinhood_chain_lighter",
                "execution_mode": "collect_only",
                "basis_collect_only": True,
                "source_age_seconds": "0.2",
                "source_var_quote_age_seconds": "0.1",
                "source_sample_quality_version": 2,
                "source_sample_pair_valid": True,
                "source_quote_source": "websocket",
                "robinhood_lighter_book_age_seconds": "0.1",
                "robinhood_lighter_continuity_ok": True,
                "robinhood_lighter_sequence_gaps": 0,
                "robinhood_lighter_market_id": 0,
                "robinhood_lighter_primary_notional_usd": "20",
                "depth_ladder": [{"notional_usd": "20"}],
                "short_edge_bps": str(index % 100),
                "long_edge_bps": str(-(index % 100)),
            }
        )
    (asset_dir / "2026-09-25.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["robinhood_anchor_source_rows"] == 5760
    assert context["robinhood_anchor_accepted_rows"] == 5760
    assert context["robinhood_anchor_rejected_rows"] == 0
    assert context["v4_anchor_ready"] is True
    assert context["v4_health_ready_observed"] is False
    assert context["v4_health_ready"] is False
    assert context["ready"] is False
    assert context["reason"] == "recent_1h_health_not_ready"


def test_v4_history_loader_joins_samples_across_long_gap(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    # The current health window is intentionally unavailable. This test
    # isolates rolling-anchor stitching from the separate health gate.
    runtime.live_inventory_basis_v4_test_skip_recent_health = True

    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    pre_gap = [
        (now - 418_000 + index * 30, Decimal(index % 10))
        for index in range(1_000)
    ]
    post_gap_start = now - 154_000
    post_gap_end = now - 120
    post_gap_step = (post_gap_end - post_gap_start) / (4_495 - 1)
    post_gap = [
        (
            post_gap_start + index * post_gap_step,
            Decimal((index + 1_000) % 10),
        )
        for index in range(4_495)
    ]
    rows = [
        {
            "asset": "ETH",
            "logged_at": datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).isoformat(),
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "quote_size_mode": "exact_base_qty_v1",
            "short_edge_bps": str(edge_bps),
        }
        for timestamp, edge_bps in [*pre_gap, *post_gap]
    ]
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")
    direction_context = context["directions"]["short_var_long_lighter"]

    assert context["ready"] is True
    assert context["reason"] == "ready"
    assert context["v4_anchor_ready"] is True
    assert Decimal(context["v4_anchor_max_sample_gap_seconds"]) > Decimal("200000")
    assert Decimal(context["v4_anchor_coverage_seconds"]) < Decimal("483840")
    assert context["v4_anchor_valid_coverage_seconds"] == 164850
    assert context["v4_anchor_coverage_mode"] == "gap_excluded_valid_samples"
    assert direction_context["v4_history_latest_sample_fresh"] is False
    assert Decimal(
        direction_context["v4_history_latest_sample_age_seconds"]
    ) > Decimal("60")


def test_v4_history_loader_uses_legacy_quote_size_rows_for_anchor(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    rows = []
    sample_rows = _v4_rolling_anchor_rows(
        now,
        [
            (now - 3001 + index * 30, Decimal(index))
            for index in range(101)
        ],
    )
    for index in range(10):
        rows.append(
            {
                "asset": "ETH",
                "logged_at": datetime.fromtimestamp(
                    now - 5000 + index * 30, tz=timezone.utc
                ).isoformat(),
                "sample_kind": "baseline",
                "sample_quality": "valid",
                "short_edge_bps": str(index),
            }
        )
    for timestamp, edge_bps in sample_rows:
        rows.append(
            {
                "asset": "ETH",
                "logged_at": datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).isoformat(),
                "sample_kind": "baseline",
                "sample_quality": "valid",
                "quote_size_mode": "exact_base_qty_v1",
                "short_edge_bps": str(edge_bps),
            }
        )
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["source_rows"] == 5770
    assert context["compatible_source_rows"] == 5760
    assert context["incompatible_quote_size_rows"] == 10
    assert context["directions"]["short_var_long_lighter"][
        "incompatible_quote_size_rows"
    ] == 10
    assert context["directions"]["short_var_long_lighter"][
        "anchor_legacy_samples"
    ] == 10
    assert context["directions"]["short_var_long_lighter"][
        "anchor_compatible_samples"
    ] == 5760
    assert context["directions"]["short_var_long_lighter"][
        "v4_anchor_source_mode"
    ] == "mixed_legacy_and_current"
    assert context["ready"] is True


def test_v4_history_loader_reports_quote_size_warmup_without_compatible_rows(
    tmp_path,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    row = {
        "asset": "ETH",
        "logged_at": datetime.fromtimestamp(
            now - 30, tz=timezone.utc
        ).isoformat(),
        "sample_kind": "baseline",
        "sample_quality": "valid",
        "short_edge_bps": "10",
    }
    (asset_dir / "2026-07-24.jsonl").write_text(
        json.dumps(row) + "\n",
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["ready"] is False
    assert context["reason"] == "insufficient_rolling_7d_anchor"
    assert context["compatible_source_rows"] == 0
    assert context["incompatible_quote_size_rows"] == 1


def test_v4_history_loader_does_not_use_legacy_health_during_size_migration(
    tmp_path,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    rows = [
        {
            "asset": "ETH",
            "logged_at": datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).isoformat(),
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "short_edge_bps": str(edge_bps),
        }
        for timestamp, edge_bps in _v4_rolling_anchor_rows(
            now,
            [
                (now - 3001 + index * 30, Decimal(index))
                for index in range(101)
            ],
        )
    ]
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["ready"] is False
    assert context["reason"] == "recent_1h_health_not_ready"
    assert context["compatible_source_rows"] == 0
    assert context["incompatible_quote_size_rows"] == 5760
    assert context["directions"]["short_var_long_lighter"]["ready"] is False
    assert context["directions"]["short_var_long_lighter"][
        "reason"
    ] == "recent_1h_health_not_ready"
    assert context["directions"]["short_var_long_lighter"][
        "v4_health_ready"
    ] is False
    assert context["directions"]["short_var_long_lighter"][
        "v4_anchor_ready"
    ] is True
    assert context["directions"]["short_var_long_lighter"][
        "v4_anchor_source_mode"
    ] == "legacy_compatibility"
    assert context["directions"]["short_var_long_lighter"][
        "v4_anchor_legacy_samples"
    ] == 5760
    assert context["directions"]["short_var_long_lighter"][
        "v4_health_count"
    ] == 0

    runtime.live_inventory_basis_v4_test_skip_recent_health = True
    threshold, bypassed_context = runtime.live_inventory_basis_v4_entry_threshold(
        now=now,
        direction="short_var_long_lighter",
        health_history_override=runtime.live_inventory_basis_v4_health_histories[
            "short_var_long_lighter"
        ],
        cache_result=False,
    )
    assert threshold is not None
    assert bypassed_context["v4_anchor_ready"] is True
    assert bypassed_context["v4_health_ready"] is True


def test_extension_disconnect_fuse_stops_flat_runtime_after_three_failures() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 0
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_extension_disconnect_fuse")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        for _ in range(3):
            await runtime.record_live_inventory_basis_quote_failure(
                asset="ETH",
                error="No extension command client connected.",
                failure_kind="command_rejected",
            )

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "variational_extension_disconnected"
        assert runtime.live_inventory_extension_disconnect_failures == 3
        assert [event for event, _ in events].count(
            "live_inventory_runtime_fuse_triggered"
        ) == 1
        failures = [
            payload
            for event, payload in events
            if event == "live_inventory_basis_quote_failed"
        ]
        assert failures[-1]["extension_consecutive_failures"] == 3

    asyncio.run(run())


def test_non_extension_quote_failure_resets_disconnect_counter() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 2
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_extension_disconnect_reset")

        async def ignore(_event: str, _payload: dict) -> None:
            return None

        runtime.append_live_inventory_log = ignore
        await runtime.record_live_inventory_basis_quote_failure(
            asset="ETH",
            error="HTTP 503",
            failure_kind="command_rejected",
        )

        assert runtime.live_inventory_extension_disconnect_failures == 0
        assert runtime.stop_flag is False

    asyncio.run(run())


def test_html_quote_failure_is_normalized_and_fuses_flat_runtime() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(
            VariationalToLighterRuntime
        )
        runtime.live_inventory_extension_disconnect_failures = 0
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_last_fatal_quote_failure_kind = None
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_html_quote_fuse")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        html_error = (
            "<!doctype html><html><style>"
            + ("x" * 10_000)
            + "</style></html>"
        )
        for _ in range(3):
            await runtime.record_live_inventory_basis_quote_failure(
                asset="ETH",
                error=html_error,
                failure_kind="command_rejected",
            )

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "variational_html_response"
        failures = [
            payload
            for event, payload in events
            if event == "live_inventory_basis_quote_failed"
        ]
        assert failures[-1]["error"] == "variational_html_response"
        assert failures[-1]["error_original_chars"] == len(html_error)
        assert failures[-1]["html_response"] is True
        assert [event for event, _ in events].count(
            "live_inventory_runtime_fuse_triggered"
        ) == 1

    asyncio.run(run())


def test_extension_disconnect_fuse_requires_review_when_position_is_open() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 2
        runtime.live_inventory_extension_failure_started_monotonic = (
            time.monotonic() - 61
        )
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = [{"lot_id": 1, "asset": "ETH"}]
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_open_extension_disconnect_fuse")
        events: list[tuple[str, dict]] = []
        reviews: list[dict] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        async def require_review(**kwargs) -> None:
            reviews.append(kwargs)
            runtime.stop_flag = True

        runtime.append_live_inventory_log = capture
        runtime.require_live_inventory_manual_review = require_review

        await runtime.record_live_inventory_basis_quote_failure(
            asset="ETH",
            error="No extension command client connected.",
            failure_kind="command_rejected",
        )

        assert runtime.stop_flag is True
        assert runtime.shutdown_reason == "variational_extension_disconnected"
        assert reviews[0]["reason"] == "variational_extension_disconnected"
        fuse = next(
            payload
            for event, payload in events
            if event == "live_inventory_runtime_fuse_triggered"
        )
        assert fuse["action"] == "manual_exchange_review_required"
        assert fuse["open_lots_total"] == 1

    asyncio.run(run())


def test_extension_disconnect_fuse_grants_open_position_recovery_window() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_inventory_extension_disconnect_failures = 2
        runtime.live_inventory_extension_failure_started_monotonic = time.monotonic()
        runtime.live_inventory_extension_disconnect_fuse_triggered = False
        runtime.live_inventory_open_lots = [{"lot_id": 1, "asset": "ETH"}]
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_open_extension_disconnect_grace")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        await runtime.record_live_inventory_basis_quote_failure(
            asset="ETH",
            error="No extension command client connected.",
            failure_kind="command_rejected",
        )

        assert runtime.stop_flag is False
        assert runtime.shutdown_reason is None
        assert not any(
            event == "live_inventory_runtime_fuse_triggered"
            for event, _ in events
        )

    asyncio.run(run())


def test_runtime_disk_guard_stops_flat_live_below_three_gb(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.output_dir = tmp_path
        runtime.live_inventory_last_disk_check_monotonic = 0.0
        runtime.live_inventory_last_disk_warning_monotonic = 0.0
        runtime.live_inventory_open_lots = []
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_runtime_disk_guard")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        monkeypatch.setattr(
            "main.shutil.disk_usage",
            lambda _path: SimpleNamespace(free=2 * 1024**3),
        )

        await runtime.maybe_enforce_live_disk_guard(asset="ETH")

        assert runtime.stop_flag is True
        assert runtime.live_inventory_disk_entry_blocked is True
        assert runtime.shutdown_reason == "disk_free_below_stop_threshold"
        fuse = next(
            payload
            for event, payload in events
            if event == "live_inventory_runtime_fuse_triggered"
        )
        assert fuse["action"] == "auto_stop_flat"

    asyncio.run(run())


def test_runtime_disk_guard_keeps_open_position_managed_but_blocks_entries(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.output_dir = tmp_path
        runtime.live_inventory_last_disk_check_monotonic = 0.0
        runtime.live_inventory_last_disk_warning_monotonic = 0.0
        runtime.live_inventory_open_lots = [{"lot_id": 1}]
        runtime.stop_flag = False
        runtime.shutdown_reason = None
        runtime.logger = logging.getLogger("test_runtime_disk_guard_open")
        events: list[tuple[str, dict]] = []

        async def capture(event: str, payload: dict) -> None:
            events.append((event, payload))

        runtime.append_live_inventory_log = capture
        monkeypatch.setattr(
            "main.shutil.disk_usage",
            lambda _path: SimpleNamespace(free=2 * 1024**3),
        )

        await runtime.maybe_enforce_live_disk_guard(asset="ETH")

        assert runtime.live_inventory_disk_entry_blocked is True
        assert runtime.stop_flag is False
        warning = next(
            payload
            for event, payload in events
            if event == "live_inventory_disk_free_warning"
        )
        assert warning["action"] == "block_new_entries_manage_existing_positions"

    asyncio.run(run())


def test_v4_history_loader_does_not_authorize_recent_1h_without_7d_anchor(
    tmp_path,
) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.output_dir = Path(tmp_path)
    runtime.live_inventory_basis_v4_profile = (
        "eth_short_execution_calibrated_20260724_n10"
    )
    runtime.live_inventory_basis_v4_history = deque()
    runtime.live_inventory_basis_v4_next_history_sample_at = 0.0
    runtime.live_inventory_basis_v4_history_ready = False
    runtime.live_inventory_basis_v4_history_reason = "not_loaded"
    asset_dir = Path(tmp_path) / "basis_samples" / "ETH"
    asset_dir.mkdir(parents=True)
    now = datetime.now(timezone.utc).timestamp()
    rows = [
        {
            "asset": "ETH",
            "logged_at": datetime.fromtimestamp(
                now - 3001 + index * 30, tz=timezone.utc
            ).isoformat(),
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "quote_size_mode": "exact_base_qty_v1",
            "short_edge_bps": str(index),
        }
        for index in range(101)
    ]
    (asset_dir / "2026-07-24.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )

    context = runtime.load_live_inventory_basis_v4_history(asset="ETH")

    assert context["ready"] is False
    assert context["v4_anchor_ready"] is False
    assert context["v4_health_ready"] is True
    assert "v4_entry_threshold_bps" not in context


def test_non_filled_event_does_not_consume_pending_match_or_double_hedge(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime.records["auto:BTC:buy:123"] = OrderLifecycle(
            trade_key="auto:BTC:buy:123",
            trade_id="auto:BTC:buy:123",
            side="buy",
            qty=Decimal("0.00022"),
            asset="BTC",
            mode="live",
            last_variational_status="submitted",
            synthetic_eager_fill=True,
            auto_live_cycle_id=1,
            auto_live_role="entry",
            auto_live_merge_path="synthetic_created",
        )
        runtime.record_order.append("auto:BTC:buy:123")
        runtime.pending_auto_live_matches = [
            PendingAutoLiveMatch(
                record_key="auto:BTC:buy:123",
                asset="BTC",
                side="buy",
                qty=Decimal("0.00022"),
                cycle_id=1,
                role="entry",
                created_at_monotonic=asyncio.get_running_loop().time(),
            )
        ]
        runtime.auto_live_match_window_seconds = 10.0
        runtime.trade_event_min_timestamp = None
        runtime.last_variational_trade_event_at = None
        runtime.variational_ticker = "BTC"
        runtime.accepted_assets = {"BTC"}
        runtime._record_lock = asyncio.Lock()
        runtime.logger = logging.getLogger("test_auto_live_fuse")
        runtime.lighter_client_order_to_trade_key = {}
        runtime.output_dir = Path(tmp_path)

        hedge_calls: list[str] = []
        append_calls: list[str] = []

        async def fake_place_lighter_order(record) -> None:
            hedge_calls.append(record.trade_key)

        async def fake_append_order_log(event_type, payload) -> None:
            append_calls.append(event_type)

        runtime.place_lighter_order = fake_place_lighter_order
        runtime.append_order_log = fake_append_order_log

        submitted_event = {
            "asset": "BTC",
            "side": "buy",
            "qty": "0.00022",
            "status": "submitted",
            "trade_id": "trade-1",
            "timestamp": "2026-06-02T08:50:10Z",
            "price": "100000",
        }
        filled_event = {
            "asset": "BTC",
            "side": "buy",
            "qty": "0.00022",
            "status": "filled",
            "trade_id": "trade-1",
            "timestamp": "2026-06-02T08:50:11Z",
            "price": "100001",
        }

        await runtime.process_variational_trade_event(submitted_event)

        assert len(runtime.pending_auto_live_matches) == 1
        assert hedge_calls == []

        await runtime.process_variational_trade_event(filled_event)

        assert len(runtime.pending_auto_live_matches) == 0
        assert hedge_calls == []
        assert append_calls == ["variational_fill"]
        assert "id:trade-1" in runtime.records
        assert runtime.records["auto:BTC:buy:123"].auto_live_merge_path == "synthetic_matched_real_var_fill"
        assert runtime.records["auto:BTC:buy:123"].matched_variational_trade_id == "trade-1"

    asyncio.run(run())


def test_live_inventory_blocks_trade_event_auto_hedge(tmp_path) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.live_inventory = True
        runtime.records = {}
        runtime.record_order = deque()
        runtime.pending_auto_live_matches = []
        runtime.auto_live_match_window_seconds = 10.0
        runtime.trade_event_min_timestamp = None
        runtime.last_variational_trade_event_at = None
        runtime.variational_ticker = "BTC"
        runtime.accepted_assets = {"BTC"}
        runtime._record_lock = asyncio.Lock()
        runtime.logger = logging.getLogger("test_auto_live_fuse")
        runtime.lighter_client_order_to_trade_key = {}
        runtime.output_dir = Path(tmp_path)

        hedge_calls: list[str] = []
        append_calls: list[str] = []

        async def fake_place_lighter_order(record) -> None:
            hedge_calls.append(record.trade_key)

        async def fake_append_order_log(event_type, payload) -> None:
            append_calls.append(event_type)

        runtime.place_lighter_order = fake_place_lighter_order
        runtime.append_order_log = fake_append_order_log

        await runtime.process_variational_trade_event(
            {
                "asset": "BTC",
                "side": "buy",
                "qty": "0.00022",
                "status": "filled",
                "trade_id": "trade-live-inventory",
                "timestamp": "2026-06-02T08:50:11Z",
                "price": "100001",
            }
        )

        assert hedge_calls == []
        assert append_calls == ["variational_fill", "lighter_blocked"]
        record = runtime.records["id:trade-live-inventory"]
        assert record.processing_stage == "blocked_by_mode"
        assert record.failure_reason == "live_inventory_blocks_trade_event_auto_hedge"

    asyncio.run(run())


def test_lighter_ws_sendtx_sends_tx_info_as_object() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_submit_timeout_seconds = 1.0
        runtime._lighter_submit_ws_lock = asyncio.Lock()

        class FakeWs:
            state = 1

            def __init__(self):
                self.sent: list[str] = []
                self.recv_messages = [json.dumps({"type": "jsonapi/sendtx", "data": {"code": 200, "tx_hash": "0xabc"}})]

            async def send(self, message):
                self.sent.append(message)

            async def recv(self):
                return self.recv_messages.pop(0)

        fake_ws = FakeWs()
        runtime._lighter_submit_ws = fake_ws

        response, wire_sent_monotonic = await runtime.send_lighter_tx_ws(
            tx_type=14,
            tx_info='{"Nonce": 1}',
        )

        sent = json.loads(fake_ws.sent[0])
        assert sent["type"] == "jsonapi/sendtx"
        assert sent["data"]["tx_type"] == 14
        assert sent["data"]["tx_info"] == {"Nonce": 1}
        assert response.code == 200
        assert response.tx_hash == "0xabc"
        assert wire_sent_monotonic is not None

    asyncio.run(run())


def test_lighter_ws_prewarm_reuses_connection(monkeypatch) -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.live_submit_timeout_seconds = 1.0
        runtime.lighter_submit_transport = "ws"
        runtime._lighter_submit_ws_lock = asyncio.Lock()
        runtime._lighter_submit_ws = None
        runtime.logger = logging.getLogger("test_lighter_ws_prewarm")

        class FakeWs:
            state = 1

            def __init__(self):
                self.sent: list[str] = []
                self.recv_messages = [
                    json.dumps({"type": "connected"}),
                    json.dumps({"type": "jsonapi/sendtx", "data": {"code": 200, "tx_hash": "0xabc"}}),
                ]

            async def send(self, message):
                self.sent.append(message)

            async def recv(self):
                return self.recv_messages.pop(0)

        fake_ws = FakeWs()
        connect_calls = 0

        async def fake_connect(*_args, **_kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return fake_ws

        monkeypatch.setattr("main.websockets.connect", fake_connect)
        monkeypatch.setattr("main.elapsed_ms_str", lambda *_args, **_kwargs: "0.001")

        await runtime.prewarm_lighter_submit_ws()
        response, wire_sent_monotonic = await runtime.send_lighter_tx_ws(
            tx_type=14,
            tx_info='{"Nonce": 1}',
        )

        assert connect_calls == 1
        assert response.code == 200
        assert wire_sent_monotonic is not None
        assert len(fake_ws.sent) == 1
        assert json.loads(fake_ws.sent[0])["type"] == "jsonapi/sendtx"

    asyncio.run(run())


def test_market_ioc_uses_ioc_expiry() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.lighter_order_mode = "market-ioc"

    class FakeClient:
        ORDER_TYPE_LIMIT = 0
        ORDER_TYPE_MARKET = 1
        ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
        ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
        DEFAULT_IOC_EXPIRY = 0
        DEFAULT_28_DAY_ORDER_EXPIRY = -1

    runtime.lighter_client = FakeClient()

    order_kwargs = {
        "market_index": 1,
        "client_order_index": 123,
        "base_amount": 45,
        "price": 100000,
        "is_ask": False,
        "order_type": (
            runtime.lighter_client.ORDER_TYPE_MARKET
            if runtime.lighter_order_mode == "market-ioc"
            else runtime.lighter_client.ORDER_TYPE_LIMIT
        ),
        "time_in_force": (
            runtime.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
            if runtime.lighter_order_mode == "market-ioc"
            else runtime.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
        ),
        "reduce_only": False,
        "trigger_price": 0,
        "order_expiry": (
            runtime.lighter_client.DEFAULT_IOC_EXPIRY
            if runtime.lighter_order_mode == "market-ioc"
            else runtime.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY
        ),
    }

    assert order_kwargs["order_type"] == runtime.lighter_client.ORDER_TYPE_MARKET
    assert order_kwargs["time_in_force"] == runtime.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    assert order_kwargs["order_expiry"] == runtime.lighter_client.DEFAULT_IOC_EXPIRY


def test_create_lighter_order_ws_accepts_order_expiry() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)

        class FakeNonceManager:
            def next_nonce(self):
                return 1, 99

            def acknowledge_failure(self, _api_key_index):
                raise AssertionError("should not acknowledge failure")

            def hard_refresh_nonce(self, _api_key_index):
                raise AssertionError("should not refresh nonce")

        class FakeClient:
            nonce_manager = FakeNonceManager()

            def sign_create_order(self, **kwargs):
                assert kwargs["order_expiry"] == 0
                return 1, "{}", "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_send_lighter_tx_ws(*, tx_type, tx_info):
            assert tx_type == 1
            assert tx_info == "{}"

            class Response:
                code = 200
                tx_hash = "0xabc"

            return Response(), time.monotonic()

        runtime.send_lighter_tx_ws = fake_send_lighter_tx_ws

        _order, response, error, wire_sent_monotonic = (
            await runtime.create_lighter_order_ws(
            market_index=1,
            client_order_index=123,
            base_amount=45,
            price=100000,
            is_ask=False,
            order_type=1,
            time_in_force=0,
            reduce_only=False,
            trigger_price=0,
            order_expiry=0,
            )
        )

        assert error is None
        assert response.code == 200
        assert wire_sent_monotonic is not None

    asyncio.run(run())


def test_place_lighter_order_from_plan_passes_reduce_only() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime._record_lock = asyncio.Lock()
        runtime._lighter_signer_lock = asyncio.Lock()
        runtime.lighter_submit_transport = "http"
        runtime.lighter_order_mode = "market-ioc"
        runtime.lighter_market_index = 1
        runtime.price_multiplier = Decimal("100")
        runtime.base_amount_multiplier = Decimal("100000000")
        runtime.risk_guard_max_base_amount = 1000000
        runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = None
        runtime.live_allowed_sides = {"buy", "sell"}
        runtime.live_allowed_assets = {"BTC"}
        runtime.live_max_qty = Decimal("0")
        runtime.live_max_notional_usd = Decimal("100")
        runtime.live_require_min_edge_bps = Decimal("0")
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("3")
        runtime.live_cooldown_seconds = 0.0
        runtime.last_live_submit_monotonic_by_asset = {}
        runtime.lighter_client_order_to_trade_key = {}
        runtime.lighter_best_bid = Decimal("99990")
        runtime.lighter_best_ask = Decimal("100010")
        runtime.lighter_order_book_lock = asyncio.Lock()
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        runtime.logger = logging.getLogger("test_auto_live_fuse")

        captured_kwargs = {}

        class FakeClient:
            ORDER_TYPE_LIMIT = 0
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
            DEFAULT_IOC_EXPIRY = 0
            DEFAULT_28_DAY_ORDER_EXPIRY = -1

            async def create_order(self, **kwargs):
                captured_kwargs.update(kwargs)
                return None, "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_append_order_log(_event_type, _payload) -> None:
            return None

        runtime.append_order_log = fake_append_order_log

        record, payload = await runtime.place_lighter_order_from_plan(
            asset="BTC",
            side="SELL",
            qty=Decimal("0.0001"),
            var_fill_price=Decimal("100000"),
            role="live_inventory_exit",
            reduce_only=True,
        )

        assert captured_kwargs["reduce_only"] is True
        assert record is not None
        assert record.lighter_reduce_only is True
        assert payload["lighter_reduce_only"] is True

    asyncio.run(run())


def test_reduce_only_lighter_order_bypasses_live_cooldown() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime._record_lock = asyncio.Lock()
        runtime._lighter_signer_lock = asyncio.Lock()
        runtime.lighter_submit_transport = "http"
        runtime.lighter_order_mode = "market-ioc"
        runtime.lighter_market_index = 1
        runtime.price_multiplier = Decimal("100")
        runtime.base_amount_multiplier = Decimal("100000000")
        runtime.risk_guard_max_base_amount = 1000000
        runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = None
        runtime.live_allowed_sides = {"buy", "sell"}
        runtime.live_allowed_assets = {"BTC"}
        runtime.live_max_qty = Decimal("0")
        runtime.live_max_notional_usd = Decimal("100")
        runtime.live_require_min_edge_bps = Decimal("0")
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("3")
        runtime.live_cooldown_seconds = 999999.0
        runtime.last_live_submit_monotonic_by_asset = {"BTC": 999999999999.0}
        runtime.lighter_client_order_to_trade_key = {}
        runtime.lighter_best_bid = Decimal("99990")
        runtime.lighter_best_ask = Decimal("100010")
        runtime.lighter_order_book_lock = asyncio.Lock()
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        runtime.logger = logging.getLogger("test_auto_live_fuse")

        captured_kwargs = {}

        class FakeClient:
            ORDER_TYPE_LIMIT = 0
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
            DEFAULT_IOC_EXPIRY = 0
            DEFAULT_28_DAY_ORDER_EXPIRY = -1

            async def create_order(self, **kwargs):
                captured_kwargs.update(kwargs)
                return None, "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_append_order_log(_event_type, _payload) -> None:
            return None

        runtime.append_order_log = fake_append_order_log

        record, payload = await runtime.place_lighter_order_from_plan(
            asset="BTC",
            side="BUY",
            qty=Decimal("0.000243"),
            var_fill_price=Decimal("98990"),
            role="live_inventory_exit",
            reduce_only=True,
        )

        assert captured_kwargs["reduce_only"] is True
        assert record is not None
        assert record.failure_reason is None
        assert payload["processing_stage"] == "live_submit_sent"

    asyncio.run(run())


def test_reduce_only_lighter_order_bypasses_entry_sizing_and_edge_limits() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime._record_lock = asyncio.Lock()
        runtime._lighter_signer_lock = asyncio.Lock()
        runtime.lighter_submit_transport = "http"
        runtime.lighter_order_mode = "market-ioc"
        runtime.lighter_market_index = 1
        runtime.price_multiplier = Decimal("100")
        runtime.base_amount_multiplier = Decimal("10000")
        runtime.risk_guard_max_base_amount = 1000000
        runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = None
        runtime.live_allowed_sides = {"buy", "sell"}
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_max_qty = Decimal("0.01")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.live_require_min_edge_bps = Decimal("999")
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("3")
        runtime.live_inventory_lighter_submit_slippage_bps = Decimal("15")
        runtime.live_inventory_lighter_exit_submit_slippage_bps = Decimal("30")
        runtime.live_cooldown_seconds = 0.0
        runtime.last_live_submit_monotonic_by_asset = {}
        runtime.lighter_client_order_to_trade_key = {}
        runtime.lighter_best_bid = Decimal("2431.94")
        runtime.lighter_best_ask = Decimal("2431.95")
        runtime.lighter_order_book_lock = asyncio.Lock()
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        runtime.logger = logging.getLogger("test_auto_live_fuse")

        captured_kwargs = {}

        class FakeClient:
            ORDER_TYPE_LIMIT = 0
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME = 1
            DEFAULT_IOC_EXPIRY = 0
            DEFAULT_28_DAY_ORDER_EXPIRY = -1

            async def create_order(self, **kwargs):
                captured_kwargs.update(kwargs)
                return None, "0xabc", None

        runtime.lighter_client = FakeClient()

        async def fake_append_order_log(_event_type, _payload) -> None:
            return None

        runtime.append_order_log = fake_append_order_log

        record, payload = await runtime.place_lighter_order_from_plan(
            asset="ETH",
            side="BUY",
            qty=Decimal("0.05720"),
            var_fill_price=Decimal("2431.72"),
            role="live_inventory_exit",
            reduce_only=True,
        )

        assert record is not None
        assert captured_kwargs["reduce_only"] is True
        assert captured_kwargs["base_amount"] == 572
        assert payload["live_notional_usd"] == "138.6896470960"
        assert payload["failure_reason"] is None
        assert payload["processing_stage"] == "live_submit_sent"

    asyncio.run(run())


def test_non_reduce_only_lighter_order_keeps_live_notional_limit() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        runtime.mode = "live"
        runtime.records = {}
        runtime.record_order = deque()
        runtime._record_lock = asyncio.Lock()
        runtime._lighter_signer_lock = asyncio.Lock()
        runtime.lighter_submit_transport = "http"
        runtime.lighter_order_mode = "market-ioc"
        runtime.lighter_market_index = 1
        runtime.price_multiplier = Decimal("100")
        runtime.base_amount_multiplier = Decimal("10000")
        runtime.risk_guard_max_base_amount = 1000000
        runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = None
        runtime.live_allowed_sides = {"buy", "sell"}
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_max_qty = Decimal("0")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.live_require_min_edge_bps = Decimal("0")
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("3")
        runtime.live_inventory_lighter_submit_slippage_bps = Decimal("15")
        runtime.live_inventory_lighter_exit_submit_slippage_bps = Decimal("30")
        runtime.live_cooldown_seconds = 0.0
        runtime.last_live_submit_monotonic_by_asset = {}
        runtime.lighter_client_order_to_trade_key = {}
        runtime.lighter_best_bid = Decimal("2431.94")
        runtime.lighter_best_ask = Decimal("2431.95")
        runtime.lighter_order_book_lock = asyncio.Lock()
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        runtime.logger = logging.getLogger("test_auto_live_fuse")

        async def fake_append_order_log(_event_type, _payload) -> None:
            return None

        runtime.append_order_log = fake_append_order_log

        record, payload = await runtime.place_lighter_order_from_plan(
            asset="ETH",
            side="BUY",
            qty=Decimal("0.05720"),
            var_fill_price=Decimal("2431.72"),
            role="live_inventory_entry",
            reduce_only=False,
        )

        assert record is not None
        assert payload["failure_reason"] == "live_notional_exceeds_limit"
        assert payload["processing_stage"] == "live_submit_started"
        assert payload["lighter_client_order_id"] is None

    asyncio.run(run())


def _live_inventory_runtime(tmp_path) -> VariationalToLighterRuntime:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.mode = "live"
    runtime.live_inventory = True
    runtime.live_inventory_dry_decisions = False
    runtime.live_inventory_sample_index = 0
    runtime.live_inventory_completed_cycles = 0
    runtime.live_inventory_max_cycles = 1
    runtime.live_inventory_next_lot_id = 1
    runtime.live_inventory_run_id = "test-live-inventory-run"
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_realized_pnl_usd = Decimal("0")
    runtime.pending_live_inventory_actual_pnl = {}
    runtime.pending_live_inventory_final_pnl = {}
    runtime.live_inventory_v4_exit_reconciliation_lot_ids = set()
    runtime.live_inventory_exit_events_logged = set()
    runtime.live_inventory_execution_loss_bps_samples = deque(maxlen=20)
    runtime.live_inventory_entry_bps = Decimal("50")
    runtime.live_inventory_exit_bps = Decimal("10")
    runtime.live_inventory_max_var_spread_bps = Decimal("5")
    runtime.live_inventory_max_var_snapshot_age_seconds = 5.0
    runtime.live_inventory_basis_max_sample_move_bps = Decimal("3")
    runtime.live_inventory_basis_sample_move_bps_samples = deque(maxlen=200)
    runtime.live_inventory_basis_var_spread_bps_samples = deque(maxlen=200)
    runtime.live_inventory_basis_lighter_spread_bps_samples = deque(maxlen=200)
    runtime.live_inventory_basis_reversion_history = deque()
    runtime.live_inventory_basis_reversion_mode = False
    runtime.live_inventory_basis_reversion_min_deviation_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_exit_deviation_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_max_entry_roundtrip_cost_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_context_gap_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_long_execution_reserve_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_short_execution_reserve_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_min_net_expected_pnl_bps = Decimal("0")
    runtime.live_inventory_execution_calibration = False
    runtime.live_inventory_basis_v4_mode = False
    runtime.live_inventory_basis_v4_profile = ""
    runtime.live_inventory_basis_v4_shadow_gradient = False
    runtime.live_inventory_basis_v4_real_gradient = False
    runtime.live_inventory_basis_entry_mode = "concurrent"
    runtime.live_inventory_basis_size_ladder_notionals_usd = ()
    runtime.live_inventory_basis_stablecoin_normalization = False
    runtime.live_inventory_stablecoin_rate_cache = {}
    runtime.live_inventory_stablecoin_basis_bps_samples = deque(maxlen=500)
    runtime.live_inventory_basis_use_normalized_edge_for_entry = False
    runtime.live_inventory_basis_min_normalized_entry_edge_bps = Decimal("0")
    runtime.live_inventory_basis_min_normalized_filter_edge_bps = None
    runtime.live_inventory_basis_max_stablecoin_edge_share = Decimal("0")
    runtime.live_inventory_basis_stablecoin_regime_entry = False
    runtime.live_inventory_basis_min_stablecoin_basis_bps = Decimal("0")
    runtime.live_inventory_basis_stablecoin_regime_buffer_bps = Decimal("0")
    runtime.live_inventory_basis_stablecoin_regime_min_normalized_edge_bps = Decimal("0")
    runtime.live_inventory_basis_stablecoin_regime_lookback_samples = 1
    runtime.live_inventory_basis_stablecoin_regime_max_change_bps = Decimal("0")
    runtime.live_inventory_basis_dynamic_entry_threshold = False
    runtime.live_inventory_basis_entry_confirm_samples = 1
    runtime.live_inventory_basis_entry_confirm_counts = {}
    runtime.live_inventory_basis_long_min_entry_edge_bps = Decimal("0")
    runtime.live_inventory_basis_short_min_entry_edge_bps = Decimal("0")
    runtime.live_inventory_basis_long_min_abs_entry_bps = Decimal("0")
    runtime.live_inventory_basis_short_min_abs_entry_bps = Decimal("0")
    runtime.live_inventory_basis_dynamic_entry_noise_buffer_bps = Decimal("0")
    runtime.live_inventory_basis_spread_regime_penalty_multiplier = Decimal("1")
    runtime.live_inventory_basis_sample_move_penalty_multiplier = Decimal("0")
    runtime.live_inventory_basis_min_entry_quality_score_bps = Decimal("0")
    runtime.live_inventory_basis_watch_candidate = False
    runtime.live_inventory_basis_watch_candidates = {}
    runtime.live_inventory_basis_negative_direction_mode = "off"
    runtime.live_inventory_basis_direction_min_samples = 1
    runtime.live_inventory_basis_direction_min_avg_pnl_bps = Decimal("0")
    runtime.live_inventory_basis_negative_direction_entry_penalty_bps = Decimal("0")
    runtime.live_inventory_basis_negative_direction_abs_penalty_bps = Decimal("0")
    runtime.live_inventory_actual_pnl_bps_by_direction = {
        "long_var_short_lighter": deque(maxlen=20),
        "short_var_long_lighter": deque(maxlen=20),
    }
    runtime.live_inventory_exit_fill_latency_ms_samples = deque(maxlen=20)
    runtime.live_inventory_basis_latency_buffer_p90_ms = Decimal("0")
    runtime.live_inventory_basis_latency_buffer_bps = Decimal("0")
    runtime.live_inventory_basis_quote_age_buffer_ms = Decimal("0")
    runtime.live_inventory_basis_quote_age_buffer_bps = Decimal("0")
    runtime.live_inventory_refresh_var_quote_before_entry = False
    runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("5")
    runtime.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = False
    runtime.live_inventory_max_lighter_slippage_bps = Decimal("3")
    runtime.live_inventory_max_lighter_book_age_seconds = 0.0
    runtime.live_inventory_entry_lighter_fill_timeout_seconds = 3.0
    runtime.live_inventory_basis_auto_close_unhedged = False
    runtime.live_inventory_exit_blocked_log_throttle_seconds = 0.0
    runtime.live_inventory_lot_notional_usd = Decimal("10")
    runtime.live_inventory_max_total_notional_usd = Decimal("10")
    runtime.live_inventory_max_total_lots = 1
    runtime.live_inventory_min_hold_samples = 0
    runtime.live_inventory_max_hold_samples = 300
    runtime.live_inventory_basis_time_decay_exit_samples = 0
    runtime.live_inventory_basis_time_decay_min_exit_pnl_bps = Decimal("0")
    runtime.live_inventory_basis_profit_take_pnl_bps = Decimal("0")
    runtime.live_inventory_basis_min_signal_reverted_exit_pnl_bps = Decimal("0")
    runtime.live_inventory_basis_reversion_signal_exit_min_pnl_bps = Decimal("0")
    runtime.live_inventory_basis_signal_exit_watch_samples = 0
    runtime.live_inventory_basis_signal_exit_watch_timeout_min_pnl_bps = Decimal("0")
    runtime.live_inventory_max_unrealized_loss_bps = Decimal("999999999")
    runtime.live_inventory_basis_max_hold_action = "exit"
    runtime.live_inventory_basis_min_abs_entry_bps = Decimal("0")
    runtime.live_inventory_basis_exit_safety_buffer_bps = Decimal("0")
    runtime.live_inventory_basis_dynamic_exit_buffer = False
    runtime.live_inventory_basis_refresh_exit_quote_before_submit = False
    runtime.live_inventory_basis_refresh_entry_quote_before_submit = False
    runtime.live_inventory_basis_max_var_quote_age_ms = 0.0
    runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(maxlen=20)
    runtime._last_live_inventory_exit_blocked_log = {}
    runtime.live_inventory_i_accept_basis_addon_diagnostic = False
    runtime.live_inventory_basis_addon_min_basis_improvement_bps = Decimal("1.5")
    runtime.live_inventory_state_file = Path(tmp_path) / "live_inventory_state.json"
    runtime.live_inventory_control_file = (
        Path(tmp_path) / "live_inventory_control.json"
    )
    runtime.live_inventory_maintenance_drain_requested = False
    runtime.live_inventory_maintenance_drain_requested_at = None
    runtime.live_inventory_maintenance_control_payload = {}
    runtime.live_inventory_maintenance_last_flat_check_monotonic = 0.0
    runtime.live_inventory_maintenance_last_block_log_monotonic = 0.0
    runtime.stop_flag = False
    runtime.orders_file = Path(tmp_path) / "order_metrics.jsonl"
    runtime._order_write_lock = asyncio.Lock()
    runtime._record_lock = asyncio.Lock()
    runtime.records = {}
    runtime.record_order = deque(maxlen=1000)
    runtime.lighter_client_order_to_trade_key = {}
    runtime.lighter_order_book_lock = asyncio.Lock()
    runtime.lighter_order_book = {
        "bids": {Decimal("59990"): Decimal("1")},
        "asks": {Decimal("60010"): Decimal("1")},
    }
    runtime.lighter_market_index = 1
    runtime.lighter_best_bid = Decimal("59990")
    runtime.lighter_best_ask = Decimal("60010")
    runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
    runtime.base_amount_multiplier = Decimal("100000000")
    runtime.risk_guard_max_base_amount = 1000000
    runtime.risk_guard_max_price_deviation_bps = Decimal("1000")
    runtime.lighter_min_base_amount = None
    runtime.lighter_min_quote_amount = None
    runtime.live_allowed_sides = {"buy", "sell"}
    runtime.live_allowed_assets = {"BTC"}
    runtime.live_max_qty = Decimal("0")
    runtime.live_max_notional_usd = Decimal("20")
    runtime.live_submit_timeout_seconds = 10.0
    runtime.live_require_min_edge_bps = Decimal("0")
    runtime.live_cooldown_seconds = 0.0
    runtime.last_live_submit_monotonic_by_asset = {}
    runtime.live_inventory_var_reject_cooldown_until = {}
    runtime.live_inventory_var_reject_cooldown_seconds = 600.0
    runtime.logger = logging.getLogger("test_auto_live_fuse")
    return runtime


def test_maintenance_drain_waits_for_positions_then_confirms_both_venues_flat(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.008",
            }
        ]
        control = {
            "schema_version": 1,
            "action": "drain_after_flat",
            "status": "requested",
            "asset": "ETH",
            "target_pid": os.getpid(),
            "target_run_id": runtime.live_inventory_run_id,
            "requested_at": "2026-08-28T13:00:00+00:00",
        }
        runtime.live_inventory_control_file.write_text(
            json.dumps(control),
            encoding="utf-8",
        )

        await runtime.poll_live_inventory_maintenance_control()

        assert runtime.live_inventory_maintenance_drain_requested is True
        assert runtime.stop_flag is False

        runtime.live_inventory_open_lots = []

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": []}}

        async def fake_fetch_lighter_account():
            return {"code": 200, "accounts": [{"positions": []}]}

        async def fake_capture_live_inventory_account_snapshot(**_kwargs):
            return None

        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.fetch_lighter_account = fake_fetch_lighter_account
        runtime.capture_live_inventory_account_snapshot = (
            fake_capture_live_inventory_account_snapshot
        )
        runtime.live_inventory_maintenance_last_flat_check_monotonic = 0.0

        await runtime.poll_live_inventory_maintenance_control()

        completed = json.loads(
            runtime.live_inventory_control_file.read_text(encoding="utf-8")
        )
        assert completed["status"] == "completed"
        assert completed["variational_position_qty"] == "0"
        assert completed["lighter_position_qty"] == "0"
        assert runtime.shutdown_reason == "maintenance_drain_completed"
        assert runtime.stop_flag is True

    asyncio.run(run())


def _inventory_entry_snapshot() -> CrossSpreadSnapshot:
    return CrossSpreadSnapshot(
        asset="BTC",
        var_bid=Decimal("59990"),
        var_ask=Decimal("60000"),
        var_mid=Decimal("59995"),
        var_half_spread_bps=Decimal("1"),
        var_buy_price=Decimal("60000"),
        var_sell_price=Decimal("59990"),
        var_full_spread_bps=Decimal("2"),
        var_spread_source="test",
        var_timestamp="2999-06-16T03:25:20.000Z",
        var_source_url="wss://example.test/prices",
        var_source_stream="instrument_price:BTC",
        lighter_bid=Decimal("60400"),
        lighter_ask=Decimal("60420"),
        lighter_mid=Decimal("60410"),
        lighter_buy_price=Decimal("60420"),
        lighter_sell_price=Decimal("60400"),
        lighter_half_spread_bps=Decimal("1"),
        lighter_buy_fill_price=Decimal("60420"),
        lighter_sell_fill_price=Decimal("60400"),
        long_var_short_lighter_pct=Decimal("0.66666667"),
        short_var_long_lighter_pct=Decimal("-0.006"),
        long_median_5m_pct=None,
        short_median_5m_pct=None,
        long_sample_count_5m=1,
        short_sample_count_5m=1,
    )


def test_v4_shadow_gradient_enters_and_guarded_strong_single_exits_without_real_order(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_basis_v4_shadow_gradient = True
        runtime.live_inventory_v4_episode_id = "episode-1"
        runtime.live_inventory_v4_next_tranche_index = 2
        runtime.live_inventory_v4_shadow_tranche = None
        runtime.live_inventory_v4_shadow_completed_episode_id = None
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_v4_effective_exit_target_bps = (
            lambda: Decimal("2.50")
        )
        runtime.live_inventory_strong_single_shortfall_bps_samples = deque(
            [Decimal("0")] * 3,
            maxlen=20,
        )
        runtime.live_inventory_open_lots = [
            {
                "asset": "ETH",
                "lot_id": 1,
                "episode_id": "episode-1",
                "direction": "short_var_long_lighter",
                "entry_kind": "basis_v4_eth_short_p97_5",
                "entry_basis_bps": "10.00",
                "entry_cost_status": "final_fills_confirmed",
            }
        ]

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            asset="ETH",
            sample_index=10,
            basis_bps=Decimal("12.10"),
            short_edge_bps=Decimal("5.00"),
            entry_threshold_bps=Decimal("4.00"),
            var_bid=Decimal("100.00"),
            var_ask=Decimal("100.01"),
            lighter_buy_price=Decimal("100.00"),
            lighter_sell_price=Decimal("99.99"),
            warm=True,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            basis_sample_move_ok=True,
        )

        assert runtime.live_inventory_v4_shadow_tranche is not None
        assert runtime.live_inventory_v4_shadow_tranche["tranche_index"] == 2
        assert set(runtime.live_inventory_v4_shadow_tranches) == {
            "0.50",
            "1.00",
            "1.50",
            "2.00",
        }
        assert len(runtime.live_inventory_open_lots) == 1
        assert runtime.live_inventory_next_lot_id == 1

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            asset="ETH",
            sample_index=11,
            basis_bps=Decimal("12.00"),
            short_edge_bps=Decimal("5.00"),
            entry_threshold_bps=Decimal("4.00"),
            var_bid=Decimal("99.99"),
            var_ask=Decimal("99.99"),
            lighter_buy_price=Decimal("100.014"),
            lighter_sell_price=Decimal("100.014"),
            warm=True,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            basis_sample_move_ok=True,
        )

        assert runtime.live_inventory_v4_shadow_tranche is not None

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            asset="ETH",
            sample_index=12,
            basis_bps=Decimal("12.00"),
            short_edge_bps=Decimal("5.00"),
            entry_threshold_bps=Decimal("4.00"),
            var_bid=Decimal("99.99"),
            var_ask=Decimal("99.99"),
            lighter_buy_price=Decimal("100.025"),
            lighter_sell_price=Decimal("100.025"),
            warm=True,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            basis_sample_move_ok=True,
        )

        assert runtime.live_inventory_v4_shadow_tranche is None
        assert runtime.live_inventory_v4_shadow_tranches == {}
        assert runtime.live_inventory_v4_shadow_completed_episode_id == "episode-1"
        rows = [json.loads(line) for line in runtime.orders_file.read_text().splitlines()]
        assert [row["event"] for row in rows].count(
            "live_inventory_v4_shadow_tranche_entered"
        ) == 4
        assert [row["event"] for row in rows].count(
            "live_inventory_v4_shadow_tranche_exited"
        ) == 4
        assert {
            row["basis_improvement_trigger_bps"]
            for row in rows
            if row["event"] == "live_inventory_v4_shadow_tranche_entered"
        } == {"0.50", "1.00", "1.50", "2.00"}
        assert all(
            row["exit_confirmation_mode"] == "strong_single"
            and row["real_orders_submitted"] == 0
            for row in rows
            if row["event"] == "live_inventory_v4_shadow_tranche_exited"
        )

    asyncio.run(run())


def test_v4_shadow_gradient_normal_exit_requires_two_fresh_samples(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_basis_v4_shadow_gradient = True
        runtime.live_inventory_v4_shadow_completed_episode_id = None
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_v4_effective_exit_target_bps = (
            lambda: Decimal("2.50")
        )
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_v4_shadow_tranche = {
            "episode_id": "episode-1",
            "tranche_index": 2,
            "direction": "short_var_long_lighter",
            "qty": "0.2",
            "notional_usd": "20",
            "entry_var_price": "100",
            "entry_lighter_price": "100",
            "entered_at": datetime.now(timezone.utc).isoformat(),
            "v4_exit_confirmation_count": 0,
        }
        kwargs = {
            "asset": "ETH",
            "basis_bps": Decimal("12"),
            "short_edge_bps": Decimal("5"),
            "entry_threshold_bps": Decimal("4"),
            "var_bid": Decimal("99.99"),
            "var_ask": Decimal("99.99"),
            "lighter_buy_price": Decimal("100.02"),
            "lighter_sell_price": Decimal("100.02"),
            "warm": True,
            "basis_sample_move_ok": True,
        }

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            sample_index=1,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            **kwargs,
        )
        assert runtime.live_inventory_v4_shadow_tranche is not None
        assert runtime.live_inventory_v4_shadow_tranche[
            "v4_exit_confirmation_count"
        ] == 1

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            sample_index=2,
            var_quote_age_ok=False,
            lighter_book_age_ok=True,
            **kwargs,
        )
        assert runtime.live_inventory_v4_shadow_tranche is not None
        assert runtime.live_inventory_v4_shadow_tranche.get(
            "v4_exit_confirmation_count", 0
        ) == 0

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            sample_index=3,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            **kwargs,
        )
        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            sample_index=4,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            **kwargs,
        )
        assert runtime.live_inventory_v4_shadow_tranche is None

    asyncio.run(run())


def test_v4_shadow_gradient_enters_lower_levels_before_full_two_bps(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_basis_v4_shadow_gradient = True
        runtime.live_inventory_v4_shadow_tranche = None
        runtime.live_inventory_v4_shadow_completed_episode_id = None
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_open_lots = [
            {
                "asset": "ETH",
                "lot_id": 1,
                "episode_id": "episode-1",
                "direction": "short_var_long_lighter",
                "entry_kind": "basis_v4_eth_short_p97_5",
                "entry_basis_bps": "10.00",
                "entry_cost_status": "final_fills_confirmed",
            }
        ]

        await runtime.maybe_update_live_inventory_basis_v4_shadow_gradient(
            asset="ETH",
            sample_index=10,
            basis_bps=Decimal("11.99"),
            short_edge_bps=Decimal("5.00"),
            entry_threshold_bps=Decimal("4.00"),
            var_bid=Decimal("100.00"),
            var_ask=Decimal("100.01"),
            lighter_buy_price=Decimal("100.00"),
            lighter_sell_price=Decimal("99.99"),
            warm=True,
            var_quote_age_ok=True,
            lighter_book_age_ok=True,
            basis_sample_move_ok=True,
        )

        assert runtime.live_inventory_v4_shadow_tranche is None
        assert set(runtime.live_inventory_v4_shadow_tranches) == {
            "0.50",
            "1.00",
            "1.50",
        }
        rows = [json.loads(line) for line in runtime.orders_file.read_text().splitlines()]
        assert {
            row["basis_improvement_trigger_bps"] for row in rows
        } == {"0.50", "1.00", "1.50"}

    asyncio.run(run())


def test_entry_slippage_limit_forces_reduce_only_cleanup_after_both_fills(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_lighter_fill_timeout_seconds = 3.0
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("6")
        runtime.records = {}

        lighter_record = OrderLifecycle(
            trade_key="lighter-entry",
            trade_id="lighter-entry",
            side="sell",
            qty=Decimal("0.01040"),
            asset="ETH",
            mode="live",
            last_variational_status="submitted",
            lighter_fill_price=Decimal("1911.40"),
        )
        lighter_record.processing_stage = "lighter_filled"
        runtime.records[lighter_record.trade_key] = lighter_record
        var_record = OrderLifecycle(
            trade_key="var-entry",
            trade_id="var-entry",
            side="sell",
            qty=Decimal("0.01040"),
            asset="ETH",
            mode="live",
            last_variational_status="filled",
            var_fill_price=Decimal("1909.33"),
        )
        match = PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01040"),
            lot_id=1,
            role="live_inventory_entry_pending_var_fill",
            created_at_monotonic=time.monotonic(),
            context={
                "direction": "short_var_long_lighter",
                "var_side": "SELL",
                "lighter_price": "1910.07",
                "lighter_record_key": lighter_record.trade_key,
                "lighter_submitted_before_var_fill": True,
            },
        )
        cleanup_calls: list[dict] = []
        reviews: list[dict] = []

        async def fake_wait_for_lighter_final_fill(*_args, **_kwargs):
            return True

        async def fake_cleanup(**kwargs):
            cleanup_calls.append(kwargs)
            return {"enabled": True, "force": kwargs["force"]}

        async def fake_manual_review(**kwargs):
            reviews.append(kwargs)

        async def fake_verify(**_kwargs):
            return False, "reference_recovery_position_mismatch", {
                "variational_position_qty": "0.01040",
                "lighter_position_qty": "0.01040",
            }

        runtime.wait_for_lighter_final_fill = fake_wait_for_lighter_final_fill
        runtime.try_auto_close_unhedged_live_inventory_leg = fake_cleanup
        runtime.require_live_inventory_manual_review = fake_manual_review
        runtime.verify_live_inventory_auto_close_positions = fake_verify

        await runtime.complete_live_inventory_entry_after_var_fill(
            match=match,
            record=var_record,
            fill_payload={},
        )

        assert len(cleanup_calls) == 1
        assert cleanup_calls[0]["close_var"] is True
        assert cleanup_calls[0]["close_lighter"] is True
        assert cleanup_calls[0]["force"] is True
        assert reviews[0]["reason"] == (
            "basis_entry_lighter_actual_slippage_exceeds_limit"
        )
        assert reviews[0]["context"]["auto_close_unhedged"]["enabled"] is True
        assert runtime.live_inventory_open_lots == []

    asyncio.run(run())


def test_entry_slippage_limit_rearms_after_verified_cleanup(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_lighter_fill_timeout_seconds = 3.0
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("6")
        runtime.records = {}

        lighter_record = OrderLifecycle(
            trade_key="lighter-entry",
            trade_id="lighter-entry",
            side="sell",
            qty=Decimal("0.01040"),
            asset="ETH",
            mode="live",
            last_variational_status="submitted",
            lighter_fill_price=Decimal("1911.40"),
        )
        lighter_record.processing_stage = "lighter_filled"
        runtime.records[lighter_record.trade_key] = lighter_record
        var_record = OrderLifecycle(
            trade_key="var-entry",
            trade_id="var-entry",
            side="sell",
            qty=Decimal("0.01040"),
            asset="ETH",
            mode="live",
            last_variational_status="filled",
            var_fill_price=Decimal("1909.33"),
        )
        match = PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01040"),
            lot_id=1,
            role="live_inventory_entry_pending_var_fill",
            created_at_monotonic=time.monotonic(),
            context={
                "direction": "short_var_long_lighter",
                "var_side": "SELL",
                "lighter_price": "1910.07",
                "lighter_record_key": lighter_record.trade_key,
                "lighter_submitted_before_var_fill": True,
            },
        )
        cleanup_calls: list[dict] = []

        async def fake_wait_for_lighter_final_fill(*_args, **_kwargs):
            return True

        async def fake_cleanup(**kwargs):
            cleanup_calls.append(kwargs)
            return {"enabled": True, "force": kwargs["force"]}

        async def fake_verify(**_kwargs):
            return True, "verified_flat", {
                "variational_position_qty": "0",
                "lighter_position_qty": "0",
            }

        async def unexpected_manual_review(**_kwargs):
            raise AssertionError("verified cleanup must not enter manual review")

        runtime.wait_for_lighter_final_fill = fake_wait_for_lighter_final_fill
        runtime.try_auto_close_unhedged_live_inventory_leg = fake_cleanup
        runtime.verify_live_inventory_auto_close_positions = fake_verify
        runtime.require_live_inventory_manual_review = unexpected_manual_review

        await runtime.complete_live_inventory_entry_after_var_fill(
            match=match,
            record=var_record,
            fill_payload={},
        )

        assert len(cleanup_calls) == 1
        assert runtime.stop_flag is False
        assert runtime.live_inventory_open_lots == []
        state = json.loads(
            runtime.live_inventory_state_file.read_text(encoding="utf-8")
        )
        assert state["status"] == "flat"
        assert state["reason"] == "basis_entry_actual_slippage_rejected_auto_closed"
        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_entry_rejected_auto_closed"
        assert rows[-1]["auto_close_unhedged"]["position_reconcile"]["ok"] is True

    asyncio.run(run())


def test_live_inventory_log_includes_run_id_and_config(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    telegram_events = []
    runtime.telegram_notifier = SimpleNamespace(
        enqueue=lambda event_type, payload: telegram_events.append(
            (event_type, payload)
        )
    )
    runtime.live_inventory_run_id = "test-run-id"
    runtime.live_inventory_signal_mode = "basis"
    runtime.live_inventory_max_lots = 1
    runtime.live_inventory_basis_z_entry = Decimal("3")
    runtime.live_inventory_basis_z_exit = Decimal("999")
    runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
    runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("3")
    runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0.5")

    asyncio.run(runtime.append_live_inventory_run_config())
    asyncio.run(runtime.append_live_inventory_log("live_inventory_test_event", {"asset": "ETH"}))

    rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

    assert rows[0]["event"] == "live_inventory_run_config"
    assert rows[0]["run_id"] == "test-run-id"
    assert rows[0]["config"]["live_inventory_signal_mode"] == "basis"
    assert rows[0]["config"]["live_inventory_max_total_notional_usd"] == "10"
    assert rows[1]["event"] == "live_inventory_test_event"
    assert rows[1]["run_id"] == "test-run-id"
    assert telegram_events[-1][0] == "live_inventory_test_event"
    assert telegram_events[-1][1]["run_id"] == "test-run-id"


def test_telegram_enqueue_failure_does_not_escape_live_log(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.telegram_notifier = SimpleNamespace(
        enqueue=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("telegram unavailable")
        )
    )

    asyncio.run(
        runtime.append_live_inventory_log(
            "live_inventory_entered",
            {"asset": "ETH", "lot_id": 1},
        )
    )

    rows = [
        json.loads(line)
        for line in runtime.orders_file.read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert rows[-1]["event"] == "live_inventory_entered"


def test_account_risk_notifications_are_owned_by_watchdog(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    telegram_events = []
    runtime.telegram_notifier = SimpleNamespace(
        enqueue=lambda event_type, payload: telegram_events.append(
            (event_type, payload)
        )
    )

    asyncio.run(
        runtime.append_live_inventory_log(
            "live_inventory_account_risk_alert",
            {
                "asset": "ETH",
                "risk_action": "warning",
                "risk_reason": "venue_equity_imbalance_warning",
            },
        )
    )

    assert telegram_events == []
    rows = [
        json.loads(line)
        for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
    ]
    assert rows[-1]["event"] == "live_inventory_account_risk_alert"


def test_manual_review_notifications_are_owned_by_watchdog(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    telegram_events = []
    runtime.telegram_notifier = SimpleNamespace(
        enqueue=lambda event_type, payload: telegram_events.append(
            (event_type, payload)
        )
    )

    asyncio.run(
        runtime.append_live_inventory_log(
            "live_inventory_manual_review_required",
            {
                "asset": "ETH",
                "reason": "basis_exit_lighter_final_fill_not_confirmed",
                "open_lots_total": 14,
            },
        )
    )

    assert telegram_events == []
    rows = [
        json.loads(line)
        for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
    ]
    assert rows[-1]["event"] == "live_inventory_manual_review_required"


def _eth_inventory_snapshot() -> CrossSpreadSnapshot:
    snapshot = _inventory_entry_snapshot()
    snapshot.asset = "ETH"
    snapshot.var_bid = Decimal("1753.00")
    snapshot.var_ask = Decimal("1753.25")
    snapshot.var_mid = Decimal("1753.125")
    snapshot.var_buy_price = Decimal("1753.25")
    snapshot.var_sell_price = Decimal("1753.00")
    snapshot.var_timestamp = "2999-06-16T03:25:20.000Z"
    snapshot.lighter_bid = Decimal("1755.00")
    snapshot.lighter_ask = Decimal("1755.10")
    snapshot.lighter_mid = Decimal("1755.05")
    snapshot.lighter_buy_price = Decimal("1755.10")
    snapshot.lighter_sell_price = Decimal("1755.00")
    snapshot.lighter_buy_fill_price = Decimal("1755.10")
    snapshot.lighter_sell_fill_price = Decimal("1755.00")
    return snapshot


def _test_basis_quote_with_metadata(
    quote: dict[str, object],
    *,
    asset: str,
    qty: object,
) -> dict[str, object]:
    return {
        **quote,
        "quote_asset": asset,
        "quote_request_qty": str(qty),
        "quote_size_mode": "exact_base_qty_v1",
    }


def _set_test_lighter_book(
    runtime: VariationalToLighterRuntime,
    *,
    bid: str,
    ask: str,
) -> None:
    bid_price = Decimal(bid)
    ask_price = Decimal(ask)
    runtime.lighter_order_book = {
        "bids": {bid_price: Decimal("1")},
        "asks": {ask_price: Decimal("1")},
    }
    runtime.lighter_best_bid = bid_price
    runtime.lighter_best_ask = ask_price


def test_basis_quote_uses_exact_base_qty_and_records_quote_size_mode(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_extension_disconnect_failures = 0
        runtime.live_inventory_extension_failure_started_monotonic = 0.0
        calls: list[dict] = []

        async def fake_send_variational_place_order(**kwargs):
            calls.append(kwargs)
            return {
                "ok": True,
                "result": {
                    "quoteId": "quote-1",
                    "bid": "2499",
                    "ask": "2501",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                    "quote_source": "passive_browser_stream",
                    "received_monotonic": time.monotonic(),
                },
            }

        runtime.send_variational_place_order = fake_send_variational_place_order

        quote, elapsed_ms = await runtime.fetch_live_inventory_basis_quote(
            asset="ETH",
            qty=Decimal("0.00800"),
            priority="background",
        )

        assert quote is not None
        assert elapsed_ms is not None
        assert Decimal(calls[0]["amount"]) == Decimal("0.008")
        assert calls[0]["priority"] == "background"
        assert Decimal(quote["quote_request_qty"]) == Decimal("0.008")
        assert quote["quote_size_mode"] == "exact_base_qty_v1"
        assert quote["quote_source"] == "direct_rfq"
        assert quote["quote_freshness_source"] == "exchange_quote_timestamp"
        assert quote["quote_latency_kind"] == "exact_rfq_roundtrip"

    asyncio.run(run())


def test_basis_quote_prefers_most_common_exact_open_lot_qty(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_allowed_assets = {"ETH"}
    runtime.live_inventory_open_lots = [
        {"qty": "0.00820"},
        {"qty": "0.00810"},
        {"qty": "0.00820"},
    ]

    quote_qty = runtime.live_inventory_basis_quote_qty(
        asset="ETH",
        reference_price=Decimal("2500"),
    )

    assert quote_qty == Decimal("0.00820")


def test_background_variational_quote_times_out_without_using_trade_lane(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_variational_order_limiter = RollingWindowRateLimiter(
            normal_limit=30,
            hard_limit=36,
        )
        runtime.variational_submit_transport = "api"
        runtime.variational_api_max_slippage = 0.005
        captured: dict = {}

        async def slow_send_variational_command(**kwargs):
            captured.update(kwargs)
            await asyncio.sleep(1)
            return {"ok": True}

        runtime.send_variational_command = slow_send_variational_command
        monkeypatch.setattr(
            "main.LIVE_INVENTORY_VARIATIONAL_BACKGROUND_QUOTE_TIMEOUT_SECONDS",
            0.01,
        )
        monkeypatch.setattr(
            "main.LIVE_INVENTORY_VARIATIONAL_BACKGROUND_COMMAND_TIMEOUT_SECONDS",
            0.01,
        )

        result = await runtime.send_variational_place_order(
            asset="ETH",
            side="BUY",
            amount="0.008",
            expected_min_btc_qty=None,
            confirm=False,
            reduce_only=False,
            priority="background",
        )

        assert result["background_timeout"] is True
        assert captured["lane"] == "background"

    asyncio.run(run())


def test_v4_background_quote_uses_passive_stream_without_rfq(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)

        async def passive_reference_quote(_asset):
            return {
                "reference_price": "2400.20",
                "timestamp": "2026-09-04T00:00:00Z",
                "received_at": "2026-09-04T00:00:01Z",
                "received_monotonic": time.monotonic(),
            }

        async def refuse_rfq(**_kwargs):
            raise AssertionError("passive V4 observation must not request an RFQ")

        runtime.get_variational_reference_quote = passive_reference_quote
        runtime.fetch_live_inventory_basis_quote = refuse_rfq
        quote, quote_ms = await runtime.get_live_inventory_basis_quote(
            asset="ETH",
            qty=Decimal("0.008"),
            priority="background",
        )

        assert quote["quote_source"] == "passive_browser_stream"
        assert quote["quoteTimestamp"] == "2026-09-04T00:00:01Z"
        assert quote["source_quote_timestamp"] == "2026-09-04T00:00:00Z"
        assert quote["quote_semantics"] == "reference_price_only"
        assert quote["bid"] == "2400.20"
        assert quote["ask"] == "2400.20"
        assert Decimal(quote["quote_request_qty"]) == Decimal("0.008")
        assert quote["rfq_consumed"] is False
        assert quote_ms == Decimal("0")

    asyncio.run(run())


def test_v4_entry_rfq_bias_is_directional_bounded_and_zero_until_ready() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    direction = "short_var_long_lighter"
    runtime.live_inventory_entry_rfq_bias_samples_by_direction = {
        direction: deque(
            [
                Decimal("-1"),
                Decimal("-2"),
                Decimal("-3"),
                Decimal("-4"),
                Decimal("-3"),
                Decimal("-3"),
                Decimal("-3"),
                Decimal("-3"),
                Decimal("-3"),
            ]
        )
    }

    cold = runtime.live_inventory_basis_v4_entry_rfq_bias_context(direction)
    assert cold["v4_entry_rfq_bias_ready"] is False
    assert cold["v4_entry_rfq_bias_applied_bps"] == "0"

    sample = runtime.record_live_inventory_entry_rfq_bias_sample(
        asset="ETH",
        direction=direction,
        passive_edge_bps=Decimal("10"),
        exact_edge_bps=Decimal("5"),
        passive_var_price=Decimal("2500"),
        exact_var_price=Decimal("2499"),
        reference_lighter_price=Decimal("2501"),
    )

    assert sample is not None
    assert sample["directional_bias_bps"] == "-5"
    assert sample["v4_entry_rfq_bias_ready"] is True
    assert sample["v4_entry_rfq_bias_applied_bps"] == "-3.0"
    assert sample["calibration_basis"] == "same_lighter_reference_var_price_delta_v1"


def test_v4_entry_rfq_bias_loader_reads_persisted_calibration(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.orders_file = tmp_path / "order_metrics.jsonl"
    runtime.orders_file.write_text(
        "\n".join(
            json.dumps(
                {
                    "event": "live_inventory_entry_rfq_calibration",
                    "logged_at": datetime.now(timezone.utc).isoformat(),
                    "calibration_basis": "same_lighter_reference_var_price_delta_v1",
                    "asset": "ETH",
                    "direction": "long_var_short_lighter",
                    "passive_edge_bps": "8",
                    "exact_edge_bps": "7",
                }
            )
            for _ in range(10)
        ),
        encoding="utf-8",
    )
    runtime.live_inventory_entry_rfq_bias_samples_by_direction = {
        "long_var_short_lighter": deque(maxlen=100),
        "short_var_long_lighter": deque(maxlen=100),
    }
    runtime.logger = logging.getLogger("test_v4_entry_rfq_bias_loader")

    runtime.load_recent_live_inventory_entry_rfq_bias()

    context = runtime.live_inventory_basis_v4_entry_rfq_bias_context(
        "long_var_short_lighter"
    )
    assert context["v4_entry_rfq_bias_sample_count"] == 10
    assert context["v4_entry_rfq_bias_applied_bps"] == "-1"


def test_v4_entry_rfq_bias_ignores_stale_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    direction = "short_var_long_lighter"
    runtime.live_inventory_entry_rfq_bias_samples_by_direction = {
        direction: deque([Decimal("-1")] * 10, maxlen=100)
    }
    runtime.live_inventory_entry_rfq_bias_sample_times_by_direction = {
        direction: deque([time.time() - 86401] * 10, maxlen=100)
    }

    context = runtime.live_inventory_basis_v4_entry_rfq_bias_context(direction)

    assert context["v4_entry_rfq_bias_sample_count"] == 0
    assert context["v4_entry_rfq_bias_ready"] is False
    assert context["v4_entry_rfq_bias_applied_bps"] == "0"


def test_v4_entry_rfq_bias_rejects_unstable_samples() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    direction = "short_var_long_lighter"
    runtime.live_inventory_entry_rfq_bias_samples_by_direction = {
        direction: deque(
            [Decimal("-5"), Decimal("5")] * 5,
            maxlen=100,
        )
    }
    runtime.live_inventory_entry_rfq_bias_sample_times_by_direction = {
        direction: deque([time.time()] * 10, maxlen=100)
    }

    context = runtime.live_inventory_basis_v4_entry_rfq_bias_context(direction)

    assert context["v4_entry_rfq_bias_ready"] is False
    assert context["v4_entry_rfq_bias_ready_reason"] == "mad_exceeds_limit"
    assert context["v4_entry_rfq_bias_applied_bps"] == "0"


def test_v4_entry_rfq_exploration_is_rate_limited() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    direction = "short_var_long_lighter"
    now = time.monotonic()

    assert runtime.live_inventory_basis_v4_entry_rfq_exploration_due(direction, now)
    assert not runtime.live_inventory_basis_v4_entry_rfq_exploration_due(direction, now + 1.0)
    assert runtime.live_inventory_basis_v4_entry_rfq_exploration_due(direction, now + 300.0)


def test_v4_entry_rfq_exploration_requires_near_threshold_edge() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)

    assert runtime.live_inventory_basis_v4_entry_rfq_exploration_allowed(
        predicted_exact_edge_bps=Decimal("1.50"),
        exact_entry_threshold_bps=Decimal("2.00"),
        bias_ready=True,
    )
    assert not runtime.live_inventory_basis_v4_entry_rfq_exploration_allowed(
        predicted_exact_edge_bps=Decimal("1.00"),
        exact_entry_threshold_bps=Decimal("2.00"),
        bias_ready=True,
    )
    assert not runtime.live_inventory_basis_v4_entry_rfq_exploration_allowed(
        predicted_exact_edge_bps=Decimal("2.00"),
        exact_entry_threshold_bps=Decimal("2.00"),
        bias_ready=True,
    )


def test_variational_stream_health_is_cached_and_reports_prices_stream() -> None:
    async def run() -> None:
        runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
        calls = 0
        last_frame_at = datetime.now(timezone.utc).isoformat()

        async def fake_fetch() -> dict:
            nonlocal calls
            calls += 1
            return {
                "ok": True,
                "result": {
                    "active": True,
                    "sockets": {"websocket": "connected"},
                    "streams": [
                        {
                            "url": "wss://example.test/prices",
                            "frameCount": 12,
                            "lastFrameAt": last_frame_at,
                        }
                    ],
                },
            }

        runtime.fetch_variational_stream_health = fake_fetch

        first = await runtime.live_inventory_variational_stream_health()
        second = await runtime.live_inventory_variational_stream_health()

        assert calls == 1
        assert first["variational_stream_health_ok"] is True
        assert first["variational_prices_stream_present"] is True
        assert first["variational_prices_stream_frame_count"] == 12
        assert first == second

    asyncio.run(run())


def test_v4_exact_rfq_threshold_translates_passive_threshold_by_directional_bias() -> None:
    assert v4_exact_rfq_entry_threshold(Decimal("5.10"), Decimal("-3.00")) == Decimal("2.10")
    assert v4_exact_rfq_entry_threshold(Decimal("5.10"), None) == Decimal("5.10")
    assert v4_exact_rfq_entry_threshold(None, Decimal("-3.00")) is None


def test_v4_exact_rfq_gradient_thresholds_use_the_same_translation() -> None:
    assert v4_exact_rfq_gradient_thresholds(
        [Decimal("5.10"), Decimal("5.96"), Decimal("6.82")],
        Decimal("-3.00"),
    ) == [Decimal("2.10"), Decimal("2.96"), Decimal("3.82")]


def test_variational_order_reuses_final_quote_id(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_variational_order_limiter = RollingWindowRateLimiter(
            normal_limit=30,
            hard_limit=36,
        )
        runtime.variational_submit_transport = "api"
        runtime.variational_api_max_slippage = 0.005
        captured: dict = {}

        async def fake_send_variational_command(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "type": "VAR_API_ORDER_RESULT"}

        runtime.send_variational_command = fake_send_variational_command

        result = await runtime.send_variational_place_order(
            asset="ETH",
            side="SELL",
            amount="0.008",
            expected_min_btc_qty=None,
            confirm=True,
            reduce_only=False,
            reuse_quote_id="quote-final",
        )

        assert result["ok"] is True
        assert captured["payload"]["type"] == "VAR_API_ORDER"
        assert captured["payload"]["reuseQuoteId"] == "quote-final"
        assert captured["payload"]["requestId"]
        assert result["rfq_consumed"] is False
        assert result["rate_limit_cost"] == 0
        assert runtime.live_inventory_variational_order_limiter.snapshot()["used"] == 0

    asyncio.run(run())


def test_variational_implicit_execution_quote_consumes_one_rfq(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_variational_order_limiter = RollingWindowRateLimiter(
            normal_limit=25,
            hard_limit=30,
        )
        runtime.variational_submit_transport = "api"
        runtime.variational_api_max_slippage = 0.005

        async def fake_send_variational_command(**_kwargs):
            return {"ok": True, "type": "VAR_API_ORDER_RESULT"}

        runtime.send_variational_command = fake_send_variational_command
        result = await runtime.send_variational_place_order(
            asset="ETH",
            side="SELL",
            amount="0.008",
            expected_min_btc_qty=None,
            confirm=True,
            reduce_only=False,
        )

        assert result["rfq_consumed"] is True
        assert result["rate_limit_cost"] == 1
        assert runtime.live_inventory_variational_order_limiter.snapshot()["used"] == 1

    asyncio.run(run())


def test_variational_account_reads_do_not_consume_rfq_budget(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        captured = []

        async def fake_send_variational_command(**kwargs):
            captured.append(kwargs)
            return {"ok": True}

        runtime.send_variational_command = fake_send_variational_command
        await runtime.fetch_variational_positions()
        await runtime.fetch_variational_portfolio()
        await runtime.fetch_variational_orders(asset="ETH", status="any")

        assert len(captured) == 3
        assert all(item["lane"] == "read" for item in captured)
        assert all("rate_limit_cost" not in item for item in captured)

    asyncio.run(run())


@pytest.mark.parametrize(
    ("refresh_entry_quote", "expected_quote_id", "lighter_started"),
    [
        (True, "refresh-quote", True),
        (False, "signal-quote", True),
        (False, "signal-quote", False),
    ],
)
def test_live_inventory_basis_real_entry_submits_var_and_lighter_concurrently(
    tmp_path,
    refresh_entry_quote,
    expected_quote_id,
    lighter_started,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.trade_event_min_timestamp = None
        runtime.pending_auto_live_matches = []
        runtime.auto_live_match_window_seconds = 60
        runtime._record_lock = asyncio.Lock()
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        runtime.lighter_client_order_to_trade_key = {}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_max_total_notional_usd = Decimal("25")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.lighter_order_book = {
            "bids": {Decimal("1755.00"): Decimal("1")},
            "asks": {Decimal("1755.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1755.00")
        runtime.lighter_best_ask = Decimal("1755.10")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("-999")
        runtime.live_inventory_basis_refresh_entry_quote_before_submit = refresh_entry_quote
        runtime.pending_live_inventory_var_fill_matches = []
        calls: list[dict] = []
        quote_calls = 0

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            nonlocal quote_calls
            quote_calls += 1
            if quote_calls == 1 or not refresh_entry_quote:
                return {
                    "quoteId": "signal-quote",
                    "bid": "1753.00",
                    "ask": "1753.25",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                    "quote_request_qty": "0.01139",
                    "quote_size_mode": "exact_base_qty_v1",
                    "quote_asset": "ETH",
                }, Decimal("10")
            return {
                "quoteId": "refresh-quote",
                "bid": "1753.10",
                "ask": "1753.35",
                    "quoteTimestamp": "2999-06-16T03:25:21.000Z",
                    "quote_request_qty": "0.01139",
                    "quote_size_mode": "exact_base_qty_v1",
                    "quote_asset": "ETH",
                }, Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            calls.append({"venue": "var", **kwargs})
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append({"venue": "lighter", **kwargs})
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = (
                "live_submit_sent" if lighter_started else "planned"
            )
            if lighter_started:
                record.lighter_fill_ts_iso = "2999-06-16T03:25:21.000Z"
                record.lighter_fill_price = Decimal("1755.00")
            runtime.records[record.trade_key] = record
            return record, {"trade_key": "entry-1"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        if not lighter_started:
            async def fail_auto_close(**_kwargs):
                raise AssertionError(
                    "must not compensate before the accepted Var order is reconciled"
                )

            runtime.try_auto_close_unhedged_live_inventory_leg = fail_auto_close

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert {call["venue"] for call in calls} == {"var", "lighter"}
        var_call = next(call for call in calls if call["venue"] == "var")
        assert var_call["confirm"] is True
        assert var_call["reuse_quote_id"] == expected_quote_id
        assert runtime.live_inventory_open_lots == []
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.pending_live_inventory_var_fill_matches[0].role == "live_inventory_entry_pending_var_fill"
        pending_context = runtime.pending_live_inventory_var_fill_matches[0].context
        if not lighter_started:
            state = json.loads(
                runtime.live_inventory_state_file.read_text(encoding="utf-8")
            )
            assert runtime.stop_flag is True
            assert pending_context["execution_unknown"] is True
            assert pending_context["reconciliation_required"] is True
            assert pending_context["lighter_submit_failed"] is True
            assert state["status"] == "manual_review_required"
            assert state["pending_actions"][0]["reconciliation_required"] is True
            return
        assert pending_context["quote_id"] == expected_quote_id
        assert pending_context["signal_quote_id"] == "signal-quote"
        assert pending_context["entry_order_reuses_quote_id"] is True
        assert pending_context["entry_order_reuses_signal_quote"] is (not refresh_entry_quote)
        assert pending_context["quote_timestamp"] == (
            "2999-06-16T03:25:21.000Z"
            if refresh_entry_quote
            else "2999-06-16T03:25:20.000Z"
        )
        assert pending_context["var_bid"] == (
            "1753.10" if refresh_entry_quote else "1753.00"
        )
        assert pending_context["var_ask"] == (
            "1753.35" if refresh_entry_quote else "1753.25"
        )
        runtime.pending_live_inventory_var_fill_matches[0].context.update(
            {
                "entry_gradient_tier": 3,
                "entry_gradient_capacity_notional_usd": "294.66",
                "entry_gradient_capacity_child_lots": 15,
                "variational_equity_usd": "100",
                "lighter_equity_usd": "98.22",
                "variational_maintenance_margin_usage_pct": "12.5",
                "lighter_maintenance_margin_usage_pct": "11.5",
                "max_projected_venue_leverage": "0.41",
            }
        )
        assert rows[-1]["event"] == "live_inventory_var_entry_submitted"
        assert rows[-1]["entry_confirmation_mode"] == "concurrent_var_and_lighter_pending_var_fill"

        await runtime.process_variational_trade_event(
            {
                "asset": "ETH",
                "side": "buy",
                "qty": var_call["amount"],
                "price": "1753.30",
                "status": "filled",
                "trade_id": "var-fill-1",
                "timestamp": "2999-06-16T03:25:21.000Z",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        assert runtime.live_inventory_open_lots[0]["entry_gradient_tier"] == 3
        assert (
            runtime.live_inventory_open_lots[0][
                "entry_gradient_capacity_notional_usd"
            ]
            == "294.66"
        )
        entered = next(
            row for row in reversed(rows) if row["event"] == "live_inventory_entered"
        )
        assert entered["entry_gradient_tier"] == 3
        assert entered["gradient_tier"] == 3
        assert entered["entry_gradient_capacity_notional_usd"] == "294.66"
        assert entered["gradient_capacity_child_lots"] == 15
        assert entered["open_child_lots"] == 1
        assert entered["open_notional_usd"] != ""
        assert entered["variational_equity_usd"] == "100"
        assert entered["lighter_equity_usd"] == "98.22"
        assert entered["max_projected_venue_leverage"] == "0.41"
        assert entered["entry_confirmation_mode"] == "concurrent_var_and_lighter_then_var_fill_confirmed"

    asyncio.run(run())


def test_live_inventory_open_notional_uses_final_fill_and_conservative_legacy_fallback(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_lot_notional_usd = Decimal("10")
    runtime.live_inventory_open_lots = [
        {"qty": "0.0002", "entry_var_fill_price": "60000"},
        {"qty": "invalid", "entry_var_fill_price": "60000"},
    ]

    assert runtime.live_inventory_open_notional_usd() == Decimal("22")


def test_live_inventory_basis_abs_entry_threshold_blocks_thin_basis(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_dry_decisions = True
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.live_inventory_basis_min_abs_entry_bps = Decimal("12")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "entry-quote",
                    "bid": "1753.00",
                    "ask": "1753.25",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots == []
        assert any(
            row["event"] == "live_inventory_entry_blocked"
            and row["reason"] == "basis_abs_entry_threshold_not_met"
            for row in rows
        )

    asyncio.run(run())


def test_live_inventory_basis_collect_only_logs_state_without_touching_inventory_state(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_dry_decisions = True
        runtime.live_inventory_collect_only = True
        runtime.live_inventory_completed_cycles = runtime.live_inventory_max_cycles
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_var_spread_bps_samples = deque(maxlen=20)
        runtime.live_inventory_basis_lighter_spread_bps_samples = deque(maxlen=20)
        runtime.live_inventory_basis_sample_move_bps_samples = deque(maxlen=20)
        runtime.live_inventory_basis_size_ladder_notionals_usd = []
        runtime.live_inventory_basis_reversion_mode = False
        runtime.live_inventory_basis_reversion_min_deviation_bps = Decimal("0")
        runtime.live_inventory_basis_reversion_exit_deviation_bps = Decimal("0")
        runtime.live_inventory_basis_reversion_min_net_expected_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_dynamic_sample_move_threshold_bps = lambda: (Decimal("3"), {})
        runtime.live_inventory_basis_reversion_medians = lambda **_kwargs: {
            300: None,
            1800: None,
            3600: None,
        }
        runtime.live_inventory_basis_reversion_execution_reserve_bps = lambda _direction: Decimal("0")
        runtime.live_inventory_record_stablecoin_basis_sample = lambda _context: None
        runtime.normalize_usdc_price_to_usdt = lambda _price, _context: None
        runtime.live_inventory_stablecoin_edge_context = lambda **_kwargs: (True, {})
        runtime.live_inventory_stablecoin_regime_context = lambda **_kwargs: (False, {})
        runtime.live_inventory_stablecoin_alignment = lambda **_kwargs: "unknown"
        runtime.live_inventory_dynamic_entry_quality_buffer_bps = lambda **_kwargs: Decimal("0")
        original_state = '{"status":"flat","completed_cycles":1}\n'
        runtime.live_inventory_state_file.write_text(original_state, encoding="utf-8")

        async def fake_fetch_live_inventory_stablecoin_context():
            return {}

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "collect-quote",
                    "bid": "1753.00",
                    "ask": "1753.25",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def refuse_submit(**_kwargs):
            raise AssertionError("collect-only must not submit orders")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.fetch_live_inventory_stablecoin_context = fake_fetch_live_inventory_stablecoin_context
        runtime.send_variational_place_order = refuse_submit
        runtime.place_lighter_order_from_plan = refuse_submit

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert [row["event"] for row in rows] == ["live_inventory_basis_state"]
        assert rows[0]["basis_collect_only"] is True
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 1
        assert runtime.live_inventory_state_file.read_text(encoding="utf-8") == original_state

    asyncio.run(run())


def test_live_inventory_basis_var_quote_age_guard_blocks_entry(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_dry_decisions = True
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.live_inventory_basis_max_var_quote_age_ms = 1.0
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "entry-quote",
                    "bid": "1753.00",
                    "ask": "1753.25",
                    "quoteTimestamp": "2000-01-01T00:00:00.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots == []
        assert any(row["event"] == "live_inventory_entry_blocked" and row["reason"] == "basis_var_quote_too_old" for row in rows)

    asyncio.run(run())


def test_live_inventory_basis_real_entry_rejected_after_concurrent_lighter_requires_review(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.trade_event_min_timestamp = None
        runtime.pending_auto_live_matches = []
        runtime.auto_live_match_window_seconds = 60
        runtime._record_lock = asyncio.Lock()
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        runtime.lighter_client_order_to_trade_key = {}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_max_total_notional_usd = Decimal("25")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.lighter_order_book = {
            "bids": {Decimal("1755.00"): Decimal("1")},
            "asks": {Decimal("1755.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1755.00")
        runtime.lighter_best_ask = Decimal("1755.10")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_z_entry = Decimal("4")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("7")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("4")
        runtime.pending_live_inventory_var_fill_matches = []
        calls: list[dict] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "entry-quote",
                    "bid": "1753.00",
                    "ask": "1753.25",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            calls.append({"venue": "var", **kwargs})
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append({"venue": "lighter", **kwargs})
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            runtime.records[record.trade_key] = record
            return record, {"trade_key": "entry-1"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())
        var_call = next(call for call in calls if call["venue"] == "var")
        await runtime.process_variational_trade_event(
            {
                "asset": "ETH",
                "side": "buy",
                "qty": var_call["amount"],
                "price": "1753.30",
                "status": "rejected",
                "trade_id": "var-reject-1",
                "timestamp": "2999-06-16T03:25:21.000Z",
            }
        )

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert {call["venue"] for call in calls} == {"var", "lighter"}
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.stop_flag is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "variational_rejected:pending_live_inventory_entry_pending_var_fill"

    asyncio.run(run())


def test_live_inventory_durable_order_intents_survive_match_window(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.auto_live_match_window_seconds = 0
    runtime.pending_live_inventory_var_fill_matches = [
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.01"),
            lot_id=1,
            role="live_inventory_entry_pending_lighter",
            created_at_monotonic=time.monotonic() - 3600,
        ),
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01"),
            lot_id=2,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic() - 3600,
        ),
    ]

    runtime.prune_pending_live_inventory_var_fill_matches()

    assert [
        item.role for item in runtime.pending_live_inventory_var_fill_matches
    ] == ["live_inventory_entry_pending_lighter", "live_inventory_exit"]


def test_live_inventory_basis_addon_submits_when_basis_expands(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_i_accept_basis_addon_diagnostic = True
        runtime.live_inventory_max_total_lots = 2
        runtime.live_inventory_max_total_notional_usd = Decimal("50")
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.live_inventory_basis_z_entry = Decimal("1.2")
        runtime.live_inventory_basis_z_exit = Decimal("0")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("0")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("5")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01158",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1726.62",
                "entry_lighter_fill_price": "1728.02",
                "entry_basis_bps": "-9.0",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -8.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_bid = Decimal("1726.00")
        snapshot.lighter_ask = Decimal("1726.10")
        snapshot.lighter_buy_price = Decimal("1726.10")
        snapshot.lighter_sell_price = Decimal("1726.00")
        snapshot.lighter_buy_fill_price = Decimal("1726.10")
        snapshot.lighter_sell_fill_price = Decimal("1726.00")
        runtime.lighter_order_book = {
            "bids": {Decimal("1726.00"): Decimal("1")},
            "asks": {Decimal("1726.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1726.00")
        runtime.lighter_best_ask = Decimal("1726.10")
        calls: list[dict] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "addon-quote",
                    "bid": "1724.00",
                    "ask": "1724.30",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            calls.append(kwargs)
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**kwargs):
            record = OrderLifecycle(
                trade_key="addon-entry-2",
                trade_id="",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            runtime.records[record.trade_key] = record
            return record, {"trade_key": "addon-entry-2"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert len(calls) == 1
        assert calls[0]["side"] == "BUY"
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.pending_live_inventory_var_fill_matches[0].context["entry_kind"] == "basis_addon"
        assert rows[-1]["event"] == "live_inventory_var_entry_submitted"

    asyncio.run(run())


def test_live_inventory_basis_max_hold_warn_does_not_exit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_basis_z_exit = Decimal("0")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("999")
        runtime.live_inventory_basis_max_hold_action = "warn"
        runtime.live_inventory_max_hold_samples = 300
        runtime.live_inventory_max_unrealized_loss_bps = Decimal("999")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01160",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1723.84",
                "entry_lighter_fill_price": "1725.05",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 300
        _set_test_lighter_book(runtime, bid="1755.00", ask="1755.10")
        submit_calls: list[str] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "exit-quote",
                    "bid": "1723.01",
                    "ask": "1723.43",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert submit_calls == []
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_completed_cycles == 0
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "basis_max_hold_reached_waiting_for_reversion"

    asyncio.run(run())


def test_live_inventory_basis_exit_submits_var_before_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("-999")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        calls: list[str] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"},
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            calls.append("var")
            return {"ok": True, "result": {"quoteId": "exit-quote"}}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            calls.append("lighter")
            assert calls == ["var", "lighter"]

            record = OrderLifecycle(
                trade_key="lighter-exit",
                trade_id="",
                side="buy",
                qty=Decimal("0.01"),
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "lighter_filled"
            record.lighter_fill_ts_iso = "2999-06-16T03:25:21.000Z"
            record.lighter_fill_price = Decimal("1710")
            return record, {"trade_key": "lighter-exit"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")
        _set_test_lighter_book(runtime, bid="1709.90", ask="1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        assert calls == ["var", "lighter"]
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 1

    asyncio.run(run())


def test_v4_exit_reuses_latest_exact_main_loop_quote_after_two_of_three(
    tmp_path,
) -> None:
    class ExitSubmitted(RuntimeError):
        pass

    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_basis_v4_profile = "test-v4"
        runtime.live_inventory_basis_v4_bidirectional = False
        runtime.live_inventory_basis_v4_real_gradient = False
        runtime.live_inventory_basis_v4_tier_independent_exit = True
        runtime.live_inventory_basis_v4_history_ready = True
        runtime.live_inventory_basis_v4_history_reason = "ready"
        runtime.live_inventory_basis_v4_history_ready_by_direction = {}
        runtime.live_inventory_basis_v4_history_reason_by_direction = {}
        runtime.live_inventory_v4_rearm_required = False
        runtime.live_inventory_v4_rearm_direction = None
        runtime.live_inventory_v4_episode_id = "episode-1"
        runtime.live_inventory_v4_next_tranche_index = 2
        runtime.live_inventory_v4_last_entry_submit_monotonic = 0.0
        runtime.live_inventory_v4_gradient_tier_exit_confirmations = {}
        runtime.live_inventory_v4_portfolio_exit_lot_ids = set()
        runtime.live_inventory_v4_portfolio_exit_context = {}
        runtime.live_inventory_v4_portfolio_exit_confirmations = deque(maxlen=3)
        runtime.live_inventory_v4_partial_detier_confirmations = deque(maxlen=3)
        runtime.live_inventory_basis_refresh_exit_quote_before_submit = True
        runtime.live_inventory_basis_max_var_quote_age_ms = 1500.0
        runtime.live_inventory_basis_z_exit = Decimal("0")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = 0.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "asset": "ETH",
                "lot_id": 1,
                "episode_id": "episode-1",
                "signal_mode": "basis",
                "direction": "short_var_long_lighter",
                "entry_kind": "basis_v4_eth_short_p97_5",
                "qty": "0.20000",
                "entry_var_side": "SELL",
                "entry_var_fill_price": "100",
                "entry_lighter_fill_price": "100",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        runtime.live_inventory_basis_quote_priority = lambda _snapshot: "trade"
        runtime.live_inventory_basis_v4_entry_directions = lambda: [
            "short_var_long_lighter"
        ]
        runtime.live_inventory_basis_v4_entry_direction = (
            lambda: "short_var_long_lighter"
        )
        runtime.live_inventory_basis_v4_select_entry_direction = (
            lambda **_kwargs: "short_var_long_lighter"
        )
        runtime.live_inventory_basis_v4_entry_threshold = lambda **_kwargs: (
            Decimal("999"),
            {"v4_anchor_ready": True, "v4_health_ready": True},
        )
        runtime.record_live_inventory_basis_v4_edge = lambda **_kwargs: False
        runtime.live_inventory_basis_v4_update_rearm = lambda **_kwargs: (
            False,
            {},
        )
        runtime.live_inventory_basis_v4_exit_shortfall_reserve_bps = (
            lambda: Decimal("0.5")
        )
        runtime.live_inventory_basis_v4_exit_calibration_payload = (
            lambda **_kwargs: {"v4_exit_shortfall_reserve_bps": "0.5"}
        )
        runtime.live_inventory_order_limiter = lambda _venue: SimpleNamespace(
            snapshot=lambda: {
                "used": 0,
                "normal_limit": 30,
                "hard_limit": 36,
                "backoff_seconds": 0,
            }
        )
        runtime.live_inventory_external_reference_context = lambda **_kwargs: {}

        async def fake_stablecoin_context():
            return {}

        async def fake_account_risk_context(**_kwargs):
            return {"risk_action": "normal", "risk_reason": "account_risk_normal"}

        async def fake_shadow_gradient(**_kwargs):
            return None

        async def fake_persist(**_kwargs):
            return None

        quotes = iter(
            (
                ("main-loop-1", "99.97", "99.98"),
                ("main-loop-2", "100.02", "100.03"),
                ("main-loop-3", "99.97", "99.98"),
            )
        )

        async def fake_fetch_live_inventory_basis_quote(**kwargs):
            quote_id, bid, ask = next(quotes)
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": quote_id,
                    "bid": bid,
                    "ask": ask,
                    "quoteTimestamp": datetime.now(timezone.utc).isoformat(),
                },
                asset="ETH",
                qty=kwargs["qty"],
            ), Decimal("50")

        submitted: list[dict] = []

        async def fake_submit_live_inventory_exit_pair(**kwargs):
            submitted.append(kwargs)
            raise ExitSubmitted

        async def refuse_fast_refresh(**_kwargs):
            raise AssertionError("exact current quote must bypass slow refresh")

        runtime.fetch_live_inventory_stablecoin_context = fake_stablecoin_context
        runtime.live_inventory_account_risk_context = fake_account_risk_context
        runtime.maybe_update_live_inventory_basis_v4_shadow_gradient = (
            fake_shadow_gradient
        )
        runtime.persist_live_inventory_memory = fake_persist
        runtime.fetch_live_inventory_basis_quote = (
            fake_fetch_live_inventory_basis_quote
        )
        runtime.submit_live_inventory_exit_pair = (
            fake_submit_live_inventory_exit_pair
        )
        runtime.live_inventory_basis_v4_fast_refresh_exit_context = (
            refuse_fast_refresh
        )
        snapshot = _eth_inventory_snapshot()
        snapshot.var_bid = Decimal("99.97")
        snapshot.var_ask = Decimal("99.98")
        snapshot.var_mid = Decimal("99.975")
        snapshot.lighter_bid = Decimal("100.02")
        snapshot.lighter_ask = Decimal("100.03")
        snapshot.lighter_mid = Decimal("100.025")
        snapshot.lighter_buy_price = Decimal("100.03")
        snapshot.lighter_sell_price = Decimal("100.02")
        snapshot.lighter_buy_fill_price = Decimal("100.03")
        snapshot.lighter_sell_fill_price = Decimal("100.02")
        _set_test_lighter_book(runtime, bid="100.02", ask="100.03")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        assert submitted == []
        assert runtime.live_inventory_open_lots[0][
            "v4_exit_confirmation_window"
        ] == [True]

        await runtime.maybe_run_live_inventory_basis(snapshot)

        assert submitted == []
        assert runtime.live_inventory_open_lots[0][
            "v4_exit_confirmation_window"
        ] == [True, False]

        with pytest.raises(ExitSubmitted):
            await runtime.maybe_run_live_inventory_basis(snapshot)

        assert len(submitted) == 1
        assert submitted[0]["reuse_quote_id"] == "main-loop-3"
        assert submitted[0]["qty"] == Decimal("0.20000")
        assert Decimal(
            submitted[0]["exit_lighter_depth"]["requested_qty"]
        ) == Decimal("0.2")

    asyncio.run(run())


def test_live_inventory_basis_exit_reconciles_var_no_position_before_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("-999")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        calls: list[str] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"},
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            calls.append("var")
            return {"ok": False, "error": {"error_message": ": No position exists for ETH to reduce"}}

        async def fake_fetch_variational_positions():
            calls.append("positions")
            return {"ok": True, "result": {"positions": []}}

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append("lighter")
            assert kwargs["reduce_only"] is True

            record = OrderLifecycle(
                trade_key="lighter-exit",
                trade_id="",
                side="buy",
                qty=Decimal("0.01"),
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "lighter_filled"
            record.lighter_fill_ts_iso = "2999-06-16T03:25:21.000Z"
            record.lighter_fill_price = Decimal("1710")
            return record, {"trade_key": "lighter-exit"}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.fetch_variational_positions = fake_fetch_variational_positions
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")
        _set_test_lighter_book(runtime, bid="1709.90", ask="1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert calls == ["var", "positions", "lighter"]
        assert any(row["event"] == "live_inventory_var_exit_reconciled_flat" for row in rows)
        assert rows[-1]["event"] == "live_inventory_exited"
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 1

    asyncio.run(run())


def test_live_inventory_basis_exit_can_skip_blocked_first_lot(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("2")
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1710",
                "entry_lighter_fill_price": "1710",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            },
            {
                "lot_id": 2,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            },
        ]
        runtime.live_inventory_sample_index = 5
        exited_lots: list[int] = []

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"},
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**kwargs):
            return {"ok": True, "result": {"quoteId": "exit-quote", "amount": kwargs["amount"]}}

        async def fake_place_lighter_order_from_plan(**kwargs):
            exited_lots.append(kwargs["cycle_id"])

            record = OrderLifecycle(
                trade_key=f"lighter-exit-{kwargs['cycle_id']}",
                trade_id="",
                side="buy",
                qty=Decimal("0.01"),
                asset="ETH",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "lighter_filled"
            record.lighter_fill_ts_iso = "2999-06-16T03:25:21.000Z"
            record.lighter_fill_price = Decimal("1710")
            return record, {"trade_key": record.trade_key}

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")
        _set_test_lighter_book(runtime, bid="1709.90", ask="1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        exited = [row for row in rows if row["event"] == "live_inventory_exited"]
        assert exited_lots == [2]
        assert exited[-1]["lot_id"] == 2
        assert [lot["lot_id"] for lot in runtime.live_inventory_open_lots] == [1]
        assert runtime.live_inventory_completed_cycles == 1

    asyncio.run(run())


def test_live_inventory_basis_exit_safety_buffer_raises_effective_threshold(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_exit_safety_buffer_bps = Decimal("1")
        runtime.live_inventory_basis_max_hold_action = "warn"
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1710",
                "entry_lighter_fill_price": "1710",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 500

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"},
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")
        _set_test_lighter_book(runtime, bid="1709.90", ask="1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert Decimal(rows[-1]["pnl_bps"]) == Decimal("0")
        assert rows[-1]["effective_min_exit_pnl_bps"] == "1"

    asyncio.run(run())


def test_live_inventory_basis_dynamic_exit_buffer_uses_recent_shortfall(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_basis_dynamic_exit_buffer = True
        runtime.live_inventory_exit_estimate_shortfall_bps_samples.extend([Decimal("2")])
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("0")
        runtime.live_inventory_basis_max_hold_action = "warn"
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1710",
                "entry_lighter_fill_price": "1710",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 500

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {"quoteId": "exit-quote", "bid": "1710", "ask": "1710.5"},
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")
        _set_test_lighter_book(runtime, bid="1709.90", ask="1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots
        blocked = next(
            row
            for row in rows
            if row["event"] == "live_inventory_exit_blocked"
            and row.get("dynamic_exit_buffer_bps") is not None
        )
        assert blocked["dynamic_exit_buffer_bps"] == "2"
        assert blocked["effective_min_exit_pnl_bps"] == "2"

    asyncio.run(run())


def test_live_inventory_basis_refresh_exit_quote_blocks_stale_profitable_exit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("1")
        runtime.live_inventory_basis_refresh_exit_quote_before_submit = True
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        runtime.live_inventory_sample_index = 5
        quotes = [
            {"quoteId": "signal-quote", "bid": "1710", "ask": "1710.5"},
            {"quoteId": "refresh-quote", "bid": "1700", "ask": "1700.5"},
        ]

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                quotes.pop(0),
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_get_lighter_best_bid_ask():
            _set_test_lighter_book(runtime, bid="1719", ask="1720")
            return Decimal("1719"), Decimal("1720")

        async def fake_send_variational_place_order(**_kwargs):
            raise AssertionError("should not submit Var exit after stale refresh")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.get_lighter_best_bid_ask = fake_get_lighter_best_bid_ask
        runtime.send_variational_place_order = fake_send_variational_place_order
        snapshot = _eth_inventory_snapshot()
        snapshot.lighter_ask = Decimal("1710")
        snapshot.lighter_buy_price = Decimal("1710")
        snapshot.lighter_buy_fill_price = Decimal("1710")
        _set_test_lighter_book(runtime, bid="1709.90", ask="1710")

        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "basis_exit_refresh_pnl_below_threshold"

    asyncio.run(run())


def test_live_inventory_basis_refresh_exit_quote_requires_quote_id(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_basis_z_exit = Decimal("999")
        runtime.live_inventory_basis_min_exit_pnl_bps = Decimal("1")
        runtime.live_inventory_basis_refresh_exit_quote_before_submit = True
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -10.0
        runtime.live_inventory_basis_state.var = 1.0
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "signal_mode": "basis",
                "direction": "long_var_short_lighter",
                "qty": "0.01",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "1700",
                "entry_lighter_fill_price": "1720",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 1,
                "status": "open",
            }
        ]
        quotes = [
            {
                "quoteId": "signal-quote",
                "bid": "1753",
                "ask": "1753.5",
            },
            {"bid": "1730", "ask": "1730.5"},
        ]

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                quotes.pop(0),
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_get_lighter_best_bid_ask():
            _set_test_lighter_book(runtime, bid="1719", ask="1720")
            return Decimal("1719"), Decimal("1720")

        async def fake_send_variational_place_order(**_kwargs):
            raise AssertionError("must not submit after a refreshed quote lost its quote id")

        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.get_lighter_best_bid_ask = fake_get_lighter_best_bid_ask
        runtime.send_variational_place_order = fake_send_variational_place_order
        _set_test_lighter_book(runtime, bid="1719", ask="1720")

        snapshot = _eth_inventory_snapshot()
        snapshot.var_sell_price = Decimal("1753")
        snapshot.lighter_buy_price = Decimal("1755.1")
        snapshot.lighter_buy_fill_price = Decimal("1755.1")
        await runtime.maybe_run_live_inventory_basis(snapshot)

        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert runtime.live_inventory_open_lots
        assert rows[-1]["event"] == "live_inventory_exit_blocked"
        assert rows[-1]["reason"] == "basis_exit_refresh_quote_id_missing"

    asyncio.run(run())


def test_live_inventory_basis_refreshed_exit_context_uses_lighter_depth(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {"quoteId": "refresh-1", "bid": "99.8", "ask": "99.9"},
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("12")

        async def fake_get_lighter_best_bid_ask():
            return Decimal("100.2"), Decimal("100.3")

        async def fake_live_inventory_lighter_depth_context(**_kwargs):
            return {
                "estimated_fill_price": "100.1",
                "reference_price": "100.2",
                "slippage_bps": "1.0",
            }

        runtime.fetch_live_inventory_basis_quote = (
            fake_fetch_live_inventory_basis_quote
        )
        runtime.get_lighter_best_bid_ask = fake_get_lighter_best_bid_ask
        runtime.live_inventory_lighter_depth_context = (
            fake_live_inventory_lighter_depth_context
        )

        context = await runtime.live_inventory_basis_refreshed_exit_context(
            asset="ETH",
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
        )

        assert context["reason"] is None
        assert context["refreshed_pnl_bps"] == Decimal("30.000")
        assert context["executable_pnl_bps"] == Decimal("20.000")
        assert context["executable_lighter_exit_price"] == Decimal("100.1")
        assert context["refresh_quote_ms"] == Decimal("12")

    asyncio.run(run())


def test_v4_fast_refresh_accepts_latest_and_two_of_three_executable_quotes(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_v4_strong_single_enabled = False
        lot: dict[str, object] = {
            "v4_exit_confirmation_window": [True, True],
            "v4_exit_confirmation_count": 2,
        }
        contexts = [
            {
                "reason": None,
                "refresh_quote_ms": Decimal("10"),
                "refreshed_pnl_bps": Decimal("4.8"),
                "executable_pnl_bps": Decimal("4.8"),
                "exit_lighter_depth": {"slippage_bps": "0.2"},
            },
            {
                "reason": None,
                "refresh_quote_ms": Decimal("11"),
                "refreshed_pnl_bps": Decimal("5.0"),
                "executable_pnl_bps": Decimal("4.4"),
                "exit_lighter_depth": {"slippage_bps": "0.2"},
                "refreshed_var_exit_price": Decimal("99.9"),
                "executable_lighter_exit_price": Decimal("100.2"),
                "executable_pnl": Decimal("0.003"),
            },
            {
                "reason": None,
                "refresh_quote_ms": Decimal("9"),
                "refreshed_pnl_bps": Decimal("5.1"),
                "executable_pnl_bps": Decimal("4.9"),
                "exit_lighter_depth": {"slippage_bps": "0.2"},
                "refreshed_var_exit_price": Decimal("99.9"),
                "executable_lighter_exit_price": Decimal("100.2"),
                "executable_pnl": Decimal("0.0031"),
            },
        ]

        async def fake_refreshed_exit_context(**_kwargs):
            return contexts.pop(0)

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot=lot,
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("4.5"),
        )

        assert result["confirmed"] is True
        assert result["attempts"] == 1
        assert result["confirmation_count"] == 3
        assert result["observations"][0]["confirmation_count"] == 3
        assert result["confirmation_mode"] == "two_of_three"
        assert result["selected_context"]["executable_pnl_bps"] == Decimal(
            "4.8"
        )
        assert lot["executable_exit_mfe_pnl_bps"] == "4.8"

    asyncio.run(run())


def test_v4_fast_refresh_requires_passive_confirmation_before_rfq(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_dynamic_exit_buffer = True
        runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
            [Decimal("3.00")] * 20,
            maxlen=20,
        )
        runtime.live_inventory_strong_single_shortfall_bps_samples = deque(
            [Decimal("3.00")] * 3,
            maxlen=20,
        )
        lot: dict[str, object] = {}
        calls = 0
        executable_values = iter((Decimal("2.40"), Decimal("3.50")))

        async def fake_refreshed_exit_context(**_kwargs):
            nonlocal calls
            calls += 1
            executable_pnl_bps = next(executable_values)
            return {
                "reason": None,
                "refresh_quote_ms": Decimal("42"),
                "refreshed_pnl_bps": executable_pnl_bps,
                "executable_pnl_bps": executable_pnl_bps,
                "exit_lighter_depth": {"slippage_bps": "0"},
                "refreshed_var_exit_price": Decimal("99.9"),
                "executable_lighter_exit_price": Decimal("100.2"),
                "executable_pnl": Decimal("0.00378"),
            }

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot=lot,
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("2.50"),
        )

        assert result["confirmed"] is False
        assert result["attempts"] == 0
        assert result["confirmation_count"] == 0
        assert result["confirmation_mode"] is None
        assert result["strong_single_threshold_bps"] == Decimal("3.50")
        assert result["observations"] == []
        assert calls == 0

    asyncio.run(run())


def test_v4_fast_refresh_rejects_unstable_strong_single_spike(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_dynamic_exit_buffer = True
        runtime.live_inventory_exit_estimate_shortfall_bps_samples = deque(
            [Decimal("3.00")] * 20,
            maxlen=20,
        )
        runtime.live_inventory_strong_single_shortfall_bps_samples = deque(
            [Decimal("3.00")] * 3,
            maxlen=20,
        )
        executable_values = iter(
            (
                Decimal("2.40"),
                Decimal("5.00"),
                Decimal("1.00"),
                Decimal("1.10"),
                Decimal("1.20"),
                Decimal("1.30"),
            )
        )

        async def fake_refreshed_exit_context(**_kwargs):
            executable_pnl_bps = next(executable_values)
            return {
                "reason": None,
                "refresh_quote_ms": Decimal("42"),
                "refreshed_pnl_bps": executable_pnl_bps,
                "executable_pnl_bps": executable_pnl_bps,
                "exit_lighter_depth": {"slippage_bps": "0"},
            }

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot={},
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("2.50"),
        )

        assert result["confirmed"] is False
        assert result["attempts"] == 0
        assert result["max_executable_pnl_bps"] is None
        assert result["observations"] == []

    asyncio.run(run())


def test_v4_fast_refresh_exhausts_with_only_one_eligible_quote_per_window(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        lot: dict[str, object] = {
            "v4_exit_confirmation_window": [True, True],
            "v4_exit_confirmation_count": 2,
        }
        executable_values = iter(
            (
                Decimal("4.4"),
            )
        )

        async def fake_refreshed_exit_context(**_kwargs):
            executable_pnl_bps = next(executable_values)
            return {
                "reason": None,
                "refresh_quote_ms": Decimal("10"),
                "refreshed_pnl_bps": Decimal("5.0"),
                "executable_pnl_bps": executable_pnl_bps,
                "exit_lighter_depth": {"slippage_bps": "0.2"},
            }

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot=lot,
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("4.5"),
        )

        assert result["confirmed"] is False
        assert result["attempts"] == 1
        assert result["confirmation_count"] == 2
        assert result["last_block_reason"] == (
            "basis_exit_lighter_depth_pnl_below_threshold"
        )
        assert result["max_executable_pnl_bps"] == Decimal("4.4")

    asyncio.run(run())


def test_v4_fast_refresh_does_not_retry_unavailable_quote(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        calls = 0

        async def fake_refreshed_exit_context(**_kwargs):
            nonlocal calls
            calls += 1
            return {
                "reason": "basis_exit_refresh_quote_unavailable",
                "refresh_quote_ms": None,
            }

        runtime.live_inventory_basis_refreshed_exit_context = (
            fake_refreshed_exit_context
        )

        result = await runtime.live_inventory_basis_v4_fast_refresh_exit_context(
            asset="ETH",
            lot={
                "v4_exit_confirmation_window": [True, True],
                "v4_exit_confirmation_count": 2,
            },
            direction="short_var_long_lighter",
            qty=Decimal("0.01"),
            entry_var_price=Decimal("100"),
            entry_lighter_price=Decimal("100"),
            exit_lighter_side="SELL",
            effective_min_exit_pnl_bps=Decimal("4.5"),
        )

        assert result["confirmed"] is False
        assert result["attempts"] == 1
        assert result["last_block_reason"] == (
            "basis_exit_refresh_quote_unavailable"
        )
        assert calls == 1

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_timeout_requires_manual_review(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 31,
                context={"direction": "long_var_short_lighter", "quote_id": "quote-1"},
            )
        ]

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "0"}]}}

        runtime.fetch_variational_positions = fake_fetch_variational_positions

        timed_out = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert timed_out is True
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        pending_context = runtime.pending_live_inventory_var_fill_matches[0].context
        assert pending_context["execution_unknown"] is True
        assert pending_context["reconciliation_required"] is True
        assert pending_context["execution_unknown_reason"] == "basis_entry_var_fill_timeout"
        assert runtime.stop_flag is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "basis_entry_var_fill_timeout"
        assert state["manual_review_context"]["lot_id"] == 1
        assert state["manual_review_context"]["variational_position_qty"] == "0"
        assert state["pending_actions"][0]["reconciliation_required"] is True
        assert rows[-1]["event"] == "live_inventory_manual_review_required"
        assert rows[-1]["reason"] == "basis_entry_var_fill_timeout"

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_timeout_detects_var_position(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 31,
            )
        ]

        async def fake_fetch_variational_positions():
            return {"ok": True, "result": {"positions": [{"instrument": {"underlying": "ETH"}, "qty": "0.011535"}]}}

        runtime.fetch_variational_positions = fake_fetch_variational_positions

        timed_out = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert timed_out is True
        assert state["status"] == "manual_review_required"
        assert state["manual_review_reason"] == "basis_entry_var_fill_timeout_position_detected"
        assert state["manual_review_context"]["variational_position_qty"] == "0.011535"
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        pending_context = runtime.pending_live_inventory_var_fill_matches[0].context
        assert pending_context["execution_unknown"] is True
        assert pending_context["reconciliation_required"] is True
        assert pending_context["execution_unknown_reason"] == (
            "basis_entry_var_fill_timeout_position_detected"
        )
        assert runtime.stop_flag is True

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_orders_rejected_clears_without_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.live_inventory_next_lot_id = 2
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 3,
                context={"rfq_id": "rfq-rejected", "direction": "long_var_short_lighter"},
            )
        ]
        calls: list[str] = []

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-rejected",
                                "order_id": "order-rejected",
                                "status": "rejected",
                                "clearing_status": "rejected_failed_taker_funding",
                                "side": "buy",
                                "qty": "20",
                            }
                        ]
                    }
                },
            }

        async def fake_place_lighter_order_from_plan(**_kwargs):
            calls.append("lighter")
            return None, None

        runtime.fetch_variational_orders = fake_fetch_variational_orders
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert resolved is True
        assert calls == []
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.stop_flag is False
        assert state["status"] == "flat"
        assert state["last_rejected_reason"] == "variational_order_rejected"
        assert rows[-1]["event"] == "live_inventory_var_entry_final_rejected"
        assert rows[-1]["clearing_status"] == "rejected_failed_taker_funding"

    asyncio.run(run())


def test_live_inventory_basis_taker_funding_reject_cooldown_blocks_next_entry(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.live_inventory_signal_mode = "basis"
        runtime.live_allowed_assets = {"ETH"}
        runtime.accepted_assets = {"ETH"}
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_max_total_notional_usd = Decimal("25")
        runtime.live_max_notional_usd = Decimal("25")
        runtime.risk_guard_max_base_amount = 10_000_000
        runtime.live_inventory_basis_state = LiveInventoryBasisState(
            half_life_seconds=300,
            warmup_samples=1,
            gap_reset_seconds=30,
            sigma_floor_bps=0,
        )
        runtime.live_inventory_basis_state.mean = -7.0
        runtime.live_inventory_basis_state.var = 0.1
        runtime.live_inventory_basis_state.seen = 10
        runtime.live_inventory_basis_state.last_ts = time.monotonic()
        runtime.live_inventory_basis_z_entry = Decimal("0")
        runtime.live_inventory_basis_min_entry_edge_bps = Decimal("-999")
        runtime.live_inventory_basis_max_entry_roundtrip_cost_bps = Decimal("999")
        runtime.live_inventory_basis_min_entry_quality_score_bps = Decimal("-999")
        runtime.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = True
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 3,
                context={"rfq_id": "rfq-rejected", "direction": "long_var_short_lighter"},
            )
        ]
        calls: list[str] = []

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-rejected",
                                "order_id": "order-rejected",
                                "status": "rejected",
                                "clearing_status": "rejected_failed_taker_funding",
                                "side": "buy",
                                "qty": "20",
                            }
                        ]
                    }
                },
            }

        async def fake_fetch_live_inventory_basis_quote(**_kwargs):
            return _test_basis_quote_with_metadata(
                {
                    "quoteId": "entry-quote",
                    "bid": "1753.00",
                    "ask": "1753.25",
                    "quoteTimestamp": "2999-06-16T03:25:20.000Z",
                },
                asset="ETH",
                qty=_kwargs["qty"],
            ), Decimal("10")

        async def fake_send_variational_place_order(**_kwargs):
            calls.append("var")
            return {"ok": True}

        runtime.fetch_variational_orders = fake_fetch_variational_orders
        runtime.fetch_live_inventory_basis_quote = fake_fetch_live_inventory_basis_quote
        runtime.send_variational_place_order = fake_send_variational_place_order

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")
        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert resolved is True
        assert calls == []
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert state["last_blocked_reason"] == "variational_taker_funding_reject_cooldown_active"
        assert rows[-1]["event"] == "live_inventory_entry_blocked"
        assert rows[-1]["reason"] == "variational_taker_funding_reject_cooldown_active"

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_cleared_without_fill_details_is_retained(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_var_fill",
                created_at_monotonic=time.monotonic() - 31,
                context={"rfq_id": "rfq-cleared-missing"},
            )
        ]

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-cleared-missing",
                                "order_id": "order-cleared-missing",
                                "status": "cleared",
                            }
                        ]
                    }
                },
            }

        runtime.fetch_variational_orders = fake_fetch_variational_orders

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(
            asset="ETH"
        )

        state = json.loads(
            runtime.live_inventory_state_file.read_text(encoding="utf-8")
        )
        assert resolved is True
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        pending_context = runtime.pending_live_inventory_var_fill_matches[0].context
        assert pending_context["execution_unknown"] is True
        assert pending_context["reconciliation_required"] is True
        assert state["status"] == "manual_review_required"
        assert state["pending_actions"][0]["rfq_id"] == "rfq-cleared-missing"
        assert state["pending_actions"][0]["reconciliation_required"] is True

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_orders_cleared_submits_lighter(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.mode = "live"
        runtime._record_lock = asyncio.Lock()
        runtime.records = {}
        runtime.record_order = deque(maxlen=1000)
        runtime.lighter_client_order_to_trade_key = {}
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 3,
                context={
                    "rfq_id": "rfq-cleared",
                    "direction": "long_var_short_lighter",
                    "var_side": "BUY",
                    "lighter_price": "1755.00",
                },
            )
        ]
        calls: list[dict] = []

        async def fake_fetch_variational_orders(**_kwargs):
            return {
                "ok": True,
                "result": {
                    "orders": {
                        "result": [
                            {
                                "rfq_id": "rfq-cleared",
                                "order_id": "order-cleared",
                                "status": "cleared",
                                "clearing_status": "success_trades_booked_into_pool",
                                "side": "buy",
                                "qty": "0.01141",
                                "price": "1751.58",
                                "execution_timestamp": "2026-06-18T00:53:48.608Z",
                            }
                        ]
                    }
                },
            }

        async def fake_place_lighter_order_from_plan(**kwargs):
            calls.append(kwargs)
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="entry-1",
                side=str(kwargs["side"]).lower(),
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="submitted",
                var_fill_price=kwargs["var_fill_price"],
                lighter_fill_price=Decimal("1755.00"),
            )
            record.processing_stage = "lighter_filled"
            record.lighter_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            return record, {"trade_key": "entry-1"}

        runtime.fetch_variational_orders = fake_fetch_variational_orders
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        resolved = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert resolved is True
        assert len(calls) == 1
        assert calls[0]["qty"] == Decimal("0.01141")
        assert calls[0]["var_fill_price"] == Decimal("1751.58")
        assert runtime.pending_live_inventory_var_fill_matches == []
        assert runtime.live_inventory_open_lots[0]["entry_var_price_source"] == "final_fill"
        events = [row["event"] for row in rows]
        assert "variational_fill" in events
        assert "live_inventory_entered" in events
        assert events.index("variational_fill") < events.index(
            "live_inventory_entered"
        )

    asyncio.run(run())


def test_live_inventory_basis_pending_entry_before_timeout_does_not_stop(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.stop_flag = False
        runtime.auto_live_match_window_seconds = 30
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="buy",
                qty=Decimal("0.011535"),
                lot_id=1,
                role="live_inventory_entry_pending_lighter",
                created_at_monotonic=time.monotonic() - 29,
            )
        ]

        timed_out = await runtime.maybe_timeout_pending_live_inventory_var_entry(asset="ETH")

        assert timed_out is False
        assert len(runtime.pending_live_inventory_var_fill_matches) == 1
        assert runtime.stop_flag is False
        assert not runtime.live_inventory_state_file.exists()

    asyncio.run(run())


def test_v4_pending_entry_reconciles_before_cycle_and_batch_gates(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.auto_live_match_window_seconds = 30
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="sell",
                qty=Decimal("0.01"),
                lot_id=2,
                role="live_inventory_entry_pending_var_fill",
                created_at_monotonic=time.monotonic(),
            )
        ]
        calls: list[str] = []

        async def fake_reconcile_pending_entry(*, asset: str) -> bool:
            calls.append(asset)
            return True

        def refuse_batch_gate(**_kwargs):
            raise AssertionError(
                "batch gate ran before pending entry reconciliation"
            )

        runtime.maybe_timeout_pending_live_inventory_var_entry = (
            fake_reconcile_pending_entry
        )
        runtime.live_inventory_v4_batch_entry_gate = refuse_batch_gate

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        assert calls == ["ETH"]

    asyncio.run(run())


def test_v4_pending_addon_reconciles_while_existing_lot_is_open(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.auto_live_match_window_seconds = 30
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "asset": "ETH",
                "direction": "short_var_long_lighter",
                "qty": "0.0082",
                "status": "open",
            }
        ]
        runtime.pending_live_inventory_var_fill_matches = [
            PendingLiveInventoryVarFillMatch(
                asset="ETH",
                side="sell",
                qty=Decimal("0.0082"),
                lot_id=2,
                role="live_inventory_entry_pending_var_fill",
                created_at_monotonic=time.monotonic(),
            )
        ]
        calls: list[str] = []

        async def fake_reconcile_pending_entry(*, asset: str) -> bool:
            calls.append(asset)
            return True

        def refuse_batch_gate(**_kwargs):
            raise AssertionError(
                "batch gate ran before pending add-on reconciliation"
            )

        runtime.maybe_timeout_pending_live_inventory_var_entry = (
            fake_reconcile_pending_entry
        )
        runtime.live_inventory_v4_batch_entry_gate = refuse_batch_gate

        await runtime.maybe_run_live_inventory_basis(_eth_inventory_snapshot())

        assert calls == ["ETH"]

    asyncio.run(run())


def test_live_inventory_entry_blocks_below_lighter_min_base_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.lighter_min_base_amount = Decimal("0.00020")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        log_line = runtime.orders_file.read_text(encoding="utf-8").strip()

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "hedge_below_lighter_min_base_amount"
        assert "live_inventory_entry_blocked" in log_line

    asyncio.run(run())


def test_live_inventory_entry_blocks_below_lighter_min_quote_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.lighter_min_base_amount = None
        runtime.lighter_min_quote_amount = Decimal("15")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "hedge_below_lighter_min_quote_amount"

    asyncio.run(run())


def test_live_inventory_entry_blocks_below_lighter_min_base_after_quantize(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("10")
        runtime.lighter_min_base_amount = Decimal("0.05")
        runtime.live_inventory_lighter_submit_slippage_bps = Decimal("15")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "hedge_below_lighter_min_base_amount"
        assert Decimal(state["last_blocked_context"]["lighter_min_base_amount"]) == Decimal("0.05")
        assert Decimal(state["last_blocked_context"]["qty"]) > Decimal("0")

    asyncio.run(run())


def test_live_inventory_entry_blocks_high_var_spread_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_max_var_spread_bps = Decimal("1")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "var_spread_exceeds_live_inventory_limit"
        assert state["last_blocked_context"]["var_spread_bps"] == "2"

    asyncio.run(run())


def test_live_inventory_entry_blocks_stale_var_snapshot_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_max_var_snapshot_age_seconds = 5.0
        snapshot = _inventory_entry_snapshot()
        snapshot.var_timestamp = "2026-06-16T03:25:20.000Z"
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(snapshot)

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "variational_quote_snapshot_stale"
        assert state["last_blocked_context"]["var_snapshot_timestamp"] == "2026-06-16T03:25:20.000Z"

    asyncio.run(run())


def test_live_inventory_refreshes_var_quote_before_entry_and_reuses_quote_id(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("0")
        runtime.live_inventory_refresh_var_quote_before_entry = True
        runtime.lighter_min_base_amount = Decimal("0.00020")
        snapshot = _inventory_entry_snapshot()
        snapshot.var_timestamp = "2026-06-16T03:25:20.000Z"
        calls: list[dict] = []

        async def fake_send_variational_place_order(**kwargs):
            calls.append(kwargs)
            if not kwargs["confirm"]:
                return {
                    "ok": True,
                    "result": {
                        "quoteId": "fresh-entry-quote",
                        "bid": "60095",
                        "ask": "60100",
                        "quoteTimestamp": "2999-06-16T03:25:21.000Z",
                    },
                }
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side="sell",
                qty=Decimal("0.000330"),
                asset="BTC",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            return record, {"trade_key": "entry-1"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(snapshot)

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        entered = next(row for row in rows if row["event"] == "live_inventory_entered")

        assert [call["confirm"] for call in calls] == [False, True]
        assert calls[1]["reuse_quote_id"] == "fresh-entry-quote"
        assert entered["var_order_quote_id"] == "fresh-entry-quote"
        assert entered["var_order_quote_execution_price"] == "60100"
        assert entered["initial_snapshot_var_price"] == "60000"

    asyncio.run(run())


def test_live_inventory_entry_blocks_dynamic_threshold_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("70")
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "edge_bps_below_dynamic_live_inventory_entry"
        assert Decimal(state["last_blocked_context"]["live_inventory_required_entry_bps"]) == Decimal("72")

    asyncio.run(run())


def test_v4_entry_preflight_keeps_percentile_floor_without_immediate_arb_floor(
    tmp_path,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("5")
        runtime.lighter_order_book = {
            "bids": {Decimal("1755.00"): Decimal("1")},
            "asks": {Decimal("1755.10"): Decimal("1")},
        }
        runtime.lighter_best_bid = Decimal("1755.00")
        runtime.lighter_best_ask = Decimal("1755.10")

        kwargs = {
            "asset": "ETH",
            "direction": "short_var_long_lighter",
            "var_side": "SELL",
            "qty": Decimal("0.01"),
            "var_price": Decimal("1753"),
            "lighter_price": Decimal("1755.10"),
            "edge_bps": Decimal("-2"),
            "var_spread_bps": Decimal("2"),
            "var_snapshot_timestamp": "2999-06-16T03:25:20.000Z",
            "min_entry_bps": Decimal("-6"),
            "dynamic_entry_buffer_bps": Decimal("5"),
        }

        standard_ok, standard_reason, standard_context = (
            await runtime.live_inventory_entry_preflight(**kwargs)
        )
        v4_ok, v4_reason, v4_context = (
            await runtime.live_inventory_entry_preflight(
                **kwargs,
                apply_dynamic_entry_floor=False,
            )
        )

        assert standard_ok is False
        assert standard_reason == "edge_bps_below_dynamic_live_inventory_entry"
        assert Decimal(standard_context["live_inventory_required_entry_bps"]) > 0
        assert standard_context["live_inventory_dynamic_entry_floor_applied"] is True
        assert v4_ok is True
        assert v4_reason == "ok"
        assert v4_context["live_inventory_required_entry_bps"] == "-6"
        assert v4_context["live_inventory_required_entry_margin_bps"] == "4"
        assert v4_context["live_inventory_dynamic_entry_floor_applied"] is False

    asyncio.run(run())


def test_live_inventory_entry_uses_recent_execution_loss_buffer_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("0")
        runtime.live_inventory_execution_loss_bps_samples.extend(
            [Decimal("50"), Decimal("60"), Decimal("65"), Decimal("70")]
        )
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "edge_bps_below_dynamic_live_inventory_entry"
        assert Decimal(state["last_blocked_context"]["live_inventory_recent_execution_loss_buffer_bps"]) == Decimal("70")
        assert Decimal(state["last_blocked_context"]["live_inventory_required_entry_bps"]) == Decimal("72")

    asyncio.run(run())


def test_live_inventory_diagnostic_can_ignore_recent_execution_loss_buffer_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_dynamic_entry_buffer_bps = Decimal("0")
        runtime.live_inventory_ignore_recent_execution_loss_buffer_for_diagnostics = True
        runtime.lighter_min_base_amount = Decimal("0.00020")
        runtime.live_inventory_execution_loss_bps_samples.extend(
            [Decimal("50"), Decimal("60"), Decimal("65"), Decimal("70")]
        )
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {
                "ok": True,
                "result": {
                    "quoteId": "diagnostic-entry",
                    "bid": "60000",
                    "ask": "60005",
                    "quoteTimestamp": "2026-06-15T00:00:00Z",
                },
            }

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            record = OrderLifecycle(
                trade_key="entry-1",
                trade_id="",
                side="sell",
                qty=Decimal("0.000330"),
                asset="BTC",
                mode="live",
                last_variational_status="",
            )
            record.processing_stage = "live_submit_sent"
            return record, {"trade_key": "entry-1"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        entered = next(row for row in rows if row["event"] == "live_inventory_entered")

        assert sorted(submit_calls) == ["lighter", "var"]
        assert runtime.live_inventory_open_lots
        assert entered["var_order_quote_id"] == "diagnostic-entry"
        assert entered["var_order_quote_execution_price"] == "60005"

    asyncio.run(run())


def test_live_inventory_entry_blocks_lighter_depth_slippage_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_entry_bps = Decimal("10")
        runtime.live_inventory_max_lighter_slippage_bps = Decimal("1")
        runtime.lighter_order_book = {
            "bids": {
                Decimal("59990"): Decimal("0.00005"),
                Decimal("59000"): Decimal("1"),
            },
            "asks": {Decimal("60010"): Decimal("1")},
        }
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, None

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "lighter_slippage_exceeds_live_inventory_limit"
        assert Decimal(state["last_blocked_context"]["lighter_order_book_slippage_bps"]) > Decimal("1")

    asyncio.run(run())


def test_live_inventory_entry_blocks_live_cooldown_before_submit(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_cooldown_seconds = 3.0
        runtime.last_live_submit_monotonic_by_asset = {"BTC": time.monotonic()}
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert submit_calls == []
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0
        assert state["status"] == "flat"
        assert state["last_blocked_reason"] == "live_cooldown_active"
        assert state["last_blocked_context"]["live_cooldown_remaining_seconds"] is not None

    asyncio.run(run())


def test_variational_api_amount_to_str_truncates_to_min_qty_tick() -> None:
    assert variational_api_amount_to_str(Decimal("0.0002432227102505721546713663434")) == "0.000243"
    assert variational_api_amount_to_str(Decimal("0.01167603668610726774903526747"), asset="ETH") == "0.01167"
    assert variational_api_amount_to_str(Decimal("0.2898975067364933127892633559"), asset="SOL") == "0.289"


def test_live_inventory_entry_concurrent_submit_uses_formatted_var_amount(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_lot_notional_usd = Decimal("20")
        runtime.lighter_min_base_amount = Decimal("0.00020")
        submit_calls: list[str] = []
        var_amounts: list[str] = []

        async def fake_send_variational_place_order(**kwargs):
            submit_calls.append("var")
            var_amounts.append(kwargs["amount"])
            return {"ok": False, "error": "quote_qty_precision"}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert sorted(submit_calls) == ["lighter", "var"]
        assert var_amounts == ["0.000330"]
        assert state["status"] == "manual_review_required"
        assert state["manual_review_context"]["var_amount"] == "0.000330"
        assert runtime.live_inventory_open_lots == []
        assert runtime.live_inventory_completed_cycles == 0

    asyncio.run(run())


def test_live_inventory_exit_concurrent_submit_uses_formatted_var_amount(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.lighter_min_base_amount = Decimal("0.00020")
        runtime.live_inventory_max_hold_samples = 300
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "short_var_long_lighter",
                "qty": "0.0002430031523195129605107581521",
                "entry_var_side": "SELL",
                "entry_var_fill_price": "61116.43",
                "entry_lighter_fill_price": "61054.70",
                "entry_cost_status": "final_fills_confirmed",
                "entered_sample_index": 0,
                "status": "open",
            }
        ]
        submit_calls: list[str] = []
        var_amounts: list[str] = []

        async def fake_send_variational_place_order(**kwargs):
            submit_calls.append("var")
            var_amounts.append(kwargs["amount"])
            return {"ok": False, "error": "quote_qty_precision"}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))

        assert sorted(submit_calls) == ["lighter", "var"]
        assert var_amounts == ["0.000243"]
        assert state["status"] == "manual_review_required"
        assert state["manual_review_context"]["var_amount"] == "0.000243"
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_completed_cycles == 0

    asyncio.run(run())


def test_live_inventory_exit_waits_for_min_hold_samples(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_min_hold_samples = 10
        runtime.live_inventory_max_hold_samples = 300
        runtime.live_inventory_open_lots = [
            {
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "0.000301",
                "entry_var_side": "BUY",
                "entry_var_fill_price": "65636.88",
                "entry_lighter_fill_price": "65670.40",
                "entered_sample_index": 0,
                "status": "open",
            }
        ]
        submit_calls: list[str] = []

        async def fake_send_variational_place_order(**_kwargs):
            submit_calls.append("var")
            return {"ok": True}

        async def fake_place_lighter_order_from_plan(**_kwargs):
            submit_calls.append("lighter")
            return None, {"submitted": True}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        await runtime.maybe_run_live_inventory(_inventory_entry_snapshot())

        assert submit_calls == []
        assert runtime.live_inventory_open_lots[0]["status"] == "open"
        assert runtime.live_inventory_completed_cycles == 0

    asyncio.run(run())


def test_live_inventory_actual_pnl_logged_after_lighter_final_fill(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_execution_calibration = True
        runtime.live_inventory_calibration_max_cycle_loss_usd = Decimal("0.10")
        runtime.live_inventory_realized_pnl_usd = Decimal("0.02478067523956343718372446020")
        runtime.pending_live_inventory_actual_pnl["exit-1"] = {
            "asset": "ETH",
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "qty": "0.000326",
            "entry_var_price": "60679.56",
            "entry_lighter_price": "60600.4",
            "exit_var_price": "60607.99",
            "exit_lighter_estimated_price": "60605.9",
            "estimated_pnl_usd": "0.02478067523956343718372446020",
            "estimated_pnl_bps": "12.51424099597953577778085240",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {
                "trade_key": "exit-1",
                "lighter_filled_price": "60605.8",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]

        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_status"] == "lighter_final_fill_confirmed"
        assert rows[-1]["exit_lighter_final_fill_price"] == "60605.8"
        assert rows[-1]["actual_pnl_usd"] == "0.02509222"
        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["asset"] == "ETH"
        assert state["realized_pnl_usd"] == "0.02509222000000000000000000000"
        assert state["reason"] == "actual_pnl_final_fill_update"
        assert "exit-1" not in runtime.pending_live_inventory_actual_pnl
        assert getattr(runtime, "live_inventory_calibration_halted_reason", None) is None
        final_rows = [
            row
            for row in rows
            if row["event"] == "live_inventory_final_pnl"
        ]
        assert len(final_rows) == 1
        assert final_rows[0]["final_pnl_source"] == (
            "actual_pnl_confirmed_fill_reconciliation"
        )
        assert final_rows[0]["final_pnl_usd"] == "0.02509222"

    asyncio.run(run())


def test_pnl_ledger_failure_does_not_block_fill_reconciliation(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_pnl_baseline_file = tmp_path / "pnl.json"
        runtime.pending_live_inventory_actual_pnl["exit-ledger-failure"] = {
            "asset": "ETH",
            "lot_id": 9,
            "direction": "short_var_long_lighter",
            "qty": "1",
            "entry_var_price": "100",
            "entry_lighter_price": "100",
            "exit_var_price": "99.9",
            "estimated_pnl_usd": "0.1",
            "estimated_pnl_bps": "10",
        }

        def fail_record(*_args, **_kwargs):
            raise OSError("disk unavailable")

        monkeypatch.setattr("main.record_pnl_cycle", fail_record)
        await runtime.maybe_append_live_inventory_actual_pnl(
            {
                "trade_key": "exit-ledger-failure",
                "lighter_filled_price": "100",
                "lighter_filled_base_amount": "1",
            }
        )

        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_status"] == "lighter_final_fill_confirmed"
        assert "exit-ledger-failure" not in runtime.pending_live_inventory_actual_pnl
        state = json.loads(runtime.live_inventory_state_file.read_text())
        assert state["reason"] == "actual_pnl_final_fill_update"

    asyncio.run(run())


def test_live_inventory_actual_pnl_uses_leg_specific_filled_qty(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_actual_pnl["exit-qty"] = {
            "asset": "ETH",
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "qty": "0.01075",
            "entry_var_final_fill_qty": "0.01075",
            "exit_var_final_fill_qty": "0.01075",
            "entry_lighter_final_fill_qty": "0.0107",
            "entry_var_price": "1859.33",
            "entry_lighter_price": "1861.33",
            "exit_var_price": "1914.82",
            "estimated_pnl_usd": "-0.003136",
            "estimated_pnl_bps": "-1.568957260048286961082131736",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {
                "trade_key": "exit-qty",
                "lighter_filled_price": "1916.531869158878504672897196",
                "lighter_filled_base_amount": "0.0107",
            }
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        row = rows[-1]
        assert row["event"] == "live_inventory_actual_pnl"
        assert row["actual_pnl_status"] == "lighter_final_fill_confirmed"
        assert row["actual_var_pnl_qty"] == "0.01075"
        assert row["actual_lighter_pnl_qty"] == "0.0107"
        assert row["cross_venue_entry_qty_delta"] == "0.00005"
        assert row["cross_venue_exit_qty_delta"] == "0.00005"
        assert row["actual_var_leg_pnl_usd"] == "-0.5965175"
        assert row["actual_lighter_leg_pnl_usd"] == "0.5906599999999999999999999972"
        assert row["actual_pnl_usd"] == "-0.0058575000000000000000000028"
        assert row["actual_pnl_bps"] == "-2.930537994493890584993170358"

    asyncio.run(run())


def test_v4_strong_single_actual_loss_auto_disables_mode(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_v4_strong_single_enabled = True
        runtime.live_inventory_v4_strong_single_disabled_reason = None
        runtime.live_inventory_v4_strong_single_disabled_at = None
        runtime.live_inventory_strong_single_shortfall_bps_samples = deque(
            maxlen=20
        )
        runtime.pending_live_inventory_actual_pnl["exit-strong-loss"] = {
            "asset": "ETH",
            "lot_id": 1,
            "direction": "short_var_long_lighter",
            "qty": "1",
            "entry_var_price": "100",
            "entry_lighter_price": "100",
            "exit_var_price": "99.95",
            "estimated_exit_lighter_price": "100.05",
            "estimated_pnl_usd": "0.10",
            "estimated_pnl_bps": "10",
            "effective_min_exit_pnl_bps": "4",
            "exit_confirmation_mode": "strong_single",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {
                "trade_key": "exit-strong-loss",
                "lighter_filled_price": "99.90",
                "lighter_filled_base_amount": "1",
            }
        )

        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        disabled = [
            row
            for row in rows
            if row["event"] == "live_inventory_v4_strong_single_auto_disabled"
        ]
        assert runtime.live_inventory_v4_strong_single_enabled is False
        assert runtime.live_inventory_v4_strong_single_disabled_reason == (
            "estimated_profitable_actual_loss"
        )
        assert len(disabled) == 1
        assert disabled[0]["action"] == "fallback_to_latest_and_2_of_3"
        assert disabled[0]["actual_pnl_bps"] == "-5.0000"
        state = json.loads(runtime.live_inventory_state_file.read_text())
        assert state["v4_strong_single_enabled"] is False

    asyncio.run(run())


def test_execution_calibration_cycle_loss_triggers_fuse(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_execution_calibration = True
        runtime.live_inventory_calibration_max_cycle_loss_usd = Decimal("0.10")
        runtime.pending_live_inventory_actual_pnl["exit-loss"] = {
            "asset": "ETH",
            "lot_id": 1,
            "direction": "long_var_short_lighter",
            "qty": "1",
            "entry_var_price": "100",
            "entry_lighter_price": "100",
            "exit_var_price": "99.8",
            "estimated_pnl_usd": "-0.2",
            "estimated_pnl_bps": "-20",
        }

        await runtime.maybe_append_live_inventory_actual_pnl(
            {"trade_key": "exit-loss", "lighter_filled_price": "100"}
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert runtime.live_inventory_calibration_halted_reason == "calibration_max_cycle_loss_reached"
        assert any(row["event"] == "live_inventory_calibration_loss_fuse_triggered" for row in rows)
        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_usd"] == "-0.2"

    asyncio.run(run())


def test_execution_calibration_can_lock_direction(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_execution_calibration = True
    runtime.live_inventory_calibration_direction = "short_var_long_lighter"

    runtime.live_inventory_completed_cycles = 0
    assert runtime.live_inventory_calibration_direction_for_cycle() == "short_var_long_lighter"
    runtime.live_inventory_completed_cycles = 1
    assert runtime.live_inventory_calibration_direction_for_cycle() == "short_var_long_lighter"


def test_execution_calibration_alternate_direction_remains_default(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_execution_calibration = True
    runtime.live_inventory_calibration_direction = "alternate"

    runtime.live_inventory_completed_cycles = 0
    assert runtime.live_inventory_calibration_direction_for_cycle() == "long_var_short_lighter"
    runtime.live_inventory_completed_cycles = 1
    assert runtime.live_inventory_calibration_direction_for_cycle() == "short_var_long_lighter"


def test_execution_calibration_weekday_gate_uses_utc(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_calibration_weekdays_only = True

    assert runtime.live_inventory_calibration_entry_time_allowed(
        datetime(2026, 7, 24, 23, 59, tzinfo=timezone.utc)
    )
    assert not runtime.live_inventory_calibration_entry_time_allowed(
        datetime(2026, 7, 25, 0, 0, tzinfo=timezone.utc)
    )


def test_v4_weekend_entry_is_continuous(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    weekend = datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc)

    runtime.live_inventory_basis_v4_test_allow_weekend = False
    assert runtime.live_inventory_basis_v4_entry_time_allowed(weekend)

    runtime.live_inventory_basis_v4_test_allow_weekend = True
    assert runtime.live_inventory_basis_v4_entry_time_allowed(weekend)


def test_register_actual_pnl_replays_fill_that_arrived_before_pending(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}

        class FilledRecord:
            lighter_fill_ts_iso = "2026-07-15T12:00:00+00:00"

            @staticmethod
            def to_payload() -> dict[str, str]:
                return {
                    "trade_key": "exit-race",
                    "lighter_filled_price": "100",
                    "lighter_filled_at": "2026-07-15T12:00:00+00:00",
                }

        finalized = await runtime.register_live_inventory_actual_pnl(
            trade_key="exit-race",
            pending={
                "asset": "ETH",
                "lot_id": 1,
                "direction": "long_var_short_lighter",
                "qty": "1",
                "entry_var_price": "100",
                "entry_lighter_price": "100",
                "exit_var_price": "100.1",
                "estimated_pnl_usd": "0.1",
                "estimated_pnl_bps": "10",
            },
            lighter_record=FilledRecord(),
        )

        rows = [json.loads(line) for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()]
        assert finalized is True
        assert "exit-race" not in runtime.pending_live_inventory_actual_pnl
        assert rows[-1]["event"] == "live_inventory_actual_pnl"
        assert rows[-1]["actual_pnl_usd"] == "0.1"

    asyncio.run(run())


def test_live_inventory_state_asset_uses_allowed_single_asset(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}

        await runtime.persist_live_inventory_memory(reason="test")

        state = json.loads(runtime.live_inventory_state_file.read_text(encoding="utf-8"))
        assert state["asset"] == "ETH"

    asyncio.run(run())


def test_live_inventory_pending_consumption_is_checkpointed(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.pending_live_inventory_var_fill_matches = []
        match = PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.0082"),
            lot_id=24,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic(),
        )

        runtime.add_pending_live_inventory_var_fill_match(match)
        await runtime.persist_live_inventory_memory(reason="exit_intent")
        persisted = json.loads(
            runtime.live_inventory_state_file.read_text(encoding="utf-8")
        )
        assert persisted["pending_actions"][0]["lot_id"] == 24

        consumed = runtime.consume_pending_live_inventory_var_fill_match(
            asset="ETH",
            side="buy",
            qty=Decimal("0.0082"),
        )
        assert consumed is match
        assert await runtime.flush_live_inventory_state_if_dirty() is True

        persisted = json.loads(
            runtime.live_inventory_state_file.read_text(encoding="utf-8")
        )
        assert persisted["pending_actions"] == []
        assert persisted["state_mutation_revision"] == 2
        assert persisted["state_revision"] == 2

    asyncio.run(run())


def test_live_inventory_exit_pending_matches_clear_all_component_lots() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.pending_live_inventory_var_fill_matches = [
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.0082"),
            lot_id=2,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic(),
        ),
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.0082"),
            lot_id=3,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic(),
        ),
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.0082"),
            lot_id=4,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic(),
        ),
    ]

    removed = runtime.remove_pending_live_inventory_var_fill_matches(
        asset="eth",
        lot_ids=("2.0", 3),
        role="live_inventory_exit",
    )

    assert removed == 2
    assert [item.lot_id for item in runtime.pending_live_inventory_var_fill_matches] == [4]


def test_live_inventory_state_writer_freezes_mutable_snapshot(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        entered_writer = threading.Event()
        release_writer = threading.Event()
        original_writer = runtime.write_live_inventory_state

        def delayed_writer(state):
            entered_writer.set()
            assert release_writer.wait(timeout=2)
            original_writer(state)

        runtime.write_live_inventory_state = delayed_writer
        open_lots = [{"lot_id": 1, "qty": "0.0081"}]
        task = asyncio.create_task(
            runtime.write_live_inventory_state_async(
                {
                    "status": "open",
                    "open_lots": open_lots,
                    "pending_actions": [],
                }
            )
        )
        assert await asyncio.to_thread(entered_writer.wait, 1)
        open_lots.clear()
        release_writer.set()
        await task

        persisted = json.loads(
            runtime.live_inventory_state_file.read_text(encoding="utf-8")
        )
        assert persisted["open_lots"] == [{"lot_id": 1, "qty": "0.0081"}]

    asyncio.run(run())


def test_v4_exit_pair_submits_both_legs_concurrently(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        var_started = asyncio.Event()
        lighter_started = asyncio.Event()

        async def fake_send_variational_place_order(**_kwargs):
            var_started.set()
            await asyncio.wait_for(lighter_started.wait(), timeout=1)
            return {"ok": True, "result": {"quoteId": "exit-var"}}

        async def fake_place_lighter_order_from_plan(**kwargs):
            assert kwargs["reduce_only"] is True
            lighter_started.set()
            await asyncio.wait_for(var_started.wait(), timeout=1)

            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": "exit-lighter"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan
        original_append_live_inventory_log = runtime.append_live_inventory_log

        async def guarded_append_live_inventory_log(event_type, payload):
            if (
                event_type == "live_inventory_execution_ledger"
                and payload.get("execution_stage") == "submit_started"
            ):
                await asyncio.wait_for(var_started.wait(), timeout=1)
                await asyncio.wait_for(lighter_started.wait(), timeout=1)
            await original_append_live_inventory_log(event_type, payload)

        runtime.append_live_inventory_log = guarded_append_live_inventory_log

        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH",
            lot={"lot_id": 7, "basis_trace_id": "trace-7"},
            direction="short_var_long_lighter",
            exit_side="BUY",
            qty=Decimal("0.01"),
            var_amount="0.01",
            var_exit_price=Decimal("1900"),
            exit_lighter_depth={"estimated_fill_price": "1901"},
        )

        var_result, _, lighter_record, lighter_payload, _, var_exc, lighter_exc, context = result
        assert var_started.is_set()
        assert lighter_started.is_set()
        assert var_result["ok"] is True
        assert lighter_record is not None
        assert lighter_payload["trade_key"] == "exit-lighter"
        assert var_exc is None
        assert lighter_exc is None
        assert context["submit_mode"] == "concurrent"
        assert context["var_submit_ok"] is True
        assert context["lighter_submit_started"] is True

        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        ledger = [row for row in rows if row["event"] == "live_inventory_execution_ledger"]
        assert [row["execution_stage"] for row in ledger] == [
            "submit_started",
            "submit_returned",
        ]

    asyncio.run(run())


def test_v4_exit_pair_preserves_one_leg_exception_outcome(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True

        async def fake_send_variational_place_order(**_kwargs):
            raise RuntimeError("var timeout unknown")

        async def fake_place_lighter_order_from_plan(**_kwargs):
            class Record:
                processing_stage = "live_submit_sent"

            return Record(), {"trade_key": "exit-lighter"}

        runtime.send_variational_place_order = fake_send_variational_place_order
        runtime.place_lighter_order_from_plan = fake_place_lighter_order_from_plan

        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH",
            lot={"lot_id": 8},
            direction="short_var_long_lighter",
            exit_side="BUY",
            qty=Decimal("0.01"),
            var_amount="0.01",
            var_exit_price=Decimal("1900"),
            exit_lighter_depth=None,
        )

        assert result[0] is None
        assert isinstance(result[5], RuntimeError)
        assert result[2] is not None
        assert result[7]["lighter_submit_started"] is True
        assert result[7]["var_submit_exception"] == "var timeout unknown"

    asyncio.run(run())


def test_lighter_exit_retries_canceled_remainder_without_alert_spam(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        initial = OrderLifecycle(
            trade_key="initial-exit",
            trade_id="initial-exit",
            side="buy",
            qty=Decimal("0.01000"),
            asset="ETH",
            mode="live",
            last_variational_status="submitted",
            lighter_client_order_id=100,
            lighter_order_status="canceled-too-much-slippage",
            lighter_filled_base_amount=Decimal("0.00400"),
            lighter_filled_quote_amount=Decimal("8.0000"),
            lighter_reduce_only=True,
            dry_run_plan_price=Decimal("2000.00"),
        )
        initial.processing_stage = "live_submit_failed"
        refresh_calls = []
        place_calls = []
        logged_events = []
        notifications = []

        class FakeNotifier:
            def enqueue(self, event_type, payload):
                notifications.append((event_type, payload))

        async def fake_append_order_log(event_type, payload):
            logged_events.append((event_type, payload))

        async def fake_refresh():
            refresh_calls.append(True)
            return {
                "ok": True,
                "request_sent": True,
                "snapshot_received": True,
                "update_at": "2999-06-02T08:50:12+00:00",
            }

        async def fake_place(**kwargs):
            place_calls.append(kwargs)
            attempt = len(place_calls)
            record = OrderLifecycle(
                trade_key=f"retry-exit-{attempt}",
                trade_id=f"retry-exit-{attempt}",
                side="buy",
                qty=kwargs["qty"],
                asset="ETH",
                mode="live",
                last_variational_status="submitted",
                lighter_client_order_id=100 + attempt,
                lighter_reduce_only=True,
            )
            record.processing_stage = "live_submit_sent"
            return record, record.to_payload()

        async def fake_wait(record):
            attempt = int(record.trade_key.rsplit("-", 1)[-1])
            if attempt == 1:
                record.lighter_order_status = "canceled-too-much-slippage"
                record.lighter_filled_base_amount = Decimal("0")
                record.lighter_filled_quote_amount = Decimal("0")
                record.processing_stage = "live_submit_failed"
                return False
            record.lighter_order_status = "filled"
            record.lighter_filled_base_amount = Decimal("0.00600")
            record.lighter_filled_quote_amount = Decimal("12.0600")
            record.lighter_fill_price = Decimal("2010.00")
            record.lighter_fill_ts_iso = "2026-09-17T00:00:02+00:00"
            record.processing_stage = "lighter_filled"
            return True

        runtime.append_order_log = fake_append_order_log
        runtime.refresh_lighter_order_book_for_exit_retry = fake_refresh
        runtime.place_lighter_order_from_plan = fake_place
        runtime.wait_for_lighter_final_fill = fake_wait
        runtime.telegram_notifier = FakeNotifier()

        result = await runtime.retry_lighter_exit_after_cancel(
            asset="ETH",
            direction="short_var_long_lighter",
            lot_id=7,
            exit_side="BUY",
            qty=Decimal("0.01000"),
            var_exit_price=Decimal("1999.00"),
            lighter_record=initial,
        )

        assert result["ok"] is True
        assert len(refresh_calls) == 2
        assert [call["qty"] for call in place_calls] == [
            Decimal("0.00600"),
            Decimal("0.00600"),
        ]
        assert all(call["reduce_only"] is True for call in place_calls)
        assert all(
            call["retry_price_boundary"] == Decimal("2000.00")
            for call in place_calls
        )
        assert [call["retry_index"] for call in place_calls] == [1, 2]

        record = result["record"]
        assert record.lighter_filled_base_amount == Decimal("0.01000")
        assert record.lighter_filled_quote_amount == Decimal("20.0600")
        assert record.lighter_fill_price == Decimal("2006")
        assert record.lighter_exit_retry_price_boundary == Decimal("2000.00")
        assert [
            item["attempt"] for item in record.lighter_exit_retry_history
        ] == [0, 1, 2]
        assert all(
            item["price_boundary"] == "2000.00"
            for item in record.lighter_exit_retry_history
        )
        assert [
            payload["result"] for _, payload in logged_events
        ] == ["retryable_cancel", "filled_after_retry"]
        assert notifications == []

        assert runtime.lighter_exit_cancel_is_retryable(
            "canceled-too-much-slippage"
        ) is True
        assert runtime.lighter_exit_cancel_is_retryable("expired") is False

    asyncio.run(run())


def test_lighter_exit_retry_refresh_requests_new_snapshot(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.last_lighter_order_book_update_at = "2999-06-02T08:50:11+00:00"
        sent = []

        class FakeWebSocket:
            closed = False

            async def send(self, message):
                sent.append(json.loads(message))
                runtime.last_lighter_order_book_update_at = (
                    "2999-06-02T08:50:12+00:00"
                )

        runtime._lighter_market_data_ws = FakeWebSocket()
        result = await runtime.refresh_lighter_order_book_for_exit_retry()

        assert result["ok"] is True
        assert result["snapshot_received"] is True
        assert sent == [{
            "type": "subscribe",
            "channel": "order_book/1",
        }]

    asyncio.run(run())


def test_lighter_exit_retry_plan_keeps_original_price_boundary(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_allowed_assets = {"ETH"}
        runtime.live_inventory_lighter_exit_submit_slippage_bps = Decimal("30")
        record = OrderLifecycle(
            trade_key="retry-boundary",
            trade_id="retry-boundary",
            side="sell",
            qty=Decimal("0.00100"),
            asset="ETH",
            mode="live",
            last_variational_status="submitted",
            var_fill_price=Decimal("60000"),
            auto_live_role="live_inventory_exit",
            lighter_reduce_only=True,
            lighter_exit_retry_price_boundary=Decimal("60020"),
        )

        plan = await runtime.build_hedge_plan(record)

        assert plan is not None
        assert plan[0] == "BUY"
        assert plan[1] == Decimal("60020")

    asyncio.run(run())


def test_v4_exit_pair_timeout_returns_execution_unknown_without_hanging(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True

        async def slow_submit(**_kwargs):
            await asyncio.sleep(1)

        runtime.send_variational_place_order = slow_submit
        runtime.place_lighter_order_from_plan = slow_submit
        monkeypatch.setattr("main.LIVE_INVENTORY_PAIR_SUBMIT_TIMEOUT_SECONDS", 0.01)

        result = await runtime.submit_live_inventory_exit_pair(
            asset="ETH",
            lot={"lot_id": 9},
            direction="short_var_long_lighter",
            exit_side="BUY",
            qty=Decimal("0.01"),
            var_amount="0.01",
            var_exit_price=Decimal("1900"),
            exit_lighter_depth=None,
        )

        assert isinstance(result[5], TimeoutError)
        assert isinstance(result[6], TimeoutError)
        assert result[7]["execution_stage"] == "submit_timeout"
        assert result[7]["execution_unknown"] is True

    asyncio.run(run())


def test_v4_completed_cycle_emits_report_and_stops(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9.0"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.stop_flag = False

        async def fake_persist_live_inventory_memory(**_kwargs):
            return None

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory
        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is True
        assert runtime.stop_flag is True
        assert runtime.live_inventory_cycle_report_emitted is True
        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_cycle_report"
        assert rows[-1]["report_status"] == "completed"
        assert rows[-1]["auto_stop"] is True

    asyncio.run(run())


def test_v4_completed_cycle_waits_for_actual_pnl_queue(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9"}
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 1
        runtime.pending_live_inventory_actual_pnl = {"exit-9": {"lot_id": 9}}
        runtime.stop_flag = False

        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert runtime.stop_flag is False
        assert runtime.live_inventory_cycle_report_emitted is False

    asyncio.run(run())


def test_v4_intermediate_cycle_checkpoints_without_stopping(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9"}
        runtime.live_inventory_v4_checkpointed_lot_ids = set()
        runtime.pending_live_inventory_final_pnl = {
            "ETH:1.0": {
                "asset": "eth",
                "lot_id": "9.0",
                "final_pnl_emitted": False,
            },
            "ETH:9": {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_emitted": True,
            },
        }
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 5
        runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
        runtime.live_inventory_realized_pnl_usd = Decimal("0.003")
        runtime.stop_flag = False

        async def fake_persist_live_inventory_memory(**_kwargs):
            return None

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory
        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert runtime.stop_flag is False
        assert runtime.live_inventory_cycle_report_emitted is False
        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_v4_cycle_checkpoint"
        assert rows[-1]["completed_cycles"] == 1
        assert rows[-1]["next_cycle"] == 2
        assert rows[-1]["cumulative_run_pnl_usd"] == "0.003"
        assert runtime.pending_live_inventory_final_pnl == {}

    asyncio.run(run())


def test_v4_continuous_cycle_checkpoints_without_stopping(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_cycle_report_emitted = False
        runtime.live_inventory_last_final_pnl_payload = None
        runtime.live_inventory_exit_events_logged = {"9"}
        runtime.live_inventory_v4_checkpointed_lot_ids = set()
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 27
        runtime.live_inventory_max_cycles = 0
        runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
        runtime.live_inventory_realized_pnl_usd = Decimal("0.12")
        runtime.stop_flag = False

        async def fake_persist_live_inventory_memory(**_kwargs):
            return None

        runtime.persist_live_inventory_memory = fake_persist_live_inventory_memory
        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 9,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert runtime.stop_flag is False
        rows = [
            json.loads(line)
            for line in runtime.orders_file.read_text(encoding="utf-8").splitlines()
        ]
        assert rows[-1]["event"] == "live_inventory_v4_cycle_checkpoint"
        assert rows[-1]["completed_cycles"] == 27
        assert rows[-1]["max_cycles"] == 0
        assert rows[-1]["next_cycle"] == 28

    asyncio.run(run())


def test_v4_continuous_entry_gate_keeps_cumulative_loss_fuse(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 0
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.05")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_realized_pnl_usd = Decimal("0.95")

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is False
    assert reason == "v4_batch_max_run_loss_reached"
    assert context["batch_run_pnl_usd"] == "-0.05"


def test_live_inventory_final_pnl_key_normalizes_numeric_lot_ids() -> None:
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        " eth ", 1
    ) == "ETH:1"
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        "ETH", "1.0"
    ) == "ETH:1"
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        "ETH", Decimal("1.00")
    ) == "ETH:1"
    assert VariationalToLighterRuntime.live_inventory_final_pnl_key(
        "ETH", "lot-a"
    ) == "ETH:lot-a"


def test_v4_checkpointed_lot_ignores_late_duplicate_fill(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_v4_checkpointed_lot_ids = {"9"}
        runtime.pending_live_inventory_final_pnl = {}

        await runtime.maybe_append_live_inventory_final_pnl_from_fill(
            {
                "auto_live_role": "live_inventory_exit",
                "auto_live_cycle_id": "9.0",
                "asset": "ETH",
                "qty": "0.01",
                "lighter_filled_price": "1900",
            }
        )

        assert runtime.pending_live_inventory_final_pnl == {}

    asyncio.run(run())


def test_v4_batch_gate_prunes_checkpointed_cycle_reconciliation(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 9
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_realized_pnl_usd = Decimal("-0.006")
    runtime.live_inventory_v4_checkpointed_lot_ids = {"1"}
    runtime.pending_live_inventory_actual_pnl = {
        "late-exit": {"asset": "ETH", "lot_id": "1.0"},
    }
    runtime.pending_live_inventory_final_pnl = {
        "ETH:1.0": {
            "asset": "ETH",
            "lot_id": 1,
            "final_pnl_emitted": False,
        },
    }
    runtime.pending_live_inventory_var_fill_matches = [
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="sell",
            qty=Decimal("0.01"),
            lot_id=1,
            role="live_inventory_entry_pending_var_fill",
            created_at_monotonic=time.monotonic(),
        ),
        PendingLiveInventoryVarFillMatch(
            asset="ETH",
            side="buy",
            qty=Decimal("0.01"),
            lot_id=1,
            role="live_inventory_exit",
            created_at_monotonic=time.monotonic(),
        ),
    ]

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is True
    assert reason == "ready"
    assert context["pending_actual_pnl"] == 0
    assert context["unresolved_final_pnl"] == 0
    assert context["reconciliation_cleanup"] == {
        "pending_actual_pnl": 1,
        "pending_final_pnl": 1,
        "pending_var_fill_matches": 2,
    }
    assert runtime.pending_live_inventory_actual_pnl == {}
    assert runtime.pending_live_inventory_final_pnl == {}
    assert runtime.pending_live_inventory_var_fill_matches == []
    assert runtime.pending_live_inventory_actions_payload() == []


def test_v4_cycle_checkpoint_waits_for_exit_registration(tmp_path) -> None:
    async def run() -> None:
        runtime = _live_inventory_runtime(tmp_path)
        runtime.live_inventory_basis_v4_mode = True
        runtime.live_inventory_open_lots = []
        runtime.live_inventory_completed_cycles = 1
        runtime.live_inventory_max_cycles = 9
        runtime.live_inventory_exit_events_logged = {"1"}
        runtime.live_inventory_v4_exit_reconciliation_lot_ids = {"1"}
        runtime.stop_flag = False

        stopped = await runtime.maybe_auto_stop_completed_v4_cycle(
            {
                "asset": "ETH",
                "lot_id": 1,
                "final_pnl_status": "var_and_lighter_final_fills_confirmed",
                "final_pnl_bps": "1.2",
            }
        )

        assert stopped is False
        assert getattr(runtime, "live_inventory_v4_checkpointed_lot_ids", set()) == set()

    asyncio.run(run())


def test_v4_batch_entry_gate_waits_for_reconciliation_and_cooldown(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 5
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.05")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 180.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_realized_pnl_usd = Decimal("0.01")
    runtime.live_inventory_v4_last_exit_monotonic = 100.0
    runtime.pending_live_inventory_actual_pnl = {"trade": {}}

    ready, reason, _ = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=200.0
    )
    assert ready is False
    assert reason == "v4_batch_waiting_for_reconciliation"

    runtime.pending_live_inventory_actual_pnl = {}
    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=200.0
    )
    assert ready is False
    assert reason == "v4_batch_cycle_cooldown"
    assert context["cooldown_remaining_seconds"] == 80.0

    ready, reason, _ = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=281.0
    )
    assert ready is True
    assert reason == "ready"


def test_v4_batch_entry_gate_supports_optional_cumulative_loss_limit(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 5
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.05")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 180.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_realized_pnl_usd = Decimal("0.95")

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is False
    assert reason == "v4_batch_max_run_loss_reached"
    assert context["batch_run_pnl_usd"] == "-0.05"


def test_v4_stop_loss_halts_the_remaining_batch(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 3
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.025")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_realized_pnl_usd = Decimal("-0.005")
    runtime.live_inventory_v4_batch_halted_reason = None

    runtime.require_live_inventory_basis_v4_rearm(
        exit_reason="max_unrealized_loss_bps",
        entry_threshold_bps=Decimal("-7.5"),
    )
    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is False
    assert reason == "v4_batch_halted_after_stop_loss"
    assert context["v4_episode_state"] == "halted"


def test_v4_max_hold_exit_enforces_thirty_minute_cooldown(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 3
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.025")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_v4_last_exit_monotonic = 100.0
    runtime.live_inventory_v4_last_exit_reason = "v4_max_hold_timeout"

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=200.0
    )

    assert ready is False
    assert reason == "v4_batch_cycle_cooldown"
    assert context["cycle_cooldown_seconds"] == 1800.0
    assert context["cooldown_remaining_seconds"] == 1700.0


def test_v4_max_hold_cooldown_survives_process_restart(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 3
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0.025")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("0")
    runtime.live_inventory_v4_last_exit_monotonic = 0.0
    runtime.live_inventory_v4_last_exit_reason = "v4_max_hold_timeout"
    runtime.live_inventory_v4_last_exit_at = datetime.now(timezone.utc).isoformat()

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate()

    assert ready is False
    assert reason == "v4_batch_cycle_cooldown"
    assert 1799.0 <= context["cooldown_remaining_seconds"] <= 1800.0


def test_v4_episode_rearms_only_after_three_confirmed_resets() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_v4_episode_id = "episode-1"
    runtime.live_inventory_v4_next_tranche_index = 2
    runtime.live_inventory_v4_batch_halted_reason = None
    runtime.require_live_inventory_basis_v4_rearm(
        exit_reason="v4_executable_net_target_reached",
        entry_threshold_bps=Decimal("-7.5"),
    )

    rearmed, _ = runtime.live_inventory_basis_v4_update_rearm(
        short_edge_bps=Decimal("-7.9"),
        entry_threshold_bps=Decimal("-7.5"),
    )
    assert rearmed is False
    assert runtime.live_inventory_v4_rearm_confirmation_count == 0

    for expected_count in (1, 2):
        rearmed, _ = runtime.live_inventory_basis_v4_update_rearm(
            short_edge_bps=Decimal("-8.1"),
            entry_threshold_bps=Decimal("-7.5"),
        )
        assert rearmed is False
        assert runtime.live_inventory_v4_rearm_confirmation_count == expected_count

    rearmed, context = runtime.live_inventory_basis_v4_update_rearm(
        short_edge_bps=Decimal("-8.2"),
        entry_threshold_bps=Decimal("-7.5"),
    )

    assert rearmed is True
    assert runtime.live_inventory_v4_rearm_required is False
    assert runtime.live_inventory_v4_episode_id is None
    assert runtime.live_inventory_v4_next_tranche_index == 1
    assert context["v4_episode_state"] == "armed"


def test_v4_episode_state_survives_restart_for_future_tranches() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.load_live_inventory_state = lambda: {
        "open_lots": [],
        "next_lot_id": 4,
        "realized_pnl_usd": "0.01",
        "completed_cycles": 2,
        "v4_episode_id": "episode-2",
        "v4_next_tranche_index": 3,
        "v4_rearm_required": True,
        "v4_rearm_confirmation_count": 2,
        "v4_rearm_reason": "v4_executable_net_target_reached",
        "v4_rearm_threshold_bps": "-7.5",
        "v4_last_exit_reason": "v4_executable_net_target_reached",
        "v4_last_exit_at": "2026-08-08T00:00:00+00:00",
    }

    runtime.sync_live_inventory_memory_from_state()

    assert runtime.live_inventory_v4_episode_id == "episode-2"
    assert runtime.live_inventory_v4_next_tranche_index == 3
    assert runtime.live_inventory_v4_rearm_required is True
    assert runtime.live_inventory_v4_rearm_confirmation_count == 2
    assert runtime.live_inventory_v4_rearm_threshold_bps == Decimal("-7.5")


def test_v4_batch_entry_gate_disables_cumulative_loss_limit_at_zero(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_max_cycles = 9
    runtime.live_inventory_basis_v4_max_run_loss_usd = Decimal("0")
    runtime.live_inventory_basis_v4_cycle_cooldown_seconds = 0.0
    runtime.live_inventory_v4_run_start_realized_pnl_usd = Decimal("1.00")
    runtime.live_inventory_realized_pnl_usd = Decimal("-10.00")

    ready, reason, context = runtime.live_inventory_v4_batch_entry_gate(
        now_monotonic=300.0
    )

    assert ready is True
    assert reason == "ready"
    assert context["batch_max_run_loss_usd"] == "0"


def test_v4_basis_state_logging_is_adaptive_but_keeps_crossings(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_last_basis_state_log_monotonic = 0.0
    runtime.live_inventory_open_lots = []

    assert runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-10", "v4_entry_threshold_bps": "-8"}
    )
    assert not runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-10", "v4_entry_threshold_bps": "-8"}
    )
    assert runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-7", "v4_entry_threshold_bps": "-8"}
    )
    assert not runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-7", "v4_entry_threshold_bps": "-8"}
    )
    assert runtime.should_log_live_inventory_basis_state(
        {"short_edge_bps": "-9", "v4_entry_threshold_bps": "-8"}
    )


def test_v4_negative_direction_shadow_logging_is_throttled(tmp_path) -> None:
    runtime = _live_inventory_runtime(tmp_path)
    runtime.live_inventory_open_lots = []
    runtime.live_inventory_negative_direction_shadow_last_monotonic = 0.0

    assert runtime.should_log_live_inventory_negative_direction_shadow()
    assert not runtime.should_log_live_inventory_negative_direction_shadow()
    runtime.live_inventory_negative_direction_shadow_last_monotonic -= 301
    assert runtime.should_log_live_inventory_negative_direction_shadow()
