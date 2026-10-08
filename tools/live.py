#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (  # noqa: E402
    LIVE_CONTROL,
    LIVE_STATE,
    LOG_DIR,
    human_bytes,
    read_json,
    write_json_atomic,
)


ALLOWED_ASSETS = {"BNB", "BTC", "ETH", "HYPE", "SOL", "XRP"}
RECOVERABLE_OPEN_STATE_MANUAL_REASONS = frozenset(
    {
        "variational_extension_disconnected",
        "variational_html_response",
        "startup_reconcile_open_state_but_variational_flat",
        "startup_reconcile_exchange_position_check_failed",
        "runtime_stopped_with_unresolved_entry_submission",
        # A failed reduce-only hedge may leave the saved paired lots intact.
        # Startup must still reconcile both venue quantities and directions.
        "basis_exit_lighter_final_fill_not_confirmed",
        "basis_entry_lighter_submit_after_var_fill_failed",
    }
)
CALIBRATION_DIRECTIONS = {
    "alternate",
    "long_var_short_lighter",
    "short_var_long_lighter",
}
LIVE_CONFIG = ROOT / "live_config.json"
V4_PROFILE_ETH_SHORT_20260724 = "eth_short_execution_calibrated_20260724_n10"
MIN_START_DISK_FREE_GB = 3.0


@dataclass(frozen=True)
class LiveConfig:
    live_max_notional_usd: str = "25"
    lot_notional_usd: str = "20"
    max_total_inventory_notional_usd: str = "60"
    max_cycles: int = 1
    max_lots: int = 1
    max_total_lots: int = 3
    max_lighter_slippage_bps: str = "6"
    lighter_submit_slippage_bps: str = "15"
    lighter_exit_submit_slippage_bps: str = "30"
    exit_blocked_log_throttle_seconds: str = "300"
    min_entry_edge_bps: str = "7"
    min_abs_entry_bps: str = "7"
    max_entry_roundtrip_cost_bps: str = "0"
    min_entry_quality_score_bps: str = "0"
    dynamic_entry_threshold: bool = True
    dynamic_entry_noise_buffer_bps: str = "2.0"
    spread_regime_penalty_multiplier: str = "1.0"
    min_exit_pnl_bps: str = "3.0"
    min_signal_reverted_exit_pnl_bps: str = "3.0"
    reversion_signal_exit_min_pnl_bps: str = "-1.0"
    profit_take_pnl_bps: str = "5.0"
    entry_confirm_samples: int = 1
    max_sample_move_bps: str = "3"
    min_normalized_entry_edge_bps: str = "1.0"
    min_normalized_filter_edge_bps: str = "0.5"
    reversion_mode: bool = False
    reversion_min_deviation_bps: str = "1.0"
    reversion_exit_deviation_bps: str = "0.0"
    reversion_max_entry_roundtrip_cost_bps: str = "3.0"
    reversion_context_gap_bps: str = "1.0"
    reversion_long_execution_reserve_bps: str = "4.5"
    reversion_short_execution_reserve_bps: str = "3.5"
    reversion_min_net_expected_pnl_bps: str = "1.5"
    reversion_min_normalized_edge_bps: str = "2.5"
    reversion_max_stablecoin_edge_share: str = "0.6"
    calibration_mode: bool = False
    calibration_direction: str = "alternate"
    calibration_weekdays_only: bool = False
    calibration_max_cycles: int = 5
    calibration_hold_samples: int = 5
    calibration_warmup_samples: int = 30
    calibration_entry_cooldown_samples: int = 180
    calibration_max_run_loss_usd: str = "0.25"
    calibration_max_cycle_loss_usd: str = "0.10"
    calibration_max_roundtrip_cost_bps: str = "6.0"
    v4_live_mode: bool = False
    v4_test_max_run_loss_usd: str = "0.025"
    v4_test_cycle_cooldown_seconds: int = 0
    entry_lighter_fill_timeout_seconds: str = "3"
    snapshot_timeout_seconds: str = "10"
    addon_min_basis_improvement_bps: str = "1.5"


DEFAULT_CONFIG = LiveConfig()


def default_config_dict() -> dict[str, Any]:
    return DEFAULT_CONFIG.__dict__.copy()


def running_main_processes() -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-af", "python.*(main.py|tools/basis_collector.py)"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return []
    return [
        line
        for line in result.stdout.splitlines()
        if line.strip()
        and "tools/live.py" not in line
        and process_belongs_to_this_project(line)
    ]


def running_live_strategy_processes() -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-af", "python.*main.py"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return []
    return [
        line
        for line in result.stdout.splitlines()
        if line.strip()
        and "tools/live.py" not in line
        and process_belongs_to_this_project(line)
    ]


def process_id_from_line(line: str) -> int | None:
    token = line.strip().split(maxsplit=1)[0] if line.strip() else ""
    try:
        return int(token)
    except ValueError:
        return None


def process_belongs_to_this_project(line: str) -> bool:
    process_id = process_id_from_line(line)
    if process_id is None:
        return False
    try:
        process_cwd = Path(os.readlink(f"/proc/{process_id}/cwd")).resolve()
        process_cwd.relative_to(ROOT.resolve())
    except (OSError, ValueError):
        return False
    return True


def request_maintenance_drain(
    *,
    asset: str,
    process_line: str,
    state: dict[str, Any],
    control_path: Path = LIVE_CONTROL,
) -> dict[str, Any]:
    process_id = process_id_from_line(process_line)
    if process_id is None:
        raise ValueError("unable_to_parse_strategy_pid")
    state_asset = str(state.get("asset") or asset).upper()
    if state_asset != asset.upper():
        raise ValueError(
            f"asset_mismatch requested={asset.upper()} state={state_asset}"
        )
    request = {
        "schema_version": 1,
        "action": "drain_after_flat",
        "status": "requested",
        "asset": asset.upper(),
        "target_pid": process_id,
        "target_run_id": state.get("run_id"),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(control_path, request)
    return request


def build_multi_asset_collector_command(assets: tuple[str, ...]) -> list[str]:
    return [
        sys.executable,
        "tools/basis_collector.py",
        "--assets",
        ",".join(assets),
    ]


def validate_state(
    config: LiveConfig,
    *,
    reset_state_after_manual_flat: bool = False,
    reset_state_only_after_manual_flat: bool = False,
    collect_only: bool = False,
    close_open_position: bool = False,
    resume_open_position: bool = False,
) -> tuple[bool, str]:
    state = read_json(LIVE_STATE)
    if not state:
        if close_open_position:
            return False, "close_requires_existing_open_state"
        if reset_state_only_after_manual_flat:
            return True, "state=missing reset_only=true exchange_verification_required=true"
        return True, "state=missing allowed=start_after_manual_exchange_flat_confirmation"

    status = str(state.get("status") or "unknown")
    open_lots = state.get("open_lots") or []
    pending_actions = state.get("pending_actions") or []
    if status == "flat" and open_lots:
        status = "open"
    elif status == "flat" and pending_actions:
        status = "pending"
    asset = str(state.get("asset") or "-").upper()
    try:
        completed_cycles = int(state.get("completed_cycles") or 0)
    except (TypeError, ValueError):
        completed_cycles = 0

    if reset_state_only_after_manual_flat:
        if status not in {"flat", "open", "pending", "manual_review_required"}:
            return False, f"reset_only_refuses_state status={status} asset={asset}"
        return (
            True,
            f"state={status} asset={asset} open_lots={len(open_lots)} "
            f"pending_actions={len(pending_actions)} "
            "reset_only=true exchange_position_and_order_verification_required=true",
        )

    if close_open_position:
        if status != "open":
            return False, f"close_requires_open_state status={status} asset={asset}"
        if not open_lots:
            return False, f"close_requires_open_lots asset={asset}"
        if pending_actions:
            return (
                False,
                f"close_refuses_pending_actions count={len(pending_actions)} asset={asset}",
            )
        return (
            True,
            f"state=open asset={asset} open_lots={len(open_lots)} "
            f"pending_actions=0 completed_cycles={completed_cycles} "
            "operator_exit_requested exchange_reconcile_required=true",
        )

    if resume_open_position:
        manual_reason = str(state.get("manual_review_reason") or "")
        state_is_resumable = status in {"open", "pending"} or (
            status == "manual_review_required"
            and manual_reason in RECOVERABLE_OPEN_STATE_MANUAL_REASONS
            and (
                manual_reason != "basis_entry_lighter_submit_after_var_fill_failed"
                or not pending_actions
            )
        )
        if not state_is_resumable:
            return (
                False,
                f"resume_refuses_state status={status} reason={manual_reason or '-'} asset={asset}",
            )
        if not open_lots and not pending_actions:
            return False, f"resume_requires_open_lots_or_pending_action asset={asset}"
        return (
            True,
            f"state={status} asset={asset} open_lots={len(open_lots)} "
            f"pending_actions={len(pending_actions)} completed_cycles={completed_cycles} "
            "resume_requested strict_exchange_reconcile_required=true",
        )

    if reset_state_after_manual_flat and not collect_only:
        if status not in {"flat", "open", "pending", "manual_review_required"}:
            return (
                False,
                f"reset_refuses_state status={status} asset={asset}",
            )
        return (
            True,
            f"state={status} asset={asset} open_lots={len(open_lots)} "
            f"pending_actions={len(pending_actions)} completed_cycles={completed_cycles} "
            "reset_after_manual_flat_requested local_state_backup_required=true "
            "exchange_reconcile_required=true",
        )
    if status != "flat":
        return False, f"state_not_flat status={status} asset={asset}"
    if open_lots:
        return False, f"open_lots_present count={len(open_lots)} asset={asset}"
    if pending_actions:
        return False, f"pending_actions_present count={len(pending_actions)} asset={asset}"
    halted_reason = str(state.get("v4_batch_halted_reason") or "")
    if (
        config.v4_live_mode
        and halted_reason
        and not collect_only
        and not reset_state_after_manual_flat
    ):
        return (
            False,
            f"state_v4_batch_halted asset={asset} reason={halted_reason} "
            "action=verify_both_exchanges_flat_then_reset_local_state_only",
        )
    effective_max_cycles = (
        config.calibration_max_cycles
        if config.calibration_mode
        else 1
        if config.reversion_mode
        else config.max_cycles
    )
    if (
        effective_max_cycles > 0
        and completed_cycles >= effective_max_cycles
        and not collect_only
    ):
        return (
            False,
            f"state_cycle_cap_reached asset={asset} completed_cycles={completed_cycles} max_cycles={effective_max_cycles} "
            "open_lots=0 pending_actions=0",
        )
    mode = " collect_only=true" if collect_only else ""
    return True, f"state=flat asset={asset} open_lots=0 pending_actions=0 completed_cycles={completed_cycles} max_cycles={effective_max_cycles}{mode}"


def disk_warning() -> str:
    usage = shutil.disk_usage(ROOT)
    used_pct = usage.used / usage.total * 100
    return f"disk_used={used_pct:.0f}% disk_free={human_bytes(usage.free)} log_dir={human_bytes(dir_size_safe(LOG_DIR))}"


def disk_start_allowed() -> bool:
    return shutil.disk_usage(ROOT).free >= MIN_START_DISK_FREE_GB * 1024**3


def dir_size_safe(path: Path) -> int:
    total = 0
    if not path.exists():
        return total
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                continue
    return total


def _positive_decimal(data: dict[str, Any], key: str) -> str:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = float(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a positive number") from exc
    if number <= 0:
        raise ValueError(f"{key} must be a positive number")
    return str(value)


def _non_negative_decimal(data: dict[str, Any], key: str) -> str:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = float(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a non-negative number") from exc
    if number < 0:
        raise ValueError(f"{key} must be a non-negative number")
    return str(value)


def _decimal(data: dict[str, Any], key: str) -> str:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        float(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a number") from exc
    return str(value)


def _positive_int(data: dict[str, Any], key: str, *, max_value: int | None = None) -> int:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a positive integer") from exc
    if number <= 0:
        raise ValueError(f"{key} must be a positive integer")
    if max_value is not None and number > max_value:
        raise ValueError(f"{key} must be <= {max_value}")
    return number


def _non_negative_int(
    data: dict[str, Any],
    key: str,
    *,
    max_value: int | None = None,
) -> int:
    value = data.get(key, getattr(DEFAULT_CONFIG, key))
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a non-negative integer") from exc
    if number < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    if max_value is not None and number > max_value:
        raise ValueError(f"{key} must be <= {max_value}")
    return number


def _calibration_direction(data: dict[str, Any]) -> str:
    value = str(data.get("calibration_direction", DEFAULT_CONFIG.calibration_direction))
    if value not in CALIBRATION_DIRECTIONS:
        raise ValueError(
            "calibration_direction must be one of "
            + ", ".join(sorted(CALIBRATION_DIRECTIONS))
        )
    return value


def load_config(path: Path) -> LiveConfig:
    if not path.exists():
        path.write_text(json.dumps(default_config_dict(), indent=2) + "\n", encoding="utf-8")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to read {path.name}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{path.name} must contain a JSON object")

    return LiveConfig(
        live_max_notional_usd=_positive_decimal(raw, "live_max_notional_usd"),
        lot_notional_usd=_positive_decimal(raw, "lot_notional_usd"),
        max_total_inventory_notional_usd=_positive_decimal(raw, "max_total_inventory_notional_usd"),
        max_cycles=_positive_int(raw, "max_cycles", max_value=10),
        max_lots=_positive_int(raw, "max_lots", max_value=10),
        max_total_lots=_positive_int(raw, "max_total_lots", max_value=10),
        max_lighter_slippage_bps=_positive_decimal(raw, "max_lighter_slippage_bps"),
        lighter_submit_slippage_bps=_positive_decimal(raw, "lighter_submit_slippage_bps"),
        lighter_exit_submit_slippage_bps=_positive_decimal(raw, "lighter_exit_submit_slippage_bps"),
        exit_blocked_log_throttle_seconds=_non_negative_decimal(
            raw,
            "exit_blocked_log_throttle_seconds",
        ),
        min_entry_edge_bps=_positive_decimal(raw, "min_entry_edge_bps"),
        min_abs_entry_bps=_positive_decimal(raw, "min_abs_entry_bps"),
        max_entry_roundtrip_cost_bps=_non_negative_decimal(raw, "max_entry_roundtrip_cost_bps"),
        min_entry_quality_score_bps=_decimal(raw, "min_entry_quality_score_bps"),
        dynamic_entry_threshold=bool(raw.get("dynamic_entry_threshold", DEFAULT_CONFIG.dynamic_entry_threshold)),
        dynamic_entry_noise_buffer_bps=_positive_decimal(raw, "dynamic_entry_noise_buffer_bps"),
        spread_regime_penalty_multiplier=_positive_decimal(raw, "spread_regime_penalty_multiplier"),
        min_exit_pnl_bps=_positive_decimal(raw, "min_exit_pnl_bps"),
        min_signal_reverted_exit_pnl_bps=_positive_decimal(raw, "min_signal_reverted_exit_pnl_bps"),
        reversion_signal_exit_min_pnl_bps=_decimal(raw, "reversion_signal_exit_min_pnl_bps"),
        profit_take_pnl_bps=_positive_decimal(raw, "profit_take_pnl_bps"),
        entry_confirm_samples=_positive_int(raw, "entry_confirm_samples", max_value=20),
        max_sample_move_bps=_positive_decimal(raw, "max_sample_move_bps"),
        min_normalized_entry_edge_bps=_positive_decimal(raw, "min_normalized_entry_edge_bps"),
        min_normalized_filter_edge_bps=_positive_decimal(raw, "min_normalized_filter_edge_bps"),
        reversion_mode=bool(raw.get("reversion_mode", DEFAULT_CONFIG.reversion_mode)),
        reversion_min_deviation_bps=_positive_decimal(raw, "reversion_min_deviation_bps"),
        reversion_exit_deviation_bps=_non_negative_decimal(raw, "reversion_exit_deviation_bps"),
        reversion_max_entry_roundtrip_cost_bps=_non_negative_decimal(raw, "reversion_max_entry_roundtrip_cost_bps"),
        reversion_context_gap_bps=_positive_decimal(raw, "reversion_context_gap_bps"),
        reversion_long_execution_reserve_bps=_non_negative_decimal(raw, "reversion_long_execution_reserve_bps"),
        reversion_short_execution_reserve_bps=_non_negative_decimal(raw, "reversion_short_execution_reserve_bps"),
        reversion_min_net_expected_pnl_bps=_positive_decimal(raw, "reversion_min_net_expected_pnl_bps"),
        reversion_min_normalized_edge_bps=_non_negative_decimal(raw, "reversion_min_normalized_edge_bps"),
        reversion_max_stablecoin_edge_share=_non_negative_decimal(raw, "reversion_max_stablecoin_edge_share"),
        calibration_mode=bool(raw.get("calibration_mode", DEFAULT_CONFIG.calibration_mode)),
        calibration_direction=_calibration_direction(raw),
        calibration_weekdays_only=bool(
            raw.get("calibration_weekdays_only", DEFAULT_CONFIG.calibration_weekdays_only)
        ),
        calibration_max_cycles=_positive_int(raw, "calibration_max_cycles", max_value=5),
        calibration_hold_samples=_positive_int(raw, "calibration_hold_samples", max_value=60),
        calibration_warmup_samples=_positive_int(raw, "calibration_warmup_samples", max_value=3600),
        calibration_entry_cooldown_samples=_positive_int(raw, "calibration_entry_cooldown_samples", max_value=3600),
        calibration_max_run_loss_usd=_positive_decimal(raw, "calibration_max_run_loss_usd"),
        calibration_max_cycle_loss_usd=_positive_decimal(raw, "calibration_max_cycle_loss_usd"),
        calibration_max_roundtrip_cost_bps=_positive_decimal(raw, "calibration_max_roundtrip_cost_bps"),
        v4_live_mode=bool(raw.get("v4_live_mode", DEFAULT_CONFIG.v4_live_mode)),
        v4_test_max_run_loss_usd=_non_negative_decimal(
            raw,
            "v4_test_max_run_loss_usd",
        ),
        v4_test_cycle_cooldown_seconds=_non_negative_int(
            raw,
            "v4_test_cycle_cooldown_seconds",
            max_value=3600,
        ),
        entry_lighter_fill_timeout_seconds=_positive_decimal(raw, "entry_lighter_fill_timeout_seconds"),
        snapshot_timeout_seconds=_positive_decimal(raw, "snapshot_timeout_seconds"),
        addon_min_basis_improvement_bps=_positive_decimal(raw, "addon_min_basis_improvement_bps"),
    )


def build_main_command(
    asset: str,
    config: LiveConfig,
    *,
    reset_state_after_manual_flat: bool = False,
    collect_only: bool = False,
    v4_test_skip_recent_health: bool = False,
    v4_test_allow_weekend: bool = False,
    v4_shadow_gradient: bool = False,
    v4_real_gradient: bool = False,
    v4_elastic_capacity: bool = False,
    v4_reverse_test: bool = False,
    v4_bidirectional: bool = False,
    v4_continuous: bool = False,
    close_open_position: bool = False,
    resume_open_position: bool = False,
    maintenance_drain_after_start: bool = False,
    reset_state_only_after_manual_flat: bool = False,
    flat_reset_confirmation: str | None = None,
) -> list[str]:
    reversion_mode = config.reversion_mode
    calibration_mode = config.calibration_mode
    v4_live_mode = config.v4_live_mode
    diagnostic_single_lot = (
        reversion_mode
        or calibration_mode
        or (v4_live_mode and not v4_real_gradient)
        or collect_only
    )
    effective_max_total_notional_usd = (
        "0"
        if v4_live_mode and v4_real_gradient
        else "25"
        if diagnostic_single_lot
        else config.max_total_inventory_notional_usd
    )
    effective_max_cycles = (
        0
        if v4_live_mode and v4_continuous
        else config.calibration_max_cycles
        if calibration_mode
        else 1
        if reversion_mode
        else config.max_cycles
    )
    effective_max_lots = (
        0
        if v4_live_mode and v4_real_gradient
        else 1
        if diagnostic_single_lot
        else config.max_lots
    )
    effective_max_total_lots = (
        0
        if v4_real_gradient
        else 1
        if diagnostic_single_lot
        else config.max_total_lots
    )
    effective_min_entry_edge_bps = "0" if diagnostic_single_lot else config.min_entry_edge_bps
    effective_min_abs_entry_bps = "0" if diagnostic_single_lot else config.min_abs_entry_bps
    effective_max_entry_roundtrip_cost_bps = (
        config.calibration_max_roundtrip_cost_bps
        if calibration_mode
        else config.reversion_max_entry_roundtrip_cost_bps
        if reversion_mode
        else config.max_entry_roundtrip_cost_bps
    )
    command = [
        sys.executable,
        "main.py",
        "--mode",
        "live",
        "--confirm-live",
        "--live-allowed-assets",
        asset,
        "--variational-submit-transport",
        "api",
        "--lighter-submit-transport",
        "ws",
        "--lighter-order-mode",
        "market-ioc",
        "--live-max-notional-usd",
        config.live_max_notional_usd,
        "--live-inventory",
        "--live-inventory-signal-mode",
        "basis",
        "--live-inventory-basis-entry-mode",
        "concurrent",
        "--live-inventory-lot-notional-usd",
        config.lot_notional_usd,
        "--live-inventory-max-total-notional-usd",
        effective_max_total_notional_usd,
        "--live-inventory-max-cycles",
        str(effective_max_cycles),
        "--live-inventory-max-lots",
        str(effective_max_lots),
        "--live-inventory-max-total-lots",
        str(effective_max_total_lots),
        "--live-inventory-max-venue-leverage",
        "5",
        "--live-inventory-margin-warning-pct",
        "40",
        "--live-inventory-margin-block-entry-pct",
        "50",
        "--live-inventory-margin-reduce-pct",
        "60",
        "--live-inventory-margin-emergency-pct",
        "75",
        "--live-inventory-equity-balance-warning-ratio",
        "0.82",
        "--live-inventory-equity-balance-block-ratio",
        "0.74",
        "--live-inventory-max-lighter-slippage-bps",
        config.max_lighter_slippage_bps,
        "--live-inventory-lighter-submit-slippage-bps",
        config.lighter_submit_slippage_bps,
        "--live-inventory-lighter-exit-submit-slippage-bps",
        config.lighter_exit_submit_slippage_bps,
        "--live-inventory-exit-blocked-log-throttle-seconds",
        config.exit_blocked_log_throttle_seconds,
        "--live-inventory-basis-min-entry-edge-bps",
        effective_min_entry_edge_bps,
        "--live-inventory-basis-min-abs-entry-bps",
        effective_min_abs_entry_bps,
        "--live-inventory-basis-max-entry-roundtrip-cost-bps",
        effective_max_entry_roundtrip_cost_bps,
        "--live-inventory-basis-min-entry-quality-score-bps",
        config.min_entry_quality_score_bps,
        "--live-inventory-basis-dynamic-entry-noise-buffer-bps",
        config.dynamic_entry_noise_buffer_bps,
        "--live-inventory-basis-spread-regime-penalty-multiplier",
        config.spread_regime_penalty_multiplier,
        "--live-inventory-basis-min-exit-pnl-bps",
        config.min_exit_pnl_bps,
        "--live-inventory-basis-min-signal-reverted-exit-pnl-bps",
        config.min_signal_reverted_exit_pnl_bps,
        "--live-inventory-basis-profit-take-pnl-bps",
        "0" if v4_live_mode else config.profit_take_pnl_bps,
        "--live-inventory-basis-entry-confirm-samples",
        str(config.entry_confirm_samples),
        "--live-inventory-basis-max-sample-move-bps",
        config.max_sample_move_bps,
        "--live-inventory-basis-stablecoin-normalization",
        "--live-inventory-entry-lighter-fill-timeout-seconds",
        config.entry_lighter_fill_timeout_seconds,
        "--live-inventory-snapshot-timeout-seconds",
        config.snapshot_timeout_seconds,
    ]
    command.append(
        "--live-inventory-force-close-open-state"
        if close_open_position
        else "--live-inventory-i-accept-open-state-resume"
        if resume_open_position
        else "--live-inventory-i-confirm-flat-start"
    )
    if maintenance_drain_after_start:
        command.append("--live-inventory-maintenance-drain-after-start")
    if collect_only:
        command.extend(["--live-inventory-dry-decisions", "--live-inventory-collect-only"])
    elif not reset_state_only_after_manual_flat:
        command.extend(["--lighter-prewarm-submit-ws", "--live-inventory-i-accept-basis-real-diagnostic"])
    if reset_state_only_after_manual_flat:
        command.extend(
            [
                "--live-inventory-reset-state-only-after-manual-flat",
                "--live-inventory-flat-reset-confirmation",
                flat_reset_confirmation or f"FLAT:{asset.upper()}",
            ]
        )
        return command
    if config.dynamic_entry_threshold and not v4_live_mode:
        command.append("--live-inventory-basis-dynamic-entry-threshold")
    if not reversion_mode and not calibration_mode and not v4_live_mode:
        command.extend(
            [
                "--live-inventory-basis-use-normalized-edge-for-entry",
                "--live-inventory-basis-stablecoin-regime-entry",
                "--live-inventory-basis-min-normalized-entry-edge-bps",
                config.min_normalized_entry_edge_bps,
                "--live-inventory-basis-min-normalized-filter-edge-bps",
                config.min_normalized_filter_edge_bps,
            ]
        )
    elif v4_live_mode:
        command.extend(
            [
                "--live-inventory-basis-v4-profile",
                V4_PROFILE_ETH_SHORT_20260724,
                "--live-inventory-i-accept-basis-v4-live",
                "--live-inventory-basis-max-hold-action",
                "exit",
                "--live-inventory-basis-refresh-exit-quote-before-submit",
                "--live-inventory-basis-dynamic-exit-buffer",
                "--live-inventory-max-unrealized-loss-bps",
                "50",
                "--live-inventory-min-hold-samples",
                "0",
                "--live-inventory-max-hold-samples",
                "2147483647",
                "--live-inventory-basis-max-var-quote-age-ms",
                "1500",
                "--live-inventory-max-lighter-book-age-seconds",
                "2",
                "--live-inventory-basis-size-ladder-notionals-usd",
                "20,40,60",
                "--live-inventory-basis-v4-max-run-loss-usd",
                config.v4_test_max_run_loss_usd,
                "--live-inventory-basis-v4-cycle-cooldown-seconds",
                str(config.v4_test_cycle_cooldown_seconds),
            ]
        )
        if v4_test_skip_recent_health:
            command.append(
                "--live-inventory-basis-v4-test-skip-recent-health"
            )
        if v4_test_allow_weekend:
            command.append("--live-inventory-basis-v4-test-allow-weekend")
        if v4_shadow_gradient:
            command.append("--live-inventory-basis-v4-shadow-gradient")
        if v4_real_gradient:
            command.append("--live-inventory-basis-v4-real-gradient")
        if v4_elastic_capacity:
            command.append("--live-inventory-basis-v4-elastic-capacity")
        if v4_reverse_test:
            command.append("--live-inventory-basis-v4-reverse-test")
        if v4_bidirectional:
            command.append("--live-inventory-basis-v4-bidirectional")
            command.append("--live-inventory-basis-disable-negative-direction")
        if v4_continuous:
            command.append("--live-inventory-basis-v4-continuous")
    elif reversion_mode:
        command.extend(
            [
                "--live-inventory-basis-max-hold-action",
                "exit",
                "--live-inventory-basis-refresh-exit-quote-before-submit",
                "--live-inventory-basis-max-var-quote-age-ms",
                "1500",
                "--live-inventory-max-lighter-book-age-seconds",
                "2",
                "--live-inventory-basis-reversion",
                "--live-inventory-basis-reversion-min-deviation-bps",
                config.reversion_min_deviation_bps,
                "--live-inventory-basis-reversion-exit-deviation-bps",
                config.reversion_exit_deviation_bps,
                "--live-inventory-basis-reversion-max-entry-roundtrip-cost-bps",
                config.reversion_max_entry_roundtrip_cost_bps,
                "--live-inventory-basis-reversion-context-gap-bps",
                config.reversion_context_gap_bps,
                "--live-inventory-basis-reversion-long-execution-reserve-bps",
                config.reversion_long_execution_reserve_bps,
                "--live-inventory-basis-reversion-short-execution-reserve-bps",
                config.reversion_short_execution_reserve_bps,
                "--live-inventory-basis-reversion-min-net-expected-pnl-bps",
                config.reversion_min_net_expected_pnl_bps,
                "--live-inventory-basis-reversion-signal-exit-min-pnl-bps",
                config.reversion_signal_exit_min_pnl_bps,
                "--live-inventory-basis-min-normalized-filter-edge-bps",
                config.reversion_min_normalized_edge_bps,
                "--live-inventory-basis-max-stablecoin-edge-share",
                config.reversion_max_stablecoin_edge_share,
            ]
        )
    else:
        command.extend(
            [
                "--live-inventory-execution-calibration",
                "--live-inventory-i-accept-execution-calibration-loss",
                "--live-inventory-calibration-direction",
                config.calibration_direction,
                "--live-inventory-calibration-warmup-samples",
                str(config.calibration_warmup_samples),
                "--live-inventory-calibration-entry-cooldown-samples",
                str(config.calibration_entry_cooldown_samples),
                "--live-inventory-calibration-max-run-loss-usd",
                config.calibration_max_run_loss_usd,
                "--live-inventory-calibration-max-cycle-loss-usd",
                config.calibration_max_cycle_loss_usd,
                "--live-inventory-min-hold-samples",
                str(config.calibration_hold_samples),
                "--live-inventory-max-hold-samples",
                str(config.calibration_hold_samples),
                "--live-inventory-basis-max-hold-action",
                "exit",
                "--live-inventory-basis-profit-take-pnl-bps",
                "0",
                "--live-inventory-basis-max-var-quote-age-ms",
                "750",
                "--live-inventory-max-lighter-book-age-seconds",
                "1",
            ]
        )
        if config.calibration_weekdays_only:
            command.append("--live-inventory-calibration-weekdays-only")
    if v4_real_gradient or effective_max_total_lots > 1:
        command.extend(
            [
                "--live-inventory-i-accept-basis-addon-diagnostic",
                "--live-inventory-basis-addon-min-basis-improvement-bps",
                config.addon_min_basis_improvement_bps,
            ]
        )
    if reset_state_after_manual_flat:
        command.append("--live-inventory-reset-state-after-manual-flat")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description="Start the real live arbitrage runner.")
    asset_group = parser.add_mutually_exclusive_group(required=True)
    asset_group.add_argument("--asset", help="Single live or collect-only asset: BTC, ETH, or SOL.")
    asset_group.add_argument("--assets", help="Comma-separated assets for the hard-isolated multi-asset collect-only process.")
    parser.add_argument("--config", default=str(LIVE_CONFIG), help="Startup config JSON. Default: live_config.json.")
    parser.add_argument("--reversion", action="store_true", help="Explicitly enable the one-lot basis reversion live test.")
    parser.add_argument(
        "--v4-live",
        action="store_true",
        help="Enable the bounded ETH V4 live profile.",
    )
    parser.add_argument(
        "--v4-test-skip-recent-health",
        action="store_true",
        help="Bounded V4 test only: bypass the recent 1h continuity gate for this run.",
    )
    parser.add_argument(
        "--v4-test-allow-weekend",
        action="store_true",
        help="Deprecated compatibility flag; V4 now trades continuously.",
    )
    parser.add_argument(
        "--v4-shadow-gradient",
        action="store_true",
        help="Compare read-only second-tranche entries at +0.5/+1.0/+1.5/+2.0 bps. No additional real order is submitted.",
    )
    parser.add_argument(
        "--v4-real-gradient",
        action="store_true",
        help="Enable five dynamic V4 leverage tiers using confirmed 20 USD child orders.",
    )
    parser.add_argument(
        "--v4-elastic-capacity",
        action="store_true",
        help=(
            "V4 real-gradient only: allow one guarded extra 20 USD child order "
            "after the first cumulative tier is full."
        ),
    )
    parser.add_argument(
        "--v4-reverse-test",
        action="store_true",
        help=(
            "Run one bounded 20 USD long-Variational/short-Lighter V4 test. "
            "Cannot be combined with gradient or continuous mode."
        ),
    )
    parser.add_argument(
        "--v4-bidirectional",
        action="store_true",
        help=(
            "Enable continuous two-way V4 opportunity selection. Each open "
            "episode remains locked to one direction until both venues are flat."
        ),
    )
    parser.add_argument(
        "--v4-continuous",
        action="store_true",
        help=(
            "Run real-gradient V4 across unlimited fully-flat cycles. "
            "Safety fuses and maintenance drain remain active."
        ),
    )
    parser.add_argument(
        "--close-open-position",
        action="store_true",
        help="One-shot: reconcile the saved open position, close all recorded lots with concurrent reduce-only orders, confirm final fills, and stop.",
    )
    parser.add_argument(
        "--resume-open-position",
        action="store_true",
        help=(
            "Resume a saved open position after a transient Variational data outage. "
            "Startup strictly reconciles both venue positions before management resumes."
        ),
    )
    parser.add_argument(
        "--drain-after-flat",
        action="store_true",
        help=(
            "Ask the currently running live strategy to block new entries, "
            "manage existing positions normally, verify both venues flat, and stop."
        ),
    )
    parser.add_argument(
        "--v4-test-max-cycles",
        type=int,
        choices=range(1, 4),
        default=1,
        metavar="1..3",
        help="Bounded V4 test batch size. Values above 1 require --v4-test-skip-recent-health.",
    )
    parser.add_argument(
        "--calibration",
        action="store_true",
        help="Explicitly enable bounded real execution-cost calibration. This intentionally submits and closes small real positions.",
    )
    parser.add_argument(
        "--calibration-direction",
        choices=sorted(CALIBRATION_DIRECTIONS),
        help="Override the execution-calibration direction for this run.",
    )
    parser.add_argument(
        "--calibration-weekdays-only",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Allow new execution-calibration entries only on UTC weekdays.",
    )
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Continuously collect executable basis states without real or dry inventory entries.",
    )
    parser.add_argument(
        "--reset-state-after-manual-flat",
        action="store_true",
        help=(
            "After manual flat confirmation, reset state and continue into the "
            "selected live strategy; use --reset-local-state-only to exit without trading."
        ),
    )
    parser.add_argument(
        "--reset-local-state-only",
        action="store_true",
        help=(
            "Verify both exchanges show zero positions and active orders, back up and "
            "reset local inventory state, then exit without starting the strategy."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print checks without starting live.")
    parser.add_argument("--verbose", action="store_true", help="Print the full main.py command.")
    args = parser.parse_args()

    asset = args.asset.upper() if args.asset else None
    assets = tuple(dict.fromkeys(token.strip().upper() for token in str(args.assets or "").split(",") if token.strip()))
    if asset is not None and asset not in ALLOWED_ASSETS:
        parser.error(f"--asset must be one of {sorted(ALLOWED_ASSETS)}")
    if assets and (len(assets) < 2 or any(item not in ALLOWED_ASSETS for item in assets)):
        parser.error(f"--assets requires at least two unique values from {sorted(ALLOWED_ASSETS)}")
    if assets and not args.collect_only:
        parser.error("--assets is hard-isolated to --collect-only and cannot be used by live/reversion/calibration")
    if args.drain_after_flat and not args.resume_open_position:
        if assets:
            parser.error("--drain-after-flat requires one --asset")
        incompatible = (
            args.reversion
            or args.calibration
            or args.v4_live
            or args.collect_only
            or args.reset_state_after_manual_flat
            or args.close_open_position
            or args.v4_shadow_gradient
            or args.v4_real_gradient
            or args.v4_reverse_test
            or args.v4_bidirectional
            or args.v4_continuous
        )
        if incompatible:
            parser.error(
                "--drain-after-flat cannot be combined with strategy startup flags"
            )
        processes = running_live_strategy_processes()
        if len(processes) != 1:
            print(
                "REFUSE_DRAIN reason=strategy_process_count_not_one "
                f"count={len(processes)}"
            )
            for process in processes:
                print(process)
            return 2
        try:
            request = request_maintenance_drain(
                asset=str(asset),
                process_line=processes[0],
                state=read_json(LIVE_STATE),
            )
        except ValueError as exc:
            print(f"REFUSE_DRAIN reason={exc}")
            return 2
        print("maintenance_drain=REQUESTED")
        print(f"asset={request['asset']}")
        print(f"target_pid={request['target_pid']}")
        print(f"target_run_id={request.get('target_run_id') or '-'}")
        print("action=block_new_entries_manage_existing_positions_until_flat")
        return 0
    try:
        config = load_config(Path(args.config))
    except ValueError as exc:
        print(f"REFUSE_START reason=config_invalid detail={exc}")
        return 2
    if sum(bool(value) for value in (args.reversion, args.calibration, args.v4_live, args.collect_only)) > 1:
        parser.error("use only one of --reversion, --calibration, --v4-live, or --collect-only")
    if args.collect_only and args.reset_state_after_manual_flat:
        parser.error("--collect-only does not allow --reset-state-after-manual-flat")
    if args.reset_local_state_only and (
        assets
        or args.reversion
        or args.calibration
        or args.v4_live
        or args.v4_shadow_gradient
        or args.v4_real_gradient
        or args.v4_reverse_test
        or args.v4_bidirectional
        or args.v4_continuous
        or args.collect_only
        or args.reset_state_after_manual_flat
        or args.close_open_position
        or args.resume_open_position
        or args.drain_after_flat
    ):
        parser.error("--reset-local-state-only is a standalone one-shot mode")
    if args.close_open_position and not args.v4_live:
        parser.error("--close-open-position requires --v4-live")
    if args.close_open_position and (
        args.reset_state_after_manual_flat
        or args.collect_only
        or args.reversion
        or args.calibration
        or args.v4_shadow_gradient
        or args.v4_real_gradient
        or args.v4_continuous
    ):
        parser.error(
            "--close-open-position cannot be combined with reset, collect-only, "
            "reversion, calibration, or gradient flags"
        )
    if args.resume_open_position and not args.v4_live:
        parser.error("--resume-open-position requires --v4-live")
    if args.resume_open_position and (
        args.close_open_position
        or args.reset_state_after_manual_flat
        or args.collect_only
        or args.reversion
        or args.calibration
    ):
        parser.error(
            "--resume-open-position cannot be combined with close, reset, "
            "collect-only, reversion, or calibration modes"
        )
    if args.reversion:
        config = replace(
            config,
            reversion_mode=True,
            calibration_mode=False,
            v4_live_mode=False,
        )
    if args.calibration:
        config = replace(config, calibration_mode=True, reversion_mode=False, v4_live_mode=False)
    if args.v4_live:
        config = replace(
            config,
            calibration_mode=False,
            reversion_mode=False,
            v4_live_mode=True,
            max_cycles=0 if args.v4_continuous else args.v4_test_max_cycles,
        )
    if args.calibration_direction is not None:
        config = replace(config, calibration_direction=args.calibration_direction)
    if args.calibration_weekdays_only is not None:
        config = replace(config, calibration_weekdays_only=args.calibration_weekdays_only)
    if args.collect_only:
        config = replace(config, calibration_mode=False, reversion_mode=False, v4_live_mode=False)
    if sum(bool(value) for value in (config.reversion_mode, config.calibration_mode, config.v4_live_mode)) > 1:
        print("REFUSE_START reason=config_invalid detail=live_strategy_modes_are_mutually_exclusive")
        return 2
    if config.v4_live_mode and asset != "ETH":
        parser.error("--v4-live requires --asset ETH")
    if args.v4_test_skip_recent_health and not config.v4_live_mode:
        parser.error("--v4-test-skip-recent-health requires --v4-live")
    if args.v4_test_allow_weekend and not (
        config.v4_live_mode and args.v4_test_skip_recent_health
    ):
        parser.error(
            "--v4-test-allow-weekend requires --v4-live and "
            "--v4-test-skip-recent-health"
        )
    if args.v4_shadow_gradient and not config.v4_live_mode:
        parser.error("--v4-shadow-gradient requires --v4-live")
    if args.v4_real_gradient and not config.v4_live_mode:
        parser.error("--v4-real-gradient requires --v4-live")
    if args.v4_real_gradient and args.v4_shadow_gradient:
        parser.error("use only one of --v4-real-gradient or --v4-shadow-gradient")
    if args.v4_elastic_capacity and not args.v4_real_gradient:
        parser.error("--v4-elastic-capacity requires --v4-real-gradient")
    if args.v4_reverse_test and not config.v4_live_mode:
        parser.error("--v4-reverse-test requires --v4-live")
    if args.v4_reverse_test and (
        args.v4_real_gradient or args.v4_shadow_gradient or args.v4_continuous
    ):
        parser.error(
            "--v4-reverse-test cannot be combined with gradient, shadow, or continuous mode"
        )
    if args.v4_bidirectional and args.v4_reverse_test:
        parser.error("--v4-bidirectional cannot be combined with --v4-reverse-test")
    if args.v4_bidirectional and not (
        config.v4_live_mode and args.v4_real_gradient and args.v4_continuous
    ):
        parser.error(
            "--v4-bidirectional requires --v4-live --v4-real-gradient --v4-continuous"
        )
    if args.v4_continuous and not (
        config.v4_live_mode and args.v4_real_gradient
    ):
        parser.error("--v4-continuous requires --v4-live and --v4-real-gradient")
    if args.v4_continuous and args.v4_test_max_cycles != 1:
        parser.error(
            "--v4-continuous cannot be combined with --v4-test-max-cycles"
        )
    if args.v4_test_max_cycles > 1 and not (
        config.v4_live_mode and args.v4_test_skip_recent_health
    ):
        parser.error(
            "--v4-test-max-cycles above 1 requires --v4-live and "
            "--v4-test-skip-recent-health"
        )
    if args.v4_test_max_cycles > 3:
        parser.error("--v4-test-max-cycles cannot exceed 3")
    if (
        (args.v4_test_max_cycles > 1 or args.v4_continuous)
        and Decimal(config.v4_test_max_run_loss_usd) <= 0
    ):
        parser.error(
            "multi-cycle or continuous V4 requires "
            "v4_test_max_run_loss_usd > 0"
        )
    if (
        args.calibration_direction is not None
        or args.calibration_weekdays_only is not None
    ) and not config.calibration_mode:
        parser.error("--calibration-direction/--calibration-weekdays-only require calibration mode")

    processes = running_main_processes()
    if processes:
        print("REFUSE_START reason=python_main_already_running")
        for process in processes:
            print(process)
        return 2

    state_ok, state_message = validate_state(
        config,
        reset_state_after_manual_flat=args.reset_state_after_manual_flat,
        reset_state_only_after_manual_flat=args.reset_local_state_only,
        collect_only=args.collect_only,
        close_open_position=args.close_open_position,
        resume_open_position=args.resume_open_position,
    )
    print(state_message)
    print(disk_warning())
    if not state_ok:
        refusal_reason = (
            "persisted_v4_batch_halt"
            if state_message.startswith("state_v4_batch_halted")
            else "local_live_state_not_flat"
        )
        print(f"REFUSE_START reason={refusal_reason}")
        return 2
    if not args.dry_run and not disk_start_allowed():
        print(
            "REFUSE_START reason=disk_free_below_3gb "
            "action=stop_processes_then_run_tools/compact_order_metrics.py_"
            "and_tools/archive_legacy_logs.py"
        )
        return 2

    command = (
        build_multi_asset_collector_command(assets)
        if assets
        else build_main_command(
            str(asset),
            config,
            reset_state_after_manual_flat=args.reset_state_after_manual_flat,
            collect_only=args.collect_only,
            v4_test_skip_recent_health=args.v4_test_skip_recent_health,
            v4_test_allow_weekend=args.v4_test_allow_weekend,
            v4_shadow_gradient=args.v4_shadow_gradient,
            v4_real_gradient=args.v4_real_gradient,
            v4_elastic_capacity=args.v4_elastic_capacity,
            v4_reverse_test=args.v4_reverse_test,
            v4_bidirectional=args.v4_bidirectional,
            v4_continuous=args.v4_continuous,
            close_open_position=args.close_open_position,
            resume_open_position=args.resume_open_position,
            maintenance_drain_after_start=(
                args.drain_after_flat and args.resume_open_position
            ),
            reset_state_only_after_manual_flat=args.reset_local_state_only,
            flat_reset_confirmation=f"FLAT:{asset}" if asset else None,
        )
    )
    effective_max_cycles = (
        config.calibration_max_cycles
        if config.calibration_mode
        else 1
        if config.reversion_mode
        else config.max_cycles
    )
    strategy_mode = (
        "manual_flat_state_reset_only"
        if args.reset_local_state_only
        else "basis_v3_collect_only"
        if args.collect_only
        else "execution_calibration"
        if config.calibration_mode
        else (
            "basis_v4_live_continuous_health_bypass"
            if args.v4_continuous and args.v4_test_skip_recent_health
            else "basis_v4_live_continuous"
            if args.v4_continuous
            else "basis_v4_live_test_health_weekend_bypass"
            if args.v4_test_allow_weekend
            else "basis_v4_live_test_health_bypass"
            if args.v4_test_skip_recent_health
            else "basis_v4_live"
        )
        if config.v4_live_mode
        else "reversion"
        if config.reversion_mode
        else "basis"
    )
    asset_text = ",".join(assets) if assets else str(asset)
    if assets:
        strategy_mode = "basis_multi_asset_collect_only"
    max_cycles_text = (
        "unlimited" if effective_max_cycles == 0 else effective_max_cycles
    )
    print(
        f"starting asset={asset_text} strategy_mode={strategy_mode} "
        f"max_cycles={max_cycles_text} lot_notional_usd={config.lot_notional_usd}"
    )
    if args.verbose:
        print("main_command=" + " ".join(command))
    if args.dry_run:
        print("DRY_RUN no live process started")
        return 0

    if args.collect_only:
        print("Starting collect-only basis logging. Real and dry inventory entries are disabled.")
    elif args.reset_local_state_only:
        if not args.dry_run:
            expected_confirmation = f"FLAT:{asset}"
            confirmation = input(
                f"确认 {asset} 在 Variational 与 Lighter 的仓位和未成交订单均为 0，输入 {expected_confirmation}: "
            ).strip().upper()
            if confirmation != expected_confirmation:
                print("REFUSE_RESET reason=operator_confirmation_mismatch")
                return 2
            command[command.index("--live-inventory-flat-reset-confirmation") + 1] = confirmation
        print("Only exchange verification and local-state reset will run; no trading loop will start.")
    else:
        print("Starting live. You are responsible for confirming both exchanges are flat before running this command.")
    return subprocess.call(command, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
