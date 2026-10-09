#!/usr/bin/env python3
from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
import os
import subprocess
import sys
from collections import Counter, deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (  # noqa: E402
    BASIS_SAMPLES_DIR,
    COLLECTOR_LOG,
    LIVE_CONTROL,
    LIVE_STATE,
    LOG_DIR,
    ORDER_METRICS,
    JSONL_TAIL_MAX_BYTES,
    JSONL_TAIL_MAX_LINE_BYTES,
    JSONL_TAIL_MAX_ROWS,
    RUNTIME_LOG,
    avg,
    fmt_decimal,
    human_bytes,
    parse_time,
    percentile,
    read_json,
    rotated_jsonl_paths,
    tail_jsonl,
    tail_jsonl_many,
    tail_text,
    to_decimal,
)
from tools.lib.basis_store import read_basis_samples  # noqa: E402


MAX_ANALYZE_TAIL_ROWS = JSONL_TAIL_MAX_ROWS
ANALYZE_SOURCE_MAX_BYTES = JSONL_TAIL_MAX_BYTES


def bounded_diagnostic_line(line: str, max_chars: int = 500) -> str:
    if len(line) <= max_chars:
        return line
    return f"{line[:max_chars]}... [truncated original_chars={len(line)}]"


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
        and "tools/analyze.py" not in line
        and process_belongs_to_this_project(line)
    ]


def process_belongs_to_this_project(line: str) -> bool:
    try:
        process_id = int(line.split(maxsplit=1)[0])
        process_cwd = Path(os.readlink(f"/proc/{process_id}/cwd")).resolve()
        process_cwd.relative_to(ROOT.resolve())
    except (IndexError, OSError, ValueError):
        return False
    return True


def latest_run_filter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_run_id = ""
    for row in reversed(rows):
        run_id = str(row.get("run_id") or "")
        if run_id:
            latest_run_id = run_id
            break
    if not latest_run_id:
        return rows
    return [row for row in rows if str(row.get("run_id") or "") == latest_run_id]


def build_v4_entry_episode_summary(
    state_rows: list[dict[str, Any]], *, max_gap_seconds: float = 90.0
) -> dict[str, dict[str, dict[str, Any]]]:
    directions = ("long_var_short_lighter", "short_var_long_lighter")
    relaxations = (Decimal("0"), Decimal("1"), Decimal("2"))
    active: dict[tuple[str, Decimal], dict[str, Any] | None] = {
        (direction, relaxation): None
        for direction in directions
        for relaxation in relaxations
    }
    summary: dict[tuple[str, Decimal], dict[str, Any]] = {
        key: {"samples": 0, "episodes": 0, "multi_sample_episodes": 0, "durations": []}
        for key in active
    }

    def finish(key: tuple[str, Decimal]) -> None:
        episode = active[key]
        if episode is None:
            return
        stats = summary[key]
        stats["episodes"] += 1
        if episode["samples"] >= 2:
            stats["multi_sample_episodes"] += 1
        stats["durations"].append(max(0.0, episode["last_at"] - episode["first_at"]))
        active[key] = None

    unique_rows: dict[tuple[str, str], dict[str, Any]] = {}
    without_sample_index: list[dict[str, Any]] = []
    for row in state_rows:
        run_id = str(row.get("run_id") or "")
        sample_index = row.get("sample_index")
        if not run_id or sample_index is None:
            without_sample_index.append(row)
            continue
        key = (run_id, str(sample_index))
        prior = unique_rows.get(key)
        prefer_row = prior is None or (
            row.get("sample_quality") == "valid"
            and prior.get("sample_quality") != "valid"
        ) or (
            row.get("sample_pair_valid") is True
            and prior.get("sample_pair_valid") is not True
        )
        if prefer_row:
            unique_rows[key] = row
    ordered_rows = [*unique_rows.values(), *without_sample_index]
    for row in sorted(ordered_rows, key=lambda item: str(item.get("logged_at") or "")):
        at = parse_time(row.get("logged_at"))
        quality = str(row.get("sample_quality") or "legacy")
        usable = (
            at is not None
            and row.get("sample_pair_valid") is not False
            and quality in {"valid", "legacy"}
        )
        edges = row.get("v4_direction_edges_bps")
        thresholds = row.get("v4_direction_thresholds_bps")
        if not isinstance(edges, dict):
            edges = {}
        if not isinstance(thresholds, dict):
            thresholds = {}
        for direction in directions:
            edge = to_decimal(edges.get(direction))
            threshold = to_decimal(thresholds.get(direction))
            if edge is None and row.get("v4_entry_direction") == direction:
                edge = to_decimal(row.get("v4_signal_edge_bps"))
                threshold = threshold or to_decimal(row.get("v4_entry_threshold_bps"))
            timestamp = at.timestamp() if at is not None else None
            for relaxation in relaxations:
                key = (direction, relaxation)
                qualifies = bool(
                    usable
                    and edge is not None
                    and threshold is not None
                    and edge >= threshold - relaxation
                )
                episode = active[key]
                if (
                    episode is not None
                    and timestamp is not None
                    and timestamp - episode["last_at"] > max_gap_seconds
                ):
                    finish(key)
                    episode = None
                if not qualifies or timestamp is None:
                    finish(key)
                    continue
                if episode is None:
                    active[key] = {
                        "first_at": timestamp,
                        "last_at": timestamp,
                        "samples": 1,
                    }
                else:
                    episode["last_at"] = timestamp
                    episode["samples"] += 1
                summary[key]["samples"] += 1
    for key in active:
        finish(key)

    result: dict[str, dict[str, dict[str, Any]]] = {}
    for (direction, relaxation), stats in summary.items():
        durations = stats.pop("durations")
        result.setdefault(direction, {})[str(relaxation)] = {
            **stats,
            "median_duration_seconds": median(durations) if durations else None,
        }
    return result


def build_v4_exit_observation_summary(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    lots: dict[str, dict[str, Any]] = {}
    for row in rows:
        event = row.get("event")
        if event == "live_inventory_v4_exit_observation":
            observations = row.get("lots")
            if not isinstance(observations, list):
                continue
            items = observations
        elif event == "live_inventory_exited":
            items = [
                {
                    "lot_id": row.get("lot_id"),
                    "direction": row.get("direction"),
                    "executable_mfe_pnl_bps": row.get(
                        "executable_exit_mfe_pnl_bps"
                    ),
                    "effective_exit_target_bps": row.get(
                        "effective_min_exit_pnl_bps"
                    ),
                    "matched_quote_observations": None,
                    "final_pnl_bps": row.get("pnl_bps"),
                }
            ]
            exited_lot_id = row.get("lot_id")
            if exited_lot_id is not None:
                stats = lots.setdefault(
                    str(exited_lot_id), {"lot_id": str(exited_lot_id)}
                )
                stats["exit_event_seen"] = True
        elif event == "live_inventory_actual_pnl":
            if row.get("actual_pnl_status") != "lighter_final_fill_confirmed":
                continue
            lot_id = row.get("lot_id")
            if lot_id is None:
                continue
            stats = lots.setdefault(str(lot_id), {"lot_id": str(lot_id)})
            stats["actual_pnl_bps"] = row.get("actual_pnl_bps")
            stats["actual_pnl_usd"] = row.get("actual_pnl_usd")
            stats["closed_child_lots"] = row.get("closed_child_lots")
            stats["actual_pnl_confirmed_at"] = row.get("confirmed_at") or row.get(
                "logged_at"
            )
            continue
        else:
            continue
        for item in items:
            if not isinstance(item, dict) or item.get("lot_id") is None:
                continue
            lot_id = str(item["lot_id"])
            stats = lots.setdefault(lot_id, {"lot_id": lot_id})
            mfe = to_decimal(item.get("executable_mfe_pnl_bps"))
            prior = to_decimal(stats.get("observed_peak_bps"))
            if mfe is not None and (prior is None or mfe > prior):
                stats["observed_peak_bps"] = str(mfe)
            for key in (
                "direction",
                "effective_exit_target_bps",
                "matched_quote_observations",
                "last_matched_at",
            ):
                if item.get(key) is not None:
                    stats[key] = item[key]
            stats["last_snapshot_at"] = row.get("observed_at") or row.get(
                "logged_at"
            )
            if item.get("final_pnl_bps") is not None:
                stats["final_pnl_bps"] = item["final_pnl_bps"]
    return lots


def build_v4_exit_target_sensitivity(
    lots: dict[str, dict[str, Any]],
    *,
    candidate_targets_bps: tuple[Decimal, ...] = (
        Decimal("3.5"),
        Decimal("4.0"),
        Decimal("4.5"),
    ),
) -> list[dict[str, Any]]:
    observed_lots = [
        stats
        for stats in lots.values()
        if to_decimal(stats.get("observed_peak_bps")) is not None
    ]
    results = []
    for target in candidate_targets_bps:
        crossed = [
            stats
            for stats in observed_lots
            if to_decimal(stats.get("observed_peak_bps")) >= target
        ]
        closed_confirmed = [
            stats
            for stats in crossed
            if to_decimal(stats.get("actual_pnl_bps")) is not None
            and int(to_decimal(stats.get("closed_child_lots")) or 0) == 1
        ]
        exited = [stats for stats in crossed if stats.get("exit_event_seen")]
        exited_without_confirmed_pnl = [
            stats
            for stats in exited
            if to_decimal(stats.get("actual_pnl_bps")) is None
        ]
        confirmed_multi_lot_exits = [
            stats
            for stats in exited
            if to_decimal(stats.get("actual_pnl_bps")) is not None
            and int(to_decimal(stats.get("closed_child_lots")) or 0) != 1
        ]
        lower_than_recorded_target = [
            stats
            for stats in closed_confirmed
            if (to_decimal(stats.get("effective_exit_target_bps")) or target)
            > target
        ]
        actual_values = [
            to_decimal(stats.get("actual_pnl_bps"))
            for stats in closed_confirmed
        ]
        valid_actuals = [value for value in actual_values if value is not None]
        results.append(
            {
                "target_bps": target,
                "observed_lots": len(observed_lots),
                "peak_crossed_lots": len(crossed),
                "open_crossed_lots": len(crossed) - len(exited),
                "exited_without_confirmed_pnl": len(exited_without_confirmed_pnl),
                "confirmed_multi_lot_exits": len(confirmed_multi_lot_exits),
                "confirmed_single_lot_closes_crossed": len(closed_confirmed),
                "closed_crossed_below_recorded_target": len(
                    lower_than_recorded_target
                ),
                "confirmed_actual_avg_pnl_bps": (
                    sum(valid_actuals, Decimal("0")) / len(valid_actuals)
                    if valid_actuals
                    else None
                ),
            }
        )
    return results


def build_v4_live_funnel(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    v4_rows = [
        row
        for row in rows
        if str(row.get("strategy_version") or "").startswith("basis-v4-live")
        or row.get("basis_v4_profile")
        or str(row.get("entry_v4_profile") or "").startswith(
            "eth_short_execution_calibrated_"
        )
        or str(row.get("profile") or "").startswith("eth_short_execution_calibrated_")
    ]
    if not v4_rows:
        return None

    state_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_basis_state"
    ]
    threshold_crossings = 0
    for row in state_rows:
        edge = to_decimal(
            row.get("v4_signal_edge_bps")
            if row.get("v4_signal_edge_bps") is not None
            else row.get("short_edge_bps")
        )
        threshold = to_decimal(row.get("v4_entry_threshold_bps"))
        if edge is not None and threshold is not None and edge > threshold:
            threshold_crossings += 1
    entry_episode_summary = build_v4_entry_episode_summary(state_rows)

    events = Counter(str(row.get("event") or "-") for row in v4_rows)
    reasons = Counter(
        str(row.get("reason") or "unknown")
        for row in v4_rows
        if row.get("event") == "live_inventory_entry_blocked"
    )
    exit_block_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_exit_blocked"
    ]
    exit_block_reasons = Counter(
        str(row.get("reason") or "unknown") for row in exit_block_rows
    )
    exit_observation_summary = build_v4_exit_observation_summary(v4_rows)
    exit_target_sensitivity = build_v4_exit_target_sensitivity(
        exit_observation_summary
    )
    shadows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_entry_shadow_candidate"
    ]
    preflight_passed = sum(row.get("shadow_status") == "passed" for row in shadows)
    preflight_blocked = sum(row.get("shadow_status") == "blocked" for row in shadows)
    dynamic_floor_blocks = reasons["edge_bps_below_dynamic_live_inventory_entry"]
    latest_state = state_rows[-1] if state_rows else {}
    exit_submit_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_execution_ledger"
        and row.get("phase") == "exit"
        and row.get("execution_stage") == "submit_returned"
    ]
    latest_exit_submit = exit_submit_rows[-1] if exit_submit_rows else {}
    cycle_reports = [
        row for row in v4_rows if row.get("event") == "live_inventory_cycle_report"
    ]
    latest_cycle_report = cycle_reports[-1] if cycle_reports else {}
    cycle_checkpoints = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_v4_cycle_checkpoint"
    ]
    latest_cycle_checkpoint = cycle_checkpoints[-1] if cycle_checkpoints else {}
    batch_wait_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_v4_batch_waiting"
    ]
    latest_batch_wait = batch_wait_rows[-1] if batch_wait_rows else {}
    batch_wait_active = bool(
        latest_batch_wait
        and str(latest_batch_wait.get("logged_at") or "")
        >= str(latest_state.get("logged_at") or "")
    )
    strategy_snapshots = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_strategy_snapshot"
    ]
    latest_strategy_snapshot = strategy_snapshots[-1] if strategy_snapshots else {}
    account_snapshots = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_account_snapshot"
    ]
    latest_account_snapshot = account_snapshots[-1] if account_snapshots else {}
    portfolio_exit_locks = [
        row
        for row in v4_rows
        if row.get("event")
        in {
            "live_inventory_v4_portfolio_exit_locked",
            "live_inventory_v4_tier_exit_locked",
        }
    ]
    latest_portfolio_exit_lock = (
        portfolio_exit_locks[-1] if portfolio_exit_locks else {}
    )
    partial_detier_locks = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_v4_partial_detier_exit_locked"
    ]
    account_risk_rows = [
        row
        for row in v4_rows
        if row.get("event")
        in {
            "live_inventory_account_risk_alert",
            "live_inventory_account_risk_recovered",
        }
    ]
    latest_account_risk = account_risk_rows[-1] if account_risk_rows else {}
    risk_snapshot_fresh = latest_account_risk.get(
        "variational_account_snapshot_fresh"
    )
    risk_snapshot_usable = latest_account_risk.get(
        "variational_account_snapshot_usable"
    )
    risk_snapshot_degraded = latest_account_risk.get(
        "variational_account_snapshot_degraded"
    )
    risk_snapshot_age = latest_account_risk.get(
        "variational_account_snapshot_age_seconds"
    )
    variational_snapshot_fresh = (
        risk_snapshot_fresh
        if risk_snapshot_fresh is not None
        else latest_account_snapshot.get("variational_snapshot_fresh")
    )
    variational_snapshot_age_seconds = (
        risk_snapshot_age
        if risk_snapshot_age is not None
        else latest_account_snapshot.get("variational_snapshot_age_seconds")
    )
    variational_snapshot_status_source = (
        "account_risk"
        if risk_snapshot_fresh is not None or risk_snapshot_age is not None
        else "account_snapshot"
        if latest_account_snapshot
        else None
    )
    shadow_gradient_entries = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_v4_shadow_tranche_entered"
    ]
    shadow_gradient_exits = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_v4_shadow_tranche_exited"
    ]
    latest_shadow_gradient_exit = (
        shadow_gradient_exits[-1] if shadow_gradient_exits else {}
    )
    shadow_gradient_levels: dict[str, dict[str, Any]] = {}
    for row in shadow_gradient_entries:
        level = str(row.get("basis_improvement_trigger_bps") or "2.00")
        stats = shadow_gradient_levels.setdefault(
            level,
            {"entries": 0, "exits": 0, "wins": 0, "pnl_bps": []},
        )
        stats["entries"] += 1
    for row in shadow_gradient_exits:
        level = str(row.get("basis_improvement_trigger_bps") or "2.00")
        stats = shadow_gradient_levels.setdefault(
            level,
            {"entries": 0, "exits": 0, "wins": 0, "pnl_bps": []},
        )
        stats["exits"] += 1
        pnl_bps = to_decimal(row.get("shadow_pnl_bps"))
        if pnl_bps is not None:
            stats["pnl_bps"].append(pnl_bps)
            if pnl_bps > 0:
                stats["wins"] += 1
    shadow_gradient_level_summary = {}
    for level, stats in sorted(
        shadow_gradient_levels.items(),
        key=lambda item: to_decimal(item[0]) or Decimal("0"),
    ):
        pnl_values = stats.pop("pnl_bps")
        exits = stats["exits"]
        shadow_gradient_level_summary[level] = {
            **stats,
            "active": stats["entries"] > exits,
            "win_rate": (stats["wins"] / exits if exits else None),
            "avg_pnl_bps": (
                fmt_decimal(sum(pnl_values) / len(pnl_values))
                if pnl_values
                else None
            ),
        }
    exit_calibration = latest_cycle_report or latest_strategy_snapshot
    final_pnl_rows = [
        row for row in v4_rows if row.get("event") == "live_inventory_final_pnl"
    ]
    latest_final_pnl = final_pnl_rows[-1] if final_pnl_rows else {}
    final_pnl_resolved = bool(
        latest_final_pnl
        and max(
            str(latest_cycle_report.get("logged_at") or ""),
            str(latest_cycle_checkpoint.get("logged_at") or ""),
        )
        >= str(latest_final_pnl.get("logged_at") or "")
    )
    fuse_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_runtime_fuse_triggered"
    ]
    stop_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_runtime_stopped"
    ]
    latest_fuse = fuse_rows[-1] if fuse_rows else {}
    latest_stop = stop_rows[-1] if stop_rows else {}
    history_rows = [
        row
        for row in v4_rows
        if row.get("event") == "live_inventory_basis_v4_history_loaded"
    ]
    anchor_context = latest_state or (history_rows[-1] if history_rows else {})

    if dynamic_floor_blocks:
        status = "ERROR_V4_IMMEDIATE_ARB_FLOOR_APPLIED"
    elif events["live_inventory_manual_review_required"]:
        status = "ERROR_MANUAL_REVIEW_REQUIRED"
    elif latest_fuse:
        status = "STOPPED_BY_RUNTIME_FUSE"
    elif latest_cycle_report.get("report_status") == "requires_reconciliation":
        status = "ERROR_RECONCILIATION_REQUIRED"
    elif latest_cycle_report.get("report_status") == "completed":
        status = "CYCLE_COMPLETE"
    elif (
        not final_pnl_resolved
        and latest_final_pnl.get("final_pnl_status")
        == "var_and_lighter_final_fills_confirmed"
        and (to_decimal(latest_final_pnl.get("remaining_child_lots")) or Decimal("0"))
        == 0
    ):
        status = "FINAL_FILLS_CONFIRMED_WAITING_FOR_REPORT"
    elif (
        latest_final_pnl
        and not final_pnl_resolved
        and (to_decimal(latest_final_pnl.get("remaining_child_lots")) or Decimal("0"))
        == 0
    ):
        status = "ERROR_FINAL_PNL_RECONCILIATION_REQUIRED"
    elif events["live_inventory_entered"] > events["live_inventory_exited"]:
        status = "POSITION_OPEN"
    elif batch_wait_active:
        status = "WAITING_FOR_NEXT_BATCH_CYCLE"
    elif latest_state.get("v4_rearm_required") is True:
        status = "WAITING_FOR_EPISODE_REARM"
    elif events["live_inventory_var_entry_submitted"]:
        status = "CANDIDATE_SUBMITTED"
    elif preflight_passed:
        status = "REVIEW_PREFLIGHT_PASSED_WITHOUT_SUBMIT"
    elif threshold_crossings:
        status = "CANDIDATES_FILTERED_BY_EXPECTED_GUARDS"
    elif latest_state.get("v4_anchor_ready") is False:
        status = "WAITING_FOR_ROLLING_7D_ANCHOR"
    elif latest_state.get("v4_health_ready") is False:
        status = "WAITING_FOR_RECENT_HEALTH_WINDOW"
    else:
        status = "WAITING_FOR_THRESHOLD_CROSSING"

    return {
        "profile": (
            latest_state.get("basis_v4_profile")
            or next(
                (
                    row.get("profile")
                    for row in reversed(v4_rows)
                    if row.get("profile")
                ),
                "-",
            )
        ),
        "samples": len(state_rows),
        "threshold_crossings": threshold_crossings,
        "entry_episode_summary": entry_episode_summary,
        "exit_observation_summary": exit_observation_summary,
        "exit_target_sensitivity": exit_target_sensitivity,
        "large_move_blocks": reasons["basis_sample_move_too_large"],
        "refreshed_edge_blocks": reasons["basis_entry_refreshed_edge_below_threshold"],
        "exit_block_reasons": dict(exit_block_reasons),
        "entry_cost_pending_exit_blocks": exit_block_reasons[
            "entry_final_fill_cost_pending"
        ],
        "exit_confirmation_pending_blocks": exit_block_reasons[
            "v4_exit_confirmation_pending"
        ],
        "preflight_reached": len(shadows),
        "preflight_passed": preflight_passed,
        "preflight_blocked": preflight_blocked,
        "dynamic_floor_blocks": dynamic_floor_blocks,
        "submits": events["live_inventory_var_entry_submitted"],
        "entered": events["live_inventory_entered"],
        "exited": events["live_inventory_exited"],
        "actual_pnl": events["live_inventory_actual_pnl"],
        "final_pnl": events["live_inventory_final_pnl"],
        "cycle_reports": len(cycle_reports),
        "cycle_checkpoints": len(cycle_checkpoints),
        "completed_cycles": (
            latest_cycle_report.get("completed_cycles")
            or latest_cycle_checkpoint.get("completed_cycles")
            or 0
        ),
        "max_cycles": next(
            (
                value
                for value in (
                    latest_cycle_report.get("max_cycles"),
                    latest_cycle_checkpoint.get("max_cycles"),
                    latest_strategy_snapshot.get("max_cycles"),
                )
                if value is not None
            ),
            None,
        ),
        "shadow_gradient_enabled": latest_strategy_snapshot.get(
            "shadow_gradient_enabled"
        ),
        "real_gradient_enabled": latest_strategy_snapshot.get(
            "real_gradient_enabled"
        ),
        "real_gradient_tiers": latest_strategy_snapshot.get(
            "real_gradient_tiers"
        ),
        "real_gradient_tier_percentiles": latest_strategy_snapshot.get(
            "real_gradient_tier_percentiles"
        ),
        "real_gradient_tier_thresholds_bps": latest_state.get(
            "v4_real_gradient_tier_thresholds_bps"
        ),
        "real_gradient_market_tier": latest_state.get(
            "v4_real_gradient_market_tier"
        ),
        "real_gradient_active_tier": latest_state.get(
            "v4_real_gradient_active_tier"
        ),
        "max_venue_leverage": latest_strategy_snapshot.get(
            "max_venue_leverage"
        ),
        "variational_snapshot_fresh": variational_snapshot_fresh,
        "variational_snapshot_usable": risk_snapshot_usable,
        "variational_snapshot_degraded": risk_snapshot_degraded,
        "variational_snapshot_age_seconds": variational_snapshot_age_seconds,
        "variational_snapshot_status_source": (
            variational_snapshot_status_source
        ),
        "variational_portfolio_refresh_attempted": latest_account_risk.get(
            "variational_portfolio_refresh_attempted"
        ),
        "variational_portfolio_refresh_ok": latest_account_risk.get(
            "variational_portfolio_refresh_ok"
        ),
        "variational_portfolio_refresh_reason": latest_account_risk.get(
            "variational_portfolio_refresh_reason"
        ),
        "variational_portfolio_refresh_error": latest_account_risk.get(
            "variational_portfolio_refresh_error"
        ),
        "sample_quality_version": latest_state.get("sample_quality_version"),
        "sample_pair_valid": latest_state.get("sample_pair_valid"),
        "quote_source": latest_state.get("quote_source"),
        "var_quote_age_seconds": latest_state.get("var_quote_age_seconds"),
        "var_signal_max_age_seconds": latest_state.get(
            "var_signal_max_age_seconds"
        ),
        "lighter_book_age_seconds": latest_state.get(
            "lighter_book_age_seconds"
        ),
        "portfolio_exit_locks": len(portfolio_exit_locks),
        "portfolio_exit_last_lot_ids": latest_portfolio_exit_lock.get("lot_ids"),
        "portfolio_exit_last_pnl_bps": latest_portfolio_exit_lock.get(
            "aggregate_executable_pnl_bps"
        ),
        "portfolio_exit_last_target_bps": latest_portfolio_exit_lock.get(
            "effective_min_exit_pnl_bps"
        ),
        "partial_detier_locks": len(partial_detier_locks),
        "robinhood_status": latest_state.get("v4_robinhood_status"),
        "robinhood_fresh": latest_state.get("v4_robinhood_fresh"),
        "robinhood_sample_age_seconds": latest_state.get(
            "v4_robinhood_sample_age_seconds"
        ),
        "robinhood_threshold_penalty_bps": latest_state.get(
            "v4_robinhood_threshold_penalty_bps"
        ),
        "variational_order_rate_used_60s": latest_state.get(
            "variational_order_rate_used_60s"
        ),
        "variational_order_rate_limits_60s": (
            latest_state.get("variational_order_rate_normal_limit_60s"),
            latest_state.get("variational_order_rate_hard_limit_60s"),
        ),
        "lighter_order_rate_used_60s": latest_state.get(
            "lighter_order_rate_used_60s"
        ),
        "lighter_order_rate_limits_60s": (
            latest_state.get("lighter_order_rate_normal_limit_60s"),
            latest_state.get("lighter_order_rate_hard_limit_60s"),
        ),
        "weekend_market_regime": latest_state.get("v4_market_regime"),
        "weekend_transition": latest_state.get("v4_weekend_transition"),
        "weekend_transition_active": latest_state.get(
            "v4_weekend_transition_active"
        ),
        "account_risk_event": latest_account_risk.get("event"),
        "account_risk_action": latest_account_risk.get("risk_action"),
        "account_risk_reason": latest_account_risk.get("risk_reason"),
        "account_recovery_required": latest_account_risk.get(
            "account_recovery_required"
        ),
        "account_recovery_confirm_count": latest_account_risk.get(
            "account_recovery_confirm_count"
        ),
        "account_recovery_confirm_samples": latest_account_risk.get(
            "account_recovery_confirm_samples"
        ),
        "variational_margin_thresholds": {
            "usage": latest_account_risk.get(
                "variational_effective_margin_usage_pct"
            ),
            "warning": latest_account_risk.get(
                "variational_effective_margin_warning_pct"
            ),
            "block": latest_account_risk.get(
                "variational_effective_margin_block_entry_pct"
            ),
            "reduce": latest_account_risk.get(
                "variational_effective_margin_reduce_pct"
            ),
            "emergency": latest_account_risk.get(
                "variational_effective_margin_emergency_pct"
            ),
        },
        "lighter_margin_thresholds": {
            "usage": latest_account_risk.get(
                "lighter_effective_margin_usage_pct"
            ),
            "warning": latest_account_risk.get(
                "lighter_effective_margin_warning_pct"
            ),
            "block": latest_account_risk.get(
                "lighter_effective_margin_block_entry_pct"
            ),
            "reduce": latest_account_risk.get(
                "lighter_effective_margin_reduce_pct"
            ),
            "emergency": latest_account_risk.get(
                "lighter_effective_margin_emergency_pct"
            ),
        },
        "shadow_gradient_entries": len(shadow_gradient_entries),
        "shadow_gradient_exits": len(shadow_gradient_exits),
        "shadow_gradient_active": bool(
            len(shadow_gradient_entries) > len(shadow_gradient_exits)
        ),
        "shadow_gradient_last_reason": latest_shadow_gradient_exit.get("reason"),
        "shadow_gradient_last_pnl_bps": latest_shadow_gradient_exit.get(
            "shadow_pnl_bps"
        ),
        "shadow_gradient_last_holding_seconds": latest_shadow_gradient_exit.get(
            "holding_seconds"
        ),
        "shadow_gradient_last_mfe_bps": latest_shadow_gradient_exit.get(
            "shadow_mfe_pnl_bps"
        ),
        "shadow_gradient_last_mae_bps": latest_shadow_gradient_exit.get(
            "shadow_mae_pnl_bps"
        ),
        "shadow_gradient_levels": shadow_gradient_level_summary,
        "exit_confirmation_policy": latest_state.get(
            "v4_exit_confirmation_policy"
        ),
        "strong_single_enabled": latest_state.get(
            "v4_strong_single_enabled"
        ),
        "strong_single_disabled_reason": latest_state.get(
            "v4_strong_single_disabled_reason"
        ),
        "strong_single_auto_disables": events[
            "live_inventory_v4_strong_single_auto_disabled"
        ],
        "strong_single_shortfall_sample_count": latest_state.get(
            "v4_strong_single_shortfall_sample_count"
        ),
        "strong_single_shortfall_raw_p80_bps": latest_state.get(
            "v4_strong_single_shortfall_raw_p80_bps"
        ),
        "strong_single_shortfall_reserve_bps": latest_state.get(
            "v4_strong_single_shortfall_reserve_bps"
        ),
        "strong_single_threshold_bps": latest_state.get(
            "v4_strong_single_threshold_bps"
        ),
        "batch_wait_reason": latest_batch_wait.get("reason"),
        "batch_cooldown_remaining_seconds": latest_batch_wait.get(
            "cooldown_remaining_seconds"
        ),
        "batch_run_pnl_usd": (
            latest_batch_wait.get("batch_run_pnl_usd")
            or latest_cycle_checkpoint.get("cumulative_run_pnl_usd")
        ),
        "exit_submit_mode": latest_exit_submit.get("submit_mode"),
        "exit_pair_submit_elapsed_ms": latest_exit_submit.get(
            "pair_submit_elapsed_ms"
        ),
        "exit_var_submit_ms": latest_exit_submit.get("var_submit_ms"),
        "exit_lighter_submit_ms": latest_exit_submit.get("lighter_submit_ms"),
        "cycle_report_status": latest_cycle_report.get("report_status"),
        "cycle_final_pnl_bps": latest_cycle_report.get("final_pnl_bps"),
        "cycle_final_pnl_usd": latest_cycle_report.get("final_pnl_usd"),
        "cycle_exit_reason": latest_cycle_report.get("exit_reason"),
        "cycle_exit_shortfall_reserve_bps": latest_cycle_report.get(
            "v4_exit_shortfall_reserve_bps"
        ),
        "cycle_effective_min_exit_pnl_bps": latest_cycle_report.get(
            "effective_min_exit_pnl_bps"
        ),
        "cycle_holding_seconds": latest_cycle_report.get("holding_seconds"),
        "cycle_mfe_pnl_bps": latest_cycle_report.get("shadow_mfe_pnl_bps"),
        "cycle_executable_mfe_pnl_bps": latest_cycle_report.get(
            "executable_exit_mfe_pnl_bps"
        ),
        "cycle_mae_pnl_bps": latest_cycle_report.get("shadow_mae_pnl_bps"),
        "exit_shortfall_sample_count": exit_calibration.get(
            "v4_exit_shortfall_sample_count"
        ),
        "exit_shortfall_min_samples": exit_calibration.get(
            "v4_exit_shortfall_min_samples"
        ),
        "exit_shortfall_full_samples": exit_calibration.get(
            "v4_exit_shortfall_full_samples"
        ),
        "exit_shortfall_calibration_ready": exit_calibration.get(
            "v4_exit_shortfall_calibration_ready"
        ),
        "exit_shortfall_fully_mature": exit_calibration.get(
            "v4_exit_shortfall_fully_mature"
        ),
        "exit_shortfall_raw_p80_bps": exit_calibration.get(
            "v4_exit_shortfall_raw_p80_bps"
        ),
        "exit_shortfall_cap_bps": exit_calibration.get(
            "v4_exit_shortfall_cap_bps"
        ),
        "exit_shortfall_applied_dynamic_bps": exit_calibration.get(
            "v4_exit_shortfall_applied_dynamic_bps"
        ),
        "exit_shortfall_prior_bps": exit_calibration.get(
            "v4_exit_shortfall_prior_bps"
        ),
        "exit_shortfall_calibration_weight": exit_calibration.get(
            "v4_exit_shortfall_calibration_weight"
        ),
        "exit_shortfall_stage_floor_bps": exit_calibration.get(
            "v4_exit_shortfall_stage_floor_bps"
        ),
        "exit_shortfall_reserve_bps": exit_calibration.get(
            "v4_exit_shortfall_reserve_bps"
        ),
        "effective_exit_target_bps": (
            exit_calibration.get("effective_min_exit_pnl_bps")
            or exit_calibration.get("quoted_exit_target_bps")
        ),
        "entry_direction": latest_state.get("v4_entry_direction")
        or "short_var_long_lighter",
        "bidirectional": bool(latest_state.get("v4_bidirectional")),
        "direction_edges_bps": latest_state.get("v4_direction_edges_bps"),
        "direction_thresholds_bps": latest_state.get(
            "v4_direction_thresholds_bps"
        ),
        "latest_edge_bps": latest_state.get("v4_signal_edge_bps")
        if latest_state.get("v4_signal_edge_bps") is not None
        else latest_state.get("short_edge_bps"),
        "latest_raw_threshold_bps": latest_state.get(
            "v4_raw_entry_threshold_bps"
        ),
        "latest_entry_execution_reserve_bps": latest_state.get(
            "v4_entry_execution_reserve_bps"
        ),
        "entry_capture_sample_count": latest_state.get(
            "v4_entry_capture_sample_count"
        ),
        "entry_capture_min_samples": latest_state.get(
            "v4_entry_capture_min_samples"
        ),
        "entry_capture_full_samples": latest_state.get(
            "v4_entry_capture_full_samples"
        ),
        "entry_capture_calibration_ready": latest_state.get(
            "v4_entry_capture_calibration_ready"
        ),
        "entry_capture_fully_mature": latest_state.get(
            "v4_entry_capture_fully_mature"
        ),
        "entry_capture_raw_p80_bps": latest_state.get(
            "v4_entry_capture_raw_p80_bps"
        ),
        "entry_capture_cap_bps": latest_state.get(
            "v4_entry_capture_cap_bps"
        ),
        "entry_capture_applied_bps": latest_state.get(
            "v4_entry_capture_applied_bps"
        ),
        "entry_capture_prior_bps": latest_state.get(
            "v4_entry_capture_prior_bps"
        ),
        "entry_capture_calibration_weight": latest_state.get(
            "v4_entry_capture_calibration_weight"
        ),
        "latest_threshold_bps": latest_state.get("v4_entry_threshold_bps"),
        "seven_day_threshold_bps": latest_state.get(
            "v4_7d_entry_threshold_bps"
        ),
        "fast_threshold_applied": latest_state.get(
            "v4_fast_threshold_applied"
        ),
        "fast_ready": latest_state.get("v4_fast_ready"),
        "fast_threshold_bps": latest_state.get("v4_fast_threshold_bps"),
        "fast_median_bps": latest_state.get("v4_fast_median_bps"),
        "mid_ready": latest_state.get("v4_mid_ready"),
        "mid_threshold_bps": latest_state.get("v4_mid_threshold_bps"),
        "mid_median_bps": latest_state.get("v4_mid_median_bps"),
        "long_ready": latest_state.get("v4_long_ready"),
        "long_threshold_bps": latest_state.get("v4_long_threshold_bps"),
        "long_median_bps": latest_state.get("v4_long_median_bps"),
        "episode_state": latest_state.get("v4_episode_state"),
        "episode_id": latest_state.get("v4_episode_id"),
        "rearm_required": latest_state.get("v4_rearm_required"),
        "rearm_confirmation_count": latest_state.get(
            "v4_rearm_confirmation_count"
        ),
        "rearm_confirm_samples": latest_state.get(
            "v4_rearm_confirm_samples"
        ),
        "rearm_reset_threshold_bps": latest_state.get(
            "v4_rearm_reset_threshold_bps"
        ),
        "latest_window_seconds": anchor_context.get("v4_baseline_window_seconds"),
        "latest_window_max_gap_seconds": anchor_context.get(
            "v4_baseline_max_sample_gap_seconds"
        ),
        "anchor_count": anchor_context.get("v4_anchor_count"),
        "anchor_effective_seconds": anchor_context.get(
            "v4_anchor_effective_seconds"
        ),
        "anchor_min_effective_seconds": anchor_context.get(
            "v4_anchor_min_effective_seconds"
        ),
        "anchor_missing_effective_seconds": anchor_context.get(
            "v4_anchor_missing_effective_seconds"
        ),
        "anchor_progress_pct": anchor_context.get("v4_anchor_progress_pct"),
        "anchor_projected_ready_seconds": anchor_context.get(
            "v4_anchor_projected_ready_seconds"
        ),
        "anchor_projected_ready_at": anchor_context.get(
            "v4_anchor_projected_ready_at"
        ),
        "anchor_ready": anchor_context.get("v4_anchor_ready"),
        "robinhood_anchor_source_rows": latest_state.get(
            "v4_robinhood_anchor_source_rows"
        ),
        "robinhood_anchor_accepted_rows": latest_state.get(
            "v4_robinhood_anchor_accepted_rows"
        ),
        "robinhood_anchor_rejected_rows": latest_state.get(
            "v4_robinhood_anchor_rejected_rows"
        ),
        "health_ready": anchor_context.get("v4_health_ready"),
        "health_ready_observed": anchor_context.get(
            "v4_health_ready_observed"
        ),
        "health_gate_bypassed": anchor_context.get(
            "v4_health_gate_bypassed"
        ),
        "health_coverage_seconds": anchor_context.get("v4_health_coverage_seconds"),
        "health_max_gap_seconds": anchor_context.get(
            "v4_health_max_sample_gap_seconds"
        ),
        "quote_failures": events["live_inventory_basis_quote_failed"],
        "runtime_fuses": len(fuse_rows),
        "runtime_stop_reason": latest_stop.get("reason")
        or latest_fuse.get("reason"),
        "runtime_stopped_at": latest_stop.get("logged_at"),
        "status": status,
    }


def print_v4_live_funnel(rows: list[dict[str, Any]]) -> None:
    funnel = build_v4_live_funnel(rows)
    if funnel is None:
        return
    print("== v4_live_funnel ==")
    max_cycles = funnel["max_cycles"]
    max_cycles_text = (
        "unknown"
        if max_cycles is None
        else "unlimited"
        if max_cycles == 0
        else max_cycles
    )
    print(
        f"profile={funnel['profile']} samples={funnel['samples']} "
        f"threshold_crossings={funnel['threshold_crossings']} "
        f"large_move_blocks={funnel['large_move_blocks']} "
        f"refreshed_edge_blocks={funnel['refreshed_edge_blocks']}"
    )
    print(
        "entry_signal_episodes=threshold_relaxation_bps:current,1,2; "
        "not executable fills; durations are observed within the selected tail"
    )
    for direction, offsets in funnel["entry_episode_summary"].items():
        for offset, stats in offsets.items():
            duration = stats["median_duration_seconds"]
            duration_text = "-" if duration is None else f"{duration:.1f}"
            print(
                f"entry_episodes direction={direction} "
                f"threshold_relaxation_bps={offset} samples={stats['samples']} "
                f"episodes={stats['episodes']} "
                f"episodes_2plus_samples={stats['multi_sample_episodes']} "
                f"median_duration_seconds={duration_text}"
            )
    if funnel["exit_observation_summary"]:
        print(
            "exit_observed_peaks=matched_quote_depth_estimate_not_confirmed_fills; "
            "per-sample MFE persisted about every 60s; funding excluded"
        )
        for result in funnel["exit_target_sensitivity"]:
            avg_actual = result["confirmed_actual_avg_pnl_bps"]
            avg_actual_text = "-" if avg_actual is None else f"{avg_actual:.3f}"
            print(
                f"exit_target_sensitivity target_bps={result['target_bps']} "
                f"peak_crossed={result['peak_crossed_lots']}/"
                f"{result['observed_lots']} "
                f"open_crossed={result['open_crossed_lots']} "
                f"exited_without_confirmed_pnl="
                f"{result['exited_without_confirmed_pnl']} "
                f"confirmed_multi_lot_exits="
                f"{result['confirmed_multi_lot_exits']} "
                f"confirmed_single_lot_closes_crossed="
                f"{result['confirmed_single_lot_closes_crossed']} "
                f"closed_crossed_below_recorded_target="
                f"{result['closed_crossed_below_recorded_target']} "
                f"those_closes_actual_avg_pnl_bps={avg_actual_text}"
            )
        ordered_lots = sorted(
            funnel["exit_observation_summary"].items(),
            key=lambda item: int(item[0]) if item[0].isdigit() else -1,
        )
        omitted = max(0, len(ordered_lots) - 25)
        for lot_id, stats in ordered_lots[-25:]:
            print(
                f"exit_lot={lot_id} direction={stats.get('direction')} "
                f"observed_peak_bps={stats.get('observed_peak_bps', '-')} "
                f"target_bps={stats.get('effective_exit_target_bps', '-')} "
                f"matched_samples={stats.get('matched_quote_observations', '-')} "
                f"last_match_at={stats.get('last_matched_at', '-')} "
                f"final_pnl_bps={stats.get('final_pnl_bps', '-')}"
            )
        if omitted:
            print(f"exit_lots_omitted={omitted}")
    print(
        f"preflight_reached={funnel['preflight_reached']} "
        f"preflight_passed={funnel['preflight_passed']} "
        f"preflight_blocked={funnel['preflight_blocked']} "
        f"dynamic_floor_blocks={funnel['dynamic_floor_blocks']} "
        f"submits={funnel['submits']} entered={funnel['entered']} "
        f"exited={funnel['exited']} actual_pnl={funnel['actual_pnl']} "
        f"final_pnl={funnel['final_pnl']} cycle_reports={funnel['cycle_reports']} "
        f"cycle_checkpoints={funnel['cycle_checkpoints']} "
        f"completed_cycles={funnel['completed_cycles']}/{max_cycles_text}"
    )
    if funnel["batch_wait_reason"]:
        print(
            f"batch_wait_reason={funnel['batch_wait_reason']} "
            f"cooldown_remaining_seconds="
            f"{funnel['batch_cooldown_remaining_seconds']} "
            f"batch_run_pnl_usd={funnel['batch_run_pnl_usd']}"
        )
    print(
        f"latest_edge_bps={funnel['latest_edge_bps']} "
        f"entry_direction={funnel['entry_direction']} "
        f"bidirectional={funnel['bidirectional']} "
        f"latest_raw_threshold_bps={funnel['latest_raw_threshold_bps']} "
        f"entry_execution_reserve_bps={funnel['latest_entry_execution_reserve_bps']} "
        f"latest_threshold_bps={funnel['latest_threshold_bps']} "
        f"latest_window_seconds={funnel['latest_window_seconds']} "
        f"latest_window_max_gap_seconds={funnel['latest_window_max_gap_seconds']} "
        f"anchor_ready={funnel['anchor_ready']} "
        f"health_ready={funnel['health_ready']} "
        f"health_observed={funnel['health_ready_observed']} "
        f"health_bypassed={funnel['health_gate_bypassed']} "
        f"health_coverage_seconds={funnel['health_coverage_seconds']} "
        f"health_max_gap_seconds={funnel['health_max_gap_seconds']} "
        f"status={funnel['status']}"
    )
    print(
        f"robinhood_anchor_source_rows={funnel['robinhood_anchor_source_rows']} "
        f"accepted={funnel['robinhood_anchor_accepted_rows']} "
        f"rejected={funnel['robinhood_anchor_rejected_rows']}"
    )
    if funnel["bidirectional"]:
        print(
            f"direction_edges_bps={funnel['direction_edges_bps']} "
            f"direction_thresholds_bps={funnel['direction_thresholds_bps']}"
        )
    print(
        f"multi_window_7d_p97_5={funnel['seven_day_threshold_bps']} "
        f"fast_1d_p95={funnel['fast_threshold_bps']} "
        f"fast_ready={funnel['fast_ready']} "
        f"fast_applied={funnel['fast_threshold_applied']} "
        f"mid_15d_p97_5={funnel['mid_threshold_bps']} "
        f"mid_ready={funnel['mid_ready']} "
        f"long_30d_p97_5={funnel['long_threshold_bps']} "
        f"long_ready={funnel['long_ready']}"
    )
    print(
        f"episode_state={funnel['episode_state']} "
        f"episode_id={funnel['episode_id']} "
        f"rearm_required={funnel['rearm_required']} "
        f"rearm_progress={funnel['rearm_confirmation_count']}/"
        f"{funnel['rearm_confirm_samples']} "
        f"rearm_reset_threshold_bps={funnel['rearm_reset_threshold_bps']}"
    )
    print(
        f"real_gradient_enabled={funnel['real_gradient_enabled']} "
        f"real_gradient_tiers={funnel['real_gradient_tiers']} "
        f"real_gradient_percentiles={funnel['real_gradient_tier_percentiles']} "
        f"real_gradient_thresholds_bps={funnel['real_gradient_tier_thresholds_bps']} "
        f"real_gradient_market_tier={funnel['real_gradient_market_tier']} "
        f"real_gradient_active_tier={funnel['real_gradient_active_tier']} "
        f"max_venue_leverage={funnel['max_venue_leverage']}"
    )
    print(
        f"variational_snapshot_fresh={funnel['variational_snapshot_fresh']} "
        f"variational_snapshot_usable={funnel['variational_snapshot_usable']} "
        f"variational_snapshot_degraded={funnel['variational_snapshot_degraded']} "
        f"variational_snapshot_age_seconds="
        f"{funnel['variational_snapshot_age_seconds']} "
        f"variational_snapshot_status_source="
        f"{funnel['variational_snapshot_status_source']}"
    )
    print(
        f"variational_portfolio_refresh_attempted="
        f"{funnel['variational_portfolio_refresh_attempted']} "
        f"variational_portfolio_refresh_ok="
        f"{funnel['variational_portfolio_refresh_ok']} "
        f"variational_portfolio_refresh_reason="
        f"{funnel['variational_portfolio_refresh_reason']} "
        f"variational_portfolio_refresh_error="
        f"{funnel['variational_portfolio_refresh_error']}"
    )
    print(
        f"basis_sample_quality_version={funnel['sample_quality_version']} "
        f"basis_sample_pair_valid={funnel['sample_pair_valid']} "
        f"quote_source={funnel['quote_source']} "
        f"var_quote_age_seconds={funnel['var_quote_age_seconds']} "
        f"var_signal_max_age_seconds={funnel['var_signal_max_age_seconds']} "
        f"lighter_book_age_seconds={funnel['lighter_book_age_seconds']}"
    )
    print(
        f"portfolio_exit_locks={funnel['portfolio_exit_locks']} "
        f"portfolio_exit_last_lot_ids={funnel['portfolio_exit_last_lot_ids']} "
        f"portfolio_exit_last_pnl_bps={funnel['portfolio_exit_last_pnl_bps']} "
        f"portfolio_exit_last_target_bps="
        f"{funnel['portfolio_exit_last_target_bps']} "
        f"partial_detier_locks={funnel['partial_detier_locks']}"
    )
    print(
        f"robinhood_status={funnel['robinhood_status']} "
        f"robinhood_fresh={funnel['robinhood_fresh']} "
        f"robinhood_sample_age_seconds="
        f"{funnel['robinhood_sample_age_seconds']} "
        f"robinhood_threshold_penalty_bps="
        f"{funnel['robinhood_threshold_penalty_bps']}"
    )
    print(
        f"order_rate_60s=variational:{funnel['variational_order_rate_used_60s']}"
        f"/{funnel['variational_order_rate_limits_60s']} "
        f"lighter:{funnel['lighter_order_rate_used_60s']}"
        f"/{funnel['lighter_order_rate_limits_60s']}"
    )
    print(
        f"weekend_market_regime={funnel['weekend_market_regime']} "
        f"weekend_transition={funnel['weekend_transition']} "
        f"weekend_transition_active={funnel['weekend_transition_active']}"
    )
    print(
        f"account_risk_event={funnel['account_risk_event']} "
        f"account_risk_action={funnel['account_risk_action']} "
        f"account_risk_reason={funnel['account_risk_reason']} "
        f"account_recovery_required={funnel['account_recovery_required']} "
        f"account_recovery_progress="
        f"{funnel['account_recovery_confirm_count']}/"
        f"{funnel['account_recovery_confirm_samples']} "
        f"variational_margin_thresholds="
        f"{funnel['variational_margin_thresholds']} "
        f"lighter_margin_thresholds={funnel['lighter_margin_thresholds']}"
    )
    print(
        f"shadow_gradient_enabled={funnel['shadow_gradient_enabled']} "
        f"shadow_entries={funnel['shadow_gradient_entries']} "
        f"shadow_exits={funnel['shadow_gradient_exits']} "
        f"shadow_active={funnel['shadow_gradient_active']} "
        f"shadow_last_reason={funnel['shadow_gradient_last_reason']} "
        f"shadow_last_pnl_bps={funnel['shadow_gradient_last_pnl_bps']} "
        f"shadow_last_holding_seconds="
        f"{funnel['shadow_gradient_last_holding_seconds']} "
        f"shadow_last_mfe_bps={funnel['shadow_gradient_last_mfe_bps']} "
        f"shadow_last_mae_bps={funnel['shadow_gradient_last_mae_bps']}"
    )
    level_parts = []
    for level, stats in funnel["shadow_gradient_levels"].items():
        win_rate = stats["win_rate"]
        level_parts.append(
            f"{level}:entries={stats['entries']},exits={stats['exits']},"
            f"active={stats['active']},win_rate="
            f"{('-' if win_rate is None else f'{win_rate:.3f}')},"
            f"avg_bps={stats['avg_pnl_bps']}"
        )
    print(f"shadow_gradient_levels={'|'.join(level_parts) or '-'}")
    print(
        f"exit_confirmation_policy={funnel['exit_confirmation_policy']} "
        f"strong_single_enabled={funnel['strong_single_enabled']} "
        f"strong_single_disabled_reason="
        f"{funnel['strong_single_disabled_reason']} "
        f"strong_single_auto_disables="
        f"{funnel['strong_single_auto_disables']} "
        f"strong_single_samples="
        f"{funnel['strong_single_shortfall_sample_count']} "
        f"strong_single_raw_p80_bps="
        f"{funnel['strong_single_shortfall_raw_p80_bps']} "
        f"strong_single_reserve_bps="
        f"{funnel['strong_single_shortfall_reserve_bps']} "
        f"strong_single_threshold_bps="
        f"{funnel['strong_single_threshold_bps']}"
    )
    print(
        f"anchor_count={funnel['anchor_count']} "
        f"anchor_effective_seconds={funnel['anchor_effective_seconds']} "
        f"anchor_min_effective_seconds={funnel['anchor_min_effective_seconds']} "
        f"anchor_missing_seconds={funnel['anchor_missing_effective_seconds']} "
        f"anchor_progress_pct={funnel['anchor_progress_pct']} "
        f"projected_ready_seconds={funnel['anchor_projected_ready_seconds']} "
        f"projected_ready_at={funnel['anchor_projected_ready_at']}"
    )
    if funnel["entry_capture_sample_count"] is not None:
        print(
            "entry_calibration_samples="
            f"{funnel['entry_capture_sample_count']} "
            f"min_samples={funnel['entry_capture_min_samples']} "
            f"full_samples={funnel['entry_capture_full_samples']} "
            f"ready={funnel['entry_capture_calibration_ready']} "
            f"fully_mature={funnel['entry_capture_fully_mature']} "
            f"raw_p80_bps={funnel['entry_capture_raw_p80_bps']} "
            f"prior_bps={funnel['entry_capture_prior_bps']} "
            f"weight={funnel['entry_capture_calibration_weight']} "
            f"applied_bps={funnel['entry_capture_applied_bps']} "
            f"cap_bps={funnel['entry_capture_cap_bps']}"
        )
    if funnel["exit_shortfall_sample_count"] is not None:
        print(
            "exit_calibration_samples="
            f"{funnel['exit_shortfall_sample_count']} "
            f"min_samples={funnel['exit_shortfall_min_samples']} "
            f"full_samples={funnel['exit_shortfall_full_samples']} "
            f"ready={funnel['exit_shortfall_calibration_ready']} "
            f"fully_mature={funnel['exit_shortfall_fully_mature']} "
            f"raw_p80_bps={funnel['exit_shortfall_raw_p80_bps']} "
            f"prior_bps={funnel['exit_shortfall_prior_bps']} "
            f"weight={funnel['exit_shortfall_calibration_weight']} "
            f"stage_floor_bps={funnel['exit_shortfall_stage_floor_bps']} "
            f"applied_dynamic_bps="
            f"{funnel['exit_shortfall_applied_dynamic_bps']} "
            f"cap_bps={funnel['exit_shortfall_cap_bps']} "
            f"reserve_bps={funnel['exit_shortfall_reserve_bps']} "
            f"effective_target_bps={funnel['effective_exit_target_bps']}"
        )
    if funnel["runtime_fuses"] or funnel["runtime_stop_reason"]:
        print(
            f"quote_failures={funnel['quote_failures']} "
            f"runtime_fuses={funnel['runtime_fuses']} "
            f"runtime_stop_reason={funnel['runtime_stop_reason']} "
            f"runtime_stopped_at={funnel['runtime_stopped_at']}"
        )
    if funnel["exit_submit_mode"] or funnel["cycle_report_status"]:
        print(
            f"exit_submit_mode={funnel['exit_submit_mode']} "
            f"pair_submit_ms={funnel['exit_pair_submit_elapsed_ms']} "
            f"var_submit_ms={funnel['exit_var_submit_ms']} "
            f"lighter_submit_ms={funnel['exit_lighter_submit_ms']} "
            f"report_status={funnel['cycle_report_status']} "
            f"final_pnl_bps={funnel['cycle_final_pnl_bps']} "
            f"final_pnl_usd={funnel['cycle_final_pnl_usd']} "
            f"exit_reason={funnel['cycle_exit_reason']} "
            f"exit_shortfall_reserve_bps={funnel['cycle_exit_shortfall_reserve_bps']} "
            f"effective_exit_target_bps={funnel['cycle_effective_min_exit_pnl_bps']} "
            f"holding_seconds={funnel['cycle_holding_seconds']} "
            f"mfe_bps={funnel['cycle_mfe_pnl_bps']} "
            f"executable_mfe_bps={funnel['cycle_executable_mfe_pnl_bps']} "
            f"mae_bps={funnel['cycle_mae_pnl_bps']}"
        )
    if funnel["exit_block_reasons"]:
        print(
            f"exit_block_reasons={funnel['exit_block_reasons']} "
            "entry_cost_pending_blocks="
            f"{funnel['entry_cost_pending_exit_blocks']} "
            "exit_confirmation_pending_blocks="
            f"{funnel['exit_confirmation_pending_blocks']}"
        )


def print_execution_calibration(rows: list[dict[str, Any]]) -> None:
    final_rows = [
        row
        for row in rows
        if row.get("event") == "live_inventory_actual_pnl"
        and row.get("strategy_version") == "execution-calibration-v1"
        and row.get("actual_pnl_status") == "lighter_final_fill_confirmed"
    ]
    print("== execution_calibration ==")
    print(f"completed_actual_cycles={len(final_rows)}")
    if not final_rows:
        print("recommendation=collect_real_calibration_cycles")
        return

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in final_rows:
        key = (str(row.get("asset") or "-").upper(), str(row.get("direction") or "unknown"))
        grouped.setdefault(key, []).append(row)

    all_pnl: list[Decimal] = []
    suggested_reserves: list[str] = []
    for (asset, direction), group in sorted(grouped.items()):
        pnl = [value for row in group if (value := to_decimal(row.get("actual_pnl_bps"))) is not None]
        shortfall = [
            value
            for row in group
            if (value := to_decimal(row.get("estimated_vs_actual_pnl_shortfall_bps"))) is not None
        ]
        entry_slippage = [
            value for row in group if (value := to_decimal(row.get("entry_lighter_slippage_bps"))) is not None
        ]
        exit_slippage = [
            value for row in group if (value := to_decimal(row.get("exit_lighter_slippage_bps"))) is not None
        ]
        all_pnl.extend(pnl)
        adverse_p20 = percentile(pnl, Decimal("20"))
        total_roundtrip_floor = max(Decimal("0"), -(adverse_p20 or Decimal("0"))) + Decimal("1.0")
        shortfall_p80 = percentile(shortfall, Decimal("80"))
        suggested_reserve = max(Decimal("0"), shortfall_p80 or Decimal("0")) + Decimal("0.5")
        suggested_reserves.append(
            f"{asset}:{direction}:{fmt_decimal(suggested_reserve)}"
        )
        print(
            f"asset={asset} direction={direction} n={len(group)} "
            f"actual_pnl_p20={fmt_decimal(percentile(pnl, Decimal('20')))} "
            f"actual_pnl_p50={fmt_decimal(percentile(pnl, Decimal('50')))} "
            f"actual_pnl_p80={fmt_decimal(percentile(pnl, Decimal('80')))} "
            f"shortfall_p80={fmt_decimal(shortfall_p80)} "
            f"entry_lighter_slip_p80={fmt_decimal(percentile(entry_slippage, Decimal('80')))} "
            f"exit_lighter_slip_p80={fmt_decimal(percentile(exit_slippage, Decimal('80')))}"
            f" suggested_shortfall_reserve={fmt_decimal(suggested_reserve)}"
            f" observed_total_roundtrip_floor={fmt_decimal(total_roundtrip_floor)}"
        )
    print(
        f"overall_actual_pnl_p50={fmt_decimal(percentile(all_pnl, Decimal('50')))} "
        f"overall_actual_pnl_p80={fmt_decimal(percentile(all_pnl, Decimal('80')))}"
    )
    if len(final_rows) < 10:
        print("recommendation=need_at_least_10_cycles_per_asset_before_setting_shortfall_reserve")
    else:
        print(
            "recommendation=shortfall_reserve_ready_for_executable_price_replay "
            f"shortfall_reserves={','.join(suggested_reserves)}"
        )


def basis_state_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    states = [row for row in rows if row.get("event") == "live_inventory_basis_state" and row.get("asset")]
    if states:
        return states
    # Compatibility fallback for older logs that did not emit a state row per sample.
    return [row for row in rows if row.get("event") == "live_inventory_entry_blocked" and row.get("asset")]


def _deduplicate_sample_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prefer baseline over burst copies of the same observed quote."""
    by_id: dict[str, dict[str, Any]] = {}
    without_id: list[dict[str, Any]] = []
    for row in rows:
        sample_id = str(row.get("sample_id") or "")
        if not sample_id:
            without_id.append(row)
            continue
        previous = by_id.get(sample_id)
        if previous is None or str(row.get("sample_kind") or "") == "baseline":
            by_id[sample_id] = row
    merged = [*without_id, *by_id.values()]
    merged.sort(key=lambda row: str(row.get("logged_at") or ""))
    return merged


def depth_slippage(row: dict[str, Any], key: str) -> Decimal | None:
    value = row.get(key)
    if not isinstance(value, dict):
        return None
    return to_decimal(value.get("slippage_bps"))


def best_entry_score(row: dict[str, Any], shortfall_buffer: Decimal, sample_move_penalty: Decimal) -> tuple[Decimal | None, str]:
    long_norm = to_decimal(row.get("normalized_long_edge_bps"))
    short_norm = to_decimal(row.get("normalized_short_edge_bps"))
    long_raw = to_decimal(row.get("long_edge_bps"))
    short_raw = to_decimal(row.get("short_edge_bps"))
    long_score = long_norm if long_norm is not None else long_raw
    short_score = short_norm if short_norm is not None else short_raw
    if long_score is None and short_score is None:
        return None, "-"
    if long_score is not None and (short_score is None or long_score >= short_score):
        direction = "long_var_short_lighter"
        edge = long_score
        roundtrip = to_decimal(row.get("long_roundtrip_pnl_bps")) or Decimal("0")
    else:
        direction = "short_var_long_lighter"
        edge = short_score or Decimal("0")
        roundtrip = to_decimal(row.get("short_roundtrip_pnl_bps")) or Decimal("0")
    sample_move = abs(to_decimal(row.get("basis_sample_move_bps")) or Decimal("0"))
    return edge + min(roundtrip, Decimal("0")) - shortfall_buffer - (sample_move * sample_move_penalty), direction


def best_direction_metrics(row: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for direction, edge_key, normalized_key, roundtrip_key, stablecoin_key in (
        ("long_var_short_lighter", "long_edge_bps", "normalized_long_edge_bps", "long_roundtrip_pnl_bps", "long_stablecoin_filter_ok"),
        ("short_var_long_lighter", "short_edge_bps", "normalized_short_edge_bps", "short_roundtrip_pnl_bps", "short_stablecoin_filter_ok"),
    ):
        edge = to_decimal(row.get(edge_key))
        if edge is None:
            continue
        normalized = to_decimal(row.get(normalized_key))
        roundtrip = to_decimal(row.get(roundtrip_key))
        candidates.append(
            {
                "direction": direction,
                "edge": edge,
                "normalized_edge": normalized,
                "roundtrip": roundtrip,
                "stablecoin_ok": row.get(stablecoin_key),
            }
        )
    if not candidates:
        return None
    return max(candidates, key=lambda item: item["edge"])


def what_if_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("event") != "live_inventory_entry_blocked":
            continue
        metrics = best_direction_metrics(row)
        sample_move = abs(to_decimal(row.get("basis_sample_move_bps")) or Decimal("0"))
        if metrics is None:
            continue
        out.append(
            {
                **metrics,
                "asset": str(row.get("asset") or "-").upper(),
                "reason": str(row.get("reason") or "unknown"),
                "sample_move": sample_move,
                "basis_seen": row.get("basis_seen"),
            }
        )
    return out


def print_what_if(rows: list[dict[str, Any]]) -> None:
    samples = what_if_rows(rows)
    print("== what_if ==")
    print(f"blocked_samples={len(samples)}")
    if not samples:
        return

    min_abs_grid = [Decimal("13"), Decimal("12"), Decimal("11.5"), Decimal("11"), Decimal("10.5"), Decimal("10")]
    sample_move_grid = [Decimal("5"), Decimal("5.5"), Decimal("6"), Decimal("7")]
    min_norm = Decimal("1.0")
    min_roundtrip = Decimal("-3.0")

    for min_abs in min_abs_grid:
        for max_move in sample_move_grid:
            passed = [
                item
                for item in samples
                if abs(item["edge"]) >= min_abs
                and item["sample_move"] <= max_move
                and (item["normalized_edge"] is None or item["normalized_edge"] >= min_norm)
                and (item["roundtrip"] is None or item["roundtrip"] >= min_roundtrip)
                and item["stablecoin_ok"] is not False
            ]
            if not passed:
                print(f"candidate min_abs={min_abs} max_move={max_move} count=0")
                continue
            edges = [item["edge"] for item in passed]
            moves = [item["sample_move"] for item in passed]
            norms = [item["normalized_edge"] for item in passed if item["normalized_edge"] is not None]
            reasons = Counter(item["reason"] for item in passed)
            dirs = Counter(item["direction"] for item in passed)
            print(
                f"candidate min_abs={min_abs} max_move={max_move} count={len(passed)} "
                f"edge_p50={fmt_decimal(percentile(edges, Decimal('50')))} "
                f"edge_p80={fmt_decimal(percentile(edges, Decimal('80')))} "
                f"move_p80={fmt_decimal(percentile(moves, Decimal('80')))} "
                f"norm_p80={fmt_decimal(percentile(norms, Decimal('80')))} "
                f"dirs={dict(dirs.most_common())} reasons={dict(reasons.most_common(3))}"
            )

    best = sorted(
        samples,
        key=lambda item: (
            item["edge"] - (item["sample_move"] * Decimal("0.5")) + min(item["roundtrip"] or Decimal("0"), Decimal("0"))
        ),
        reverse=True,
    )[:5]
    for index, item in enumerate(best, start=1):
        print(
            f"what_if_top_{index}=asset={item['asset']} dir={item['direction']} "
            f"edge={fmt_decimal(item['edge'])} norm={fmt_decimal(item['normalized_edge'])} "
            f"roundtrip={fmt_decimal(item['roundtrip'])} move={fmt_decimal(item['sample_move'])} "
            f"stablecoin_ok={item['stablecoin_ok']} reason={item['reason']}"
        )


def _entry_semantics_values(row: dict[str, Any], direction: str) -> tuple[Decimal | None, Decimal | None, Decimal | None, bool]:
    if direction == "long_var_short_lighter":
        raw_edge = to_decimal(row.get("long_edge_bps"))
        normalized_edge = to_decimal(row.get("normalized_long_edge_bps"))
        stablecoin_ok = row.get("long_stablecoin_filter_ok") is not False
    else:
        raw_edge = to_decimal(row.get("short_edge_bps"))
        normalized_edge = to_decimal(row.get("normalized_short_edge_bps"))
        stablecoin_ok = row.get("short_stablecoin_filter_ok") is not False
    return to_decimal(row.get("basis_bps")), raw_edge, normalized_edge, stablecoin_ok


def _entry_semantics_gate(
    row: dict[str, Any],
    *,
    direction: str,
    primary: str,
    primary_threshold: Decimal,
    min_abs_entry: Decimal,
    min_normalized_filter: Decimal,
) -> bool:
    basis, raw_edge, normalized_edge, stablecoin_ok = _entry_semantics_values(row, direction)
    if basis is None or raw_edge is None or normalized_edge is None:
        return False
    if row.get("basis_sample_move_ok") is False or not stablecoin_ok:
        return False
    if not _direction_abs_entry_ok(direction, basis, min_abs_entry):
        return False
    if normalized_edge < min_normalized_filter:
        return False
    edge = normalized_edge if primary == "normalized" else raw_edge
    return edge >= primary_threshold


def _entry_semantics_forward_pnl_bps(entry: dict[str, Any], future: dict[str, Any], direction: str) -> Decimal | None:
    if direction == "long_var_short_lighter":
        entry_var = to_decimal(entry.get("var_ask"))
        entry_lighter = to_decimal(entry.get("lighter_sell_price"))
        exit_var = to_decimal(future.get("var_bid"))
        exit_lighter = to_decimal(future.get("lighter_buy_price"))
        if entry_var is None or entry_lighter is None or exit_var is None or exit_lighter is None or entry_var <= 0:
            return None
        pnl_per_unit = (exit_var - entry_var) + (entry_lighter - exit_lighter)
    else:
        entry_var = to_decimal(entry.get("var_bid"))
        entry_lighter = to_decimal(entry.get("lighter_buy_price"))
        exit_var = to_decimal(future.get("var_ask"))
        exit_lighter = to_decimal(future.get("lighter_sell_price"))
        if entry_var is None or entry_lighter is None or exit_var is None or exit_lighter is None or entry_var <= 0:
            return None
        pnl_per_unit = (entry_var - exit_var) + (exit_lighter - entry_lighter)
    return pnl_per_unit / entry_var * Decimal("10000")


def build_entry_semantics(
    rows: list[dict[str, Any]],
    *,
    primary_threshold: Decimal,
    min_abs_entry: Decimal,
    min_normalized_filter: Decimal,
    horizons: tuple[int, ...] = (1, 3, 5, 10),
) -> dict[str, dict[str, Any]]:
    by_run_asset: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in basis_state_rows(rows):
        asset = str(row.get("asset") or "-").upper()
        run_id = str(row.get("run_id") or "legacy")
        by_run_asset.setdefault((run_id, asset), []).append(row)

    results: dict[str, dict[str, Any]] = {}
    for (_run_id, asset), run_rows in by_run_asset.items():
        run_rows.sort(key=lambda row: str(row.get("logged_at") or ""))
        item = results.setdefault(
            asset,
            {
                "rows": 0,
                "normalized_primary_candidates": 0,
                "raw_primary_candidates": 0,
                "forward": {horizon: {"attempts": 0, "retained": 0, "raw_edge_deltas": [], "pnl_bps": []} for horizon in horizons},
            },
        )
        item["rows"] += len(run_rows)
        for index, row in enumerate(run_rows):
            for direction in ("long_var_short_lighter", "short_var_long_lighter"):
                normalized_candidate = _entry_semantics_gate(
                    row,
                    direction=direction,
                    primary="normalized",
                    primary_threshold=primary_threshold,
                    min_abs_entry=min_abs_entry,
                    min_normalized_filter=min_normalized_filter,
                )
                raw_candidate = _entry_semantics_gate(
                    row,
                    direction=direction,
                    primary="raw",
                    primary_threshold=primary_threshold,
                    min_abs_entry=min_abs_entry,
                    min_normalized_filter=min_normalized_filter,
                )
                if normalized_candidate:
                    item["normalized_primary_candidates"] += 1
                if not raw_candidate:
                    continue
                item["raw_primary_candidates"] += 1
                _, entry_raw_edge, _, _ = _entry_semantics_values(row, direction)
                for horizon in horizons:
                    future_index = index + horizon
                    if future_index >= len(run_rows):
                        continue
                    future = run_rows[future_index]
                    forward = item["forward"][horizon]
                    forward["attempts"] += 1
                    if _entry_semantics_gate(
                        future,
                        direction=direction,
                        primary="raw",
                        primary_threshold=primary_threshold,
                        min_abs_entry=min_abs_entry,
                        min_normalized_filter=min_normalized_filter,
                    ):
                        forward["retained"] += 1
                    _, future_raw_edge, _, _ = _entry_semantics_values(future, direction)
                    if entry_raw_edge is not None and future_raw_edge is not None:
                        forward["raw_edge_deltas"].append(future_raw_edge - entry_raw_edge)
                    if (pnl_bps := _entry_semantics_forward_pnl_bps(row, future, direction)) is not None:
                        forward["pnl_bps"].append(pnl_bps)
    return results


def print_entry_semantics(
    rows: list[dict[str, Any]],
    *,
    primary_threshold: Decimal,
    min_abs_entry: Decimal,
    min_normalized_filter: Decimal,
) -> None:
    print("== entry_semantics ==")
    print(
        "model=static_proxy_forward_pnl_uses_logged_executable_prices_excludes_latency_fees_and_actual_fills "
        f"primary_threshold={fmt_decimal(primary_threshold)} min_abs_entry={fmt_decimal(min_abs_entry)} "
        f"min_normalized_filter={fmt_decimal(min_normalized_filter)}"
    )
    results = build_entry_semantics(
        rows,
        primary_threshold=primary_threshold,
        min_abs_entry=min_abs_entry,
        min_normalized_filter=min_normalized_filter,
    )
    if not results:
        print("ENTRY_SEMANTICS action=WAIT reason=no_basis_state_rows")
        return
    for asset, item in sorted(results.items()):
        print(
            f"asset={asset} rows={item['rows']} normalized_primary_candidates={item['normalized_primary_candidates']} "
            f"raw_primary_normalized_filter_candidates={item['raw_primary_candidates']}"
        )
        for horizon, forward in item["forward"].items():
            attempts = forward["attempts"]
            retained = forward["retained"]
            retained_pct = Decimal(retained) / Decimal(attempts) * Decimal("100") if attempts else None
            pnl_values = forward["pnl_bps"]
            positive_pct = (
                Decimal(sum(value > 0 for value in pnl_values)) / Decimal(len(pnl_values)) * Decimal("100")
                if pnl_values
                else None
            )
            print(
                f"asset={asset} raw_primary_forward_samples={horizon} attempts={attempts} retained={retained} "
                f"retained_pct={fmt_decimal(retained_pct)} raw_edge_delta_p50={fmt_decimal(percentile(forward['raw_edge_deltas'], Decimal('50')))} "
                f"pnl_proxy_p50={fmt_decimal(percentile(pnl_values, Decimal('50')))} "
                f"pnl_proxy_p80={fmt_decimal(percentile(pnl_values, Decimal('80')))} positive_pct={fmt_decimal(positive_pct)}"
            )


V2_HORIZONS_SECONDS = (1, 3, 5, 10, 30, 60, 300)
V2_DIRECTIONS = ("long_var_short_lighter", "short_var_long_lighter")
DEFAULT_FILTER_SWEEP_HORIZONS = (5, 60, 300)
V2_WINDOWS_SECONDS = (300, 1800, 3600)
V3_WINDOWS_SECONDS = (3600, 21600, 86400, 604800)
V3_VARIANTS = {
    "main_p90_to_p55": (Decimal("90"), Decimal("55")),
    "explore_p85_to_p60": (Decimal("85"), Decimal("60")),
}
# V4 is deliberately a small, pre-registered entry grid. It evaluates
# convergence with executable PnL, rather than using the same-direction edge
# as a proxy for the prices available when both legs are closed.
V4_ENTRY_PERCENTILES = (Decimal("90"), Decimal("95"), Decimal("97.5"))
V4_NET_EXIT_TARGET_BPS = Decimal("1.0")
DEFAULT_FILTER_SWEEP_EVENT_COOLDOWN_SECONDS = 300
DEFAULT_FILTER_SWEEP_HOLDOUT_FRACTION = Decimal("0.30")
DEFAULT_FILTER_SWEEP_MIN_INDEPENDENT_SAMPLES = 30


class _RollingMedian:
    """Maintain a time-windowed median without using future observations."""

    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = float(window_seconds)
        self.queue: deque[tuple[float, Decimal]] = deque()
        self.sorted_values: list[Decimal] = []

    def _prune(self, timestamp: float) -> None:
        cutoff = timestamp - self.window_seconds
        while self.queue and self.queue[0][0] <= cutoff:
            _, value = self.queue.popleft()
            index = bisect_left(self.sorted_values, value)
            if index < len(self.sorted_values) and self.sorted_values[index] == value:
                self.sorted_values.pop(index)

    def median_before(self, timestamp: float) -> Decimal | None:
        self._prune(timestamp)
        if not self.sorted_values:
            return None
        middle = len(self.sorted_values) // 2
        if len(self.sorted_values) % 2:
            return self.sorted_values[middle]
        return (self.sorted_values[middle - 1] + self.sorted_values[middle]) / Decimal("2")

    def percentile_before(self, timestamp: float, pct: Decimal) -> Decimal | None:
        self._prune(timestamp)
        if not self.sorted_values:
            return None
        index = int(
            (Decimal(len(self.sorted_values) - 1) * pct / Decimal("100")).to_integral_value(
                rounding="ROUND_HALF_UP"
            )
        )
        return self.sorted_values[max(0, min(index, len(self.sorted_values) - 1))]

    def history_context(self, timestamp: float) -> tuple[int, float]:
        self._prune(timestamp)
        if not self.queue:
            return 0, 0.0
        return len(self.queue), max(0.0, timestamp - self.queue[0][0])

    def add(self, timestamp: float, value: Decimal) -> None:
        self._prune(timestamp)
        self.queue.append((timestamp, value))
        index = bisect_right(self.sorted_values, value)
        self.sorted_values.insert(index, value)


def _basis_v3_alignment(row: dict[str, Any], direction: str) -> str:
    key = "long_stablecoin_alignment" if direction == "long_var_short_lighter" else "short_stablecoin_alignment"
    value = str(row.get(key) or "unknown").strip().lower()
    return value if value in {"aligned", "neutral", "opposed"} else "unknown"


def _basis_v3_quotes_fresh(
    row: dict[str, Any],
    *,
    max_quote_age_ms: Decimal,
    max_lighter_book_age_seconds: Decimal,
) -> bool:
    sample_quality = str(row.get("sample_quality") or "valid").lower()
    if sample_quality != "valid" or row.get("lighter_continuity_ok") is False:
        return False
    quote_age_seconds = to_decimal(row.get("var_quote_age_seconds"))
    lighter_age_seconds = to_decimal(row.get("lighter_book_age_seconds"))
    return (
        (
            max_quote_age_ms <= 0
            or quote_age_seconds is not None
            and quote_age_seconds * Decimal("1000") <= max_quote_age_ms
        )
        and (
            max_lighter_book_age_seconds <= 0
            or lighter_age_seconds is not None
            and lighter_age_seconds <= max_lighter_book_age_seconds
        )
    )


def _basis_v3_simulate_episode(
    *,
    rows: list[dict[str, Any]],
    times: list[float],
    entry_index: int,
    direction: str,
    target_exit_edge_bps: Decimal,
    max_hold_seconds: int,
    shortfall_reserve_bps: Decimal,
    max_quote_age_ms: Decimal,
    max_lighter_book_age_seconds: Decimal,
    max_sample_gap_seconds: int = 60,
) -> dict[str, Any] | None:
    entry_time = times[entry_index]
    last_index = bisect_right(times, entry_time + max_hold_seconds) - 1
    if last_index <= entry_index:
        return None
    exit_index: int | None = None
    exit_reason = "max_hold_timeout"
    mfe: Decimal | None = None
    mae: Decimal | None = None
    last_usable_time = entry_time
    for index in range(entry_index + 1, last_index + 1):
        if not _basis_v3_quotes_fresh(
            rows[index],
            max_quote_age_ms=max_quote_age_ms,
            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
        ):
            continue
        if max_sample_gap_seconds > 0 and times[index] - last_usable_time > max_sample_gap_seconds:
            return {"blocked_reason": "sample_gap"}
        last_usable_time = times[index]
        exit_index = index
        pnl = _entry_semantics_forward_pnl_bps(rows[entry_index], rows[index], direction)
        if pnl is not None:
            mfe = pnl if mfe is None else max(mfe, pnl)
            mae = pnl if mae is None else min(mae, pnl)
        future_edge = _basis_v2_candidate_edge(rows[index], direction)
        if future_edge is not None and future_edge <= target_exit_edge_bps:
            exit_index = index
            exit_reason = "target_quantile_reached"
            break
    if exit_index is None:
        return None
    if exit_reason == "max_hold_timeout" and times[-1] < entry_time + max_hold_seconds:
        return None
    executable_pnl = _entry_semantics_forward_pnl_bps(rows[entry_index], rows[exit_index], direction)
    if executable_pnl is None:
        return None
    exit_time = times[exit_index]
    return {
        "exit_index": exit_index,
        "exit_timestamp": exit_time,
        "holding_seconds": Decimal(str(max(0.0, exit_time - entry_time))),
        "exit_reason": exit_reason,
        "executable_pnl_bps": executable_pnl,
        "net_pnl_bps": executable_pnl - shortfall_reserve_bps,
        "mfe_bps": mfe,
        "mae_bps": mae,
    }


def _basis_v4_simulate_episode(
    *,
    rows: list[dict[str, Any]],
    times: list[float],
    entry_index: int,
    direction: str,
    max_hold_seconds: int,
    shortfall_reserve_bps: Decimal,
    net_exit_target_bps: Decimal,
    max_quote_age_ms: Decimal,
    max_lighter_book_age_seconds: Decimal,
    max_sample_gap_seconds: int = 60,
) -> dict[str, Any] | None:
    """Close only once logged executable PnL meets the net target, or at timeout."""
    entry_time = times[entry_index]
    last_index = bisect_right(times, entry_time + max_hold_seconds) - 1
    if last_index <= entry_index:
        return None
    exit_index: int | None = None
    exit_reason = "max_hold_timeout"
    mfe: Decimal | None = None
    mae: Decimal | None = None
    last_usable_time = entry_time
    for index in range(entry_index + 1, last_index + 1):
        if not _basis_v3_quotes_fresh(
            rows[index],
            max_quote_age_ms=max_quote_age_ms,
            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
        ):
            continue
        if max_sample_gap_seconds > 0 and times[index] - last_usable_time > max_sample_gap_seconds:
            return {"blocked_reason": "sample_gap"}
        last_usable_time = times[index]
        exit_index = index
        executable_pnl = _entry_semantics_forward_pnl_bps(rows[entry_index], rows[index], direction)
        if executable_pnl is None:
            continue
        net_pnl = executable_pnl - shortfall_reserve_bps
        mfe = net_pnl if mfe is None else max(mfe, net_pnl)
        mae = net_pnl if mae is None else min(mae, net_pnl)
        if net_pnl >= net_exit_target_bps:
            exit_reason = "executable_net_target_reached"
            break
    if exit_index is None:
        return None
    if exit_reason == "max_hold_timeout" and times[-1] < entry_time + max_hold_seconds:
        # The observation ends before this episode's timeout. Counting the
        # last available quote as an exit would introduce look-end bias.
        return None
    executable_pnl = _entry_semantics_forward_pnl_bps(rows[entry_index], rows[exit_index], direction)
    if executable_pnl is None:
        return None
    exit_time = times[exit_index]
    return {
        "exit_index": exit_index,
        "exit_timestamp": exit_time,
        "holding_seconds": Decimal(str(max(0.0, exit_time - entry_time))),
        "exit_reason": exit_reason,
        "executable_pnl_bps": executable_pnl,
        "net_pnl_bps": executable_pnl - shortfall_reserve_bps,
        "mfe_bps": mfe,
        "mae_bps": mae,
    }


def build_basis_v3_replay(
    rows: list[dict[str, Any]],
    *,
    asset_filter: str | None = None,
    evaluation_interval_seconds: int = 60,
    history_sample_seconds: int = 30,
    episode_cooldown_seconds: int = 180,
    max_hold_seconds: int = 21600,
    min_window_coverage: Decimal = Decimal("0.80"),
    min_history_samples: int = 100,
    long_shortfall_reserve_bps: Decimal = Decimal("1.0"),
    short_shortfall_reserve_bps: Decimal = Decimal("1.0"),
    min_net_expected_bps: Decimal = Decimal("1.0"),
    max_quote_age_ms: Decimal = Decimal("1500"),
    max_lighter_book_age_seconds: Decimal = Decimal("2"),
    max_sample_gap_seconds: int = 60,
) -> dict[str, dict[str, Any]]:
    """Replay strictly-prior multiscale quantile entries and target exits.

    Logged executable prices already contain crossing spread. The configured
    reserve therefore covers only unmodeled fill shortfall, not spread again.
    """
    grouped: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for row in basis_state_rows(rows):
        asset = str(row.get("asset") or "-").upper()
        if asset_filter and asset != asset_filter.upper():
            continue
        timestamp = parse_time(row.get("logged_at"))
        if timestamp is None:
            continue
        grouped.setdefault(asset, []).append((timestamp.timestamp(), row))

    results: dict[str, dict[str, Any]] = {}
    for asset, group in grouped.items():
        group.sort(key=lambda item: item[0])
        times = [timestamp for timestamp, _ in group]
        group_rows = [row for _, row in group]
        rolling = {
            direction: {window: _RollingMedian(window) for window in V3_WINDOWS_SECONDS}
            for direction in V2_DIRECTIONS
        }
        next_evaluation_at = times[0] if times else 0.0
        next_history_sample_at = times[0] if times else 0.0
        next_episode_at = {
            (variant, direction): 0.0
            for variant in V3_VARIANTS
            for direction in V2_DIRECTIONS
        }
        item: dict[str, Any] = {
            "rows": len(group_rows),
            "started_at": times[0] if times else None,
            "ended_at": times[-1] if times else None,
            "latest_quantiles": {},
            "episodes": [],
            "candidate_checks": Counter(),
            "blocked": Counter(),
        }

        for row_index, (timestamp, row) in enumerate(group):
            should_evaluate = timestamp >= next_evaluation_at
            if should_evaluate:
                next_evaluation_at = timestamp + evaluation_interval_seconds
                for direction in V2_DIRECTIONS:
                    edge = _basis_v2_candidate_edge(row, direction)
                    if edge is None:
                        continue
                    quantiles_by_window: dict[int, dict[str, Any]] = {}
                    mature_windows: list[int] = []
                    for window in V3_WINDOWS_SECONDS:
                        tracker = rolling[direction][window]
                        count, coverage_seconds = tracker.history_context(timestamp)
                        mature = (
                            count >= min_history_samples
                            and Decimal(str(coverage_seconds)) >= Decimal(window) * min_window_coverage
                        )
                        quantiles_by_window[window] = {
                            "count": count,
                            "coverage_seconds": coverage_seconds,
                            "mature": mature,
                            "p55": tracker.percentile_before(timestamp, Decimal("55")),
                            "p60": tracker.percentile_before(timestamp, Decimal("60")),
                            "p85": tracker.percentile_before(timestamp, Decimal("85")),
                            "p90": tracker.percentile_before(timestamp, Decimal("90")),
                        }
                        if mature:
                            mature_windows.append(window)
                    item["latest_quantiles"][direction] = quantiles_by_window
                    if not mature_windows:
                        item["blocked"]["insufficient_multiscale_history"] += 1
                        continue
                    baseline_window = max(mature_windows)
                    baseline = quantiles_by_window[baseline_window]
                    reserve = (
                        long_shortfall_reserve_bps
                        if direction == "long_var_short_lighter"
                        else short_shortfall_reserve_bps
                    )
                    safety_ok = (
                        row.get("basis_sample_move_ok") is not False
                        and _basis_v3_quotes_fresh(
                            row,
                            max_quote_age_ms=max_quote_age_ms,
                            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
                        )
                    )
                    for variant, (entry_pct, exit_pct) in V3_VARIANTS.items():
                        key = (variant, direction)
                        item["candidate_checks"][key] += 1
                        if timestamp < next_episode_at[key]:
                            continue
                        entry_threshold = baseline[f"p{int(entry_pct)}"]
                        exit_target = baseline[f"p{int(exit_pct)}"]
                        if entry_threshold is None or exit_target is None or edge < entry_threshold:
                            continue
                        expected_capture = edge - exit_target
                        net_expected = expected_capture - reserve
                        if net_expected < min_net_expected_bps:
                            item["blocked"][f"{variant}:net_expected_below_threshold"] += 1
                            continue
                        if not safety_ok:
                            item["blocked"][f"{variant}:market_data_guard"] += 1
                            continue
                        simulated = _basis_v3_simulate_episode(
                            rows=group_rows,
                            times=times,
                            entry_index=row_index,
                            direction=direction,
                            target_exit_edge_bps=exit_target,
                            max_hold_seconds=max_hold_seconds,
                            shortfall_reserve_bps=reserve,
                            max_quote_age_ms=max_quote_age_ms,
                            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
                            max_sample_gap_seconds=max_sample_gap_seconds,
                        )
                        if simulated is not None and simulated.get("blocked_reason"):
                            item["blocked"][f"{variant}:{simulated['blocked_reason']}"] += 1
                            continue
                        if simulated is None:
                            item["blocked"][f"{variant}:no_executable_exit"] += 1
                            continue
                        episode_id = f"{asset}:{direction}:{variant}:{int(timestamp * 1000)}"
                        episode = {
                            "episode_id": episode_id,
                            "timestamp": timestamp,
                            "asset": asset,
                            "direction": direction,
                            "variant": variant,
                            "baseline_window_seconds": baseline_window,
                            "entry_percentile": entry_pct,
                            "exit_percentile": exit_pct,
                            "entry_edge_bps": edge,
                            "entry_threshold_bps": entry_threshold,
                            "target_exit_edge_bps": exit_target,
                            "expected_capture_bps": expected_capture,
                            "shortfall_reserve_bps": reserve,
                            "net_expected_bps": net_expected,
                            "stablecoin_alignment": _basis_v3_alignment(row, direction),
                            **simulated,
                        }
                        item["episodes"].append(episode)
                        next_episode_at[key] = simulated["exit_timestamp"] + episode_cooldown_seconds

            if str(row.get("sample_kind") or "baseline") == "baseline" and timestamp >= next_history_sample_at:
                next_history_sample_at = timestamp + history_sample_seconds
                for direction in V2_DIRECTIONS:
                    edge = _basis_v2_candidate_edge(row, direction)
                    if edge is not None:
                        for window in V3_WINDOWS_SECONDS:
                            rolling[direction][window].add(timestamp, edge)

        results[asset] = item
    return results


def build_basis_v4_replay(
    rows: list[dict[str, Any]],
    *,
    asset_filter: str | None = None,
    evaluation_interval_seconds: int = 1,
    history_sample_seconds: int = 30,
    episode_cooldown_seconds: int = 180,
    max_hold_seconds: int = 21600,
    min_window_coverage: Decimal = Decimal("0.80"),
    min_history_samples: int = 100,
    long_shortfall_reserve_bps: Decimal = Decimal("1.0"),
    short_shortfall_reserve_bps: Decimal = Decimal("1.0"),
    net_exit_target_bps: Decimal = V4_NET_EXIT_TARGET_BPS,
    max_quote_age_ms: Decimal = Decimal("1500"),
    max_lighter_book_age_seconds: Decimal = Decimal("2"),
    max_sample_gap_seconds: int = 60,
) -> dict[str, dict[str, Any]]:
    """Replay extreme entries with exits based only on executable net PnL."""
    grouped: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for row in basis_state_rows(rows):
        asset = str(row.get("asset") or "-").upper()
        if asset_filter and asset != asset_filter.upper():
            continue
        timestamp = parse_time(row.get("logged_at"))
        if timestamp is not None:
            grouped.setdefault(asset, []).append((timestamp.timestamp(), row))

    results: dict[str, dict[str, Any]] = {}
    for asset, group in grouped.items():
        group.sort(key=lambda entry: entry[0])
        times = [timestamp for timestamp, _ in group]
        group_rows = [row for _, row in group]
        rolling = {
            direction: {window: _RollingMedian(window) for window in V3_WINDOWS_SECONDS}
            for direction in V2_DIRECTIONS
        }
        next_evaluation_at = times[0] if times else 0.0
        next_history_sample_at = times[0] if times else 0.0
        next_episode_at = {
            (entry_percentile, direction): 0.0
            for entry_percentile in V4_ENTRY_PERCENTILES
            for direction in V2_DIRECTIONS
        }
        item: dict[str, Any] = {
            "rows": len(group_rows),
            "started_at": times[0] if times else None,
            "ended_at": times[-1] if times else None,
            "episodes": [],
            "blocked": Counter(),
        }
        for row_index, (timestamp, row) in enumerate(group):
            if timestamp >= next_evaluation_at:
                next_evaluation_at = timestamp + evaluation_interval_seconds
                for direction in V2_DIRECTIONS:
                    edge = _basis_v2_candidate_edge(row, direction)
                    if edge is None:
                        continue
                    mature_windows: list[int] = []
                    for window in V3_WINDOWS_SECONDS:
                        tracker = rolling[direction][window]
                        count, coverage_seconds = tracker.history_context(timestamp)
                        if (
                            count >= min_history_samples
                            and Decimal(str(coverage_seconds)) >= Decimal(window) * min_window_coverage
                        ):
                            mature_windows.append(window)
                    if not mature_windows:
                        item["blocked"]["insufficient_multiscale_history"] += 1
                        continue
                    baseline_window = max(mature_windows)
                    tracker = rolling[direction][baseline_window]
                    reserve = (
                        long_shortfall_reserve_bps
                        if direction == "long_var_short_lighter"
                        else short_shortfall_reserve_bps
                    )
                    safety_ok = (
                        row.get("basis_sample_move_ok") is not False
                        and _basis_v3_quotes_fresh(
                            row,
                            max_quote_age_ms=max_quote_age_ms,
                            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
                        )
                    )
                    for entry_percentile in V4_ENTRY_PERCENTILES:
                        key = (entry_percentile, direction)
                        if timestamp < next_episode_at[key]:
                            continue
                        entry_threshold = tracker.percentile_before(timestamp, entry_percentile)
                        # Equality is common in a tightly quoted market; require a true
                        # tail excursion rather than repeatedly entering at the percentile.
                        if entry_threshold is None or edge <= entry_threshold:
                            continue
                        if not safety_ok:
                            item["blocked"][f"p{entry_percentile}:market_data_guard"] += 1
                            continue
                        simulated = _basis_v4_simulate_episode(
                            rows=group_rows,
                            times=times,
                            entry_index=row_index,
                            direction=direction,
                            max_hold_seconds=max_hold_seconds,
                            shortfall_reserve_bps=reserve,
                            net_exit_target_bps=net_exit_target_bps,
                            max_quote_age_ms=max_quote_age_ms,
                            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
                            max_sample_gap_seconds=max_sample_gap_seconds,
                        )
                        if simulated is not None and simulated.get("blocked_reason"):
                            item["blocked"][f"p{entry_percentile}:{simulated['blocked_reason']}"] += 1
                            continue
                        if simulated is None:
                            item["blocked"][f"p{entry_percentile}:no_executable_exit"] += 1
                            continue
                        item["episodes"].append(
                            {
                                "timestamp": timestamp,
                                "entry_logged_at": row.get("logged_at"),
                                "asset": asset,
                                "direction": direction,
                                "entry_percentile": entry_percentile,
                                "baseline_window_seconds": baseline_window,
                                "entry_edge_bps": edge,
                                "entry_threshold_bps": entry_threshold,
                                "entry_var_spread_bps": to_decimal(row.get("var_spread_bps")),
                                "entry_lighter_spread_bps": to_decimal(row.get("lighter_spread_bps")),
                                "shortfall_reserve_bps": reserve,
                                "net_exit_target_bps": net_exit_target_bps,
                                "stablecoin_alignment": _basis_v3_alignment(row, direction),
                                **simulated,
                            }
                        )
                        next_episode_at[key] = simulated["exit_timestamp"] + episode_cooldown_seconds
            if str(row.get("sample_kind") or "baseline") == "baseline" and timestamp >= next_history_sample_at:
                next_history_sample_at = timestamp + history_sample_seconds
                for direction in V2_DIRECTIONS:
                    edge = _basis_v2_candidate_edge(row, direction)
                    if edge is not None:
                        for window in V3_WINDOWS_SECONDS:
                            rolling[direction][window].add(timestamp, edge)
        results[asset] = item
    return results


def _basis_v3_split_stats(
    episodes: list[dict[str, Any]],
    *,
    holdout_fraction: Decimal,
    min_independent_samples: int,
) -> dict[str, Any]:
    ordered = sorted(episodes, key=lambda episode: float(episode["timestamp"]))
    if len(ordered) < 2:
        train, holdout = ordered, []
    else:
        split = int(Decimal(len(ordered)) * (Decimal("1") - holdout_fraction))
        split = min(max(split, 1), len(ordered) - 1)
        train, holdout = ordered[:split], ordered[split:]

    def stats(items: list[dict[str, Any]]) -> dict[str, Any]:
        pnl = [item["net_pnl_bps"] for item in items]
        return {
            "n": len(items),
            "p20": percentile(pnl, Decimal("20")),
            "p50": percentile(pnl, Decimal("50")),
            "positive_pct": (
                Decimal(sum(value > 0 for value in pnl)) / Decimal(len(pnl)) * Decimal("100")
                if pnl
                else None
            ),
        }

    train_stats = stats(train)
    holdout_stats = stats(holdout)
    if train_stats["n"] < min_independent_samples or holdout_stats["n"] < min_independent_samples:
        verdict = "insufficient_independent_data"
    elif (
        train_stats["p20"] is None
        or holdout_stats["p20"] is None
        or train_stats["p20"] <= 0
        or holdout_stats["p20"] <= 0
    ):
        verdict = "net_p20_not_positive"
    else:
        verdict = "bounded_live_candidate"
    return {"train": train_stats, "holdout": holdout_stats, "verdict": verdict}


def _basis_v4_episode_stats(
    episodes: list[dict[str, Any]],
    *,
    holdout_fraction: Decimal,
    min_independent_samples: int,
) -> dict[str, Any]:
    pnl = [episode["net_pnl_bps"] for episode in episodes]
    holding = [episode["holding_seconds"] for episode in episodes]
    target_exits = sum(
        episode["exit_reason"] == "executable_net_target_reached"
        for episode in episodes
    )
    return {
        "n": len(episodes),
        "net_p20": percentile(pnl, Decimal("20")),
        "net_p50": percentile(pnl, Decimal("50")),
        "hold_p50": percentile(holding, Decimal("50")),
        "target_exit_pct": (
            Decimal(target_exits) / Decimal(len(episodes)) * Decimal("100")
            if episodes
            else None
        ),
        "split": _basis_v3_split_stats(
            episodes,
            holdout_fraction=holdout_fraction,
            min_independent_samples=min_independent_samples,
        ),
    }


def build_basis_v4_stratification(
    episodes: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    *,
    holdout_fraction: Decimal,
    min_independent_samples: int,
) -> dict[str, Any]:
    spread_values: list[Decimal] = []
    for row in baseline_rows:
        var_spread = to_decimal(row.get("var_spread_bps"))
        lighter_spread = to_decimal(row.get("lighter_spread_bps"))
        if var_spread is not None and lighter_spread is not None:
            spread_values.append(var_spread + lighter_spread)
    spread_p33 = percentile(spread_values, Decimal("33"))
    spread_p67 = percentile(spread_values, Decimal("67"))

    periods: dict[str, list[dict[str, Any]]] = {"weekday": [], "weekend": []}
    utc_blocks: dict[str, list[dict[str, Any]]] = {}
    liquidity: dict[str, list[dict[str, Any]]] = {
        "tight": [],
        "normal": [],
        "wide": [],
        "unknown": [],
    }
    for episode in episodes:
        entered_at = datetime.fromtimestamp(float(episode["timestamp"]), tz=timezone.utc)
        periods["weekday" if entered_at.weekday() < 5 else "weekend"].append(episode)
        block_start = (entered_at.hour // 4) * 4
        block = f"{block_start:02d}-{(block_start + 4) % 24:02d}"
        utc_blocks.setdefault(block, []).append(episode)
        var_spread = episode.get("entry_var_spread_bps")
        lighter_spread = episode.get("entry_lighter_spread_bps")
        total_spread = (
            var_spread + lighter_spread
            if var_spread is not None and lighter_spread is not None
            else None
        )
        if total_spread is None or spread_p33 is None or spread_p67 is None:
            bucket = "unknown"
        elif total_spread <= spread_p33:
            bucket = "tight"
        elif total_spread <= spread_p67:
            bucket = "normal"
        else:
            bucket = "wide"
        liquidity[bucket].append(episode)

    overall = _basis_v4_episode_stats(
        episodes,
        holdout_fraction=holdout_fraction,
        min_independent_samples=min_independent_samples,
    )
    period_stats = {
        key: _basis_v4_episode_stats(
            items,
            holdout_fraction=holdout_fraction,
            min_independent_samples=min_independent_samples,
        )
        for key, items in periods.items()
    }
    utc_stats = {
        key: _basis_v4_episode_stats(
            items,
            holdout_fraction=holdout_fraction,
            min_independent_samples=min_independent_samples,
        )
        for key, items in sorted(utc_blocks.items())
    }
    liquidity_stats = {
        key: _basis_v4_episode_stats(
            items,
            holdout_fraction=holdout_fraction,
            min_independent_samples=min_independent_samples,
        )
        for key, items in liquidity.items()
        if items
    }
    max_utc_share_pct = (
        Decimal(max((len(items) for items in utc_blocks.values()), default=0))
        / Decimal(len(episodes))
        * Decimal("100")
        if episodes
        else None
    )
    period_max_utc_share_pct: dict[str, Decimal | None] = {}
    for period, items in periods.items():
        counts = Counter(
            (datetime.fromtimestamp(float(episode["timestamp"]), tz=timezone.utc).hour // 4) * 4
            for episode in items
        )
        period_max_utc_share_pct[period] = (
            Decimal(max(counts.values())) / Decimal(len(items)) * Decimal("100")
            if items
            else None
        )
    return {
        "overall": overall,
        "periods": period_stats,
        "utc_blocks": utc_stats,
        "liquidity": liquidity_stats,
        "spread_p33": spread_p33,
        "spread_p67": spread_p67,
        "max_utc_share_pct": max_utc_share_pct,
        "period_max_utc_share_pct": period_max_utc_share_pct,
    }


def _basis_v4_candidate_verdict(
    stratification: dict[str, Any],
    *,
    effective_coverage_hours: Decimal,
) -> tuple[str, list[str]]:
    overall = stratification["overall"]
    weekday = stratification["periods"]["weekday"]
    weekend = stratification["periods"]["weekend"]
    max_utc_share = stratification["max_utc_share_pct"]
    weekday_max_utc_share = stratification["period_max_utc_share_pct"]["weekday"]
    common_reasons: list[str] = []
    if effective_coverage_hours < Decimal("168"):
        common_reasons.append("effective_coverage_below_168h")
    if overall["n"] < 30:
        common_reasons.append("episodes_below_30")
    if overall["target_exit_pct"] is None or overall["target_exit_pct"] < Decimal("85"):
        common_reasons.append("target_exit_pct_below_85")
    if overall["split"]["verdict"] != "bounded_live_candidate":
        common_reasons.append(f"overall_{overall['split']['verdict']}")
    if max_utc_share is None or max_utc_share > Decimal("50"):
        common_reasons.append("single_utc_block_dominance")
    all_week_reasons = list(common_reasons)
    for label, stats in (("weekday", weekday), ("weekend", weekend)):
        if stats["n"] < 5:
            all_week_reasons.append(f"{label}_episodes_below_5")
        elif stats["net_p20"] is None or stats["net_p20"] <= 0:
            all_week_reasons.append(f"{label}_net_p20_not_positive")
    if not all_week_reasons:
        return "bounded_all_week_real_calibration_candidate", []

    weekday_reasons: list[str] = []
    if effective_coverage_hours < Decimal("168"):
        weekday_reasons.append("effective_coverage_below_168h")
    if weekday["n"] < 30:
        weekday_reasons.append("weekday_episodes_below_30")
    if weekday["target_exit_pct"] is None or weekday["target_exit_pct"] < Decimal("85"):
        weekday_reasons.append("weekday_target_exit_pct_below_85")
    if weekday["split"]["verdict"] != "bounded_live_candidate":
        weekday_reasons.append(f"weekday_{weekday['split']['verdict']}")
    if weekday_max_utc_share is None or weekday_max_utc_share > Decimal("50"):
        weekday_reasons.append("single_utc_block_dominance")
    if not weekday_reasons:
        return "bounded_weekday_real_calibration_candidate", all_week_reasons
    return "wait", sorted(set([*all_week_reasons, *weekday_reasons]))


def print_basis_v4_stratified(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    evaluation_interval_seconds: int,
    history_sample_seconds: int,
    episode_cooldown_seconds: int,
    max_hold_seconds: int,
    max_sample_gap_seconds: int,
    min_window_coverage: Decimal,
    min_history_samples: int,
    long_shortfall_reserve_bps: Decimal,
    short_shortfall_reserve_bps: Decimal,
    net_exit_target_bps: Decimal,
    holdout_fraction: Decimal,
    min_independent_samples: int,
) -> None:
    formal_profile = _basis_v4_formal_profile(
        asset=asset,
        evaluation_interval_seconds=evaluation_interval_seconds,
        history_sample_seconds=history_sample_seconds,
        episode_cooldown_seconds=episode_cooldown_seconds,
        max_hold_seconds=max_hold_seconds,
        max_sample_gap_seconds=max_sample_gap_seconds,
        min_window_coverage=min_window_coverage,
        min_history_samples=min_history_samples,
        long_shortfall_reserve_bps=long_shortfall_reserve_bps,
        short_shortfall_reserve_bps=short_shortfall_reserve_bps,
        net_exit_target_bps=net_exit_target_bps,
    )
    print("== basis_v4_stratified ==")
    print(
        f"formal_candidate=p97.5 profile={formal_profile or 'none'} "
        f"long_reserve={fmt_decimal(long_shortfall_reserve_bps)}bps "
        f"short_reserve={fmt_decimal(short_shortfall_reserve_bps)}bps "
        "max_hold=21600s net_exit_target=1bps "
        "segments=weekday_weekend,utc_4h,relative_total_spread"
    )
    results = build_basis_v4_replay(
        rows,
        asset_filter=asset,
        evaluation_interval_seconds=evaluation_interval_seconds,
        history_sample_seconds=history_sample_seconds,
        episode_cooldown_seconds=episode_cooldown_seconds,
        max_hold_seconds=max_hold_seconds,
        max_sample_gap_seconds=max_sample_gap_seconds,
        min_window_coverage=min_window_coverage,
        min_history_samples=min_history_samples,
        long_shortfall_reserve_bps=long_shortfall_reserve_bps,
        short_shortfall_reserve_bps=short_shortfall_reserve_bps,
        net_exit_target_bps=net_exit_target_bps,
    )
    formal_config = formal_profile is not None
    if not formal_config:
        print("formal_config=false verdict=non_formal_diagnostic_only")
    if not results:
        print("BASIS_V4_STRATIFIED action=WAIT reason=no_time_aligned_basis_state_rows")
        return
    for asset_name, item in sorted(results.items()):
        baseline_rows = [
            row
            for row in basis_state_rows(rows)
            if str(row.get("asset") or "-").upper() == asset_name
            and str(row.get("sample_kind") or "baseline") == "baseline"
            and str(row.get("sample_quality") or "valid") == "valid"
        ]
        baseline_times = sorted(
            parsed.timestamp()
            for row in baseline_rows
            if (parsed := parse_time(row.get("logged_at"))) is not None
        )
        gaps = [right - left for left, right in zip(baseline_times, baseline_times[1:])]
        raw_seconds = baseline_times[-1] - baseline_times[0] if len(baseline_times) >= 2 else 0.0
        excluded_seconds = sum(gap for gap in gaps if gap > max_sample_gap_seconds)
        effective_hours = Decimal(str(max(0.0, raw_seconds - excluded_seconds) / 3600.0))
        print(
            f"asset={asset_name} valid_baseline={len(baseline_rows)} "
            f"raw_hours={fmt_decimal(Decimal(str(raw_seconds / 3600.0)))} "
            f"excluded_gap_hours={fmt_decimal(Decimal(str(excluded_seconds / 3600.0)))} "
            f"effective_hours={fmt_decimal(effective_hours)}"
        )
        for direction in V2_DIRECTIONS:
            episodes = [
                episode
                for episode in item["episodes"]
                if episode["entry_percentile"] == Decimal("97.5")
                and episode["direction"] == direction
            ]
            strata = build_basis_v4_stratification(
                episodes,
                baseline_rows,
                holdout_fraction=holdout_fraction,
                min_independent_samples=min_independent_samples,
            )
            overall = strata["overall"]
            verdict, reasons = _basis_v4_candidate_verdict(
                strata,
                effective_coverage_hours=effective_hours,
            )
            print(
                f"asset={asset_name} direction={direction} n={overall['n']} "
                f"target_exit_pct={fmt_decimal(overall['target_exit_pct'])} "
                f"net_p20={fmt_decimal(overall['net_p20'])} net_p50={fmt_decimal(overall['net_p50'])} "
                f"hold_p50={fmt_decimal(overall['hold_p50'])} "
                f"train_n={overall['split']['train']['n']} "
                f"train_p20={fmt_decimal(overall['split']['train']['p20'])} "
                f"holdout_n={overall['split']['holdout']['n']} "
                f"holdout_p20={fmt_decimal(overall['split']['holdout']['p20'])} "
                f"max_utc_share_pct={fmt_decimal(strata['max_utc_share_pct'])} "
                f"weekday_max_utc_share_pct="
                f"{fmt_decimal(strata['period_max_utc_share_pct']['weekday'])}"
            )
            for period, stats in strata["periods"].items():
                print(
                    f"asset={asset_name} direction={direction} period={period} n={stats['n']} "
                    f"target_exit_pct={fmt_decimal(stats['target_exit_pct'])} "
                    f"net_p20={fmt_decimal(stats['net_p20'])} net_p50={fmt_decimal(stats['net_p50'])} "
                    f"train_p20={fmt_decimal(stats['split']['train']['p20'])} "
                    f"holdout_p20={fmt_decimal(stats['split']['holdout']['p20'])}"
                )
            print(
                f"asset={asset_name} direction={direction} liquidity_thresholds "
                f"total_spread_p33={fmt_decimal(strata['spread_p33'])} "
                f"total_spread_p67={fmt_decimal(strata['spread_p67'])}"
            )
            for bucket, stats in strata["liquidity"].items():
                print(
                    f"asset={asset_name} direction={direction} liquidity={bucket} n={stats['n']} "
                    f"net_p20={fmt_decimal(stats['net_p20'])} net_p50={fmt_decimal(stats['net_p50'])}"
                )
            for block, stats in strata["utc_blocks"].items():
                print(
                    f"asset={asset_name} direction={direction} utc={block} n={stats['n']} "
                    f"net_p20={fmt_decimal(stats['net_p20'])} net_p50={fmt_decimal(stats['net_p50'])}"
                )
            print(
                f"asset={asset_name} direction={direction} verdict="
                f"{verdict if formal_config else 'non_formal_diagnostic_only'} "
                f"reasons={','.join(reasons) if reasons else '-'}"
            )


def _basis_v4_formal_profile(
    *,
    asset: str | None,
    evaluation_interval_seconds: int,
    history_sample_seconds: int,
    episode_cooldown_seconds: int,
    max_hold_seconds: int,
    max_sample_gap_seconds: int,
    min_window_coverage: Decimal,
    min_history_samples: int,
    long_shortfall_reserve_bps: Decimal,
    short_shortfall_reserve_bps: Decimal,
    net_exit_target_bps: Decimal,
) -> str | None:
    common_formal_config = (
        evaluation_interval_seconds == 1
        and history_sample_seconds == 30
        and episode_cooldown_seconds == 180
        and max_hold_seconds == 21600
        and max_sample_gap_seconds == 60
        and min_window_coverage == Decimal("0.80")
        and min_history_samples == 100
        and net_exit_target_bps == Decimal("1")
    )
    if not common_formal_config:
        return None
    if long_shortfall_reserve_bps == Decimal("2") and short_shortfall_reserve_bps == Decimal("2"):
        return "fixed_2bps_v1"
    if (
        str(asset or "").upper() == "ETH"
        and long_shortfall_reserve_bps == Decimal("2")
        and short_shortfall_reserve_bps == Decimal("0.50")
    ):
        return "eth_short_execution_calibrated_20260724_n10"
    return None


def print_basis_v3(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    evaluation_interval_seconds: int,
    history_sample_seconds: int,
    episode_cooldown_seconds: int,
    max_hold_seconds: int,
    max_sample_gap_seconds: int,
    min_window_coverage: Decimal,
    min_history_samples: int,
    long_shortfall_reserve_bps: Decimal,
    short_shortfall_reserve_bps: Decimal,
    min_net_expected_bps: Decimal,
    holdout_fraction: Decimal,
    min_independent_samples: int,
) -> None:
    print("== basis_v3 ==")
    print(
        "model=strictly_prior_1h_6h_24h_7d_quantiles_executable_target_exit "
        "reserve_is_unmodeled_shortfall_only_no_spread_double_count"
    )
    print(
        f"evaluation_interval_seconds={evaluation_interval_seconds} history_sample_seconds={history_sample_seconds} "
        f"episode_cooldown_seconds={episode_cooldown_seconds} max_hold_seconds={max_hold_seconds} "
        f"max_sample_gap_seconds={max_sample_gap_seconds} "
        f"min_window_coverage={fmt_decimal(min_window_coverage)} min_history_samples={min_history_samples} "
        f"long_shortfall_reserve={fmt_decimal(long_shortfall_reserve_bps)} "
        f"short_shortfall_reserve={fmt_decimal(short_shortfall_reserve_bps)} "
        f"min_net_expected={fmt_decimal(min_net_expected_bps)}"
    )
    results = build_basis_v3_replay(
        rows,
        asset_filter=asset,
        evaluation_interval_seconds=evaluation_interval_seconds,
        history_sample_seconds=history_sample_seconds,
        episode_cooldown_seconds=episode_cooldown_seconds,
        max_hold_seconds=max_hold_seconds,
        max_sample_gap_seconds=max_sample_gap_seconds,
        min_window_coverage=min_window_coverage,
        min_history_samples=min_history_samples,
        long_shortfall_reserve_bps=long_shortfall_reserve_bps,
        short_shortfall_reserve_bps=short_shortfall_reserve_bps,
        min_net_expected_bps=min_net_expected_bps,
    )
    if not results:
        print("BASIS_V3 action=WAIT reason=no_time_aligned_basis_state_rows")
        return
    for asset_name, item in sorted(results.items()):
        coverage_seconds = (
            float(item["ended_at"]) - float(item["started_at"])
            if item["started_at"] is not None and item["ended_at"] is not None
            else 0.0
        )
        coverage_days = Decimal(str(coverage_seconds / 86400.0))
        print(
            f"asset={asset_name} rows={item['rows']} coverage_days={fmt_decimal(coverage_days)} "
            f"episodes={len(item['episodes'])} blocked={dict(item['blocked'])}"
        )
        for direction, windows in item["latest_quantiles"].items():
            for window, context in windows.items():
                print(
                    f"asset={asset_name} direction={direction} window={window}s count={context['count']} "
                    f"coverage_seconds={context['coverage_seconds']:.0f} mature={context['mature']} "
                    f"p55={fmt_decimal(context['p55'])} p60={fmt_decimal(context['p60'])} "
                    f"p85={fmt_decimal(context['p85'])} p90={fmt_decimal(context['p90'])}"
                )
        for variant in V3_VARIANTS:
            for direction in V2_DIRECTIONS:
                episodes = [
                    episode
                    for episode in item["episodes"]
                    if episode["variant"] == variant and episode["direction"] == direction
                ]
                split = _basis_v3_split_stats(
                    episodes,
                    holdout_fraction=holdout_fraction,
                    min_independent_samples=min_independent_samples,
                )
                pnl = [episode["net_pnl_bps"] for episode in episodes]
                holding = [episode["holding_seconds"] for episode in episodes]
                mfe = [episode["mfe_bps"] for episode in episodes if episode["mfe_bps"] is not None]
                mae = [episode["mae_bps"] for episode in episodes if episode["mae_bps"] is not None]
                episodes_per_day = (
                    Decimal(len(episodes)) / coverage_days if coverage_days > 0 else None
                )
                alignments = Counter(episode["stablecoin_alignment"] for episode in episodes)
                print(
                    f"asset={asset_name} variant={variant} direction={direction} n={len(episodes)} "
                    f"episodes_per_day={fmt_decimal(episodes_per_day)} net_p20={fmt_decimal(percentile(pnl, Decimal('20')))} "
                    f"net_p50={fmt_decimal(percentile(pnl, Decimal('50')))} "
                    f"hold_p50={fmt_decimal(percentile(holding, Decimal('50')))} "
                    f"hold_p80={fmt_decimal(percentile(holding, Decimal('80')))} "
                    f"mfe_p50={fmt_decimal(percentile(mfe, Decimal('50')))} "
                    f"mae_p20={fmt_decimal(percentile(mae, Decimal('20')))} "
                    f"train_n={split['train']['n']} train_p20={fmt_decimal(split['train']['p20'])} "
                    f"holdout_n={split['holdout']['n']} holdout_p20={fmt_decimal(split['holdout']['p20'])} "
                    f"alignments={dict(alignments)} verdict={split['verdict']}"
                )
    print("recommendation=require_positive_time_holdout_before_any_basis_v3_live_start")


def print_basis_v4(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    evaluation_interval_seconds: int,
    history_sample_seconds: int,
    episode_cooldown_seconds: int,
    max_hold_seconds: int,
    max_sample_gap_seconds: int,
    min_window_coverage: Decimal,
    min_history_samples: int,
    long_shortfall_reserve_bps: Decimal,
    short_shortfall_reserve_bps: Decimal,
    net_exit_target_bps: Decimal,
    holdout_fraction: Decimal,
    min_independent_samples: int,
) -> None:
    print("== basis_v4 ==")
    print(
        "model=strictly_prior_extreme_raw_edge_entry_executable_net_pnl_exit "
        "pre_registered_entry_percentiles=90,95,97.5 no_same_direction_exit_proxy"
    )
    print(
        f"evaluation_interval_seconds={evaluation_interval_seconds} history_sample_seconds={history_sample_seconds} "
        f"episode_cooldown_seconds={episode_cooldown_seconds} max_hold_seconds={max_hold_seconds} "
        f"max_sample_gap_seconds={max_sample_gap_seconds} "
        f"min_window_coverage={fmt_decimal(min_window_coverage)} min_history_samples={min_history_samples} "
        f"long_shortfall_reserve={fmt_decimal(long_shortfall_reserve_bps)} "
        f"short_shortfall_reserve={fmt_decimal(short_shortfall_reserve_bps)} "
        f"net_exit_target={fmt_decimal(net_exit_target_bps)}"
    )
    results = build_basis_v4_replay(
        rows,
        asset_filter=asset,
        evaluation_interval_seconds=evaluation_interval_seconds,
        history_sample_seconds=history_sample_seconds,
        episode_cooldown_seconds=episode_cooldown_seconds,
        max_hold_seconds=max_hold_seconds,
        max_sample_gap_seconds=max_sample_gap_seconds,
        min_window_coverage=min_window_coverage,
        min_history_samples=min_history_samples,
        long_shortfall_reserve_bps=long_shortfall_reserve_bps,
        short_shortfall_reserve_bps=short_shortfall_reserve_bps,
        net_exit_target_bps=net_exit_target_bps,
    )
    if not results:
        print("BASIS_V4 action=WAIT reason=no_time_aligned_basis_state_rows")
        return
    for asset_name, item in sorted(results.items()):
        coverage_seconds = (
            float(item["ended_at"]) - float(item["started_at"])
            if item["started_at"] is not None and item["ended_at"] is not None
            else 0.0
        )
        coverage_days = Decimal(str(coverage_seconds / 86400.0))
        print(
            f"asset={asset_name} rows={item['rows']} coverage_days={fmt_decimal(coverage_days)} "
            f"episodes={len(item['episodes'])} blocked={dict(item['blocked'])}"
        )
        for entry_percentile in V4_ENTRY_PERCENTILES:
            variant = f"p{entry_percentile}"
            for direction in V2_DIRECTIONS:
                episodes = [
                    episode
                    for episode in item["episodes"]
                    if episode["entry_percentile"] == entry_percentile and episode["direction"] == direction
                ]
                split = _basis_v3_split_stats(
                    episodes,
                    holdout_fraction=holdout_fraction,
                    min_independent_samples=min_independent_samples,
                )
                pnl = [episode["net_pnl_bps"] for episode in episodes]
                holding = [episode["holding_seconds"] for episode in episodes]
                target_exits = sum(episode["exit_reason"] == "executable_net_target_reached" for episode in episodes)
                episodes_per_day = Decimal(len(episodes)) / coverage_days if coverage_days > 0 else None
                print(
                    f"asset={asset_name} variant={variant} direction={direction} n={len(episodes)} "
                    f"episodes_per_day={fmt_decimal(episodes_per_day)} target_exit_pct="
                    f"{fmt_decimal(Decimal(target_exits) / Decimal(len(episodes)) * Decimal('100') if episodes else None)} "
                    f"net_p20={fmt_decimal(percentile(pnl, Decimal('20')))} net_p50={fmt_decimal(percentile(pnl, Decimal('50')))} "
                    f"hold_p50={fmt_decimal(percentile(holding, Decimal('50')))} "
                    f"train_n={split['train']['n']} train_p20={fmt_decimal(split['train']['p20'])} "
                    f"holdout_n={split['holdout']['n']} holdout_p20={fmt_decimal(split['holdout']['p20'])} "
                    f"verdict={split['verdict']}"
                )
    print("recommendation=require_positive_time_holdout_before_any_basis_v4_live_start")


def _basis_v2_stats() -> dict[str, Any]:
    return {"attempts": 0, "pnl_bps": [], "edge_deltas": []}


def _basis_v2_add_stat(
    item: dict[str, Any],
    *,
    horizon: int,
    pnl_bps: Decimal | None,
    edge_delta: Decimal | None,
    dimension: str,
    bucket: str,
) -> None:
    if dimension == "short_vs_long":
        horizon_stats = item["horizons"][horizon]
        horizon_stats["attempts"] += 1
        if pnl_bps is not None:
            horizon_stats["pnl_bps"].append(pnl_bps)
        if edge_delta is not None:
            horizon_stats["edge_deltas"].append(edge_delta)
    bucket_stats = item["contexts"][dimension].setdefault(bucket, {})
    bucket_horizon = bucket_stats.setdefault(horizon, _basis_v2_stats())
    bucket_horizon["attempts"] += 1
    if pnl_bps is not None:
        bucket_horizon["pnl_bps"].append(pnl_bps)
    if edge_delta is not None:
        bucket_horizon["edge_deltas"].append(edge_delta)


def _basis_v2_quote_age_bucket(row: dict[str, Any], fresh_quote_ms: Decimal) -> str:
    age_seconds = to_decimal(row.get("var_quote_age_seconds"))
    if age_seconds is None:
        return "unknown"
    age_ms = age_seconds * Decimal("1000")
    if age_ms <= fresh_quote_ms:
        return "fresh"
    if age_ms <= fresh_quote_ms * Decimal("3"):
        return "aged"
    return "stale"


def _basis_v2_context_bucket(
    medians: dict[int, Decimal | None],
    gap_threshold_bps: Decimal,
) -> str:
    short_median = medians.get(300)
    long_median = medians.get(3600)
    if short_median is None or long_median is None:
        return "insufficient_history"
    gap = short_median - long_median
    if gap <= -gap_threshold_bps:
        return "short_below_long"
    if gap >= gap_threshold_bps:
        return "short_above_long"
    return "aligned"


def _basis_v2_cost_bucket(
    row: dict[str, Any],
    var_spread_p80: Decimal | None,
    lighter_spread_p80: Decimal | None,
) -> str:
    var_spread = _spread_bps_from_bid_ask(row, "var_bid", "var_ask")
    lighter_spread = _spread_bps_from_bid_ask(row, "lighter_bid", "lighter_ask")
    if var_spread is None or lighter_spread is None or var_spread_p80 is None or lighter_spread_p80 is None:
        return "unknown"
    if var_spread > var_spread_p80 or lighter_spread > lighter_spread_p80:
        return "wide"
    return "normal"


def _basis_v2_candidate_edge(row: dict[str, Any], direction: str) -> Decimal | None:
    key = "long_edge_bps" if direction == "long_var_short_lighter" else "short_edge_bps"
    return to_decimal(row.get(key))


def _basis_v2_normalized_edge(row: dict[str, Any], direction: str) -> Decimal | None:
    key = "normalized_long_edge_bps" if direction == "long_var_short_lighter" else "normalized_short_edge_bps"
    return to_decimal(row.get(key))


def _basis_v2_stablecoin_edge_share(row: dict[str, Any], direction: str) -> Decimal | None:
    key = "long_stablecoin_edge_share" if direction == "long_var_short_lighter" else "short_stablecoin_edge_share"
    return to_decimal(row.get(key))


def build_basis_v2_replay(
    rows: list[dict[str, Any]],
    *,
    asset_filter: str | None = None,
    min_raw_edge_bps: Decimal = Decimal("7"),
    max_quote_age_ms: Decimal = Decimal("0"),
    max_var_spread_bps: Decimal = Decimal("0"),
    max_lighter_spread_bps: Decimal = Decimal("0"),
    context_gap_bps: Decimal = Decimal("1"),
    fresh_quote_ms: Decimal = Decimal("500"),
    min_normalized_edge_bps: Decimal | None = None,
    max_stablecoin_edge_share: Decimal = Decimal("0"),
    min_reversion_deviation_bps: Decimal = Decimal("0"),
    horizons: tuple[int, ...] = V2_HORIZONS_SECONDS,
    event_cooldown_seconds: int = 0,
) -> dict[str, dict[str, Any]]:
    """Replay directional executable edges using strictly prior rolling context.

    This is intentionally a diagnostic model. It does not model order submission,
    fees, or actual fills, and it never changes the live strategy.
    """
    groups: dict[tuple[str, str], list[tuple[float, dict[str, Any]]]] = {}
    for row in basis_state_rows(rows):
        asset = str(row.get("asset") or "-").upper()
        if asset_filter and asset != asset_filter.upper():
            continue
        timestamp = parse_time(row.get("logged_at"))
        if timestamp is None:
            continue
        run_id = str(row.get("run_id") or "legacy")
        groups.setdefault((run_id, asset), []).append((timestamp.timestamp(), row))

    results: dict[str, dict[str, Any]] = {}
    for (_run_id, asset), group in groups.items():
        group.sort(key=lambda item: item[0])
        times = [timestamp for timestamp, _ in group]
        group_rows = [row for _, row in group]
        var_spreads = [
            value for row in group_rows if (value := _spread_bps_from_bid_ask(row, "var_bid", "var_ask")) is not None
        ]
        lighter_spreads = [
            value for row in group_rows if (value := _spread_bps_from_bid_ask(row, "lighter_bid", "lighter_ask")) is not None
        ]
        var_spread_p80 = percentile(var_spreads, Decimal("80"))
        lighter_spread_p80 = percentile(lighter_spreads, Decimal("80"))
        item = results.setdefault(
            asset,
            {
                "rows": 0,
                "candidate_count": 0,
                "raw_candidate_count": 0,
                "direction_counts": Counter(),
                "horizons": {horizon: _basis_v2_stats() for horizon in horizons},
                "contexts": {"short_vs_long": {}, "quote_age": {}, "cost_regime": {}},
                "candidate_edges": [],
                "candidate_events": [],
                "var_spread_values": [],
                "lighter_spread_values": [],
            },
        )
        item["rows"] += len(group_rows)
        item["var_spread_values"].extend(var_spreads)
        item["lighter_spread_values"].extend(lighter_spreads)
        rolling = {
            direction: {window: _RollingMedian(window) for window in V2_WINDOWS_SECONDS}
            for direction in V2_DIRECTIONS
        }
        last_event_at: dict[str, float | None] = {direction: None for direction in V2_DIRECTIONS}

        for timestamp, row in group:
            medians_by_direction: dict[str, dict[int, Decimal | None]] = {}
            for direction in V2_DIRECTIONS:
                medians_by_direction[direction] = {
                    window: rolling[direction][window].median_before(timestamp)
                    for window in V2_WINDOWS_SECONDS
                }

            for direction in V2_DIRECTIONS:
                edge = _basis_v2_candidate_edge(row, direction)
                if edge is None:
                    continue
                medians = medians_by_direction[direction]
                if min_reversion_deviation_bps > 0:
                    median_5m = medians[300]
                    if median_5m is None or edge - median_5m < min_reversion_deviation_bps:
                        continue
                normalized_edge = _basis_v2_normalized_edge(row, direction)
                if min_normalized_edge_bps is not None and (
                    normalized_edge is None or normalized_edge < min_normalized_edge_bps
                ):
                    continue
                stablecoin_edge_share = _basis_v2_stablecoin_edge_share(row, direction)
                if max_stablecoin_edge_share > 0 and (
                    stablecoin_edge_share is None or stablecoin_edge_share > max_stablecoin_edge_share
                ):
                    continue
                if edge < min_raw_edge_bps:
                    continue
                quote_age = to_decimal(row.get("var_quote_age_seconds"))
                if max_quote_age_ms > 0 and (quote_age is None or quote_age * Decimal("1000") > max_quote_age_ms):
                    continue
                var_spread = _spread_bps_from_bid_ask(row, "var_bid", "var_ask")
                if max_var_spread_bps > 0 and (var_spread is None or var_spread > max_var_spread_bps):
                    continue
                lighter_spread = _spread_bps_from_bid_ask(row, "lighter_bid", "lighter_ask")
                if max_lighter_spread_bps > 0 and (lighter_spread is None or lighter_spread > max_lighter_spread_bps):
                    continue
                item["raw_candidate_count"] += 1
                previous_event_at = last_event_at[direction]
                if (
                    event_cooldown_seconds > 0
                    and previous_event_at is not None
                    and timestamp < previous_event_at + event_cooldown_seconds
                ):
                    continue
                last_event_at[direction] = timestamp
                item["candidate_count"] += 1
                item["direction_counts"][direction] += 1
                item["candidate_edges"].append(edge)
                candidate_event = {
                    "timestamp": timestamp,
                    "direction": direction,
                    "pnl_bps": {},
                }
                item["candidate_events"].append(candidate_event)
                for horizon in horizons:
                    future_index = bisect_left(times, timestamp + horizon)
                    if future_index >= len(group_rows):
                        continue
                    future = group_rows[future_index]
                    pnl_bps = _entry_semantics_forward_pnl_bps(row, future, direction)
                    if pnl_bps is not None:
                        candidate_event["pnl_bps"][horizon] = pnl_bps
                    future_edge = _basis_v2_candidate_edge(future, direction)
                    edge_delta = future_edge - edge if future_edge is not None else None
                    _basis_v2_add_stat(
                        item,
                        horizon=horizon,
                        pnl_bps=pnl_bps,
                        edge_delta=edge_delta,
                        dimension="short_vs_long",
                        bucket=_basis_v2_context_bucket(medians, context_gap_bps),
                    )
                    _basis_v2_add_stat(
                        item,
                        horizon=horizon,
                        pnl_bps=pnl_bps,
                        edge_delta=edge_delta,
                        dimension="quote_age",
                        bucket=_basis_v2_quote_age_bucket(row, fresh_quote_ms),
                    )
                    _basis_v2_add_stat(
                        item,
                        horizon=horizon,
                        pnl_bps=pnl_bps,
                        edge_delta=edge_delta,
                        dimension="cost_regime",
                        bucket=_basis_v2_cost_bucket(row, var_spread_p80, lighter_spread_p80),
                    )

            for direction in V2_DIRECTIONS:
                edge = _basis_v2_candidate_edge(row, direction)
                if edge is not None:
                    for window in V2_WINDOWS_SECONDS:
                        rolling[direction][window].add(timestamp, edge)

    for item in results.values():
        item["var_spread_p80"] = percentile(item.pop("var_spread_values"), Decimal("80"))
        item["lighter_spread_p80"] = percentile(item.pop("lighter_spread_values"), Decimal("80"))
    return results


def _basis_v2_stat_text(stats: dict[str, Any]) -> str:
    pnl_values = stats["pnl_bps"]
    positive_pct = (
        Decimal(sum(value > 0 for value in pnl_values)) / Decimal(len(pnl_values)) * Decimal("100")
        if pnl_values
        else None
    )
    return (
        f"n={len(pnl_values)} attempts={stats['attempts']} "
        f"p20={fmt_decimal(percentile(pnl_values, Decimal('20')))} "
        f"p50={fmt_decimal(percentile(pnl_values, Decimal('50')))} "
        f"p80={fmt_decimal(percentile(pnl_values, Decimal('80')))} "
        f"positive_pct={fmt_decimal(positive_pct)}"
    )


def print_basis_v2(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    min_raw_edge_bps: Decimal,
    max_quote_age_ms: Decimal,
    max_var_spread_bps: Decimal,
    max_lighter_spread_bps: Decimal,
    context_gap_bps: Decimal,
    fresh_quote_ms: Decimal,
) -> None:
    print("== basis_v2 ==")
    print(
        "model=read_only_directional_executable_edge_time_aligned_strict_prior_context "
        "excludes_fees_submission_latency_and_actual_fills"
    )
    print(
        f"min_raw_edge_bps={fmt_decimal(min_raw_edge_bps)} max_quote_age_ms={fmt_decimal(max_quote_age_ms)} "
        f"max_var_spread_bps={fmt_decimal(max_var_spread_bps)} max_lighter_spread_bps={fmt_decimal(max_lighter_spread_bps)} "
        f"context_gap_bps={fmt_decimal(context_gap_bps)} fresh_quote_ms={fmt_decimal(fresh_quote_ms)}"
    )
    results = build_basis_v2_replay(
        rows,
        asset_filter=asset,
        min_raw_edge_bps=min_raw_edge_bps,
        max_quote_age_ms=max_quote_age_ms,
        max_var_spread_bps=max_var_spread_bps,
        max_lighter_spread_bps=max_lighter_spread_bps,
        context_gap_bps=context_gap_bps,
        fresh_quote_ms=fresh_quote_ms,
    )
    if not results:
        print("BASIS_V2 action=WAIT reason=no_time_aligned_basis_state_rows")
        return
    for asset_name, item in sorted(results.items()):
        print(
            f"asset={asset_name} rows={item['rows']} candidates={item['candidate_count']} "
            f"directions={dict(item['direction_counts'])} "
            f"candidate_edge_p50={fmt_decimal(percentile(item['candidate_edges'], Decimal('50')))} "
            f"candidate_edge_p80={fmt_decimal(percentile(item['candidate_edges'], Decimal('80')))}"
        )
        print(
            f"asset={asset_name} cost_reference_var_p80={fmt_decimal(item['var_spread_p80'])} "
            f"cost_reference_lighter_p80={fmt_decimal(item['lighter_spread_p80'])}"
        )
        for horizon, stats in item["horizons"].items():
            print(f"asset={asset_name} horizon={horizon}s {_basis_v2_stat_text(stats)}")
        for dimension, buckets in item["contexts"].items():
            for bucket, horizon_stats in sorted(buckets.items()):
                five_second = horizon_stats.get(5)
                if five_second is not None:
                    print(
                        f"asset={asset_name} context={dimension} bucket={bucket} "
                        f"horizon=5s {_basis_v2_stat_text(five_second)}"
                    )
    print("recommendation=review_holdout_and_execution_reserve_before_shadow")


def _basis_v2_net_pnl_stats(
    events: list[dict[str, Any]],
    *,
    horizon: int,
    execution_reserve_bps: Decimal,
) -> dict[str, Any]:
    pnl_values = [
        pnl_bps - execution_reserve_bps
        for event in events
        if (pnl_bps := event["pnl_bps"].get(horizon)) is not None
    ]
    positive_pct = (
        Decimal(sum(value > 0 for value in pnl_values)) / Decimal(len(pnl_values)) * Decimal("100")
        if pnl_values
        else None
    )
    return {
        "n": len(pnl_values),
        "p20": percentile(pnl_values, Decimal("20")),
        "p50": percentile(pnl_values, Decimal("50")),
        "positive_pct": positive_pct,
    }


def summarize_basis_v2_sweep_events(
    events: list[dict[str, Any]],
    *,
    horizons: tuple[int, ...],
    holdout_fraction: Decimal,
    min_independent_samples: int,
    execution_reserve_bps: Decimal,
) -> dict[int, dict[str, Any]]:
    """Split de-duplicated events chronologically and score net forward PnL.

    The split is diagnostic only: it prevents a threshold from being judged solely
    on the same events that made it look attractive.
    """
    ordered_events = sorted(events, key=lambda event: float(event["timestamp"]))
    if len(ordered_events) < 2:
        train_events = ordered_events
        holdout_events: list[dict[str, Any]] = []
    else:
        split_index = int(Decimal(len(ordered_events)) * (Decimal("1") - holdout_fraction))
        split_index = min(max(split_index, 1), len(ordered_events) - 1)
        train_events = ordered_events[:split_index]
        holdout_events = ordered_events[split_index:]

    summary: dict[int, dict[str, Any]] = {}
    for horizon in horizons:
        train = _basis_v2_net_pnl_stats(
            train_events,
            horizon=horizon,
            execution_reserve_bps=execution_reserve_bps,
        )
        holdout = _basis_v2_net_pnl_stats(
            holdout_events,
            horizon=horizon,
            execution_reserve_bps=execution_reserve_bps,
        )
        if train["n"] < min_independent_samples or holdout["n"] < min_independent_samples:
            verdict = "insufficient_independent_data"
        elif train["p20"] is None or holdout["p20"] is None or train["p20"] <= 0 or holdout["p20"] <= 0:
            verdict = "net_p20_not_positive"
        else:
            verdict = "manual_review_candidate"
        summary[horizon] = {"train": train, "holdout": holdout, "verdict": verdict}
    return summary


def print_basis_v2_filter_sweep(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    min_raw_edge_bps: Decimal,
    max_quote_age_ms: Decimal,
    max_var_spread_bps: Decimal,
    max_lighter_spread_bps: Decimal,
    context_gap_bps: Decimal,
    fresh_quote_ms: Decimal,
    normalized_thresholds: tuple[Decimal, ...],
    stablecoin_share_thresholds: tuple[Decimal, ...],
    min_reversion_deviation_bps: Decimal,
    horizons: tuple[int, ...],
    event_cooldown_seconds: int,
    holdout_fraction: Decimal,
    min_independent_samples: int,
    execution_reserve_bps: Decimal,
) -> None:
    print("== basis_v2_filter_sweep ==")
    print(
        "model=raw_edge_plus_prior_5m_reversion_with_normalized_and_stablecoin_filters "
        "forward_pnl_uses_executable_bid_ask_minus_configured_execution_reserve"
    )
    print(
        f"event_cooldown_seconds={event_cooldown_seconds} holdout_fraction={fmt_decimal(holdout_fraction)} "
        f"min_independent_samples={min_independent_samples} execution_reserve_bps={fmt_decimal(execution_reserve_bps)}"
    )
    for normalized_threshold in normalized_thresholds:
        for stablecoin_share_threshold in stablecoin_share_thresholds:
            results = build_basis_v2_replay(
                rows,
                asset_filter=asset,
                min_raw_edge_bps=min_raw_edge_bps,
                max_quote_age_ms=max_quote_age_ms,
                max_var_spread_bps=max_var_spread_bps,
                max_lighter_spread_bps=max_lighter_spread_bps,
                context_gap_bps=context_gap_bps,
                fresh_quote_ms=fresh_quote_ms,
                min_normalized_edge_bps=normalized_threshold,
                max_stablecoin_edge_share=stablecoin_share_threshold,
                min_reversion_deviation_bps=min_reversion_deviation_bps,
                horizons=horizons,
                event_cooldown_seconds=event_cooldown_seconds,
            )
            item = results.get(asset.upper()) if asset and results else None
            if item is None and results:
                item = next(iter(results.values()))
            if item is None:
                print(
                    f"min_norm={fmt_decimal(normalized_threshold)} max_share={fmt_decimal(stablecoin_share_threshold)} "
                    "candidate_rows=0 independent_events=0 horizon=5s train_n=0 holdout_n=0 verdict=insufficient_independent_data"
                )
                continue
            spread_reference = (item["var_spread_p80"] or Decimal("0")) + (
                item["lighter_spread_p80"] or Decimal("0")
            )
            event_summary = summarize_basis_v2_sweep_events(
                item["candidate_events"],
                horizons=horizons,
                holdout_fraction=holdout_fraction,
                min_independent_samples=min_independent_samples,
                execution_reserve_bps=execution_reserve_bps,
            )
            horizon_text: list[str] = []
            for horizon in horizons:
                stats = event_summary[horizon]
                train = stats["train"]
                holdout = stats["holdout"]
                horizon_text.append(
                    f"h{horizon}_train_n={train['n']} h{horizon}_train_p20={fmt_decimal(train['p20'])} "
                    f"h{horizon}_holdout_n={holdout['n']} h{horizon}_holdout_p20={fmt_decimal(holdout['p20'])} "
                    f"h{horizon}_holdout_p50={fmt_decimal(holdout['p50'])} h{horizon}_verdict={stats['verdict']}"
                )
            print(
                f"asset={asset or '-'} min_norm={fmt_decimal(normalized_threshold)} "
                f"max_share={fmt_decimal(stablecoin_share_threshold)} "
                f"candidate_rows={item['raw_candidate_count']} independent_events={item['candidate_count']} "
                f"spread_ref_p80={fmt_decimal(spread_reference)} "
                + " ".join(horizon_text)
            )
    print(f"min_reversion_deviation_bps={fmt_decimal(min_reversion_deviation_bps)}")


def _collect_decimal(rows: list[dict[str, Any]], key: str) -> list[Decimal]:
    values: list[Decimal] = []
    for row in rows:
        value = to_decimal(row.get(key))
        if value is not None:
            values.append(value)
    return values


def _best_edges(rows: list[dict[str, Any]], *, normalized: bool) -> list[Decimal]:
    values: list[Decimal] = []
    keys = (
        ("normalized_long_edge_bps", "normalized_short_edge_bps")
        if normalized
        else ("long_edge_bps", "short_edge_bps")
    )
    for row in rows:
        candidates = [to_decimal(row.get(key)) for key in keys]
        candidates = [value for value in candidates if value is not None]
        if candidates:
            values.append(max(candidates))
    return values


def _abs_values(values: list[Decimal]) -> list[Decimal]:
    return [abs(value) for value in values]


def _spread_bps_from_bid_ask(row: dict[str, Any], bid_key: str, ask_key: str) -> Decimal | None:
    bid = to_decimal(row.get(bid_key))
    ask = to_decimal(row.get(ask_key))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    if ask < bid:
        bid, ask = ask, bid
    mid = (bid + ask) / Decimal("2")
    if mid <= 0:
        return None
    return (ask - bid) / mid * Decimal("10000")


def _spread_regime(current: Decimal | None, values: list[Decimal]) -> tuple[str, Decimal]:
    if current is None or not values:
        return "unknown", Decimal("0")
    median = percentile(values, Decimal("50"))
    p80 = percentile(values, Decimal("80"))
    p95 = percentile(values, Decimal("95"))
    if median is None or p80 is None or p95 is None:
        return "unknown", Decimal("0")
    if current > p95:
        return "extreme_filter", current - median
    if current > p80:
        return "wide_add_buffer", current - median
    return "normal", Decimal("0")


def _direction_abs_entry_ok(direction: str, basis: Decimal, threshold: Decimal) -> bool:
    if direction == "long_var_short_lighter":
        return basis <= -threshold
    if direction == "short_var_long_lighter":
        return basis >= threshold
    return False


def _direction_basis_pnl_bps(direction: str, entry_basis: Decimal, current_basis: Decimal) -> Decimal:
    if direction == "long_var_short_lighter":
        return current_basis - entry_basis
    return entry_basis - current_basis


def dynamic_cost_summary(rows: list[dict[str, Any]], *, asset_filter: str | None = None) -> dict[str, Any]:
    signal_rows = [
        row
        for row in basis_state_rows(rows)
        if row.get("asset")
        and (asset_filter is None or str(row.get("asset") or "").upper() == asset_filter.upper())
    ]
    var_spreads: list[Decimal] = []
    lighter_spreads: list[Decimal] = []
    latest: dict[str, Any] | None = None
    latest_var_spread = None
    latest_lighter_spread = None
    for row in signal_rows:
        var_spread = _spread_bps_from_bid_ask(row, "var_bid", "var_ask")
        lighter_spread = _spread_bps_from_bid_ask(row, "lighter_bid", "lighter_ask")
        if var_spread is not None:
            var_spreads.append(var_spread)
        if lighter_spread is not None:
            lighter_spreads.append(lighter_spread)
        if var_spread is not None or lighter_spread is not None:
            latest = row
            latest_var_spread = var_spread
            latest_lighter_spread = lighter_spread

    slippage_rows = [row for row in rows if row.get("event") == "live_inventory_actual_pnl"]
    entry_slippage = _collect_decimal(slippage_rows, "entry_lighter_slippage_bps")
    exit_slippage = _collect_decimal(slippage_rows, "exit_lighter_slippage_bps")
    slippage_pool = [value for value in entry_slippage + exit_slippage if value >= 0]
    expected_slippage = percentile(slippage_pool, Decimal("80")) or Decimal("0")
    var_regime, var_penalty = _spread_regime(latest_var_spread, var_spreads)
    lighter_regime, lighter_penalty = _spread_regime(latest_lighter_spread, lighter_spreads)
    current_var_cost = latest_var_spread or Decimal("0")
    current_lighter_cost = latest_lighter_spread or Decimal("0")
    dynamic_cost = current_var_cost + current_lighter_cost + expected_slippage + var_penalty + lighter_penalty
    return {
        "rows": len(signal_rows),
        "asset": str(latest.get("asset") or asset_filter or "-").upper() if latest else (asset_filter or "-").upper(),
        "latest_var_spread": latest_var_spread,
        "latest_lighter_spread": latest_lighter_spread,
        "var_spread_p50": percentile(var_spreads, Decimal("50")),
        "var_spread_p80": percentile(var_spreads, Decimal("80")),
        "var_spread_p95": percentile(var_spreads, Decimal("95")),
        "lighter_spread_p50": percentile(lighter_spreads, Decimal("50")),
        "lighter_spread_p80": percentile(lighter_spreads, Decimal("80")),
        "lighter_spread_p95": percentile(lighter_spreads, Decimal("95")),
        "expected_slippage": expected_slippage,
        "var_regime": var_regime,
        "lighter_regime": lighter_regime,
        "spread_regime_penalty": var_penalty + lighter_penalty,
        "dynamic_roundtrip_cost": dynamic_cost,
    }


def print_dynamic_cost(rows: list[dict[str, Any]], *, asset: str | None = None) -> None:
    item = dynamic_cost_summary(rows, asset_filter=asset)
    print("== dynamic_cost ==")
    print(
        f"asset={item['asset']} rows={item['rows']} "
        f"current_var_spread={fmt_decimal(item['latest_var_spread'])} "
        f"current_lighter_spread={fmt_decimal(item['latest_lighter_spread'])} "
        f"expected_slippage_p80={fmt_decimal(item['expected_slippage'])} "
        f"spread_penalty={fmt_decimal(item['spread_regime_penalty'])} "
        f"dynamic_roundtrip_cost={fmt_decimal(item['dynamic_roundtrip_cost'])}"
    )
    print(
        f"var_spread p50={fmt_decimal(item['var_spread_p50'])} p80={fmt_decimal(item['var_spread_p80'])} p95={fmt_decimal(item['var_spread_p95'])} "
        f"regime={item['var_regime']}"
    )
    print(
        f"lighter_spread p50={fmt_decimal(item['lighter_spread_p50'])} p80={fmt_decimal(item['lighter_spread_p80'])} p95={fmt_decimal(item['lighter_spread_p95'])} "
        f"regime={item['lighter_regime']}"
    )


def print_ladder_what_if(
    rows: list[dict[str, Any]],
    *,
    asset: str | None,
    lot_notional: Decimal,
    max_lots: int,
    addon_step: Decimal,
    min_entry_edge: Decimal,
    min_abs_entry: Decimal,
    min_norm_entry: Decimal,
    min_exit_pnl: Decimal,
    profit_take_pnl: Decimal,
) -> None:
    print("== ladder_what_if ==")
    print("model=rough_latest_cost_not_execution_replay")
    cost = dynamic_cost_summary(rows, asset_filter=asset)
    dynamic_cost = cost["dynamic_roundtrip_cost"] or Decimal("0")
    signal_rows = [
        row
        for row in basis_state_rows(rows)
        if row.get("asset")
        and (asset is None or str(row.get("asset") or "").upper() == asset.upper())
    ]
    open_lots: list[dict[str, Any]] = []
    entries = 0
    exits = 0
    blocked_cost = 0
    portfolio_pnls: list[Decimal] = []
    for row in signal_rows:
        basis = to_decimal(row.get("basis_bps"))
        if basis is None:
            continue
        if open_lots:
            direction = str(open_lots[0]["direction"])
            lot_pnls = [_direction_basis_pnl_bps(direction, lot["entry_basis"], basis) for lot in open_lots]
            gross_portfolio_pnl = sum(lot_pnls) / Decimal(len(lot_pnls))
            executable_pnl = gross_portfolio_pnl - dynamic_cost
            portfolio_pnls.append(executable_pnl)
            exit_target = profit_take_pnl if executable_pnl >= profit_take_pnl else min_exit_pnl
            if executable_pnl >= exit_target:
                exits += len(open_lots)
                open_lots.clear()
                continue

        metrics = best_direction_metrics(row)
        if metrics is None:
            continue
        direction = str(metrics["direction"])
        edge = metrics["edge"]
        normalized = metrics["normalized_edge"]
        roundtrip = metrics["roundtrip"]
        stablecoin_ok = metrics["stablecoin_ok"]
        if open_lots and direction != open_lots[0]["direction"]:
            continue
        if not _direction_abs_entry_ok(direction, basis, min_abs_entry):
            continue
        dynamic_entry_threshold = max(min_entry_edge, dynamic_cost + min_exit_pnl + Decimal("2.0"))
        if edge < dynamic_entry_threshold:
            continue
        if normalized is not None and normalized < min_norm_entry:
            continue
        if stablecoin_ok is False:
            continue
        if roundtrip is not None and roundtrip < Decimal("0"):
            blocked_cost += 1
            continue
        if edge < dynamic_cost:
            blocked_cost += 1
            continue
        if not open_lots:
            open_lots.append({"direction": direction, "entry_basis": basis, "notional": lot_notional})
            entries += 1
            continue
        if len(open_lots) >= max_lots:
            continue
        entry_bases = [lot["entry_basis"] for lot in open_lots]
        if direction == "long_var_short_lighter":
            addon_ok = basis <= min(entry_bases) - addon_step
        else:
            addon_ok = basis >= max(entry_bases) + addon_step
        if addon_ok:
            open_lots.append({"direction": direction, "entry_basis": basis, "notional": lot_notional})
            entries += 1

    print(
        f"asset={cost['asset']} rows={len(signal_rows)} lot_notional={fmt_decimal(lot_notional)} max_lots={max_lots} "
        f"addon_step_bps={fmt_decimal(addon_step)} dynamic_cost={fmt_decimal(dynamic_cost)} "
        f"dynamic_entry_threshold={fmt_decimal(max(min_entry_edge, dynamic_cost + min_exit_pnl + Decimal('2.0')))}"
    )
    print(
        f"entries={entries} exits={exits} open_lots={len(open_lots)} blocked_by_cost={blocked_cost} "
        f"portfolio_pnl_p50={fmt_decimal(percentile(portfolio_pnls, Decimal('50')))} "
        f"portfolio_pnl_p80={fmt_decimal(percentile(portfolio_pnls, Decimal('80')))} "
        f"portfolio_pnl_p95={fmt_decimal(percentile(portfolio_pnls, Decimal('95')))}"
    )
    if open_lots:
        basis = to_decimal(signal_rows[-1].get("basis_bps")) if signal_rows else None
        if basis is not None:
            direction = str(open_lots[0]["direction"])
            gross = sum(_direction_basis_pnl_bps(direction, lot["entry_basis"], basis) for lot in open_lots) / Decimal(len(open_lots))
            print(
                f"open_direction={direction} open_entry_basis={[fmt_decimal(lot['entry_basis']) for lot in open_lots]} "
                f"latest_basis={fmt_decimal(basis)} executable_pnl={fmt_decimal(gross - dynamic_cost)}"
            )


def print_basis_regime(rows: list[dict[str, Any]]) -> None:
    regimes = build_basis_regimes(rows)
    signal_rows = basis_state_rows(rows)
    print("== basis_regime ==")
    print(f"signal_rows={len(signal_rows)}")
    if not regimes:
        return

    for asset, item in sorted(regimes.items()):
        print(
            f"asset={asset} rows={item['rows']} blocked={item['blocked_count']} "
            f"latest_basis={fmt_decimal(item['latest_basis'])} latest_norm_basis={fmt_decimal(item['latest_norm_basis'])} "
            f"latest_best_raw={fmt_decimal(item['latest_best_raw'])} latest_best_norm={fmt_decimal(item['latest_best_norm'])} "
            f"latest_move={fmt_decimal(item['latest_move'])}"
        )
        print(
            f"asset={asset} basis_p10={fmt_decimal(item['basis_p10'])} "
            f"basis_p50={fmt_decimal(item['basis_p50'])} "
            f"basis_p90={fmt_decimal(item['basis_p90'])} "
            f"abs_basis_p90={fmt_decimal(item['abs_basis_p90'])} "
            f"norm_basis_p90={fmt_decimal(item['norm_basis_p90'])}"
        )
        print(
            f"asset={asset} raw_edge_p80={fmt_decimal(item['raw_edge_p80'])} "
            f"raw_edge_p95={fmt_decimal(item['raw_edge_p95'])} "
            f"norm_edge_p80={fmt_decimal(item['norm_edge_p80'])} "
            f"norm_edge_p95={fmt_decimal(item['norm_edge_p95'])} "
            f"sample_move_p80={fmt_decimal(item['sample_move_p80'])} "
            f"z_abs_p95={fmt_decimal(item['z_abs_p95'])}"
        )
        print(f"asset={asset} blocked_reasons={dict(item['blocked'].most_common(5))}")
        print(f"asset={asset} strategy_suggestion={item['suggestion']}")


def build_basis_regimes(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    signal_rows = basis_state_rows(rows)
    by_asset: dict[str, list[dict[str, Any]]] = {}
    for row in signal_rows:
        by_asset.setdefault(str(row.get("asset") or "-").upper(), []).append(row)

    regimes: dict[str, dict[str, Any]] = {}
    for asset, asset_rows in by_asset.items():
        basis = _collect_decimal(asset_rows, "basis_bps")
        norm_basis = _collect_decimal(asset_rows, "normalized_basis_bps")
        best_raw = _best_edges(asset_rows, normalized=False)
        best_norm = _best_edges(asset_rows, normalized=True)
        sample_moves = _abs_values(_collect_decimal(asset_rows, "basis_sample_move_bps"))
        z_values = _collect_decimal(asset_rows, "z")
        blocked = Counter(
            str(row.get("reason") or "-")
            for row in rows
            if row.get("event") == "live_inventory_entry_blocked"
            and str(row.get("asset") or "-").upper() == asset
        )
        latest = asset_rows[-1]
        latest_best_raw = max(
            [value for value in [to_decimal(latest.get("long_edge_bps")), to_decimal(latest.get("short_edge_bps"))] if value is not None],
            default=None,
        )
        latest_best_norm = max(
            [value for value in [to_decimal(latest.get("normalized_long_edge_bps")), to_decimal(latest.get("normalized_short_edge_bps"))] if value is not None],
            default=None,
        )
        raw_p95 = percentile(best_raw, Decimal("95"))
        norm_p80 = percentile(best_norm, Decimal("80"))
        norm_p95 = percentile(best_norm, Decimal("95"))
        move_p80 = percentile(sample_moves, Decimal("80"))
        if raw_p95 is None or len(asset_rows) < 1000:
            suggestion = "collect_more_data"
        elif raw_p95 < Decimal("10"):
            suggestion = "do_not_lower_threshold_market_edge_too_low"
        elif norm_p95 is not None and norm_p95 < Decimal("0.75"):
            suggestion = "wait_or_switch_asset_normalized_edge_weak"
        elif move_p80 is not None and move_p80 > Decimal("6"):
            suggestion = "avoid_looser_entries_sample_move_high"
        elif raw_p95 < Decimal("12"):
            suggestion = "test_small_only_min_abs_11_no_size_increase"
        else:
            suggestion = "current_threshold_reasonable_or_test_min_abs_12"
        regimes[asset] = {
            "rows": len(asset_rows),
            "blocked_count": sum(blocked.values()),
            "blocked": blocked,
            "latest_basis": to_decimal(latest.get("basis_bps")),
            "latest_norm_basis": to_decimal(latest.get("normalized_basis_bps")),
            "latest_best_raw": latest_best_raw,
            "latest_best_norm": latest_best_norm,
            "latest_move": abs(to_decimal(latest.get("basis_sample_move_bps")) or Decimal("0")),
            "basis_p10": percentile(basis, Decimal("10")),
            "basis_p50": percentile(basis, Decimal("50")),
            "basis_p90": percentile(basis, Decimal("90")),
            "abs_basis_p90": percentile(_abs_values(basis), Decimal("90")),
            "norm_basis_p90": percentile(norm_basis, Decimal("90")),
            "raw_edge_p80": percentile(best_raw, Decimal("80")),
            "raw_edge_p95": raw_p95,
            "norm_edge_p80": norm_p80,
            "norm_edge_p95": norm_p95,
            "sample_move_p80": move_p80,
            "z_abs_p95": percentile(_abs_values(z_values), Decimal("95")),
            "suggestion": suggestion,
        }
    return regimes


def print_strategy(rows: list[dict[str, Any]], events: Counter[str], status: str, open_lots: list[Any], pending_actions: list[Any], access_restricted: int) -> None:
    print("== strategy ==")
    regimes = build_basis_regimes(rows)
    if events["live_inventory_manual_review_required"] or status == "manual_review_required":
        print("STRATEGY action=STOP reason=manual_review_required_check_both_exchanges")
        return
    if status == "missing":
        print("STRATEGY action=WAIT reason=state_missing_confirm_exchanges_before_live")
        return
    if status != "flat" or open_lots or pending_actions:
        print("STRATEGY action=HOLD reason=state_not_flat_do_not_start_new_live")
        return
    if access_restricted:
        print("STRATEGY action=STOP reason=access_restricted_detected")
        return
    if not regimes:
        print("STRATEGY action=WAIT reason=collect_more_data")
        return

    ranked: list[tuple[Decimal, str, dict[str, Any]]] = []
    for asset, item in regimes.items():
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        score = raw + (norm * Decimal("2")) - max(move - Decimal("4"), Decimal("0"))
        ranked.append((score, asset, item))
    ranked.sort(reverse=True, key=lambda row: row[0])
    score, asset, item = ranked[0]
    print(
        f"STRATEGY best_asset={asset} score={fmt_decimal(score)} suggestion={item['suggestion']} "
        f"raw_p95={fmt_decimal(item['raw_edge_p95'])} norm_p95={fmt_decimal(item['norm_edge_p95'])} "
        f"move_p80={fmt_decimal(item['sample_move_p80'])} blocked={item['blocked_count']} rows={item['rows']}"
    )
    if item["suggestion"] == "current_threshold_reasonable_or_test_min_abs_12":
        print(f"STRATEGY action=CONTINUE_OR_TEST asset={asset} min_abs=12 size=20u reason=good_recent_edge_quality")
    elif item["suggestion"] == "test_small_only_min_abs_11_no_size_increase":
        print(f"STRATEGY action=TEST_SMALL asset={asset} min_abs=11 size=20u reason=edge_exists_but_not_enough_for_size_increase")
    elif item["suggestion"] == "avoid_looser_entries_sample_move_high":
        print(f"STRATEGY action=WAIT_OR_SWITCH asset={asset} reason=sample_move_high_do_not_loosen")
    elif item["suggestion"] == "wait_or_switch_asset_normalized_edge_weak":
        print(f"STRATEGY action=WAIT_OR_SWITCH asset={asset} reason=normalized_edge_weak_do_not_lower_threshold")
    elif item["suggestion"] == "do_not_lower_threshold_market_edge_too_low":
        print(f"STRATEGY action=WAIT_OR_SWITCH asset={asset} reason=raw_edge_too_low")
    else:
        print(f"STRATEGY action=WAIT asset={asset} reason=need_more_data")


def print_config_advice(rows: list[dict[str, Any]]) -> None:
    print("== config_advice ==")
    regimes = build_basis_regimes(rows)
    if not regimes:
        print("CONFIG_ADVICE action=NONE reason=collect_more_data")
        return

    ranked: list[tuple[Decimal, str, dict[str, Any]]] = []
    for asset, item in regimes.items():
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        score = raw + (norm * Decimal("2")) - max(move - Decimal("4"), Decimal("0"))
        ranked.append((score, asset, item))
    ranked.sort(reverse=True, key=lambda row: row[0])
    _, asset, item = ranked[0]

    raw_p95 = item["raw_edge_p95"] or Decimal("0")
    norm_p95 = item["norm_edge_p95"] or Decimal("0")
    move_p80 = item["sample_move_p80"] or Decimal("99")
    suggestion = item["suggestion"]

    if suggestion in {"wait_or_switch_asset_normalized_edge_weak", "do_not_lower_threshold_market_edge_too_low"}:
        print(
            f"CONFIG_ADVICE action=KEEP asset={asset} min_abs=current size=current "
            f"reason={suggestion} raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)} move_p80={fmt_decimal(move_p80)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=edge_quality_not_size_limited")
        return
    if suggestion == "avoid_looser_entries_sample_move_high":
        print(
            f"CONFIG_ADVICE action=KEEP_OR_SWITCH asset={asset} min_abs=current size=current "
            f"reason=sample_move_high move_p80={fmt_decimal(move_p80)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=sample_move_high")
        return
    if suggestion == "test_small_only_min_abs_11_no_size_increase":
        print(
            f"CONFIG_ADVICE action=SET asset={asset} min_entry_edge_bps=11 min_abs_entry_bps=11 "
            f"entry_confirm_samples=1 max_sample_move_bps=5 lot_notional_usd=20 live_max_notional_usd=25 "
            f"reason=some_edge_but_not_enough_for_larger_size raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=needs_successful_actual_pnl_first")
        return
    if suggestion == "current_threshold_reasonable_or_test_min_abs_12":
        if norm_p95 >= Decimal("1.5") and move_p80 <= Decimal("4") and raw_p95 >= Decimal("13"):
            print(
                f"CONFIG_ADVICE action=OPTIONAL_SIZE_TEST asset={asset} min_entry_edge_bps=12 min_abs_entry_bps=12 "
                f"entry_confirm_samples=1 lot_notional_usd=30 live_max_notional_usd=35 "
                f"reason=strong_edge_quality raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)} move_p80={fmt_decimal(move_p80)}"
            )
            return
        print(
            f"CONFIG_ADVICE action=SET asset={asset} min_entry_edge_bps=12 min_abs_entry_bps=12 "
            f"entry_confirm_samples=1 lot_notional_usd=20 live_max_notional_usd=25 "
            f"reason=reasonable_edge_but_size_not_justified raw_p95={fmt_decimal(raw_p95)} norm_p95={fmt_decimal(norm_p95)} move_p80={fmt_decimal(move_p80)}"
        )
        print("CONFIG_ADVICE size_increase=NO reason=need_actual_pnl_before_30u")
        return
    print(f"CONFIG_ADVICE action=NONE asset={asset} reason={suggestion}")


def print_asset_scores(rows: list[dict[str, Any]]) -> None:
    print("== asset_scores ==")
    regimes = build_basis_regimes(rows)
    if not regimes:
        print("ASSET_SCORE action=WAIT reason=collect_more_data")
        return

    scored: list[tuple[Decimal, str, dict[str, Any], str]] = []
    for asset, item in regimes.items():
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        rows_count = Decimal(str(item["rows"]))
        data_penalty = Decimal("0") if rows_count >= Decimal("1000") else Decimal("5")
        move_penalty = max(move - Decimal("4"), Decimal("0"))
        score = raw + (norm * Decimal("2")) - move_penalty - data_penalty
        if item["suggestion"] in {"wait_or_switch_asset_normalized_edge_weak", "do_not_lower_threshold_market_edge_too_low"}:
            action = "WAIT"
        elif item["suggestion"] == "avoid_looser_entries_sample_move_high":
            action = "AVOID_LOOSENING"
        elif item["suggestion"] == "test_small_only_min_abs_11_no_size_increase":
            action = "TEST_SMALL"
        elif item["suggestion"] == "current_threshold_reasonable_or_test_min_abs_12":
            action = "TEST_OR_CONTINUE"
        else:
            action = "COLLECT_MORE"
        scored.append((score, asset, item, action))

    scored.sort(reverse=True, key=lambda row: row[0])
    for rank, (score, asset, item, action) in enumerate(scored, start=1):
        print(
            f"ASSET_SCORE rank={rank} asset={asset} score={fmt_decimal(score)} action={action} "
            f"raw_p95={fmt_decimal(item['raw_edge_p95'])} norm_p95={fmt_decimal(item['norm_edge_p95'])} "
            f"move_p80={fmt_decimal(item['sample_move_p80'])} rows={item['rows']} blocked={item['blocked_count']} "
            f"suggestion={item['suggestion']}"
        )

    best_score, best_asset, best_item, best_action = scored[0]
    if best_action in {"TEST_SMALL", "TEST_OR_CONTINUE"}:
        print(
            f"ASSET_RECOMMENDATION asset={best_asset} action={best_action} size=20u "
            f"reason=best_current_quality score={fmt_decimal(best_score)}"
        )
    else:
        print(
            f"ASSET_RECOMMENDATION asset={best_asset} action=WAIT_OR_ROTATE "
            f"reason={best_item['suggestion']} score={fmt_decimal(best_score)}"
        )
    if len(scored) == 1:
        print("ASSET_RECOMMENDATION note=single_asset_log_use_all_runs_after_rotating_assets_for_comparison")


def print_run_scores(rows: list[dict[str, Any]]) -> None:
    print("== run_scores ==")
    by_run: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "")
        if not run_id:
            continue
        by_run.setdefault(run_id, []).append(row)
    scored: list[tuple[Decimal, str, str, dict[str, Any], str]] = []
    for run_id, run_rows in by_run.items():
        regimes = build_basis_regimes(run_rows)
        if not regimes:
            continue
        asset, item = max(regimes.items(), key=lambda pair: pair[1]["rows"])
        raw = item["raw_edge_p95"] or Decimal("0")
        norm = item["norm_edge_p95"] or Decimal("0")
        move = item["sample_move_p80"] or Decimal("99")
        rows_count = Decimal(str(item["rows"]))
        data_penalty = Decimal("0") if rows_count >= Decimal("1000") else Decimal("5")
        score = raw + (norm * Decimal("2")) - max(move - Decimal("4"), Decimal("0")) - data_penalty
        suggestion = str(item["suggestion"])
        scored.append((score, run_id, asset, item, suggestion))
    if not scored:
        print("RUN_SCORE action=WAIT reason=no_run_data")
        return
    scored.sort(reverse=True, key=lambda row: row[0])
    for rank, (score, run_id, asset, item, suggestion) in enumerate(scored, start=1):
        print(
            f"RUN_SCORE rank={rank} run_id={run_id} asset={asset} score={fmt_decimal(score)} "
            f"suggestion={suggestion} raw_p95={fmt_decimal(item['raw_edge_p95'])} "
            f"norm_p95={fmt_decimal(item['norm_edge_p95'])} move_p80={fmt_decimal(item['sample_move_p80'])} "
            f"rows={item['rows']} blocked={item['blocked_count']}"
        )
    best_score, best_run_id, best_asset, best_item, best_suggestion = scored[0]
    if best_suggestion in {"current_threshold_reasonable_or_test_min_abs_12", "test_small_only_min_abs_11_no_size_increase"}:
        action = "TEST_SMALL"
    else:
        action = "WAIT_OR_ROTATE"
    print(
        f"RUN_RECOMMENDATION asset={best_asset} run_id={best_run_id} action={action} "
        f"reason={best_suggestion} score={fmt_decimal(best_score)}"
    )


def file_size(path: Path) -> str:
    try:
        return human_bytes(path.stat().st_size)
    except FileNotFoundError:
        return "missing"


def parse_decimal_list(value: str, *, label: str) -> tuple[Decimal, ...]:
    values: list[Decimal] = []
    for token in str(value).split(","):
        token = token.strip()
        if not token:
            continue
        try:
            values.append(Decimal(token))
        except Exception as exc:
            raise ValueError(f"{label} contains invalid decimal: {token}") from exc
    if not values:
        raise ValueError(f"{label} must contain at least one decimal")
    return tuple(values)


def parse_positive_int_list(value: str, *, label: str) -> tuple[int, ...]:
    values: list[int] = []
    for token in str(value).split(","):
        token = token.strip()
        if not token:
            continue
        try:
            number = int(token)
        except ValueError as exc:
            raise ValueError(f"{label} contains invalid integer: {token}") from exc
        if number <= 0:
            raise ValueError(f"{label} values must be > 0")
        values.append(number)
    if not values:
        raise ValueError(f"{label} must contain at least one positive integer")
    return tuple(values)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze real live trading logs.")
    parser.add_argument(
        "--tail",
        type=int,
        default=5000,
        help=(
            "Maximum merged JSONL rows to inspect (hard cap 10000; each source "
            "also has a 2 MiB retained-byte limit). Default: 5000."
        ),
    )
    parser.add_argument("--include-rotated", action="store_true", help="Include rotated order_metrics.jsonl.N and .gz files.")
    parser.add_argument("--all-runs", action="store_true", help="Analyze all tailed rows instead of only the latest run_id.")
    parser.add_argument(
        "--data-cutoff-utc",
        help="Optional inclusive ISO-8601 UTC cutoff for reproducible offline reports.",
    )
    parser.add_argument(
        "--execution-calibration",
        action="store_true",
        help="Summarize versioned real execution-calibration cycles by asset and direction.",
    )
    parser.add_argument("--top", type=int, default=5, help="Number of blocked candidates/reasons to print. Default: 5.")
    parser.add_argument("--what-if", action="store_true", help="Replay blocked entries against looser threshold grids.")
    parser.add_argument("--basis-regime", action="store_true", help="Summarize recent basis/edge percentiles for strategy tuning.")
    parser.add_argument("--strategy", action="store_true", help="Print a concise live strategy recommendation.")
    parser.add_argument("--config-advice", action="store_true", help="Print recommended live_config.json changes, if any.")
    parser.add_argument("--asset-scores", action="store_true", help="Score assets seen in the selected log rows for rotation decisions.")
    parser.add_argument("--run-scores", action="store_true", help="Score each run_id separately for asset rotation decisions.")
    parser.add_argument("--dynamic-cost", action="store_true", help="Summarize current spread regime and dynamic roundtrip cost.")
    parser.add_argument("--ladder-what-if", action="store_true", help="Simulate the SOL 20U x 3 add-on strategy over selected rows.")
    parser.add_argument("--entry-semantics", action="store_true", help="Compare normalized-primary and raw-primary-plus-normalized-filter entry proxies.")
    parser.add_argument("--semantics-primary-threshold", default="7", help="Static primary edge threshold for --entry-semantics. Default: 7.")
    parser.add_argument("--semantics-min-abs-entry-bps", default="7", help="Static absolute basis threshold for --entry-semantics. Default: 7.")
    parser.add_argument("--semantics-min-normalized-filter-bps", default="1", help="Normalized filter threshold for --entry-semantics. Default: 1.")
    parser.add_argument("--basis-v2", action="store_true", help="Replay directional executable edges with strict time-aligned multiscale context.")
    parser.add_argument("--basis-v3", action="store_true", help="Replay independent multiscale quantile convergence episodes using executable prices.")
    parser.add_argument("--basis-v4", action="store_true", help="Replay extreme entries with executable net-PnL exits; read-only offline analysis.")
    parser.add_argument(
        "--basis-v4-stratified",
        action="store_true",
        help="Print the pre-registered p97.5 V4 candidate split by weekday, UTC block, and relative spread.",
    )
    parser.add_argument("--basis-v4-evaluation-interval-seconds", type=int, default=1, help="Evaluate V4 entries at most once per interval. Default: 1.")
    parser.add_argument("--basis-v3-evaluation-interval-seconds", type=int, default=60, help="Evaluate V3 entries at most once per interval. Default: 60.")
    parser.add_argument("--basis-v3-history-sample-seconds", type=int, default=30, help="Downsample prior quantile history to this interval. Default: 30.")
    parser.add_argument("--basis-v3-episode-cooldown-seconds", type=int, default=180, help="Cooldown after each independent V3 episode. Default: 180.")
    parser.add_argument("--basis-v3-max-hold-seconds", type=int, default=21600, help="Maximum V3 episode holding horizon. Default: 21600.")
    parser.add_argument("--basis-v3-max-sample-gap-seconds", type=int, default=60, help="Reject replay paths crossing a larger observation gap. Default: 60.")
    parser.add_argument("--basis-v3-min-window-coverage", default="0.80", help="Required elapsed coverage for a quantile window. Default: 0.80.")
    parser.add_argument("--basis-v3-min-history-samples", type=int, default=100, help="Minimum strictly-prior samples in a mature window. Default: 100.")
    parser.add_argument("--basis-v3-long-shortfall-reserve-bps", default="1.0", help="Unmodeled shortfall reserve for long-Var episodes. Default: 1.0.")
    parser.add_argument("--basis-v3-short-shortfall-reserve-bps", default="1.0", help="Unmodeled shortfall reserve for short-Var episodes. Default: 1.0.")
    parser.add_argument("--basis-v3-min-net-expected-bps", default="1.0", help="Minimum entry quantile distance after shortfall reserve. Default: 1.0.")
    parser.add_argument("--basis-v4-net-exit-target-bps", default="1.0", help="Required net executable PnL before V4 exits. Default: 1.0.")
    parser.add_argument("--basis-v3-holdout-fraction", default="0.30", help="Chronological V3 holdout fraction. Default: 0.30.")
    parser.add_argument("--basis-v3-min-independent-samples", type=int, default=5, help="Minimum train and holdout episodes for a bounded-live verdict. Default: 5.")
    parser.add_argument("--basis-v2-min-raw-edge-bps", default="7", help="Basis V2 raw directional edge floor. Default: 7.")
    parser.add_argument("--basis-v2-max-quote-age-ms", default="0", help="Optional Basis V2 Var quote age cap; 0 disables the filter.")
    parser.add_argument("--basis-v2-max-var-spread-bps", default="0", help="Optional Basis V2 Var spread cap; 0 disables the filter.")
    parser.add_argument("--basis-v2-max-lighter-spread-bps", default="0", help="Optional Basis V2 Lighter spread cap; 0 disables the filter.")
    parser.add_argument("--basis-v2-context-gap-bps", default="1", help="Short/long median separation used for context buckets. Default: 1.")
    parser.add_argument("--basis-v2-fresh-quote-ms", default="500", help="Quote age boundary for Basis V2 context buckets. Default: 500.")
    parser.add_argument("--basis-v2-filter-sweep", action="store_true", help="Sweep normalized edge and stablecoin share filters over the Basis V2 forward proxy.")
    parser.add_argument("--basis-v2-sweep-normalized-thresholds", default="0,1,1.5,2,2.5", help="Comma-separated normalized edge floors for --basis-v2-filter-sweep.")
    parser.add_argument("--basis-v2-sweep-stablecoin-share-thresholds", default="0.6,0.75,1.0", help="Comma-separated stablecoin edge share caps for --basis-v2-filter-sweep.")
    parser.add_argument("--basis-v2-sweep-min-reversion-deviation-bps", default="1.0", help="Prior 5m deviation floor for --basis-v2-filter-sweep. Default: 1.")
    parser.add_argument("--basis-v2-sweep-horizons", default="5,60,300", help="Comma-separated forward horizons for --basis-v2-filter-sweep. Default: 5,60,300.")
    parser.add_argument(
        "--basis-v2-sweep-event-cooldown-seconds",
        type=int,
        default=DEFAULT_FILTER_SWEEP_EVENT_COOLDOWN_SECONDS,
        help="Collapse repeated same-direction candidates into independent events. Default: 300.",
    )
    parser.add_argument(
        "--basis-v2-sweep-holdout-fraction",
        default=str(DEFAULT_FILTER_SWEEP_HOLDOUT_FRACTION),
        help="Chronological fraction reserved for out-of-sample validation. Default: 0.30.",
    )
    parser.add_argument(
        "--basis-v2-sweep-min-independent-samples",
        type=int,
        default=DEFAULT_FILTER_SWEEP_MIN_INDEPENDENT_SAMPLES,
        help="Minimum train and holdout event count before a setting is reviewable. Default: 30.",
    )
    parser.add_argument(
        "--basis-v2-sweep-execution-reserve-bps",
        default="0",
        help="Bps reserve subtracted from each forward proxy for unmodeled execution loss. Default: 0.",
    )
    parser.add_argument("--asset", help="Optional asset filter for dynamic-cost and ladder what-if sections.")
    parser.add_argument("--ladder-lot-notional-usd", default="20", help="What-if lot size. Default: 20.")
    parser.add_argument("--ladder-max-lots", type=int, default=3, help="What-if max open lots. Default: 3.")
    parser.add_argument("--ladder-addon-step-bps", default="2.0", help="Basis improvement required for each add-on. Default: 2.0.")
    parser.add_argument("--ladder-min-entry-edge-bps", default="7", help="Static floor for what-if entry edge. Default: 7.")
    parser.add_argument("--ladder-min-abs-entry-bps", default="7", help="Static floor for what-if absolute basis. Default: 7.")
    parser.add_argument("--ladder-min-normalized-entry-edge-bps", default="1.0", help="Minimum normalized edge for what-if. Default: 1.0.")
    parser.add_argument("--ladder-min-exit-pnl-bps", default="3.0", help="Minimum executable portfolio exit PnL. Default: 3.0.")
    parser.add_argument("--ladder-profit-take-pnl-bps", default="5.0", help="Portfolio profit-take PnL. Default: 5.0.")
    args = parser.parse_args()
    if args.tail <= 0:
        parser.error("--tail must be > 0")
    if args.tail > MAX_ANALYZE_TAIL_ROWS:
        parser.error(
            f"--tail must be <= {MAX_ANALYZE_TAIL_ROWS}; "
            "use a smaller tail and filter by --asset for bounded analysis"
        )
    if args.top <= 0:
        parser.error("--top must be > 0")
    if args.ladder_max_lots <= 0:
        parser.error("--ladder-max-lots must be > 0")
    try:
        data_cutoff = parse_time(args.data_cutoff_utc) if args.data_cutoff_utc else None
        if args.data_cutoff_utc and data_cutoff is None:
            raise ValueError("--data-cutoff-utc must be an ISO-8601 timestamp")
        ladder_lot_notional = Decimal(str(args.ladder_lot_notional_usd))
        ladder_addon_step = Decimal(str(args.ladder_addon_step_bps))
        ladder_min_entry = Decimal(str(args.ladder_min_entry_edge_bps))
        ladder_min_abs_entry = Decimal(str(args.ladder_min_abs_entry_bps))
        ladder_min_norm_entry = Decimal(str(args.ladder_min_normalized_entry_edge_bps))
        ladder_min_exit = Decimal(str(args.ladder_min_exit_pnl_bps))
        ladder_profit_take = Decimal(str(args.ladder_profit_take_pnl_bps))
        semantics_primary_threshold = Decimal(str(args.semantics_primary_threshold))
        semantics_min_abs_entry = Decimal(str(args.semantics_min_abs_entry_bps))
        semantics_min_normalized_filter = Decimal(str(args.semantics_min_normalized_filter_bps))
        basis_v2_min_raw_edge = Decimal(str(args.basis_v2_min_raw_edge_bps))
        basis_v2_max_quote_age = Decimal(str(args.basis_v2_max_quote_age_ms))
        basis_v2_max_var_spread = Decimal(str(args.basis_v2_max_var_spread_bps))
        basis_v2_max_lighter_spread = Decimal(str(args.basis_v2_max_lighter_spread_bps))
        basis_v2_context_gap = Decimal(str(args.basis_v2_context_gap_bps))
        basis_v2_fresh_quote = Decimal(str(args.basis_v2_fresh_quote_ms))
        basis_v2_sweep_min_reversion_deviation = Decimal(str(args.basis_v2_sweep_min_reversion_deviation_bps))
        basis_v2_sweep_holdout_fraction = Decimal(str(args.basis_v2_sweep_holdout_fraction))
        basis_v2_sweep_execution_reserve = Decimal(str(args.basis_v2_sweep_execution_reserve_bps))
        basis_v3_min_window_coverage = Decimal(str(args.basis_v3_min_window_coverage))
        basis_v3_long_shortfall_reserve = Decimal(str(args.basis_v3_long_shortfall_reserve_bps))
        basis_v3_short_shortfall_reserve = Decimal(str(args.basis_v3_short_shortfall_reserve_bps))
        basis_v3_min_net_expected = Decimal(str(args.basis_v3_min_net_expected_bps))
        basis_v4_net_exit_target = Decimal(str(args.basis_v4_net_exit_target_bps))
        basis_v3_holdout_fraction = Decimal(str(args.basis_v3_holdout_fraction))
        basis_v2_sweep_normalized_thresholds = parse_decimal_list(
            args.basis_v2_sweep_normalized_thresholds,
            label="--basis-v2-sweep-normalized-thresholds",
        )
        basis_v2_sweep_stablecoin_share_thresholds = parse_decimal_list(
            args.basis_v2_sweep_stablecoin_share_thresholds,
            label="--basis-v2-sweep-stablecoin-share-thresholds",
        )
        basis_v2_sweep_horizons = parse_positive_int_list(
            args.basis_v2_sweep_horizons,
            label="--basis-v2-sweep-horizons",
        )
    except Exception as exc:
        parser.error(f"invalid numeric or timestamp option: {exc}")
    if ladder_lot_notional <= 0 or ladder_addon_step <= 0:
        parser.error("ladder lot notional and addon step must be > 0")
    if semantics_primary_threshold <= 0 or semantics_min_abs_entry <= 0 or semantics_min_normalized_filter < 0:
        parser.error("entry semantics thresholds must be positive, except normalized filter may be 0")
    if (
        basis_v2_min_raw_edge < 0
        or basis_v2_max_quote_age < 0
        or basis_v2_max_var_spread < 0
        or basis_v2_max_lighter_spread < 0
        or basis_v2_context_gap <= 0
        or basis_v2_fresh_quote <= 0
        or basis_v2_sweep_min_reversion_deviation < 0
        or basis_v2_sweep_execution_reserve < 0
        or any(value < 0 for value in basis_v2_sweep_normalized_thresholds)
        or any(value < 0 for value in basis_v2_sweep_stablecoin_share_thresholds)
    ):
        parser.error("basis V2 thresholds must be non-negative, with positive context gap and fresh quote age")
    if args.basis_v2_sweep_event_cooldown_seconds <= 0:
        parser.error("--basis-v2-sweep-event-cooldown-seconds must be > 0")
    if not Decimal("0") < basis_v2_sweep_holdout_fraction < Decimal("1"):
        parser.error("--basis-v2-sweep-holdout-fraction must be between 0 and 1")
    if args.basis_v2_sweep_min_independent_samples <= 0:
        parser.error("--basis-v2-sweep-min-independent-samples must be > 0")
    if not Decimal("0") < basis_v3_min_window_coverage <= Decimal("1"):
        parser.error("--basis-v3-min-window-coverage must be between 0 and 1")
    if basis_v3_long_shortfall_reserve < 0 or basis_v3_short_shortfall_reserve < 0:
        parser.error("basis V3 shortfall reserves must be >= 0")
    if basis_v3_min_net_expected < 0:
        parser.error("--basis-v3-min-net-expected-bps must be >= 0")
    if basis_v4_net_exit_target < 0:
        parser.error("--basis-v4-net-exit-target-bps must be >= 0")
    if not Decimal("0") < basis_v3_holdout_fraction < Decimal("1"):
        parser.error("--basis-v3-holdout-fraction must be between 0 and 1")
    if (
        args.basis_v3_evaluation_interval_seconds <= 0
        or args.basis_v3_history_sample_seconds <= 0
        or args.basis_v3_episode_cooldown_seconds < 0
        or args.basis_v3_max_hold_seconds <= 0
        or args.basis_v3_max_sample_gap_seconds <= 0
        or args.basis_v3_min_history_samples <= 0
        or args.basis_v3_min_independent_samples <= 0
        or args.basis_v4_evaluation_interval_seconds <= 0
    ):
        parser.error("basis V3 intervals, history, hold, and sample counts must be positive; cooldown may be 0")

    source_paths = rotated_jsonl_paths(ORDER_METRICS) if args.include_rotated else [ORDER_METRICS]
    metrics_tail_stats: dict[str, int] = {}
    basis_tail_stats: dict[str, int] = {}
    if args.include_rotated:
        legacy_rows = tail_jsonl_many(
            source_paths,
            args.tail,
            max_bytes=ANALYZE_SOURCE_MAX_BYTES,
            max_line_bytes=JSONL_TAIL_MAX_LINE_BYTES,
            stats=metrics_tail_stats,
        )
    else:
        legacy_rows = tail_jsonl(
            ORDER_METRICS,
            args.tail,
            max_bytes=ANALYZE_SOURCE_MAX_BYTES,
            max_line_bytes=JSONL_TAIL_MAX_LINE_BYTES,
            stats=metrics_tail_stats,
        )
    collector_rows = read_basis_samples(
        BASIS_SAMPLES_DIR,
        limit=args.tail,
        asset_filter=args.asset,
        max_total_rows=args.tail,
        max_total_bytes=ANALYZE_SOURCE_MAX_BYTES,
        max_line_bytes=JSONL_TAIL_MAX_LINE_BYTES,
        stats=basis_tail_stats,
    )
    merged_rows = _deduplicate_sample_rows([*legacy_rows, *collector_rows])
    if data_cutoff is not None:
        merged_rows = [
            row
            for row in merged_rows
            if (logged_at := parse_time(row.get("logged_at"))) is None or logged_at <= data_cutoff
        ]
    raw_rows = merged_rows[-args.tail :]
    rows = raw_rows if args.all_runs else latest_run_filter(raw_rows)
    current_run_rows = latest_run_filter(raw_rows)
    state = read_json(LIVE_STATE)
    processes = running_main_processes()

    if args.execution_calibration:
        print_execution_calibration(rows)

    events = Counter(str(row.get("event") or "-") for row in rows)
    assets = Counter(str(row.get("asset") or "-").upper() for row in rows if row.get("asset"))
    sample_kinds = Counter(str(row.get("sample_kind") or "legacy") for row in rows if row.get("event") == "live_inventory_basis_state")
    sample_quality = Counter(str(row.get("sample_quality") or "legacy") for row in rows if row.get("event") == "live_inventory_basis_state")
    blocked_reasons = Counter(str(row.get("reason") or "unknown") for row in rows if row.get("event") == "live_inventory_entry_blocked")
    state_rows = basis_state_rows(rows)
    pre_gate_large_moves = sum(row.get("basis_sample_move_ok") is False for row in state_rows)
    qualified_large_move_blocks = sum(
        row.get("event") == "live_inventory_entry_blocked"
        and row.get("reason") == "basis_sample_move_too_large"
        and row.get("candidate_qualified") is True
        for row in rows
    )

    actual_pnl_bps: list[Decimal] = []
    actual_pnl_usd: list[Decimal] = []
    shortfalls: list[Decimal] = []
    entry_slippage: list[Decimal] = []
    exit_slippage: list[Decimal] = []
    all_sample_moves = _abs_values(_collect_decimal(basis_state_rows(rows), "basis_sample_move_bps"))
    blocked_sample_moves: list[Decimal] = []
    normalized_edges = [
        value
        for row in basis_state_rows(rows)
        for key in ("normalized_long_edge_bps", "normalized_short_edge_bps")
        if (value := to_decimal(row.get(key))) is not None
    ]
    blocked_scores: list[tuple[Decimal, str, str, str]] = []
    access_restricted = 0
    current_run_access_restricted = 0

    for row in rows:
        text = str(row)
        if "Access Restricted" in text:
            access_restricted += 1
        event = row.get("event")
        if event == "live_inventory_actual_pnl":
            if (value := to_decimal(row.get("actual_pnl_bps"))) is not None:
                actual_pnl_bps.append(value)
            if (value := to_decimal(row.get("actual_pnl_usd"))) is not None:
                actual_pnl_usd.append(value)
            if (value := to_decimal(row.get("estimated_vs_actual_pnl_shortfall_bps"))) is not None:
                shortfalls.append(value)
            if (value := to_decimal(row.get("entry_lighter_slippage_bps"))) is not None:
                entry_slippage.append(value)
            if (value := to_decimal(row.get("exit_lighter_slippage_bps"))) is not None:
                exit_slippage.append(value)
        if event == "live_inventory_entry_blocked":
            if (move := to_decimal(row.get("basis_sample_move_bps"))) is not None:
                blocked_sample_moves.append(abs(move))
            score, direction = best_entry_score(row, Decimal("5.5"), Decimal("0.5"))
            if score is not None:
                blocked_scores.append((score, str(row.get("asset") or "-").upper(), direction, str(row.get("reason") or "unknown")))
    for row in current_run_rows:
        if "Access Restricted" in str(row):
            current_run_access_restricted += 1
    strategy_access_restricted = current_run_access_restricted if args.all_runs else access_restricted

    latest_at = "-"
    latest_dt = None
    for row in reversed(rows):
        latest_dt = parse_time(row.get("logged_at"))
        if latest_dt is not None:
            latest_at = latest_dt.isoformat()
            break
    age = "-"
    if latest_dt is not None:
        age = f"{(datetime.now(timezone.utc) - latest_dt).total_seconds():.0f}s"

    status = str(state.get("status") or ("missing" if not state else "unknown"))
    state_asset = str(state.get("asset") or "-").upper()
    open_lots = state.get("open_lots") or []
    pending_actions = state.get("pending_actions") or []
    if status == "flat" and open_lots:
        status = "open"
    elif status == "flat" and pending_actions:
        status = "pending"
    maintenance_control = read_json(LIVE_CONTROL)

    print("== live ==")
    if data_cutoff is not None:
        print(f"data_cutoff_utc={data_cutoff.isoformat()}")
    print(f"process={'YES' if processes else 'NO'}")
    for process in processes:
        print(f"process_detail={process}")
    print(f"state={status} asset={state_asset} open_lots={len(open_lots)} pending_actions={len(pending_actions)}")
    print(
        "maintenance_drain_status="
        f"{maintenance_control.get('status') or 'not_requested'} "
        f"target_pid={maintenance_control.get('target_pid') or '-'} "
        f"requested_at={maintenance_control.get('requested_at') or '-'} "
        f"completed_at={maintenance_control.get('completed_at') or '-'}"
    )
    print(f"logs order_metrics={file_size(ORDER_METRICS)} runtime={file_size(RUNTIME_LOG)} log_dir={human_bytes(sum(p.stat().st_size for p in LOG_DIR.rglob('*') if p.is_file())) if LOG_DIR.exists() else 'missing'}")
    collector_health = read_json(BASIS_SAMPLES_DIR / "health.json")
    if collector_health:
        print(
            f"collector run_id={collector_health.get('run_id')} assets={collector_health.get('assets')} "
            f"disk_free_gb={collector_health.get('disk_free_gb')} extension_failures={collector_health.get('extension_consecutive_failures')}"
        )
    print(f"rows={len(rows)}/{len(raw_rows)} latest_at={latest_at} latest_age={age}")
    analysis_truncated = bool(
        metrics_tail_stats.get("evicted_rows", 0)
        or basis_tail_stats.get("evicted_rows", 0)
        or metrics_tail_stats.get("oversized_lines", 0)
        or basis_tail_stats.get("oversized_lines", 0)
    )
    print(
        "analysis_coverage="
        f"first_at={raw_rows[0].get('logged_at') if raw_rows else '-'} "
        f"last_at={raw_rows[-1].get('logged_at') if raw_rows else '-'} "
        f"truncated={analysis_truncated} "
        "historical_counts_incomplete="
        f"{analysis_truncated}; use tools/profitability_audit.py for a full window"
    )
    print(
        "analysis_input_bounds="
        f"requested_rows={args.tail} max_rows={MAX_ANALYZE_TAIL_ROWS} "
        f"metrics_retained_rows={metrics_tail_stats.get('retained_rows', 0)} "
        f"metrics_retained_bytes={metrics_tail_stats.get('retained_bytes', 0)} "
        f"metrics_evicted_rows={metrics_tail_stats.get('evicted_rows', 0)} "
        f"metrics_oversized_lines={metrics_tail_stats.get('oversized_lines', 0)} "
        f"basis_retained_rows={len(collector_rows)} "
        f"basis_retained_bytes={basis_tail_stats.get('retained_bytes', 0)} "
        f"basis_evicted_rows={basis_tail_stats.get('evicted_rows', 0)} "
        f"basis_oversized_lines={basis_tail_stats.get('oversized_lines', 0)} "
        f"basis_byte_limit={ANALYZE_SOURCE_MAX_BYTES}"
    )

    print("== events ==")
    print(
        " ".join(
            f"{name}={events[name]}"
            for name in [
                "live_inventory_entered",
                "live_inventory_exited",
                "live_inventory_actual_pnl",
                "live_inventory_entry_blocked",
                "live_inventory_manual_review_required",
                "live_inventory_basis_quote_failed",
                "live_inventory_runtime_fuse_triggered",
                "live_inventory_runtime_stopped",
                "lighter_filled",
                "variational_filled",
            ]
        )
    )
    print(f"assets={dict(assets.most_common())}")
    print(f"sample_kinds={dict(sample_kinds.most_common())} sample_quality={dict(sample_quality.most_common())}")
    print(f"blocked_reasons={dict(blocked_reasons.most_common(args.top))}")
    print(
        f"large_move_pre_gate_samples={pre_gate_large_moves} "
        f"qualified_large_move_blocks={qualified_large_move_blocks}"
    )
    print(f"access_restricted_rows={access_restricted}")
    if args.all_runs:
        print(f"access_restricted_rows_current_run={current_run_access_restricted}")

    print("== pnl ==")
    print(
        f"actual_n={len(actual_pnl_bps)} avg_bps={fmt_decimal(avg(actual_pnl_bps))} "
        f"p50_bps={fmt_decimal(percentile(actual_pnl_bps, Decimal('50')))} "
        f"sum_usd={fmt_decimal(sum(actual_pnl_usd) if actual_pnl_usd else None, '0.0001')}"
    )
    print(
        f"shortfall_n={len(shortfalls)} avg={fmt_decimal(avg(shortfalls))} "
        f"p80={fmt_decimal(percentile(shortfalls, Decimal('80')))} "
        f"entry_slip_p80={fmt_decimal(percentile(entry_slippage, Decimal('80')))} "
        f"exit_slip_p80={fmt_decimal(percentile(exit_slippage, Decimal('80')))}"
    )

    print("== signal_quality ==")
    latest_signal_row = next((row for row in reversed(rows) if row.get("event") == "live_inventory_basis_state"), None)
    print(
        f"all_sample_move_p50={fmt_decimal(percentile(all_sample_moves, Decimal('50')))} "
        f"all_sample_move_p80={fmt_decimal(percentile(all_sample_moves, Decimal('80')))} "
        f"blocked_event_move_p80={fmt_decimal(percentile(blocked_sample_moves, Decimal('80')))} "
        f"normalized_edge_p80={fmt_decimal(percentile(normalized_edges, Decimal('80')))} "
        f"latest_max_sample_move={fmt_decimal(to_decimal(latest_signal_row.get('basis_max_sample_move_bps')) if latest_signal_row else None)} "
        f"latest_sample_move_p80={fmt_decimal(to_decimal(latest_signal_row.get('basis_sample_move_p80_bps')) if latest_signal_row else None)}"
    )
    for index, (score, asset, direction, reason) in enumerate(sorted(blocked_scores, reverse=True)[: args.top], start=1):
        print(f"blocked_observation_{index}=asset={asset} dir={direction} score={fmt_decimal(score)} reason={reason}")
    print_v4_live_funnel(rows)
    if args.what_if:
        print_what_if(rows)
    if args.basis_regime:
        print_basis_regime(rows)
    if args.strategy:
        print_strategy(rows, events, status, open_lots, pending_actions, strategy_access_restricted)
    if args.config_advice:
        print_config_advice(rows)
    if args.asset_scores:
        print_asset_scores(rows)
    if args.run_scores:
        print_run_scores(rows)
    if args.dynamic_cost:
        print_dynamic_cost(rows, asset=args.asset)
    if args.ladder_what_if:
        print_ladder_what_if(
            rows,
            asset=args.asset,
            lot_notional=ladder_lot_notional,
            max_lots=args.ladder_max_lots,
            addon_step=ladder_addon_step,
            min_entry_edge=ladder_min_entry,
            min_abs_entry=ladder_min_abs_entry,
            min_norm_entry=ladder_min_norm_entry,
            min_exit_pnl=ladder_min_exit,
            profit_take_pnl=ladder_profit_take,
        )
    if args.entry_semantics:
        print_entry_semantics(
            rows,
            primary_threshold=semantics_primary_threshold,
            min_abs_entry=semantics_min_abs_entry,
            min_normalized_filter=semantics_min_normalized_filter,
        )
    if args.basis_v2:
        print_basis_v2(
            rows,
            asset=args.asset,
            min_raw_edge_bps=basis_v2_min_raw_edge,
            max_quote_age_ms=basis_v2_max_quote_age,
            max_var_spread_bps=basis_v2_max_var_spread,
            max_lighter_spread_bps=basis_v2_max_lighter_spread,
            context_gap_bps=basis_v2_context_gap,
            fresh_quote_ms=basis_v2_fresh_quote,
        )
    if args.basis_v3:
        print_basis_v3(
            rows,
            asset=args.asset,
            evaluation_interval_seconds=args.basis_v3_evaluation_interval_seconds,
            history_sample_seconds=args.basis_v3_history_sample_seconds,
            episode_cooldown_seconds=args.basis_v3_episode_cooldown_seconds,
            max_hold_seconds=args.basis_v3_max_hold_seconds,
            max_sample_gap_seconds=args.basis_v3_max_sample_gap_seconds,
            min_window_coverage=basis_v3_min_window_coverage,
            min_history_samples=args.basis_v3_min_history_samples,
            long_shortfall_reserve_bps=basis_v3_long_shortfall_reserve,
            short_shortfall_reserve_bps=basis_v3_short_shortfall_reserve,
            min_net_expected_bps=basis_v3_min_net_expected,
            holdout_fraction=basis_v3_holdout_fraction,
            min_independent_samples=args.basis_v3_min_independent_samples,
        )
    if args.basis_v4:
        print_basis_v4(
            rows,
            asset=args.asset,
            evaluation_interval_seconds=args.basis_v4_evaluation_interval_seconds,
            history_sample_seconds=args.basis_v3_history_sample_seconds,
            episode_cooldown_seconds=args.basis_v3_episode_cooldown_seconds,
            max_hold_seconds=args.basis_v3_max_hold_seconds,
            max_sample_gap_seconds=args.basis_v3_max_sample_gap_seconds,
            min_window_coverage=basis_v3_min_window_coverage,
            min_history_samples=args.basis_v3_min_history_samples,
            long_shortfall_reserve_bps=basis_v3_long_shortfall_reserve,
            short_shortfall_reserve_bps=basis_v3_short_shortfall_reserve,
            net_exit_target_bps=basis_v4_net_exit_target,
            holdout_fraction=basis_v3_holdout_fraction,
            min_independent_samples=args.basis_v3_min_independent_samples,
        )
    if args.basis_v4_stratified:
        print_basis_v4_stratified(
            rows,
            asset=args.asset,
            evaluation_interval_seconds=args.basis_v4_evaluation_interval_seconds,
            history_sample_seconds=args.basis_v3_history_sample_seconds,
            episode_cooldown_seconds=args.basis_v3_episode_cooldown_seconds,
            max_hold_seconds=args.basis_v3_max_hold_seconds,
            max_sample_gap_seconds=args.basis_v3_max_sample_gap_seconds,
            min_window_coverage=basis_v3_min_window_coverage,
            min_history_samples=args.basis_v3_min_history_samples,
            long_shortfall_reserve_bps=basis_v3_long_shortfall_reserve,
            short_shortfall_reserve_bps=basis_v3_short_shortfall_reserve,
            net_exit_target_bps=basis_v4_net_exit_target,
            holdout_fraction=basis_v3_holdout_fraction,
            min_independent_samples=args.basis_v3_min_independent_samples,
        )
    if args.basis_v2_filter_sweep:
        print_basis_v2_filter_sweep(
            rows,
            asset=args.asset,
            min_raw_edge_bps=basis_v2_min_raw_edge,
            max_quote_age_ms=basis_v2_max_quote_age,
            max_var_spread_bps=basis_v2_max_var_spread,
            max_lighter_spread_bps=basis_v2_max_lighter_spread,
            context_gap_bps=basis_v2_context_gap,
            fresh_quote_ms=basis_v2_fresh_quote,
            normalized_thresholds=basis_v2_sweep_normalized_thresholds,
            stablecoin_share_thresholds=basis_v2_sweep_stablecoin_share_thresholds,
            min_reversion_deviation_bps=basis_v2_sweep_min_reversion_deviation,
            horizons=basis_v2_sweep_horizons,
            event_cooldown_seconds=args.basis_v2_sweep_event_cooldown_seconds,
            holdout_fraction=basis_v2_sweep_holdout_fraction,
            min_independent_samples=args.basis_v2_sweep_min_independent_samples,
            execution_reserve_bps=basis_v2_sweep_execution_reserve,
        )

    operational_readiness = "unknown"
    if events["live_inventory_manual_review_required"] or status == "manual_review_required":
        operational_readiness = "manual_review_required_check_both_exchanges"
    elif status == "missing":
        operational_readiness = "state_missing_manual_exchange_flat_confirmation_required_before_start"
    elif status != "flat" or open_lots or pending_actions:
        operational_readiness = "not_ready_state_not_flat"
    elif strategy_access_restricted:
        operational_readiness = "not_ready_access_restricted_detected"
    elif not processes:
        operational_readiness = "flat_manual_exchange_confirmation_required"
    else:
        operational_readiness = "live_process_running"
    print(f"operational_readiness={operational_readiness}")

    runtime_tail = tail_text(COLLECTOR_LOG, 5) if collector_health else tail_text(RUNTIME_LOG, 5)
    if runtime_tail:
        print("== collector_tail ==" if collector_health else "== runtime_tail ==")
        for line in runtime_tail:
            print(bounded_diagnostic_line(line))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
