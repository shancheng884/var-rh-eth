from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

from tools.lib.risk_alert_control import notifications_allowed, read_alert_control
from tools.lib.alert_localization import action_cn, reason_cn


ALERT_CONTROL_PATH = Path(__file__).resolve().parents[2] / "log" / "risk_wakeup_alert_control.json"


TELEGRAM_EVENT_TYPES = {
    "live_inventory_entered",
    "live_inventory_exited",
    "live_inventory_final_pnl",
    "live_inventory_entry_blocked",
    "live_inventory_v4_entry_blocked",
    "live_inventory_exit_blocked",
    "live_inventory_manual_review_required",
    "live_inventory_loss_review_alert",
    "live_inventory_runtime_fuse_triggered",
    "live_inventory_basis_quote_failed",
    "live_inventory_v4_strong_single_auto_disabled",
    "live_inventory_account_snapshot",
    "live_inventory_pnl_summary",
    "live_inventory_account_risk_alert",
    "live_inventory_account_risk_recovered",
    "live_inventory_maintenance_drain_requested",
    "live_inventory_maintenance_drain_blocked",
    "live_inventory_maintenance_drain_completed",
}

TELEGRAM_CRITICAL_EXIT_BLOCK_REASONS = {
    "entry_final_fill_cost_pending",
}

TELEGRAM_THROTTLED_EVENT_TYPES = {
    "live_inventory_entry_blocked",
    "live_inventory_v4_entry_blocked",
    "live_inventory_basis_quote_failed",
    "live_inventory_exit_blocked",
    "live_inventory_account_risk_alert",
    "live_inventory_maintenance_drain_blocked",
}


def _value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    blocked_context = payload.get("blocked_context")
    if isinstance(blocked_context, dict):
        for key in keys:
            value = blocked_context.get(key)
            if value not in (None, ""):
                return value
    return "-"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _localized_number(value: Any, *, places: int) -> str | None:
    if value in (None, "", "-"):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return str(value)
    return f"{number:.{places}f}".rstrip("0").rstrip(".")


def _localized_money(payload: dict[str, Any], key: str) -> str:
    value = _localized_number(_value(payload, key), places=6)
    return f"{value} U" if value is not None else "暂不可用"


def _localized_percent(payload: dict[str, Any], key: str) -> str:
    value = _localized_number(_value(payload, key), places=4)
    return f"{value}%" if value is not None else "暂不可用"


def _direction_cn(value: Any) -> str:
    return {
        "short_var_long_lighter": "做空 Variational / 做多 Lighter",
        "long_var_short_lighter": "做多 Variational / 做空 Lighter",
    }.get(str(value), str(value))


def _reason_cn(value: Any) -> str:
    return reason_cn(value)


def _action_cn(value: Any) -> str:
    return action_cn(value)


def _venue_name(value: Any) -> str:
    return {
        "variational": "Variational",
        "lighter": "Lighter",
    }.get(str(value).strip().lower(), str(value))


def _stage_cn(value: Any) -> str:
    return {
        "startup_flat": "启动空仓",
        "entry_confirmed": "开仓确认",
        "exit_confirmed_flat": "平仓确认",
        "under_6h": "持仓不足 6 小时",
        "6h_to_12h": "持仓 6 至 12 小时",
        "12h_to_24h": "持仓 12 至 24 小时",
        "after_24h": "持仓超过 24 小时",
    }.get(str(value), str(value))


def _status_cn(value: Any) -> str:
    return {
        "complete": "完整",
        "partial": "部分数据",
        "var_and_lighter_final_fills_confirmed": "双边最终成交已确认",
        "lighter_final_fill_confirmed": "Lighter 最终成交已确认",
        "pending_lighter_final_fill": "等待 Lighter 最终成交",
    }.get(str(value), str(value))


def format_telegram_trade_message(
    event_type: str,
    payload: dict[str, Any],
) -> str:
    asset = _value(payload, "asset")
    direction = _value(payload, "direction")
    run_id = _value(payload, "run_id")
    lot_id = _value(payload, "lot_id", "cycle_id")

    if event_type == "live_inventory_entered":
        return "\n".join(
            [
                "[Var/Lighter] 开仓/加仓确认",
                f"资产：{asset}｜方向：{_direction_cn(direction)}",
                "当前档位："
                f"{_value(payload, 'gradient_tier', 'entry_gradient_tier')} / 5",
                "已开子单："
                f"{_value(payload, 'open_child_lots', 'open_lots_total')} / "
                f"{_value(payload, 'gradient_capacity_child_lots', 'entry_gradient_capacity_child_lots')}",
                f"本笔数量：{_value(payload, 'qty')}｜批次：{lot_id}",
                f"开仓价差：{_value(payload, 'edge_bps')} bps",
                "单边总仓位："
                f"{_value(payload, 'open_notional_usd', 'current_notional_usd', 'proposed_notional_usd')} U",
                f"Variational 权益：{_value(payload, 'variational_equity_usd')} U",
                f"Lighter 权益：{_value(payload, 'lighter_equity_usd')} U",
                "Variational 保证金使用率："
                f"{_value(payload, 'variational_maintenance_margin_usage_pct')}%",
                "Lighter 保证金使用率："
                f"{_value(payload, 'lighter_maintenance_margin_usage_pct')}%",
                "最高单边预计杠杆："
                f"{_value(payload, 'max_projected_venue_leverage')}x / 5x",
            ]
        )
    if event_type == "live_inventory_exited":
        return "\n".join(
            [
                "[Var/Lighter] 平仓确认",
                f"资产：{asset}｜方向：{_direction_cn(direction)}",
                f"数量：{_value(payload, 'qty')}｜批次：{lot_id}",
                f"原因：{_reason_cn(_value(payload, 'exit_reason'))}",
                f"预计盈亏：{_value(payload, 'pnl_usd')} U",
                f"预计收益：{_value(payload, 'pnl_bps')} bps",
                f"持仓时间：{_value(payload, 'holding_seconds')} 秒",
            ]
        )
    if event_type == "live_inventory_final_pnl":
        return "\n".join(
            [
                "[Var/Lighter] 档位平仓完成",
                f"资产：{asset}｜平仓档位：{_value(payload, 'exit_gradient_tier')} / 5",
                "本次平仓子单："
                f"{_value(payload, 'portfolio_component_lot_count')}｜"
                f"剩余子单：{_value(payload, 'remaining_child_lots')}",
                f"实际盈亏：{_value(payload, 'final_pnl_usd')} U",
                f"实际收益：{_value(payload, 'final_pnl_bps')} bps",
                f"当前市场档位：{_value(payload, 'market_gradient_tier')} / 5",
                f"Variational 权益：{_value(payload, 'variational_equity_usd')} U",
                f"Lighter 权益：{_value(payload, 'lighter_equity_usd')} U",
                "Variational 保证金使用率："
                f"{_value(payload, 'variational_margin_usage_pct')}%",
                "Lighter 保证金使用率："
                f"{_value(payload, 'lighter_margin_usage_pct')}%",
                f"原因：{_reason_cn(_value(payload, 'exit_reason'))}",
            ]
        )
    if event_type == "live_inventory_account_snapshot":
        return "\n".join(
            [
                "[Var/Lighter] 账户权益快照",
                f"资产：{asset}｜阶段：{_stage_cn(_value(payload, 'snapshot_stage'))}",
                f"批次：{lot_id}｜状态：{_status_cn(_value(payload, 'snapshot_status'))}",
                f"Variational 权益：{_value(payload, 'variational_equity_usd')} U",
                f"Lighter 权益：{_value(payload, 'lighter_equity_usd')} U",
                f"双边总权益：{_value(payload, 'combined_equity_usd')} U",
                f"双边权益平衡度：{_value(payload, 'equity_balance_ratio')}",
            ]
        )
    if event_type == "live_inventory_maintenance_drain_requested":
        return "\n".join(
            [
                "[Var/Lighter] 维护排空已启动",
                f"资产：{asset}",
                "动作：已禁止新开仓和加仓",
                "现有仓位：继续按各档原盈利条件管理和平仓",
                f"当前未平子单：{_value(payload, 'open_lots_total')}",
                f"待确认动作：{_value(payload, 'pending_actions_total')}",
                f"运行编号：{run_id}",
            ]
        )
    if event_type == "live_inventory_maintenance_drain_blocked":
        return "\n".join(
            [
                "[Var/Lighter] 维护排空等待确认",
                f"资产：{asset}",
                f"原因：{_reason_cn(_value(payload, 'reason'))}",
                "动作：继续禁止开仓并自动重试",
                "Variational 仓位："
                f"{_value(payload, 'variational_position_qty')}",
                f"Lighter 仓位：{_value(payload, 'lighter_position_qty')}",
                f"运行编号：{run_id}",
            ]
        )
    if event_type == "live_inventory_maintenance_drain_completed":
        return "\n".join(
            [
                "[Var/Lighter] 维护排空完成",
                f"资产：{asset}",
                "双边仓位：已确认全部为空",
                "策略状态：已自动停止，可以安全更新并重启",
                "Variational 仓位："
                f"{_value(payload, 'variational_position_qty')}",
                f"Lighter 仓位：{_value(payload, 'lighter_position_qty')}",
                f"运行编号：{run_id}",
            ]
        )
    if event_type == "live_inventory_pnl_summary":
        if payload.get("summary_scope") == "realized_platform_activity":
            status = "完整" if payload.get("summary_status") == "complete" else "部分时段"
            lines = [
                "[Var/RH] 北京时间每日收益",
                f"日期：{_value(payload, 'beijing_day')}｜资产：{asset}｜状态：{status}",
                f"双平台成交笔数：{_value(payload, 'daily_trade_count')} 笔（开仓、平仓均计）",
                "双平台总成交量："
                f"{_localized_money(payload, 'daily_volume_usd')}",
                "当日平仓收益："
                f"{_localized_money(payload, 'daily_realized_pnl_usd')}",
                "当日资金费："
                f"{_localized_money(payload, 'daily_funding_usd')}",
                "当日双平台总盈亏："
                f"{_localized_money(payload, 'daily_net_pnl_usd')}",
                f"累计双平台总成交量：{_localized_money(payload, 'cumulative_volume_usd')}",
                f"累计平仓收益：{_localized_money(payload, 'cumulative_realized_pnl_usd')}",
                f"累计资金费：{_localized_money(payload, 'cumulative_funding_usd')}",
                f"累计双平台总盈亏：{_localized_money(payload, 'cumulative_net_pnl_usd')}",
                f"累计简单年化：{_localized_percent(payload, 'annualized_simple_pct')}",
                f"统计起始日：{_value(payload, 'statistics_start_day')}",
                f"统计本金（按净充提调整）：{_localized_money(payload, 'capital_usd')}",
                f"期初本金：{_localized_money(payload, 'initial_capital_usd')}",
                f"统计期间净充提：{_localized_money(payload, 'external_cashflow_usd')}",
                f"最新双边权益：{_localized_money(payload, 'combined_equity_usd')}",
                f"Variational 权益：{_localized_money(payload, 'variational_equity_usd')}",
                f"RH 权益：{_localized_money(payload, 'lighter_equity_usd')}",
                f"权益快照：{_value(payload, 'account_snapshot_at')}",
                "口径：两边平台已实现平仓盈亏＋已结算资金费；手续费按当前0计；未平仓浮动盈亏不计",
                "说明：手动成交计入；充值提现单独记账并调整统计本金",
            ]
            if status != "完整":
                detail = payload.get("source_detail") or {}
                reason_labels = {
                    "variational_export_missing_or_incomplete": "Variational历史CSV缺失或解析不完整",
                    "variational_export_stale_or_timestamp_missing": "Variational导出超过30小时或时间缺失",
                    "variational_export_may_be_truncated": "Variational导出可能达到10000行上限",
                    "variational_coverage_not_confirmed_or_export_changed": "Variational导出覆盖范围未确认或文件有变化",
                    "variational_coverage_does_not_cover_report_period": "Variational导出未覆盖统计区间",
                    "variational_rows_unclassified": "存在未识别的Variational流水",
                    "rh_history_sync_or_pagination_incomplete": "RH历史同步或分页不完整",
                    "rh_rows_unclassified": "存在未识别的RH流水",
                    "rh_nonzero_fees_need_currency_normalization": "RH发现非零手续费，尚未完成币种换算",
                }
                reasons = [
                    reason_labels.get(str(reason), str(reason))
                    for reason in detail.get("reasons", [])
                ]
                lines.append("数据未完整原因：" + ("；".join(reasons) or "来源检查未通过"))
            return "\n".join(lines)
        if payload.get("summary_scope") == "realized_balance_daily":
            status = {
                "complete": "完整日",
                "partial": "部分时段",
                "unavailable": "数据不足",
            }.get(str(_value(payload, "summary_status")), _value(payload, "summary_status"))
            lines = [
                "[Var/RH] 北京时间每日收益",
                f"日期：{_value(payload, 'beijing_day')}｜资产：{asset}｜状态：{status}",
                f"今日平台成交笔数：{_value(payload, 'daily_trade_count')} 笔（开仓、平仓子单均计）",
                f"今日平台总成交量：{_localized_money(payload, 'daily_volume_usd')}",
                f"今日双平台总盈亏：{_localized_money(payload, 'daily_net_pnl_usd')}",
                f"累计双平台成交笔数：{_value(payload, 'cumulative_trade_count')} 笔",
                f"累计双平台总成交量：{_localized_money(payload, 'cumulative_volume_usd')}",
                f"累计双平台总盈亏：{_localized_money(payload, 'cumulative_net_pnl_usd')}",
                f"累计简单年化：{_localized_percent(payload, 'annualized_simple_pct')}",
                f"统计起始日：{_value(payload, 'statistics_start_day')}",
                f"Variational 权益：{_localized_money(payload, 'variational_equity_usd')}",
                f"RH 权益：{_localized_money(payload, 'lighter_equity_usd')}",
            ]
            return "\n".join(lines)
        if payload.get("summary_scope") == "account_equity_daily":
            status = {
                "complete": "完整日",
                "partial": "部分时段",
                "unavailable": "数据不足",
            }.get(str(_value(payload, "summary_status")), _value(payload, "summary_status"))
            lines = [
                "[Var/RH] 北京时间每日收益",
                f"日期：{_value(payload, 'beijing_day')}｜资产：{asset}｜状态：{status}",
                f"程序记录平仓子单：{_value(payload, 'daily_closed_child_lots')} 笔｜"
                f"平仓批次：{_value(payload, 'daily_completed_close_groups')} 组",
                "程序记录四腿成交量："
                f"{_localized_money(payload, 'daily_four_leg_volume_usd')}",
                "程序记录平仓盈亏（辅助）："
                f"{_localized_money(payload, 'daily_confirmed_pnl_usd')}",
                "当日双边账户总收益（含未实现）："
                f"{_localized_money(payload, 'beijing_day_actual_pnl_usd')}",
            ]
            if payload.get("summary_status") == "complete":
                lines.extend(
                    [
                        "当日收益率："
                        f"{_localized_percent(payload, 'beijing_day_return_pct')}",
                        "当日简单年化："
                        f"{_localized_percent(payload, 'daily_annualized_simple_pct')}",
                    ]
                )
            elif payload.get("observed_period_change_usd") is not None:
                lines.append(
                    "已采样时段权益变化（非全天收益）："
                    f"{_localized_money(payload, 'observed_period_change_usd')}"
                )
            lines.extend(
                [
                    "当日权益采样："
                    f"{_value(payload, 'daily_first_sample_at')} 至 "
                    f"{_value(payload, 'daily_latest_sample_at')}",
                    "累计程序记录平仓盈亏（辅助）："
                    f"{_localized_money(payload, 'cumulative_confirmed_pnl_usd')}",
                    "累计程序记录四腿成交量（辅助）："
                    f"{_localized_money(payload, 'cumulative_four_leg_volume_usd')}",
                    "统计期双边账户总收益（含未实现）："
                    f"{_localized_money(payload, 'run_actual_pnl_usd')}",
                    "累计账户收益率："
                    f"{_localized_percent(payload, 'return_pct')}",
                    "累计简单年化："
                    f"{_localized_percent(payload, 'annualized_simple_pct')}",
                    f"统计起始日：{_value(payload, 'account_baseline_day')}",
                    f"统计起始权益快照：{_value(payload, 'account_baseline_at')}",
                    f"期初双边权益：{_localized_money(payload, 'tracking_start_equity_usd')}",
                    f"统计本金（按已登记净充提调整）：{_localized_money(payload, 'capital_usd')}",
                    f"统计期间已登记净充提：{_localized_money(payload, 'external_cashflow_usd')}",
                    f"最新双边权益：{_localized_money(payload, 'combined_equity_usd')}",
                    f"Variational 权益：{_localized_money(payload, 'variational_equity_usd')}",
                    f"RH 权益：{_localized_money(payload, 'lighter_equity_usd')}",
                    f"快照时间：{_value(payload, 'account_snapshot_at')}",
                    "口径：双边账户权益净变化扣除已登记净充提；资金费已反映在权益中",
                    "说明：账户收益含未平仓浮盈亏和手动成交；充提需登记；成交统计为程序记录辅助项",
                ]
            )
            return "\n".join(lines)
        if payload.get("summary_scope") == "beijing_daily":
            return "\n".join(
                [
                    "[Var/Lighter] 北京时间每日收益",
                    f"日期：{_value(payload, 'beijing_day')}｜资产：{asset}",
                    "平仓子单："
                    f"{_value(payload, 'daily_closed_child_lots')} 笔｜"
                    "平仓批次："
                    f"{_value(payload, 'daily_completed_close_groups')} 组",
                    "双平台四腿成交量："
                    f"{_localized_money(payload, 'daily_four_leg_volume_usd')}",
                    "确认成交盈亏："
                    f"{_localized_money(payload, 'beijing_day_actual_pnl_usd')}",
                    "当日收益率："
                    f"{_localized_percent(payload, 'beijing_day_return_pct')}",
                    "当日简单年化："
                    f"{_localized_percent(payload, 'daily_annualized_simple_pct')}",
                    "累计确认盈亏："
                    f"{_localized_money(payload, 'run_actual_pnl_usd')}",
                    "累计四腿成交量："
                    f"{_localized_money(payload, 'cumulative_four_leg_volume_usd')}",
                    "累计收益率："
                    f"{_localized_percent(payload, 'return_pct')}",
                    "累计简单年化："
                    f"{_localized_percent(payload, 'annualized_simple_pct')}",
                    f"统计本金：{_localized_money(payload, 'capital_usd')}",
                    "最新双边权益："
                    f"{_localized_money(payload, 'combined_equity_usd')}",
                    "口径：双边最终成交价；手续费和资金费未单独计入",
                    "说明：单日及短周期年化仅供观察",
                ]
            )
        status = {
            "complete": "完整",
            "partial": "部分数据",
            "test": "测试",
        }.get(str(_value(payload, "summary_status")), _value(payload, "summary_status"))
        return_source = {
            "account_equity_delta": "账户权益净变化",
            "confirmed_pair_fills": "已确认双边成交盈亏",
            "beijing_daily_confirmed_pair_fills": (
                "北京时间当日已确认双边成交盈亏"
            ),
        }.get(
            str(_value(payload, "return_pnl_source")),
            _value(payload, "return_pnl_source"),
        )
        reliability = {
            "sample_under_30_days": "样本不足30天，仅供参考",
            "observable": "样本期已满30天",
            "unavailable": "暂不可计算",
            "beijing_daily_projection": (
                "按北京时间当日收益率乘以365，仅供参考"
            ),
        }.get(
            str(_value(payload, "annualized_reliability")),
            _value(payload, "annualized_reliability"),
        )
        return "\n".join(
            [
                "[Var/Lighter] 平仓收益汇总",
                f"资产：{asset}｜批次：{lot_id}｜状态：{status}",
                "统计日期："
                f"{_value(payload, 'beijing_day')}（北京时间）",
                f"本轮实际盈亏：{_localized_money(payload, 'cycle_actual_pnl_usd')}",
                "北京时间今日累计盈亏："
                f"{_localized_money(payload, 'beijing_day_actual_pnl_usd')}",
                "北京时间今日完成轮数："
                f"{_value(payload, 'beijing_day_completed_cycles')}",
                f"本统计周期累计盈亏：{_localized_money(payload, 'run_actual_pnl_usd')}",
                f"账户权益净变化：{_localized_money(payload, 'account_net_change_usd')}",
                "权益变化与成交盈亏差额："
                f"{_localized_money(payload, 'account_minus_fill_pnl_usd')}",
                f"Variational 权益：{_localized_money(payload, 'variational_equity_usd')}",
                f"Lighter 权益：{_localized_money(payload, 'lighter_equity_usd')}",
                f"双边总权益：{_localized_money(payload, 'combined_equity_usd')}",
                f"统计本金：{_localized_money(payload, 'capital_usd')}",
                f"已完成轮数：{_value(payload, 'completed_cycles')}",
                "北京时间当日收益率："
                f"{_localized_percent(payload, 'beijing_day_return_pct')}",
                f"统计周期累计收益率：{_localized_percent(payload, 'return_pct')}",
                f"收益口径：{return_source}",
                "统计周期简单年化收益率："
                f"{_localized_percent(payload, 'annualized_simple_pct')}",
                f"年化说明：{reliability}",
            ]
        )
    if event_type in {
        "live_inventory_entry_blocked",
        "live_inventory_v4_entry_blocked",
    }:
        return "\n".join(
            [
                "[Var/Lighter] 开仓被拦截",
                f"资产：{asset}｜方向：{_direction_cn(direction)}",
                f"原因：{_reason_cn(_value(payload, 'reason'))}",
                f"当前价差：{_value(payload, 'edge_bps', 'short_edge_bps')} bps",
                "开仓阈值："
                f"{_value(payload, 'v4_entry_threshold_bps', 'entry_threshold_bps')} bps",
                f"已合并重复提醒：{_value(payload, 'telegram_suppressed_repeats')}",
            ]
        )
    if event_type == "live_inventory_basis_quote_failed":
        return "\n".join(
            [
                "[Var/Lighter] 行情获取失败",
                f"资产：{asset}",
                f"原因：{_reason_cn(_value(payload, 'reason', 'error'))}",
                f"已合并重复提醒：{_value(payload, 'telegram_suppressed_repeats')}",
            ]
        )
    if event_type == "live_inventory_exit_blocked":
        return "\n".join(
            [
                "[Var/Lighter] 平仓条件未满足",
                f"资产：{asset}｜方向：{_direction_cn(direction)}",
                f"原因：{_reason_cn(_value(payload, 'reason'))}",
                f"当前预计收益：{_value(payload, 'pnl_bps')} bps",
                f"目标收益：{_value(payload, 'effective_min_exit_pnl_bps')} bps",
                f"持仓时间：{_value(payload, 'holding_seconds')} 秒",
                f"已合并重复提醒：{_value(payload, 'telegram_suppressed_repeats')}",
            ]
        )
    if event_type in {
        "live_inventory_manual_review_required",
        "live_inventory_loss_review_alert",
    }:
        return "\n".join(
            [
                "[Var/Lighter] 需要人工核对",
                f"资产：{asset}｜原因：{_reason_cn(_value(payload, 'reason'))}",
                f"当前预计亏损：{_value(payload, 'pnl_bps')} bps｜提醒阈值：{_value(payload, 'threshold_bps')} bps",
                f"本地未平仓层数：{_value(payload, 'open_lots_total')}",
                "动作：不自动止损；暂停新开仓和加仓，现有仓位继续按正常退出条件管理",
                f"运行编号：{run_id}",
            ]
        )
    if event_type == "live_inventory_runtime_fuse_triggered":
        return "\n".join(
            [
                "[Var/Lighter] 运行保护已触发",
                f"资产：{asset}｜原因：{_reason_cn(_value(payload, 'reason'))}",
                f"动作：{_action_cn(_value(payload, 'action'))}",
                f"本地未平仓层数：{_value(payload, 'open_lots_total')}",
                f"运行编号：{run_id}",
            ]
        )
    if event_type == "live_inventory_v4_strong_single_auto_disabled":
        return "\n".join(
            [
                "[Var/Lighter] 平仓确认模式已降级",
                f"资产：{asset}｜方向：{_direction_cn(direction)}",
                f"原因：{_reason_cn(_value(payload, 'reason'))}",
                f"实际收益：{_value(payload, 'actual_pnl_bps')} bps",
                "强单次确认预留："
                f"{_value(payload, 'strong_single_shortfall_reserve_bps')} bps",
                f"动作：{_action_cn(_value(payload, 'action'))}",
                f"运行编号：{run_id}",
            ]
        )
    if event_type == "live_inventory_account_risk_alert":
        lines = [
            "[Var/Lighter] 账户风险提醒",
            f"资产：{asset}｜原因：{_reason_cn(_value(payload, 'risk_reason', 'reason'))}",
            f"动作：{_action_cn(_value(payload, 'risk_action', 'action'))}",
            f"Variational 权益：{_value(payload, 'variational_equity_usd')} U",
            f"Lighter 权益：{_value(payload, 'lighter_equity_usd')} U",
            f"双边权益平衡度：{_value(payload, 'equity_balance_ratio')}",
            f"双边权益差距：{_value(payload, 'equity_imbalance_pct')}%",
            f"最高单边预计杠杆：{_value(payload, 'max_projected_venue_leverage')}x / 5x",
            f"最高维持保证金使用率：{_value(payload, 'max_maintenance_margin_usage_pct')}%",
            "Variational 快照年龄："
            f"{_value(payload, 'variational_account_snapshot_age_seconds')} 秒",
        ]
        if payload.get("rebalance_recommended") is True:
            lines[7:7] = [
                "建议平衡方向："
                f"{_venue_name(_value(payload, 'rebalance_from_venue'))} → "
                f"{_venue_name(_value(payload, 'rebalance_to_venue'))}",
                "建议平衡金额：约 "
                f"{_value(payload, 'rebalance_suggested_amount_usd')} U"
                f"（目标权益差距 {_value(payload, 'rebalance_target_imbalance_pct')}%）",
            ]
        elif payload.get("rebalance_recommended") is False:
            lines[7:7] = [
                "建议平衡方向：无需调整",
                "建议平衡金额：0 U（双边权益尚未达到平衡预警线）",
            ]
        else:
            lines[7:7] = [
                "建议平衡方向：暂不可计算",
                "建议平衡金额：暂不可计算（需双边最新权益）",
            ]
        return "\n".join(lines)
    if event_type == "live_inventory_account_risk_recovered":
        return "\n".join(
            [
                "[Var/Lighter] 账户风险已恢复",
                f"资产：{asset}",
                "动作：连续账户检查已通过，恢复允许开仓和加仓",
                f"Variational 权益：{_value(payload, 'variational_equity_usd')} U",
                f"Lighter 权益：{_value(payload, 'lighter_equity_usd')} U",
                f"双边权益差距：{_value(payload, 'equity_imbalance_pct')}%",
            ]
        )
    raise ValueError(f"Unsupported Telegram event: {event_type}")


class TelegramNotifier:
    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        logger: logging.Logger,
        queue_size: int = 100,
        throttle_seconds: float = 1800.0,
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.logger = logger
        self.throttle_seconds = max(0.0, throttle_seconds)
        self.enabled = bool(self.bot_token and self.chat_id)
        self.last_enqueued_at: dict[tuple[str, str, str], float] = {}
        self.suppressed_counts: dict[tuple[str, str, str], int] = {}
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]] | None] = (
            asyncio.Queue(maxsize=queue_size)
        )
        self.worker_task: asyncio.Task[None] | None = None

    @classmethod
    def from_env(cls, *, logger: logging.Logger) -> "TelegramNotifier":
        return cls(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            logger=logger,
            throttle_seconds=_env_float(
                "TELEGRAM_ALERT_THROTTLE_SECONDS",
                1800.0,
            ),
        )

    async def start(self) -> None:
        if not self.enabled or (
            self.worker_task is not None and not self.worker_task.done()
        ):
            return
        self.worker_task = asyncio.create_task(
            self._worker(),
            name="telegram_notifier",
        )
        self.logger.info("telegram_notifications_enabled")

    def enqueue(self, event_type: str, payload: dict[str, Any]) -> bool:
        if not self.enabled or event_type not in TELEGRAM_EVENT_TYPES:
            return False
        # The operator control is shared with the independent watchdog so a
        # single silence action also suppresses queued strategy notifications.
        if not notifications_allowed(read_alert_control(ALERT_CONTROL_PATH)):
            return False
        if (
            event_type == "live_inventory_account_snapshot"
            and payload.get("snapshot_stage") != "startup_flat"
        ):
            return False
        if event_type == "live_inventory_exited":
            return False
        if event_type in {
            "live_inventory_entry_blocked",
            "live_inventory_v4_entry_blocked",
        }:
            return False
        if (
            event_type == "live_inventory_pnl_summary"
            and not payload.get("account_snapshot_flat")
        ):
            return False
        if (
            event_type == "live_inventory_exit_blocked"
            and str(payload.get("reason") or "")
            not in TELEGRAM_CRITICAL_EXIT_BLOCK_REASONS
        ):
            return False
        if event_type in TELEGRAM_THROTTLED_EVENT_TYPES:
            throttle_key = (
                event_type,
                str(payload.get("asset") or "-"),
                str(
                    payload.get("risk_reason")
                    or payload.get("reason")
                    or payload.get("error")
                    or "-"
                ),
            )
            now = time.monotonic()
            previous = self.last_enqueued_at.get(throttle_key)
            if (
                previous is not None
                and now - previous < self.throttle_seconds
            ):
                self.suppressed_counts[throttle_key] = (
                    self.suppressed_counts.get(throttle_key, 0) + 1
                )
                return False
            self.last_enqueued_at[throttle_key] = now
            payload = dict(payload)
            payload["telegram_suppressed_repeats"] = (
                self.suppressed_counts.pop(throttle_key, 0)
            )
        try:
            self.queue.put_nowait((event_type, dict(payload)))
        except asyncio.QueueFull:
            self.logger.warning(
                "telegram_notification_dropped event=%s reason=queue_full",
                event_type,
            )
            return False
        return True

    async def stop(self, *, flush_timeout_seconds: float = 10.0) -> None:
        task = self.worker_task
        if task is None:
            return
        try:
            await asyncio.wait_for(
                self.queue.join(),
                timeout=flush_timeout_seconds,
            )
        except TimeoutError:
            self.logger.warning("telegram_notification_flush_timeout")
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            while True:
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    self.queue.task_done()
            self.worker_task = None
            return
        if not task.done():
            self.queue.put_nowait(None)
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.worker_task = None

    def send_now(
        self,
        text: str,
        *,
        reply_markup: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID"
        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json=payload,
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return False, f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return False, f"http_status_{response.status_code}"
        return True, "sent"

    def get_updates(
        self,
        *,
        offset: int | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        if not self.enabled:
            return [], "missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID"
        params: dict[str, Any] = {
            "timeout": 0,
            "allowed_updates": json.dumps(["callback_query"]),
        }
        if offset is not None:
            params["offset"] = int(offset)
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{self.bot_token}/getUpdates",
                params=params,
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return [], f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return [], f"http_status_{response.status_code}"
        try:
            body = response.json()
        except Exception:
            return [], "invalid_json_response"
        if not body.get("ok") or not isinstance(body.get("result"), list):
            return [], "telegram_api_error"
        return [item for item in body["result"] if isinstance(item, dict)], "ok"

    def answer_callback_query(
        self,
        callback_query_id: str,
        *,
        text: str,
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID"
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery",
                json={"callback_query_id": callback_query_id, "text": text},
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return False, f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return False, f"http_status_{response.status_code}"
        return True, "sent"

    def clear_inline_keyboard(
        self,
        *,
        chat_id: str,
        message_id: int,
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "missing_TELEGRAM_BOT_TOKEN_or_TELEGRAM_CHAT_ID"
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/editMessageReplyMarkup",
                json={
                    "chat_id": chat_id,
                    "message_id": int(message_id),
                    "reply_markup": {"inline_keyboard": []},
                },
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return False, f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return False, f"http_status_{response.status_code}"
        return True, "sent"

    def discover_chat_ids(self) -> tuple[list[dict[str, str]], str]:
        if not self.bot_token:
            return [], "missing_TELEGRAM_BOT_TOKEN"
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{self.bot_token}/getUpdates",
                timeout=(3.0, 5.0),
            )
        except Exception as exc:
            return [], f"request_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return [], f"http_status_{response.status_code}"
        try:
            body = response.json()
        except Exception:
            return [], "invalid_json_response"
        if not body.get("ok"):
            return [], "telegram_api_error"

        chats: dict[str, dict[str, str]] = {}
        for update in body.get("result", []):
            message = (
                update.get("message")
                or update.get("edited_message")
                or update.get("channel_post")
                or {}
            )
            chat = message.get("chat") or {}
            chat_id = chat.get("id")
            if chat_id is None:
                continue
            key = str(chat_id)
            chats[key] = {
                "chat_id": key,
                "type": str(chat.get("type") or "-"),
                "name": str(
                    chat.get("username")
                    or chat.get("title")
                    or chat.get("first_name")
                    or "-"
                ),
            }
        if not chats:
            return [], "no_chat_found_send_the_bot_a_message_then_retry"
        return list(chats.values()), "found"

    async def _worker(self) -> None:
        while True:
            item = await self.queue.get()
            try:
                if item is None:
                    return
                event_type, payload = item
                try:
                    message = format_telegram_trade_message(
                        event_type,
                        payload,
                    )
                    ok, detail = await asyncio.to_thread(
                        self.send_now,
                        message,
                    )
                except Exception as exc:
                    ok = False
                    detail = f"worker_failed:{type(exc).__name__}"
                if not ok:
                    self.logger.warning(
                        "telegram_notification_failed event=%s detail=%s",
                        event_type,
                        detail,
                    )
                else:
                    self.logger.info(
                        "telegram_notification_sent event=%s",
                        event_type,
                    )
            finally:
                self.queue.task_done()
