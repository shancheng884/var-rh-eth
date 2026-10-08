from __future__ import annotations

from typing import Any


ACTION_LABELS = {
    "normal": "正常运行",
    "warning": "仅提醒",
    "block_entry": "禁止新开仓和加仓",
    "force_reduce": "降低一层仓位",
    "emergency_exit": "紧急退出全部仓位",
    "auto_stop_flat": "空仓自动停机",
    "manual_exchange_review_required": "需要人工核对双边账户",
    "fallback_to_latest_and_2_of_3": "回退到最新报价，并采用三次确认中至少两次通过",
    "confirm_both_exchange_positions_manually_before_restart": "重启前人工确认双边仓位",
    "stop_live_inventory_until_manual_flat_confirmed": "确认双边空仓前停止实盘策略",
}


REASON_LABELS = {
    "account_risk_normal": "账户风险正常",
    "account_risk_not_configured": "账户风险监控未配置",
    "account_equity_unavailable": "账户权益暂不可用，禁止新开仓",
    "lighter_account_equity_unavailable": "Robinhood Chain（Lighter）账户权益查询暂不可用，已暂停新开仓",
    "variational_account_equity_unavailable": "Variational 账户权益查询暂不可用，已暂停新开仓",
    "both_accounts_equity_unavailable": "Variational 与 Lighter 账户权益均暂不可用，已暂停新开仓",
    "venue_leverage_exceeds_hard_entry_limit": "单个平台杠杆超过开仓硬上限",
    "venue_leverage_exceeds_hard_limit": "单个平台杠杆超过硬上限",
    "venue_leverage_above_entry_cap_monitoring_margin": "单个平台杠杆接近开仓上限",
    "maintenance_margin_usage_warning": "维持保证金使用率进入预警区",
    "maintenance_margin_usage_blocks_entry": "维持保证金使用率过高，禁止加仓",
    "maintenance_margin_usage_reduce": "维持保证金使用率过高，执行降杠杆",
    "maintenance_margin_usage_emergency": "维持保证金使用率进入紧急平仓区",
    "venue_equity_imbalance_warning": "双边权益不均衡，建议补齐较少一侧",
    "venue_equity_imbalance_blocks_entry": "双边权益严重失衡，禁止新开仓",
    "variational_account_snapshot_stale": "Variational 账户快照过旧，暂停开仓和加仓",
    "variational_account_recovery_confirmation_pending": "Variational 账户链路已恢复，正在重新确认",
    "variational_html_response": "Variational 返回网页而非行情数据",
    "variational_extension_disconnected": "Variational 浏览器扩展断开",
    "exchange_flat_confirmation_failed": "双边空仓确认暂时失败",
    "exchange_positions_not_flat": "交易所仍有未平仓仓位",
    "startup_reconcile_exchange_position_check_failed": "启动时双边仓位核对失败",
    "startup_reconcile_exchange_position_mismatch": "启动时本地与交易所仓位不一致",
    "startup_reconcile_local_flat_but_exchange_position_open": "本地显示空仓，但交易所仍有仓位",
    "startup_flat_pending_reconciled": "启动空仓待处理状态已核对",
    "startup_open_state_reconciled": "启动时未完成仓位已核对",
    "runtime_stopped_with_unresolved_entry_submission": "策略停止时仍有未确认的开仓提交",
    "basis_entry_var_fill_timeout_pending_reconcile": "Variational 开仓成交确认超时，等待核对",
    "basis_entry_var_fill_missing_price": "Variational 开仓成交缺少成交价",
    "basis_entry_concurrent_lighter_record_missing_after_var_fill": "Variational 已成交，但缺少 Lighter 对冲记录",
    "basis_entry_lighter_submit_after_var_fill_failed": "Variational 已成交，但 Lighter 对冲提交失败",
    "basis_entry_lighter_final_fill_not_confirmed": "Lighter 开仓最终成交未确认",
    "basis_entry_lighter_final_fill_not_confirmed_after_var_fill": "Variational 已成交，但 Lighter 最终成交未确认",
    "basis_entry_lighter_actual_slippage_exceeds_limit": "Lighter 开仓实际滑点超过限制",
    "basis_entry_actual_slippage_rejected_auto_closed": "开仓实际滑点超限，已自动平仓",
    "basis_entry_lighter_depth_insufficient_after_refresh": "刷新后 Lighter 盘口深度不足",
    "basis_entry_lighter_depth_insufficient_after_quantize": "数量取整后 Lighter 盘口深度不足",
    "basis_entry_lighter_submit_failed": "Lighter 开仓提交失败",
    "basis_entry_execution_unknown_pending_reconcile": "开仓执行结果未知，等待双边核对",
    "basis_entry_submit_exception_pending_reconcile": "开仓提交发生异常，等待双边核对",
    "basis_entry_var_exception_pending_reconcile": "Variational 开仓提交异常，等待双边核对",
    "basis_entry_lighter_exception_pending_reconcile": "Lighter 开仓提交异常，等待双边核对",
    "basis_entry_refresh_quote_unavailable": "刷新开仓报价不可用",
    "basis_entry_refreshed_var_quote_too_old": "刷新后的 Variational 报价过旧",
    "basis_entry_refreshed_quote_metadata_invalid": "刷新后的开仓报价元数据无效",
    "basis_entry_refreshed_edge_below_threshold": "刷新后价差低于开仓阈值",
    "basis_entry_refreshed_quality_score_too_low": "刷新后报价质量分低于要求",
    "basis_entry_refreshed_roundtrip_below_threshold": "刷新后扣除往返成本的价差低于阈值",
    "basis_entry_quantized_edge_below_threshold": "数量取整后价差低于开仓阈值",
    "basis_entry_quantized_quality_score_too_low": "数量取整后报价质量分低于要求",
    "basis_entry_quantized_roundtrip_below_threshold": "数量取整后扣除往返成本的价差低于阈值",
    "basis_entry_exact_rfq_cooldown": "精确报价仍在冷却期",
    "basis_entry_confirmation_pending": "等待开仓报价确认",
    "basis_entry_watch_pending": "开仓机会等待确认",
    "basis_entry_quality_score_too_low": "开仓报价质量分低于要求",
    "basis_entry_roundtrip_below_threshold": "扣除往返成本的价差低于阈值",
    "basis_entry_abs_entry_threshold_not_met": "绝对价差未达到开仓阈值",
    "basis_sample_move_too_large": "价差瞬时变化过大",
    "strong_single_sample_move_recheck": "价差变化过快，重新检查单次机会",
    "v4_real_gradient_tier_capacity_reached": "当前价差档位的累计仓位已满",
    "live_inventory_total_notional_exceeds_limit": "累计名义本金超过限制",
    "variational_taker_funding_reject_cooldown_active": "Variational 资金条件拒绝仍在冷却期",
    "variational_amount_below_min_tick_after_quantize": "数量取整后低于 Variational 最小交易单位",
    "hedge_below_lighter_min_base_amount": "对冲数量低于 Lighter 最小交易数量",
    "basis_var_quote_too_old": "Variational 报价过旧",
    "basis_lighter_book_too_old": "Lighter 盘口过旧",
    "basis_exit_market_data_too_old": "平仓市场数据过旧",
    "basis_exit_refresh_quote_unavailable": "刷新平仓报价不可用",
    "basis_exit_refresh_pnl_below_threshold": "刷新后可执行收益低于平仓目标",
    "basis_exit_lighter_depth_insufficient": "Lighter 平仓盘口深度不足",
    "basis_exit_lighter_depth_pnl_below_threshold": "Lighter 平仓深度对应收益低于目标",
    "basis_exit_lighter_submit_failed": "Lighter 平仓提交失败",
    "basis_exit_lighter_final_fill_not_confirmed": "Lighter 平仓最终成交未确认",
    "basis_exit_var_exception_pending_reconcile": "Variational 平仓提交异常，等待双边核对",
    "basis_exit_execution_unknown_pending_reconcile": "平仓执行结果未知，等待双边核对",
    "basis_exit_quote_id_missing": "平仓报价缺少报价编号",
    "basis_exit_quote_reuse_invalid": "平仓报价重复使用无效",
    "v4_passive_exit_confirmation_pending": "等待平仓价格确认",
    "v4_exit_confirmation_pending": "等待平仓价格确认",
    "v4_executable_pnl_below_threshold": "可执行净收益低于平仓目标",
    "v4_portfolio_exit_refresh_below_threshold": "组合刷新后可执行收益低于平仓目标",
    "v4_portfolio_exit_locked": "组合平仓仍在锁定期",
    "v4_partial_detier_exit_locked": "部分降档平仓仍在锁定期",
    "basis_max_hold_reached_waiting_for_reversion": "达到最长持仓观察时间，等待价差回归",
    "basis_signal_exit_watch_waiting_for_pnl": "平仓信号已出现，等待达到最低收益",
    "entry_final_fill_cost_pending": "等待开仓最终成交成本确认",
    "max_unrealized_loss_bps": "触发最大未实现亏损保护",
    "max_unrealized_loss_manual_review": "未实现亏损达到人工审核阈值",
    "v4_max_hold_timeout": "旧版最长持仓超时",
    "operator_requested_exit": "人工请求安全退出",
    "operator_requested_atomic_exit_locked": "人工退出请求已锁定，等待双边处理",
    "account_risk_atomic_exit_locked": "账户风险退出请求已锁定，等待双边处理",
    "maintenance_drain_requested": "维护排空已请求",
    "maintenance_drain_completed": "维护排空已完成",
    "maintenance_drain_blocked": "维护排空仍被阻止",
    "v4_direction_paused_negative_recent_pnl": "该方向近期实际收益为负，暂时暂停",
    "basis_direction_paused_negative_recent_pnl": "该方向近期实际收益为负，暂时暂停",
    "v4_waiting_for_episode_rearm": "等待本轮策略重新启用",
    "account_equity_delta": "账户权益净变化",
}


INCIDENT_KEY_LABELS = {
    "critical_account_risk": "账户出现紧急风险",
    "strategy_stopped_with_exposure": "策略停止但仍有仓位",
    "strategy_stopped_flat": "策略已停止且当前空仓",
    "pending_action_stale": "双边成交确认超时",
    "risk_heartbeat_stale_with_exposure": "持仓期间风险监控失联",
    "variational_authentication_required": "Variational 需要重新登录",
    "variational_reference_feed_stale": "Variational 参考价流失联",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def action_cn(value: Any) -> str:
    text = _text(value)
    if not text or text == "-":
        return "暂不可用"
    if any("\u4e00" <= char <= "\u9fff" for char in text):
        return text
    return ACTION_LABELS.get(text, "未分类处理动作")


def reason_cn(value: Any) -> str:
    text = _text(value)
    if not text or text == "-":
        return "暂不可用"
    if any("\u4e00" <= char <= "\u9fff" for char in text):
        return text
    return REASON_LABELS.get(text, "未分类风险原因")


def incident_key_cn(value: Any) -> str:
    text = _text(value)
    if not text or text == "-":
        return "未分类风险事件"
    if text in INCIDENT_KEY_LABELS:
        return INCIDENT_KEY_LABELS[text]
    parts = text.split(":")
    if parts[0] == "account_risk" and len(parts) >= 3:
        return f"账户风险：{action_cn(parts[1])}；{reason_cn(':'.join(parts[2:]))}"
    if parts[0] in {"manual_review", "unreconciled_manual_review"} and len(parts) >= 2:
        return f"人工核对：{reason_cn(':'.join(parts[1:]))}"
    if parts[0] == "data_visibility" and len(parts) >= 2:
        return f"账户数据可见性：{reason_cn(':'.join(parts[1:]))}"
    return "未分类风险事件"
