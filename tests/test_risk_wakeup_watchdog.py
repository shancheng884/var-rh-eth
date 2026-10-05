from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from main import VariationalToLighterRuntime
from tools.risk_wakeup_watchdog import (
    Incident,
    RiskWakeupWatchdog,
    WatchdogConfig,
    evaluate_incidents,
)
from tools.lib.wakeup_notifiers import NotificationResult


def config(**overrides) -> WatchdogConfig:
    values = {
        "enabled": True,
        "alert_when_flat_strategy_stopped": True,
        "poll_seconds": 5,
        "risk_heartbeat_max_age_seconds": 45,
        "pending_action_max_age_seconds": 30,
        "data_unavailable_critical_seconds": 300,
        "max_phone_attempts_per_incident": 3,
    }
    values.update(overrides)
    return WatchdogConfig(**values)


class FakeBark:
    enabled = True
    config_path = None

    def __init__(self):
        self.sent = []
        self.failures_remaining = 0

    def send(self, **kwargs):
        self.sent.append(kwargs)
        if self.failures_remaining:
            self.failures_remaining -= 1
            return NotificationResult(False, "temporary_failure")
        return NotificationResult(True, "sent")


class FakeFeishu:
    enabled = True
    config_path = None

    def __init__(self):
        self.messages = []
        self.phones = []
        self.message_failures_remaining = 0
        self.phone_failures_remaining = 0

    def send_message(self, **kwargs):
        self.messages.append(kwargs)
        if self.message_failures_remaining:
            self.message_failures_remaining -= 1
            return NotificationResult(False, "temporary_message_failure")
        return NotificationResult(
            True,
            "sent",
            f"message-{len(self.messages)}",
        )

    def phone_urgent(self, message_id):
        self.phones.append(message_id)
        if self.phone_failures_remaining:
            self.phone_failures_remaining -= 1
            return NotificationResult(False, "temporary_phone_failure")
        return NotificationResult(True, "sent")


class FakeTelegram:
    enabled = True
    chat_id = "123"

    def __init__(self):
        self.sent = []
        self.updates = []
        self.answered = []
        self.cleared = []
        self.failures_remaining = 0

    def send_now(self, text, *, reply_markup=None):
        self.sent.append((text, reply_markup))
        if self.failures_remaining:
            self.failures_remaining -= 1
            return False, "temporary_telegram_failure"
        return True, "sent"

    def get_updates(self, *, offset=None):
        updates = [
            update
            for update in self.updates
            if offset is None or update["update_id"] >= offset
        ]
        return updates, "ok"

    def answer_callback_query(self, callback_query_id, *, text):
        self.answered.append((callback_query_id, text))
        return True, "sent"

    def clear_inline_keyboard(self, *, chat_id, message_id):
        self.cleared.append((chat_id, message_id))
        return True, "sent"


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def test_open_position_and_stopped_strategy_is_critical() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
        risk_health={"updated_at": now.isoformat(), "risk_action": "normal"},
        events=[],
        strategy_running=False,
        config=config(),
        now=now,
    )

    assert any(
        item.key == "critical_account_risk"
        and item.severity == "critical"
        and "主策略已停止" in item.message
        for item in incidents
    )


def test_flat_stopped_strategy_is_warning_only() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={"updated_at": now.isoformat(), "risk_action": "normal"},
        events=[],
        strategy_running=False,
        config=config(),
        now=now,
    )

    assert [(item.key, item.severity) for item in incidents] == [
        ("strategy_stopped_flat", "warning")
    ]


def test_recent_degraded_account_snapshot_does_not_raise_incident() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={
            "updated_at": now.isoformat(),
            "risk_action": "normal",
            "risk_reason": "account_risk_normal",
            "variational_account_snapshot_fresh": False,
            "variational_account_snapshot_usable": True,
            "variational_account_snapshot_degraded": True,
            "variational_account_snapshot_age_seconds": 120,
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    assert incidents == []


def test_watchdog_uses_debounced_account_risk_notification_state() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    base = {
        "updated_at": now.isoformat(),
        "risk_action": "warning",
        "risk_reason": "venue_equity_imbalance_warning",
    }

    assert evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={
            **base,
            "risk_notification_action": "normal",
            "risk_notification_reason": "account_risk_normal",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    ) == []

    incidents = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={
            **base,
            "risk_notification_action": "warning",
            "risk_notification_reason": "venue_equity_imbalance_warning",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )
    assert [item.key for item in incidents] == [
        "account_risk:warning:venue_equity_imbalance_warning"
    ]
    assert incidents[0].rearm_seconds == 900


def test_lighter_outage_and_recovery_confirmation_keep_one_incident_key() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    state = {
        "status": "open",
        "asset": "ETH",
        "open_lots": [{"lot_id": 1}],
        "updated_at": now.isoformat(),
    }
    failure = {
        "updated_at": now.isoformat(),
        "risk_action": "block_entry",
        "risk_reason": "lighter_account_equity_unavailable",
        "risk_notification_action": "block_entry",
        "risk_notification_reason": "lighter_account_equity_unavailable",
    }
    confirming = {
        **failure,
        "account_recovery_required": True,
        "account_recovery_confirm_count": 2,
        "account_recovery_confirm_samples": 3,
    }

    failure_incidents = evaluate_incidents(
        state=state,
        risk_health=failure,
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )
    confirming_incidents = evaluate_incidents(
        state=state,
        risk_health=confirming,
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    failure_risk = next(
        item for item in failure_incidents if item.key.startswith("data_visibility:")
    )
    confirming_risk = next(
        item
        for item in confirming_incidents
        if item.key.startswith("data_visibility:")
    )
    assert failure_risk.key == confirming_risk.key
    assert failure_risk.key == "data_visibility:lighter_account_equity_unavailable"
    assert "Lighter" in failure_risk.message


def test_empty_pending_intent_does_not_raise_stale_action_incident() -> None:
    now = datetime(2026, 8, 30, 0, 1, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={
            "status": "open",
            "asset": "ETH",
            "open_lots": [{"lot_id": 1}],
            "updated_at": (now - timedelta(minutes=10)).isoformat(),
            "pending_actions": [
                {
                    "asset": "ETH",
                    "lot_id": 2,
                    "role": "live_inventory_exit",
                    "submitted_at": None,
                    "rfq_id": None,
                    "submitted_order_id": None,
                    "lighter_started": False,
                    "lighter_record_key": None,
                    "execution_unknown": False,
                    "reconciliation_required": False,
                    "context": {},
                }
            ],
        },
        risk_health={
            "updated_at": now.isoformat(),
            "pending_actions_total": 1,
            "risk_action": "normal",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    assert not any(item.key == "pending_action_stale" for item in incidents)


def test_submitted_pending_action_still_raises_after_timeout() -> None:
    now = datetime(2026, 8, 30, 0, 1, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={
            "status": "flat",
            "asset": "ETH",
            "open_lots": [],
            "pending_actions": [
                {
                    "asset": "ETH",
                    "lot_id": 2,
                    "role": "live_inventory_exit",
                    "submitted_at": (now - timedelta(seconds=31)).isoformat(),
                }
            ],
        },
        risk_health={
            "updated_at": now.isoformat(),
            "risk_action": "normal",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    stale = next(
        item for item in incidents if "pending_action_stale" in item.fingerprint
    )
    assert stale.severity == "critical"


def test_force_reduce_margin_and_position_mismatch_are_critical() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={
            "status": "manual_review_required",
            "asset": "ETH",
            "open_lots": [{"lot_id": 1}],
            "manual_review_reason": "startup_reconcile_exchange_position_mismatch",
        },
        risk_health={
            "updated_at": now.isoformat(),
            "risk_action": "force_reduce",
            "risk_reason": "maintenance_margin_usage_reduce",
            "max_maintenance_margin_usage_pct": "81",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    critical = next(item for item in incidents if item.severity == "critical")
    assert critical.key == "critical_account_risk"
    assert "启动时本地与交易所仓位不一致" in critical.message
    assert "维持保证金使用率过高，执行降杠杆" in critical.message


def test_critical_fingerprint_ignores_changing_wait_and_heartbeat_age() -> None:
    started = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    state = {
        "status": "manual_review_required",
        "asset": "ETH",
        "open_lots": [{"lot_id": 1}],
        "pending_actions": [{"submitted_at": started.isoformat()}],
        "manual_review_reason": "variational_html_response",
    }
    risk_health = {
        "updated_at": started.isoformat(),
        "risk_action": "normal",
    }

    first = evaluate_incidents(
        state=state,
        risk_health=risk_health,
        events=[],
        strategy_running=False,
        config=config(alert_when_flat_strategy_stopped=False),
        now=started + timedelta(seconds=63),
    )
    second = evaluate_incidents(
        state=state,
        risk_health=risk_health,
        events=[],
        strategy_running=False,
        config=config(alert_when_flat_strategy_stopped=False),
        now=started + timedelta(seconds=81),
    )
    first_critical = next(item for item in first if item.severity == "critical")
    second_critical = next(item for item in second if item.severity == "critical")

    assert first_critical.message != second_critical.message
    assert first_critical.fingerprint == second_critical.fingerprint


def test_stale_reference_feed_has_stable_flat_warning() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    base_risk = {
        "updated_at": now.isoformat(),
        "risk_action": "normal",
        "variational_reference_quote_fresh": False,
    }
    before_limit = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={**base_risk, "variational_reference_quote_stale_seconds": 119},
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )
    first = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={**base_risk, "variational_reference_quote_stale_seconds": 121},
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )
    second = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={**base_risk, "variational_reference_quote_stale_seconds": 900},
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    assert before_limit == []
    assert first[0].key == "variational_reference_feed_stale"
    assert first[0].severity == "warning"
    assert first[0].fingerprint == second[0].fingerprint
    assert first[0].message == second[0].message
    assert first[0].notify_recovery is False


def test_stale_reference_feed_is_critical_with_exposure() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
        risk_health={
            "updated_at": now.isoformat(),
            "risk_action": "normal",
            "variational_reference_quote_fresh": False,
            "variational_reference_quote_stale_seconds": 61,
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    critical = next(item for item in incidents if item.severity == "critical")
    assert critical.key == "critical_account_risk"
    assert critical.notify_recovery is False
    assert critical.rearm_seconds == 1800
    assert "参考价流已连续失联" in critical.message


def test_authentication_failure_is_one_stable_actionable_incident() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    state = {
        "status": "manual_review_required",
        "run_id": "run-changes-on-restart",
        "asset": "ETH",
        "open_lots": [],
        "manual_review_reason": "startup_reconcile_exchange_position_check_failed",
        "manual_review_context": {
            "errors": {
                "variational": (
                    "VAR_API_POSITIONS_RESULT httpStatus=401 {message: No token}"
                )
            }
        },
    }
    first = evaluate_incidents(
        state=state,
        risk_health={"updated_at": now.isoformat(), "risk_action": "normal"},
        events=[],
        strategy_running=False,
        config=config(alert_when_flat_strategy_stopped=False),
        now=now,
    )
    state["run_id"] = "another-run-id"
    second = evaluate_incidents(
        state=state,
        risk_health={"updated_at": now.isoformat(), "risk_action": "normal"},
        events=[],
        strategy_running=False,
        config=config(alert_when_flat_strategy_stopped=False),
        now=now,
    )

    assert [item.key for item in first] == ["variational_authentication_required"]
    assert [item.key for item in second] == ["variational_authentication_required"]
    assert "重新登录" in first[0].message


def test_remote_heartbeat_stale_key_does_not_change_with_age(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 0, 0, 30, tzinfo=timezone.utc)]
    # This test uses the remote monitor's stable identity directly so the
    # assertion covers the failure mode where only the elapsed age changes.
    from tools.risk_wakeup_backup import BackupAlertMonitor, BackupConfig

    remote_config = BackupConfig()
    remote_config.enabled = True
    remote_config.token = "token"
    remote_config.expected_node_id = "vps-a"
    remote = BackupAlertMonitor(
        config=remote_config,
        heartbeat_path=tmp_path / "missing-heartbeat.json",
        state_path=tmp_path / "remote-state.json",
        bark=FakeBark(),
        feishu=FakeFeishu(),
        telegram=FakeTelegram(),
        clock=lambda: current[0],
    )
    remote.memory["seen_heartbeat"] = True
    first = remote._desired_incidents(current[0])["remote_heartbeat_stale"]
    current[0] += timedelta(seconds=60)
    second = remote._desired_incidents(current[0])["remote_heartbeat_stale"]

    assert first["incident_signature"] == second["incident_signature"]


def test_stale_reference_alert_waits_while_recovery_is_in_progress() -> None:
    now = datetime(2026, 8, 30, 0, 0, tzinfo=timezone.utc)
    incidents = evaluate_incidents(
        state={"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
        risk_health={
            "updated_at": now.isoformat(),
            "risk_action": "normal",
            "variational_reference_quote_fresh": False,
            "variational_reference_quote_stale_seconds": 900,
            "variational_reference_feed_recovery_state": "waiting_for_fresh_data",
            "variational_reference_feed_recovery_attempt": 1,
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=now,
    )

    assert incidents == []


def test_feed_only_critical_rearm_also_suppresses_flat_warning(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        watchdog_config=config(reference_feed_rearm_seconds=1800),
    )
    write_json(
        tmp_path / "state.json",
        {"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
    )
    write_json(
        tmp_path / "risk.json",
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "normal",
            "variational_reference_quote_fresh": False,
            "variational_reference_quote_stale_seconds": 61,
        },
    )
    assert watchdog.run_once()

    current[0] += timedelta(seconds=1)
    write_json(
        tmp_path / "state.json",
        {"status": "flat", "asset": "ETH", "open_lots": []},
    )
    write_json(
        tmp_path / "risk.json",
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "normal",
            "variational_reference_quote_fresh": False,
            "variational_reference_quote_stale_seconds": 61,
        },
    )

    assert watchdog.run_once() == []


def test_stale_reference_feed_rearms_without_recovery_spam(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
        watchdog_config=config(reference_feed_rearm_seconds=1800),
    )
    state = {"status": "flat", "asset": "ETH", "open_lots": []}
    risk_path = tmp_path / "risk.json"
    write_json(tmp_path / "state.json", state)

    def write_health(*, fresh: bool, stale_seconds: float) -> None:
        write_json(
            risk_path,
            {
                "updated_at": current[0].isoformat(),
                "risk_action": "normal",
                "variational_reference_quote_fresh": fresh,
                "variational_reference_quote_stale_seconds": stale_seconds,
            },
        )

    write_health(fresh=False, stale_seconds=121)
    assert watchdog.run_once()[0].key == "variational_reference_feed_stale"
    assert bark.sent == []
    assert feishu.phones == []

    current[0] += timedelta(seconds=1)
    write_health(fresh=True, stale_seconds=0)
    assert watchdog.run_once() == []
    assert bark.sent == []
    assert feishu.messages == [] or len(feishu.messages) == 1

    current[0] += timedelta(minutes=10)
    write_health(fresh=False, stale_seconds=121)
    assert watchdog.run_once() == []
    assert bark.sent == []

    current[0] += timedelta(minutes=31)
    write_health(fresh=False, stale_seconds=121)
    assert watchdog.run_once()[0].key == "variational_reference_feed_stale"
    assert bark.sent == []


def test_stale_reference_reload_is_bounded_and_exposure_verified() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_collect_only = False
    runtime.live_inventory_open_lots = []
    runtime.pending_live_inventory_actions_payload = lambda: []
    runtime.live_inventory_reference_feed_stale_since_monotonic = {
        "ETH": time.monotonic() - 121,
    }
    runtime.live_inventory_reference_feed_last_recovery_monotonic = {}
    runtime.live_inventory_reference_feed_recovery_attempts = {}
    runtime.live_inventory_reference_feed_recovery_state = {}
    runtime.live_inventory_reference_feed_recovery_block_reason = {}
    runtime.live_inventory_reference_feed_recovery_last_error = {}
    runtime.live_inventory_reference_feed_recovery_reconcile_pending = set()
    runtime.live_inventory_reference_feed_recovery_inflight = set()
    runtime.logger = type(
        "Logger",
        (),
        {"info": lambda *args, **kwargs: None, "warning": lambda *args, **kwargs: None},
    )()
    calls = []

    async def send_command(**kwargs):
        calls.append(kwargs)
        return {"ok": True}

    runtime.send_variational_command = send_command

    asyncio.run(
        runtime.maybe_recover_stale_variational_reference_feed(
            asset="ETH",
            quote_age_ok=False,
        )
    )
    asyncio.run(
        runtime.maybe_recover_stale_variational_reference_feed(
            asset="ETH",
            quote_age_ok=False,
        )
    )

    assert len(calls) == 1
    assert calls[0]["payload"]["type"] == "VAR_API_RELOAD_PAGE"

    runtime.live_inventory_open_lots = [
        {
            "lot_id": 1,
            "qty": "0.01",
            "direction": "short_var_long_lighter",
        }
    ]
    runtime.live_inventory_reference_feed_last_recovery_monotonic = {}
    runtime.live_inventory_reference_feed_recovery_attempts = {}
    runtime.fetch_variational_positions = lambda: _resolved(
        {"ok": True, "positions": [{"asset": "ETH", "qty": "-0.01"}]}
    )
    runtime.fetch_lighter_account = lambda: _resolved(
        {
            "accounts": [
                {
                    "positions": [
                        {"symbol": "ETH", "position": "0.01", "sign": 1}
                    ]
                }
            ]
        }
    )
    asyncio.run(
        runtime.maybe_recover_stale_variational_reference_feed(
            asset="ETH",
            quote_age_ok=False,
        )
    )
    assert len(calls) == 2


async def _resolved(value):
    return value


def test_reference_recovery_requires_post_reload_position_reconcile() -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_basis_v4_mode = True
    runtime.live_inventory_collect_only = False
    runtime.live_inventory_open_lots = [
        {
            "lot_id": 1,
            "qty": "0.01",
            "direction": "short_var_long_lighter",
        }
    ]
    runtime.pending_live_inventory_actions_payload = lambda: []
    runtime.live_inventory_reference_feed_stale_since_monotonic = {
        "ETH": time.monotonic() - 121,
    }
    runtime.live_inventory_reference_feed_stale_since_at = {}
    runtime.live_inventory_reference_feed_last_recovery_monotonic = {}
    runtime.live_inventory_reference_feed_recovery_attempts = {"ETH": 1}
    runtime.live_inventory_reference_feed_recovery_state = {
        "ETH": "waiting_for_fresh_data"
    }
    runtime.live_inventory_reference_feed_recovery_block_reason = {}
    runtime.live_inventory_reference_feed_recovery_last_error = {}
    runtime.live_inventory_reference_feed_recovery_reconcile_pending = {"ETH"}
    runtime.live_inventory_reference_feed_recovery_inflight = set()

    runtime.fetch_variational_positions = lambda: _resolved(
        {"ok": True, "positions": [{"asset": "ETH", "qty": "-0.01"}]}
    )
    runtime.fetch_lighter_account = lambda: _resolved(
        {
            "accounts": [
                {
                    "positions": [
                        {"symbol": "ETH", "position": "0.01", "sign": 1}
                    ]
                }
            ]
        }
    )

    asyncio.run(
        runtime.maybe_recover_stale_variational_reference_feed(
            asset="ETH",
            quote_age_ok=True,
        )
    )

    assert runtime.live_inventory_reference_feed_recovery_reconcile_pending == set()
    assert runtime.live_inventory_reference_feed_recovery_state["ETH"] == "healthy"
    assert runtime.live_inventory_reference_feed_recovery_attempts == {}


def build_watchdog(
    tmp_path,
    *,
    current,
    bark=None,
    feishu=None,
    watchdog_config=None,
):
    return RiskWakeupWatchdog(
        config=(
            watchdog_config
            if watchdog_config is not None
            else config(channel_retry_seconds=10)
        ),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        watchdog_control_path=tmp_path / "control.json",
        bark=bark or FakeBark(),
        feishu=feishu or FakeFeishu(),
        telegram=FakeTelegram(),
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )


def test_critical_incident_uses_feishu_phone_without_routine_bark(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    watchdog.run_once(synthetic=incident)

    assert bark.sent == []
    assert len(feishu.messages) == 1
    assert feishu.phones == ["message-1"]


def test_warning_incident_is_telegram_only_and_localized(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    telegram = FakeTelegram()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    watchdog.telegram = telegram
    incidents = evaluate_incidents(
        state={"status": "flat", "asset": "ETH", "open_lots": []},
        risk_health={
            "updated_at": current[0].isoformat(),
            "risk_action": "warning",
            "risk_reason": "venue_equity_imbalance_warning",
            "risk_notification_action": "warning",
            "risk_notification_reason": "venue_equity_imbalance_warning",
        },
        events=[],
        strategy_running=True,
        config=config(),
        now=current[0],
    )

    watchdog.run_once(synthetic=incidents[0])

    assert bark.sent == []
    assert feishu.messages == []
    assert feishu.phones == []
    assert len(telegram.sent) == 1
    assert "仅提醒" in telegram.sent[0][0]
    assert "双边权益不均衡" in telegram.sent[0][0]
    assert "venue_equity_imbalance_warning" not in telegram.sent[0][0]


def test_failed_channel_retries_without_repeating_successful_channels(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    telegram = FakeTelegram()
    telegram.failures_remaining = 1
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    watchdog.telegram = telegram
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    current[0] += timedelta(seconds=11)
    watchdog.run_once(synthetic=incident)

    assert bark.sent == []
    assert len(feishu.messages) == 1
    assert len(feishu.phones) == 1
    assert len(telegram.sent) == 1


def test_critical_incident_falls_back_to_bark_only_when_primary_paths_fail(
    tmp_path,
) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    feishu.message_failures_remaining = 1
    telegram = FakeTelegram()
    telegram.failures_remaining = 1
    watchdog = RiskWakeupWatchdog(
        config=config(),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        watchdog_control_path=tmp_path / "control.json",
        bark=bark,
        feishu=feishu,
        telegram=telegram,
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)

    assert len(bark.sent) == 1
    assert bark.sent[0]["critical"] is True
    assert feishu.phones == []
    assert len(telegram.sent) == 1


def test_new_critical_reason_realerts_all_channels(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    first = Incident("critical", "critical", "risk", "reason one", ("ETH", "one"))
    second = Incident("critical", "critical", "risk", "reason two", ("ETH", "two"))

    watchdog.run_once(synthetic=first)
    current[0] += timedelta(seconds=5)
    watchdog.run_once(synthetic=second)

    assert bark.sent == []
    assert len(feishu.messages) == 2
    assert len(feishu.phones) == 2


def test_dynamic_message_does_not_realert_same_root_cause(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    first = Incident(
        "critical",
        "critical",
        "risk",
        "heartbeat age 63 seconds",
        ("ETH", "risk"),
        fingerprint="variational_html_response",
    )
    second = Incident(
        "critical",
        "critical",
        "risk",
        "heartbeat age 81 seconds",
        ("ETH", "risk"),
        fingerprint="variational_html_response",
    )

    watchdog.run_once(synthetic=first)
    current[0] += timedelta(seconds=20)
    watchdog.run_once(synthetic=second)

    assert bark.sent == []
    assert len(feishu.messages) == 1
    assert len(feishu.phones) == 1


def test_telegram_acknowledgement_stops_failed_channel_retries(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    bark.failures_remaining = 2
    feishu = FakeFeishu()
    telegram = FakeTelegram()
    watchdog = RiskWakeupWatchdog(
        config=config(channel_retry_seconds=10),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        watchdog_control_path=tmp_path / "control.json",
        bark=bark,
        feishu=feishu,
        telegram=telegram,
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )
    incident = Incident(
        "critical",
        "critical",
        "risk",
        "failure",
        ("ETH", "risk"),
        fingerprint="root-failure",
    )

    watchdog.run_once(synthetic=incident)
    record = watchdog.memory["active_incidents"][incident.key]
    token = record["acknowledgement_token"]
    telegram.updates.append(
        {
            "update_id": 10,
            "callback_query": {
                "id": "callback-1",
                "data": f"risk_ack:{token}",
                "message": {
                    "message_id": 88,
                    "chat": {"id": 123},
                },
            },
        }
    )
    current[0] += timedelta(seconds=11)
    watchdog.run_once(synthetic=incident)

    assert bark.sent == []
    assert record["acknowledged_signature"] == record["incident_signature"]
    assert telegram.answered[-1][0] == "callback-1"
    assert telegram.cleared == [("123", 88)]
    assert telegram.sent[0][1]["inline_keyboard"][0][0]["callback_data"] == (
        f"risk_ack:{token}"
    )


def test_telegram_global_silence_persists_and_stops_delivery(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    telegram = FakeTelegram()
    watchdog = RiskWakeupWatchdog(
        config=config(channel_retry_seconds=10),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        watchdog_control_path=tmp_path / "control.json",
        alert_control_path=tmp_path / "alert-control.json",
        bark=bark,
        feishu=feishu,
        telegram=telegram,
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )
    incident = Incident(
        "critical",
        "critical",
        "risk",
        "failure",
        ("ETH", "risk"),
        fingerprint="root-failure",
    )

    watchdog.run_once(synthetic=incident)
    token = watchdog.memory["active_incidents"][incident.key][
        "acknowledgement_token"
    ]
    telegram.updates.append(
        {
            "update_id": 12,
            "callback_query": {
                "id": "callback-silence",
                "data": f"risk_silence:{token}",
                "message": {"message_id": 90, "chat": {"id": 123}},
            },
        }
    )
    current[0] += timedelta(seconds=11)
    watchdog.run_once(synthetic=incident)

    assert bark.sent == []
    assert len(feishu.messages) == 1
    assert len(telegram.sent) == 1
    assert telegram.answered[-1][1] == "已全局静默 2 小时"


def test_severity_escalation_realerts_after_acknowledgement(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    telegram = FakeTelegram()
    watchdog = RiskWakeupWatchdog(
        config=config(channel_retry_seconds=10),
        state_path=tmp_path / "state.json",
        risk_health_path=tmp_path / "risk.json",
        metrics_path=tmp_path / "metrics.jsonl",
        watchdog_state_path=tmp_path / "watchdog.json",
        watchdog_health_path=tmp_path / "health.json",
        watchdog_control_path=tmp_path / "control.json",
        bark=bark,
        feishu=feishu,
        telegram=telegram,
        clock=lambda: current[0],
        strategy_check=lambda _pid: True,
    )
    warning = Incident(
        "data_visibility",
        "warning",
        "risk",
        "temporarily unavailable",
        ("ETH", "risk"),
        fingerprint="account-data-unavailable",
    )
    critical = Incident(
        "data_visibility",
        "critical",
        "risk",
        "unavailable too long",
        ("ETH", "risk"),
        fingerprint="account-data-unavailable",
    )

    watchdog.run_once(synthetic=warning)
    record = watchdog.memory["active_incidents"][warning.key]
    telegram.updates.append(
        {
            "update_id": 11,
            "callback_query": {
                "id": "callback-2",
                "data": f"risk_ack:{record['acknowledgement_token']}",
                "message": {"message_id": 89, "chat": {"id": 123}},
            },
        }
    )
    current[0] += timedelta(seconds=5)
    watchdog.run_once(synthetic=critical)

    assert bark.sent == []
    assert len(feishu.messages) == 1
    assert len(feishu.phones) == 1
    assert "acknowledged_at" not in record


def test_recovery_sends_non_phone_recovery_notifications(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    incident = Incident("test", "critical", "title", "message", ("ETH", "test"))

    watchdog.run_once(synthetic=incident)
    watchdog.run_once()

    assert bark.sent == []
    assert len(feishu.messages) == 1
    assert len(feishu.phones) == 1


def test_persistent_account_data_loss_with_position_escalates(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    state_path = tmp_path / "state.json"
    risk_path = tmp_path / "risk.json"
    write_json(
        state_path,
        {"status": "open", "asset": "ETH", "open_lots": [{"lot_id": 1}]},
    )
    write_json(
        risk_path,
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "block_entry",
            "risk_reason": "variational_account_snapshot_stale",
            "open_lots_total": 1,
        },
    )
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )

    first = watchdog.run_once()
    assert first[0].severity == "warning"
    assert bark.sent == []
    assert len(feishu.phones) == 0

    current[0] += timedelta(seconds=301)
    write_json(
        risk_path,
        {
            "updated_at": current[0].isoformat(),
            "risk_action": "block_entry",
            "risk_reason": "variational_account_snapshot_stale",
            "open_lots_total": 1,
        },
    )
    promoted = watchdog.run_once()
    assert promoted[0].severity == "critical"
    assert bark.sent == []
    assert len(feishu.phones) == 1


def test_heartbeat_only_mode_suppresses_strategy_incidents(tmp_path) -> None:
    current = [datetime(2026, 8, 30, 16, 0, tzinfo=timezone.utc)]
    bark = FakeBark()
    feishu = FakeFeishu()
    watchdog = build_watchdog(
        tmp_path,
        current=current,
        bark=bark,
        feishu=feishu,
    )
    write_json(tmp_path / "control.json", {"monitor_strategy": False})
    write_json(
        tmp_path / "state.json",
        {"status": "manual_review_required", "open_lots": [{"lot_id": 1}]},
    )

    incidents = watchdog.run_once()
    health = json.loads((tmp_path / "health.json").read_text())

    assert incidents == []
    assert bark.sent == []
    assert feishu.phones == []
    assert health["mode"] == "heartbeat_only"


def test_main_risk_loop_writes_sanitized_heartbeat(tmp_path) -> None:
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_risk_health_file = tmp_path / "risk.json"
    runtime.live_inventory_open_lots = [{"qty": "0.01"}]
    runtime.live_inventory_run_id = "run-1"
    runtime.pending_live_inventory_actions_payload = lambda: []
    runtime.live_inventory_state_asset = lambda: "ETH"

    asyncio.run(
        runtime.write_live_inventory_risk_health(
            {
                "risk_action": "normal",
                "risk_reason": "account_risk_normal",
                "variational_equity_usd": "100",
                "lighter_equity_usd": "100",
            }
        )
    )
    body = json.loads(runtime.live_inventory_risk_health_file.read_text())

    assert body["status"] == "open"
    assert body["expected_open_qty"] == "0.01"
    assert body["open_lots_total"] == 1
    assert body["risk_action"] == "normal"
    assert body["variational_reference_quote_present"] is False
    assert body["variational_reference_quote_fresh"] is False
    assert "private" not in json.dumps(body).lower()
