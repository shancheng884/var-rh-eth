from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import signal
import stat
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows development hosts
    fcntl = None

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import read_json, write_json_atomic
from tools.lib.alert_localization import action_cn, incident_key_cn, reason_cn
from tools.lib.risk_alert_control import (
    notifications_allowed,
    read_alert_control,
    resume_alerts,
    silence_alerts,
    suppression_reason,
)
from tools.lib.risk_wakeup_remote import (
    RemoteHeartbeatPublisher,
    build_heartbeat_payload,
)
from tools.lib.telegram_notifier import TelegramNotifier
from tools.lib.wakeup_notifiers import (
    BarkNotifier,
    FeishuUrgentNotifier,
    private_config_error,
)


STATE_PATH = ROOT / "log" / "live_inventory_state.json"
RISK_HEALTH_PATH = ROOT / "log" / "live_inventory_risk_health.json"
ORDER_METRICS_PATH = ROOT / "log" / "order_metrics.jsonl"
WATCHDOG_STATE_PATH = ROOT / "log" / "risk_wakeup_watchdog_state.json"
WATCHDOG_HEALTH_PATH = ROOT / "log" / "risk_wakeup_watchdog_health.json"
WATCHDOG_CONTROL_PATH = ROOT / "log" / "risk_wakeup_watchdog_control.json"
ALERT_CONTROL_PATH = ROOT / "log" / "risk_wakeup_alert_control.json"
WATCHDOG_LOCK_PATH = ROOT / "log" / "risk_wakeup_watchdog.lock"


CRITICAL_MANUAL_REVIEW_MARKERS = (
    "mismatch",
    "position",
    "submit_failed",
    "hedge_failed",
    "hedge_not_started",
    "pending",
)


def authentication_failure(value: Any) -> bool:
    text = json.dumps(value, ensure_ascii=True, default=str).lower()
    return any(
        marker in text
        for marker in (
            "no token",
            "unauthorized",
            "authentication required",
            "session expired",
            "login required",
            '"httpstatus": 401',
        )
    )


def stable_manual_review_reason(reason: Any, context: Any = None) -> str:
    if authentication_failure(context if context is not None else reason):
        return "authentication_required"
    text = str(reason or "unknown").strip()
    lowered = text.lower()
    if "extension" in lowered or "no token" in lowered:
        return "variational_extension_disconnected"
    if "reference" in lowered and "feed" in lowered:
        return "variational_reference_feed_stale"
    return text or "unknown"


def stable_incident_root_key(key: str) -> str:
    if key.startswith("manual_review:"):
        return "manual_review:" + stable_manual_review_reason(
            key.split(":", 1)[1]
        )
    if key.startswith("unreconciled_manual_review:"):
        return "manual_review:" + stable_manual_review_reason(
            key.split(":", 1)[1]
        )
    return key


def is_persistent_incident_root(key: str) -> bool:
    root = stable_incident_root_key(key)
    return root in {
        "variational_authentication_required",
        "variational_extension_disconnected",
        "variational_reference_feed_stale",
        "manual_review:authentication_required",
        "manual_review:variational_extension_disconnected",
        "manual_review:variational_reference_feed_stale",
        "manual_review:variational_html_response",
        "risk_heartbeat_stale_with_exposure",
    }


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        result = datetime.fromisoformat(text)
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except ValueError:
        return None


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def process_is_strategy(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    command_path = Path("/proc") / str(pid) / "cmdline"
    try:
        command = command_path.read_bytes().replace(b"\x00", b" ").decode(
            "utf-8",
            errors="replace",
        )
    except OSError:
        return False
    if "main.py" not in command or "python" not in command.lower():
        return False
    try:
        process_cwd = Path(os.readlink(f"/proc/{pid}/cwd")).resolve()
        process_cwd.relative_to(ROOT.resolve())
    except (OSError, ValueError):
        return False
    return True


@contextlib.contextmanager
def watchdog_process_lock(path: Path = WATCHDOG_LOCK_PATH):
    """Prevent two watchdogs from polling and delivering the same incident."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        if fcntl is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("risk_wakeup_watchdog_already_running") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True)
class Incident:
    key: str
    severity: str
    title: str
    message: str
    alert_params: tuple[str, ...]
    fingerprint: str | None = None
    rearm_seconds: float = 0.0
    notify_recovery: bool = True


@dataclass(frozen=True)
class WatchdogConfig:
    enabled: bool
    alert_when_flat_strategy_stopped: bool
    poll_seconds: float
    risk_heartbeat_max_age_seconds: float
    pending_action_max_age_seconds: float
    data_unavailable_critical_seconds: float
    max_phone_attempts_per_incident: int
    monitor_strategy: bool = True
    channel_retry_seconds: float = 10.0
    max_channel_attempts_per_incident: int = 3
    reference_feed_flat_stale_seconds: float = 120.0
    reference_feed_exposure_stale_seconds: float = 60.0
    reference_feed_rearm_seconds: float = 1800.0
    reference_feed_recovery_max_attempts: int = 2
    account_risk_rearm_seconds: float = 900.0

    @classmethod
    def from_env(cls) -> "WatchdogConfig":
        return cls(
            enabled=env_bool("RISK_WAKEUP_ENABLED", False),
            alert_when_flat_strategy_stopped=env_bool(
                "RISK_WAKEUP_ALERT_WHEN_FLAT_STRATEGY_STOPPED",
                True,
            ),
            poll_seconds=max(1.0, env_float("RISK_WAKEUP_POLL_SECONDS", 5.0)),
            risk_heartbeat_max_age_seconds=max(
                15.0,
                env_float("RISK_WAKEUP_HEARTBEAT_MAX_AGE_SECONDS", 45.0),
            ),
            pending_action_max_age_seconds=max(
                5.0,
                env_float("RISK_WAKEUP_PENDING_MAX_AGE_SECONDS", 30.0),
            ),
            data_unavailable_critical_seconds=max(
                60.0,
                env_float(
                    "RISK_WAKEUP_DATA_UNAVAILABLE_CRITICAL_SECONDS",
                    300.0,
                ),
            ),
            max_phone_attempts_per_incident=max(
                1,
                env_int("RISK_WAKEUP_MAX_PHONE_ATTEMPTS", 1),
            ),
            monitor_strategy=env_bool(
                "RISK_WAKEUP_MONITOR_STRATEGY",
                False,
            ),
            channel_retry_seconds=max(
                30.0,
                env_float("RISK_WAKEUP_CHANNEL_RETRY_SECONDS", 60.0),
            ),
            max_channel_attempts_per_incident=max(
                1,
                env_int("RISK_WAKEUP_MAX_CHANNEL_ATTEMPTS", 3),
            ),
            reference_feed_flat_stale_seconds=max(
                30.0,
                env_float("RISK_WAKEUP_REFERENCE_FEED_FLAT_STALE_SECONDS", 120.0),
            ),
            reference_feed_exposure_stale_seconds=max(
                30.0,
                env_float(
                    "RISK_WAKEUP_REFERENCE_FEED_EXPOSURE_STALE_SECONDS",
                    60.0,
                ),
            ),
            reference_feed_rearm_seconds=max(
                0.0,
                env_float("RISK_WAKEUP_REFERENCE_FEED_REARM_SECONDS", 1800.0),
            ),
            reference_feed_recovery_max_attempts=max(
                1,
                env_int("RISK_WAKEUP_REFERENCE_FEED_RECOVERY_MAX_ATTEMPTS", 2),
            ),
            account_risk_rearm_seconds=max(
                0.0,
                env_float("RISK_WAKEUP_ACCOUNT_RISK_REARM_SECONDS", 900.0),
            ),
        )


class JsonlFollower:
    def __init__(self, path: Path, *, initial_bytes: int = 2_000_000) -> None:
        self.path = path
        self.initial_bytes = max(4096, initial_bytes)
        self.offset: int | None = None
        self.rows: deque[dict[str, Any]] = deque(maxlen=500)

    def read(self) -> list[dict[str, Any]]:
        try:
            size = self.path.stat().st_size
        except OSError:
            return list(self.rows)
        if self.offset is None or size < self.offset:
            start = max(0, size - self.initial_bytes)
        else:
            start = self.offset
        try:
            with self.path.open("rb") as handle:
                handle.seek(start)
                data = handle.read()
        except OSError:
            return list(self.rows)
        self.offset = start + len(data)
        lines = data.splitlines()
        if start > 0 and lines:
            lines = lines[1:]
        for line in lines:
            try:
                value = json.loads(line.decode("utf-8", errors="replace"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                self.rows.append(value)
        return list(self.rows)


def _pending_age_seconds(item: dict[str, Any], now: datetime) -> float | None:
    for key in (
        "submitted_at",
        "created_at",
        "record_created_at",
        "last_updated_at",
    ):
        started = parse_time(item.get(key))
        if started is not None:
            return max(0.0, (now - started).total_seconds())
    return None


def _pending_action_is_actionable(item: Any) -> bool:
    """Return whether a pending entry represents an order that was touched."""
    if not isinstance(item, dict):
        return False
    context = item.get("context") if isinstance(item.get("context"), dict) else {}
    for key in (
        "submitted_at",
        "created_at",
        "record_created_at",
        "last_updated_at",
        "rfq_id",
        "submitted_order_id",
        "lighter_record_key",
        "execution_unknown_reason",
    ):
        if item.get(key) not in (None, "") or context.get(key) not in (None, ""):
            return True
    return any(
        bool(item.get(key)) or bool(context.get(key))
        for key in (
            "lighter_started",
            "execution_unknown",
            "reconciliation_required",
        )
    )


def evaluate_incidents(
    *,
    state: dict[str, Any],
    risk_health: dict[str, Any],
    events: list[dict[str, Any]],
    strategy_running: bool,
    config: WatchdogConfig,
    now: datetime,
) -> list[Incident]:
    incidents: list[Incident] = []
    open_lots = state.get("open_lots") if isinstance(state.get("open_lots"), list) else []
    state_pending = state.get("pending_actions")
    pending = state_pending if isinstance(state_pending, list) else []
    actionable_pending = [
        item for item in pending if _pending_action_is_actionable(item)
    ]
    try:
        heartbeat_open_lots = int(risk_health.get("open_lots_total") or 0)
    except (TypeError, ValueError):
        heartbeat_open_lots = 0
    try:
        heartbeat_pending = int(risk_health.get("pending_actions_total") or 0)
    except (TypeError, ValueError):
        heartbeat_pending = 0
    open_lot_count = max(len(open_lots), heartbeat_open_lots)
    # A state-file list is authoritative when present. The runtime records an
    # intent before touching either venue; those empty intents are not stale
    # executions and must not be revived by the heartbeat aggregate.
    pending_count = (
        len(actionable_pending)
        if isinstance(state_pending, list)
        else heartbeat_pending
    )
    exposure = bool(open_lot_count or pending_count)
    asset = str(state.get("asset") or risk_health.get("asset") or "ETH")
    status = str(state.get("status") or "unknown")
    reason = str(
        state.get("manual_review_reason")
        or state.get("last_blocked_reason")
        or "unknown"
    )
    manual_review_context = state.get("manual_review_context")
    manual_review_root = stable_manual_review_reason(
        reason,
        manual_review_context,
    )
    lot_text = f"未平子单 {open_lot_count}，待确认动作 {pending_count}"

    if (
        not exposure
        and not strategy_running
        and config.alert_when_flat_strategy_stopped
    ):
        incidents.append(
            Incident(
                key="strategy_stopped_flat",
                severity="warning",
                title="套利策略已停止",
                message=f"{asset}：当前空仓，但主策略进程未运行。",
                alert_params=(asset, "策略已停止"),
            )
        )

    if exposure and not strategy_running:
        incidents.append(
            Incident(
                key="strategy_stopped_with_exposure",
                severity="critical",
                title="套利策略停止但仍有仓位",
                message=f"{asset}：主策略已停止；{lot_text}。请立即检查双边仓位。",
                alert_params=(asset, "策略停止且仍有仓位"),
            )
        )

    if status == "manual_review_required":
        if manual_review_root == "authentication_required":
            incidents.append(
                Incident(
                    key="variational_authentication_required",
                    severity="critical" if exposure else "warning",
                    title="Variational 需要重新登录",
                    message=(
                        f"{asset}：Variational 会话已失效（{reason_cn(reason)}）。"
                        "请重新登录 Variational、刷新页面和扩展后再继续执行。"
                    ),
                    alert_params=(asset, "请重新登录 Variational"),
                )
            )
        else:
            critical = exposure or any(
                marker in reason for marker in CRITICAL_MANUAL_REVIEW_MARKERS
            )
            incidents.append(
                Incident(
                    key=f"manual_review:{manual_review_root}",
                    severity="critical" if critical else "warning",
                    title="套利账户需要人工核对",
                    message=f"{asset}：原因 {reason_cn(reason)}；{lot_text}。",
                    alert_params=(asset, "双边账户需要人工核对"),
                )
            )

    if pending_count:
        ages = [
            age
            for age in (
                _pending_age_seconds(item, now) for item in actionable_pending
            )
            if age is not None
        ]
        state_updated_at = parse_time(state.get("updated_at"))
        oldest = max(ages) if ages else (
            max(0.0, (now - state_updated_at).total_seconds())
            if state_updated_at is not None
            else None
        )
        if (oldest is not None and oldest >= config.pending_action_max_age_seconds) or not strategy_running:
            incidents.append(
                Incident(
                    key="pending_action_stale",
                    severity="critical",
                    title="套利双边成交确认超时",
                    message=(
                        f"{asset}：{pending_count} 个动作未完成，"
                        f"最长等待 {oldest:.0f} 秒。"
                        if oldest is not None
                        else f"{asset}：{pending_count} 个动作未完成。"
                    ),
                    alert_params=(asset, "双边成交确认超时"),
                )
            )

    health_at = parse_time(risk_health.get("updated_at"))
    health_age = (
        max(0.0, (now - health_at).total_seconds())
        if health_at is not None
        else None
    )
    if exposure and (
        health_age is None
        or health_age > config.risk_heartbeat_max_age_seconds
    ):
        age_text = "不可用" if health_age is None else f"{health_age:.0f} 秒"
        incidents.append(
            Incident(
                key="risk_heartbeat_stale_with_exposure",
                severity="critical",
                title="套利账户风险监控失联",
                message=f"{asset}：风险心跳年龄 {age_text}；{lot_text}。",
                alert_params=(asset, "账户风险监控失联"),
            )
        )

    raw_action = str(risk_health.get("risk_action") or "normal")
    raw_reason = str(risk_health.get("risk_reason") or "account_risk_normal")
    notification_action = str(
        risk_health.get("risk_notification_action") or raw_action
    )
    notification_reason = str(
        risk_health.get("risk_notification_reason") or raw_reason
    )
    action = (
        raw_action
        if raw_action in {"force_reduce", "emergency_exit"}
        else notification_action
    )
    risk_reason = (
        raw_reason
        if raw_action in {"force_reduce", "emergency_exit"}
        else notification_reason
    )
    if raw_action in {"force_reduce", "emergency_exit"}:
        margin = risk_health.get("max_maintenance_margin_usage_pct")
        leverage = risk_health.get("max_projected_venue_leverage")
        incidents.append(
            Incident(
                key=f"account_risk:{action}:{risk_reason}",
                severity="critical",
                title="套利账户触发强风险动作",
                message=(
                    f"{asset}：动作 {action_cn(action)}，原因 {reason_cn(risk_reason)}，"
                    f"最高保证金使用率 {margin or '-'}%，"
                    f"最高单边杠杆 {leverage or '-'}x。"
                ),
                alert_params=(asset, "保证金风险已触发强制处理"),
            )
        )
    elif action in {"warning", "block_entry"}:
        data_visibility_risk = exposure and risk_reason in {
            "variational_account_snapshot_stale",
            "account_equity_unavailable",
            "lighter_account_equity_unavailable",
            "variational_account_equity_unavailable",
            "both_accounts_equity_unavailable",
        }
        incidents.append(
            Incident(
                key=(
                    f"data_visibility:{risk_reason}"
                    if data_visibility_risk
                    else f"account_risk:{action}:{risk_reason}"
                ),
                severity="warning",
                title="套利账户风险提醒",
                message=f"{asset}：动作 {action_cn(action)}，原因 {reason_cn(risk_reason)}。",
                alert_params=(asset, "账户风险提醒"),
                rearm_seconds=config.account_risk_rearm_seconds,
            )
        )

    if exposure:
        latest_manual_review = next(
            (
                row
                for row in reversed(events)
                if row.get("event") == "live_inventory_manual_review_required"
            ),
            None,
        )
        latest_reconcile = next(
            (
                row
                for row in reversed(events)
                if row.get("event") == "live_inventory_startup_reconcile_ok"
            ),
            None,
        )
        manual_at = parse_time(
            latest_manual_review.get("logged_at") if latest_manual_review else None
        )
        reconcile_at = parse_time(
            latest_reconcile.get("logged_at") if latest_reconcile else None
        )
        if latest_manual_review and manual_at is not None and (
            reconcile_at is None or manual_at > reconcile_at
        ):
            event_reason = str(latest_manual_review.get("reason") or "unknown")
            event_root = stable_manual_review_reason(
                event_reason,
                latest_manual_review.get("manual_review_context"),
            )
            incidents.append(
                Incident(
                    key=f"unreconciled_manual_review:{event_root}",
                    severity="critical",
                    title="套利双边状态尚未重新核对",
                    message=f"{asset}：故障 {reason_cn(event_reason)} 后尚无成功双边核对；{lot_text}。",
                    alert_params=(asset, "双边仓位尚未重新核对"),
                )
            )

    # The strategy deliberately rejects an old passive reference. Alert only
    # after the persisted health file proves that the reference stream has
    # been stale for a sustained interval, not on every changing age value.
    reference_fresh_raw = risk_health.get("variational_reference_quote_fresh")
    if "variational_reference_quote_fresh" in risk_health:
        if isinstance(reference_fresh_raw, bool):
            reference_fresh = reference_fresh_raw
        else:
            reference_fresh = str(reference_fresh_raw).strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        try:
            reference_stale_seconds = float(
                risk_health.get("variational_reference_quote_stale_seconds")
            )
        except (TypeError, ValueError):
            reference_stale_seconds = None
        stale_limit = (
            config.reference_feed_exposure_stale_seconds
            if exposure
            else config.reference_feed_flat_stale_seconds
        )
        recovery_state = str(
            risk_health.get("variational_reference_feed_recovery_state") or ""
        ).strip().lower()
        try:
            recovery_attempt = int(
                risk_health.get(
                    "variational_reference_feed_recovery_attempt",
                    0,
                )
                or 0
            )
        except (TypeError, ValueError):
            recovery_attempt = 0
        if recovery_state == "authentication_required":
            incidents.append(
                Incident(
                    key="variational_authentication_required",
                    severity="critical" if exposure else "warning",
                    title="Variational 需要重新登录",
                    message=(
                        f"{asset}：Variational 会话已失效。"
                        "请重新登录 Variational、刷新页面和扩展后再继续执行。"
                    ),
                    alert_params=(asset, "请重新登录 Variational"),
                )
            )
        recovery_in_progress = recovery_state in {
            "stale_grace",
            "repairing",
            "waiting_for_fresh_data",
            "reconciling",
            "reload_failed",
            "authentication_required",
        } and recovery_attempt <= config.reference_feed_recovery_max_attempts
        if (
            not reference_fresh
            and reference_stale_seconds is not None
            and reference_stale_seconds >= stale_limit
            and not recovery_in_progress
        ):
            incident_message = (
                f"{asset}：Variational 参考价流已连续失联，"
                + (
                    f"当前存在{lot_text}，无法安全继续执行。"
                    if exposure
                    else "策略暂时无法安全判断开仓机会。"
                )
            )
            incidents.append(
                Incident(
                    key="variational_reference_feed_stale",
                    severity="critical" if exposure else "warning",
                    title=(
                        "持仓期间参考价流失联"
                        if exposure
                        else "Variational 参考价流失联"
                    ),
                    message=incident_message,
                    alert_params=(
                        asset,
                        "持仓期间参考价流失联"
                        if exposure
                        else "参考价流失联，无法判断开仓",
                    ),
                    fingerprint=(
                        f"{asset}:variational_reference_feed_stale:"
                        f"{'exposure' if exposure else 'flat'}"
                    ),
                    rearm_seconds=config.reference_feed_rearm_seconds,
                    notify_recovery=False,
                )
            )
    critical = [item for item in incidents if item.severity == "critical"]
    if critical:
        messages = list(dict.fromkeys(item.message for item in critical))
        # The incident identity must describe the fault episode, never its
        # changing age, lot count, or the set of secondary consequences.  The
        # durable manual-review reason is the strongest root cause when it is
        # available; this keeps a stopped strategy from becoming a new alert
        # every polling cycle.
        if status == "manual_review_required" and manual_review_root != "unknown":
            fingerprint_keys = {f"manual_review:{manual_review_root}"}
        else:
            fingerprint_keys = set()
            for item in critical:
                fingerprint_key = stable_incident_root_key(item.key)
                if fingerprint_key.startswith("unreconciled_manual_review:"):
                    fingerprint_key = "manual_review:" + fingerprint_key.split(":", 1)[1]
                fingerprint_keys.add(fingerprint_key)
            if len(fingerprint_keys) > 1:
                # A stale heartbeat is normally a consequence of another
                # explicit fault. Do not turn its changing age into a second
                # alarm episode.
                fingerprint_keys.discard("risk_heartbeat_stale_with_exposure")
        episode_id = (
            "persistent"
            if fingerprint_keys and all(
                is_persistent_incident_root(key)
                for key in fingerprint_keys
            )
            else str(
                state.get("run_id")
                or risk_health.get("run_id")
                or state.get("started_at")
                or "unknown"
            )
        )
        stable_fingerprint = "|".join(
            [asset, episode_id, *sorted(fingerprint_keys)]
        )
        incidents = [item for item in incidents if item.severity != "critical"]
        incidents.append(
            Incident(
                key="critical_account_risk",
                severity="critical",
                title="Var/Lighter 账户紧急风险",
                message="\n".join(messages),
                alert_params=(asset, "账户出现紧急风险，请立即检查"),
                fingerprint=stable_fingerprint,
                rearm_seconds=(
                    config.reference_feed_rearm_seconds
                    if all(
                        item.key == "variational_reference_feed_stale"
                        for item in critical
                    )
                    else 0.0
                ),
                notify_recovery=not all(
                    item.key == "variational_reference_feed_stale"
                    for item in critical
                ),
            )
        )

    unique: dict[str, Incident] = {}
    for incident in incidents:
        unique[incident.key] = incident
    return list(unique.values())


class RiskWakeupWatchdog:
    def __init__(
        self,
        *,
        config: WatchdogConfig,
        state_path: Path = STATE_PATH,
        risk_health_path: Path = RISK_HEALTH_PATH,
        metrics_path: Path = ORDER_METRICS_PATH,
        watchdog_state_path: Path = WATCHDOG_STATE_PATH,
        watchdog_health_path: Path = WATCHDOG_HEALTH_PATH,
        watchdog_control_path: Path = WATCHDOG_CONTROL_PATH,
        alert_control_path: Path = ALERT_CONTROL_PATH,
        bark: BarkNotifier | None = None,
        feishu: FeishuUrgentNotifier | None = None,
        telegram: TelegramNotifier | None = None,
        remote_publisher: RemoteHeartbeatPublisher | None = None,
        clock: Callable[[], datetime] = utc_now,
        strategy_check: Callable[[int | None], bool] = process_is_strategy,
        dry_run: bool = False,
    ) -> None:
        self.config = config
        self.state_path = state_path
        self.risk_health_path = risk_health_path
        self.watchdog_state_path = watchdog_state_path
        self.watchdog_health_path = watchdog_health_path
        self.watchdog_control_path = watchdog_control_path
        self.alert_control_path = alert_control_path
        self.bark = bark or BarkNotifier.from_config()
        self.feishu = feishu or FeishuUrgentNotifier.from_config()
        self.telegram = telegram or TelegramNotifier.from_env(
            logger=logging.getLogger("risk_wakeup_watchdog.telegram")
        )
        self.remote_publisher = remote_publisher or RemoteHeartbeatPublisher.from_env(
            logger=logging.getLogger("risk_wakeup_watchdog.remote")
        )
        self.clock = clock
        self.strategy_check = strategy_check
        self.dry_run = dry_run
        self.metrics = JsonlFollower(metrics_path)
        self.memory = read_json(watchdog_state_path)
        if not isinstance(self.memory.get("active_incidents"), dict):
            self.memory["active_incidents"] = {}
        if not isinstance(self.memory.get("incident_rearm_until"), dict):
            self.memory["incident_rearm_until"] = {}
        self.stop_requested = False

    def configuration_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.bark.enabled:
            errors.append("bark_not_configured")
        if not self.feishu.enabled:
            errors.append("feishu_not_configured")
        for label, notifier in (("bark", self.bark), ("feishu", self.feishu)):
            config_path = getattr(notifier, "config_path", None)
            if config_path is None:
                continue
            problem = private_config_error(config_path)
            if problem:
                errors.append(f"{label}_config_{problem}")
        env_path = ROOT / ".env"
        if not env_path.exists():
            errors.append("env_file_missing")
        elif os.name != "nt":
            try:
                permissions = stat.S_IMODE(env_path.stat().st_mode)
            except OSError:
                errors.append("env_file_permissions_unavailable")
            else:
                if permissions & 0o077:
                    errors.append("env_file_permissions_not_600")
        return errors

    def monitor_strategy_enabled(self) -> bool:
        control = read_json(self.watchdog_control_path)
        value = control.get("monitor_strategy")
        if isinstance(value, bool):
            return value
        return self.config.monitor_strategy

    def alert_suppression_reason(self, *, now: datetime) -> str | None:
        return suppression_reason(
            read_alert_control(self.alert_control_path),
            now=now,
        )

    def alerts_allowed(self, *, now: datetime) -> bool:
        return notifications_allowed(
            read_alert_control(self.alert_control_path),
            now=now,
        )

    def _notify_telegram(
        self,
        message: str,
        *,
        acknowledgement_token: str | None = None,
        record: dict[str, Any] | None = None,
    ) -> bool:
        now = self.clock()
        if self.dry_run:
            if record is not None:
                record["telegram_status"] = "dry_run"
            return False
        if not self.telegram.enabled:
            if record is not None:
                record["telegram_status"] = "not_configured"
            return False
        if not self.alerts_allowed(now=now):
            if record is not None:
                record["telegram_status"] = "suppressed"
            return False
        reply_markup = None
        if acknowledgement_token:
            reply_markup = {
                "inline_keyboard": [
                    [
                        {
                            "text": "我已知晓，停止重复提醒",
                            "callback_data": (
                                f"risk_ack:{acknowledgement_token}"
                            ),
                        },
                    ],
                    [
                        {
                            "text": "全局静默 2 小时",
                            "callback_data": (
                                f"risk_silence:{acknowledgement_token}"
                            ),
                        },
                    ]
                ]
            }
        try:
            try:
                result = self.telegram.send_now(
                    message,
                    reply_markup=reply_markup,
                )
            except TypeError:
                # Keeps custom notifiers compatible while the built-in notifier
                # supports Telegram inline keyboards.
                result = self.telegram.send_now(message)
        except Exception as exc:
            if record is not None:
                record["telegram_attempts"] = int(
                    record.get("telegram_attempts") or 0
                ) + 1
                record["last_telegram_attempt_at"] = iso_time(now)
                record["telegram_status"] = (
                    f"telegram_failed:{type(exc).__name__}"
                )
            return False
        if isinstance(result, (tuple, list)):
            ok = bool(result[0]) if result else False
            detail = str(result[1]) if len(result) > 1 else (
                "sent" if ok else "telegram_failed"
            )
        else:
            ok = bool(result)
            detail = "sent" if ok else "telegram_failed"
        if record is not None:
            record["telegram_attempts"] = int(
                record.get("telegram_attempts") or 0
            ) + 1
            record["last_telegram_attempt_at"] = iso_time(now)
            record["telegram_status"] = "sent" if ok else detail
            if ok:
                record["telegram_sent_at"] = iso_time(now)
        return ok

    @staticmethod
    def _incident_signature(incident: Incident) -> str:
        source = (
            f"{incident.severity}\n{incident.fingerprint}"
            if incident.fingerprint is not None
            else f"{incident.severity}\n{incident.title}\n{incident.message}"
        )
        return hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _new_acknowledgement_token(
        incident: Incident,
        *,
        now: datetime,
    ) -> str:
        source = f"{incident.key}\n{iso_time(now)}\n{time.time_ns()}"
        return hashlib.sha256(source.encode("utf-8")).hexdigest()[:20]

    @staticmethod
    def _reset_delivery_state(record: dict[str, Any]) -> None:
        for key in (
            "bark_status",
            "bark_sent_at",
            "last_bark_attempt_at",
            "feishu_message_status",
            "feishu_message_sent_at",
            "feishu_message_id",
            "last_feishu_message_attempt_at",
            "feishu_phone_status",
            "feishu_phone_sent_at",
            "last_feishu_phone_attempt_at",
            "telegram_status",
            "telegram_sent_at",
            "last_telegram_attempt_at",
            "acknowledged_at",
            "acknowledged_by",
            "acknowledged_signature",
        ):
            record.pop(key, None)
        record["bark_attempts"] = 0
        record["feishu_message_attempts"] = 0
        record["feishu_phone_attempts"] = 0
        record["telegram_attempts"] = 0

    def _poll_telegram_acknowledgements(self, *, now: datetime) -> None:
        if self.dry_run or not self.telegram.enabled:
            return
        get_updates = getattr(self.telegram, "get_updates", None)
        if not callable(get_updates):
            return
        offset_value = self.memory.get("telegram_update_offset")
        try:
            offset = int(offset_value) if offset_value is not None else None
        except (TypeError, ValueError):
            offset = None
        try:
            updates, detail = get_updates(offset=offset)
        except Exception as exc:
            logging.getLogger("risk_wakeup_watchdog").warning(
                "telegram_ack_poll_failed detail=exception:%s",
                type(exc).__name__,
            )
            return
        if detail != "ok":
            logging.getLogger("risk_wakeup_watchdog").warning(
                "telegram_ack_poll_failed detail=%s",
                detail,
            )
            return
        next_offset = offset
        active = self.memory["active_incidents"]
        for update in updates:
            try:
                update_id = int(update.get("update_id"))
            except (TypeError, ValueError):
                continue
            next_offset = max(next_offset or 0, update_id + 1)
            callback = update.get("callback_query")
            if not isinstance(callback, dict):
                continue
            data = str(callback.get("data") or "")
            if not (
                data.startswith("risk_ack:")
                or data.startswith("risk_silence:")
            ):
                continue
            message = callback.get("message")
            message = message if isinstance(message, dict) else {}
            chat = message.get("chat")
            chat = chat if isinstance(chat, dict) else {}
            callback_chat_id = str(chat.get("id") or "")
            if callback_chat_id != str(getattr(self.telegram, "chat_id", "")):
                continue
            action, token = data.split(":", 1)
            matched = False
            for record in active.values():
                if not isinstance(record, dict):
                    continue
                if str(record.get("acknowledgement_token") or "") != token:
                    continue
                matched = True
                if action == "risk_ack":
                    record["acknowledged_at"] = iso_time(now)
                    record["acknowledged_by"] = callback_chat_id
                    record["acknowledged_signature"] = record.get(
                        "incident_signature"
                    )
                break
            if matched and action == "risk_silence":
                silence_alerts(
                    self.alert_control_path,
                    minutes=120,
                    reason="telegram_global_silence",
                    now=now,
                )
                for record in active.values():
                    if not isinstance(record, dict):
                        continue
                    record["acknowledged_at"] = iso_time(now)
                    record["acknowledged_by"] = callback_chat_id
                    record["acknowledged_signature"] = record.get(
                        "incident_signature"
                    )
            answer = getattr(self.telegram, "answer_callback_query", None)
            if callable(answer):
                try:
                    answer(
                        str(callback.get("id") or ""),
                        text=(
                            "已全局静默 2 小时"
                            if matched and action == "risk_silence"
                            else "已停止本次故障的重复提醒"
                            if matched
                            else "该故障已恢复或失效"
                        ),
                    )
                except Exception as exc:
                    logging.getLogger("risk_wakeup_watchdog").warning(
                        "telegram_ack_answer_failed detail=exception:%s",
                        type(exc).__name__,
                    )
            clear_keyboard = getattr(self.telegram, "clear_inline_keyboard", None)
            if matched and callable(clear_keyboard):
                try:
                    message_id = int(message.get("message_id"))
                except (TypeError, ValueError):
                    message_id = 0
                if message_id:
                    try:
                        clear_keyboard(
                            chat_id=callback_chat_id,
                            message_id=message_id,
                        )
                    except Exception as exc:
                        logging.getLogger("risk_wakeup_watchdog").warning(
                            "telegram_ack_keyboard_clear_failed detail=exception:%s",
                            type(exc).__name__,
                        )
        if next_offset is not None:
            self.memory["telegram_update_offset"] = next_offset

    def _persist(self, *, now: datetime) -> None:
        self.memory["schema_version"] = 1
        self.memory["updated_at"] = iso_time(now)
        write_json_atomic(self.watchdog_state_path, self.memory)

    def _write_health(
        self,
        *,
        now: datetime,
        strategy_running: bool,
        incidents: list[Incident],
    ) -> None:
        control = read_alert_control(self.alert_control_path)
        write_json_atomic(
            self.watchdog_health_path,
            {
                "schema_version": 1,
                "updated_at": iso_time(now),
                "status": "running" if self.config.enabled else "disabled",
                "strategy_running": strategy_running,
                "active_incidents": [item.key for item in incidents],
                "critical_incidents": sum(
                    item.severity == "critical" for item in incidents
                ),
                "monitor_strategy": self.monitor_strategy_enabled(),
                "mode": (
                    "strategy_monitoring"
                    if self.monitor_strategy_enabled()
                    else "heartbeat_only"
                ),
                "bark_configured": self.bark.enabled,
                "feishu_configured": self.feishu.enabled,
                "telegram_configured": self.telegram.enabled,
                "dry_run": self.dry_run,
                "alerts_allowed": notifications_allowed(control, now=now),
                "alert_suppression_reason": suppression_reason(control, now=now),
                "alert_control_updated_at": control.get("updated_at"),
                "pid": os.getpid(),
            },
        )

    def _send_new_incident(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        self._notify_telegram(
            "\n".join(
                [
                    "[Var/Lighter] 独立风险监控",
                    f"级别：{'紧急' if incident.severity == 'critical' else '提醒'}",
                    incident.message,
                ]
            ),
            acknowledgement_token=str(
                record.get("acknowledgement_token") or ""
            ),
            record=record,
        )
        if not self.dry_run:
            self._deliver_incident(incident, record, now=now, force=True)

    def _retry_due(
        self,
        record: dict[str, Any],
        key: str,
        *,
        now: datetime,
        force: bool,
    ) -> bool:
        if force:
            return True
        attempted_at = parse_time(record.get(key))
        return attempted_at is None or (
            now - attempted_at
        ).total_seconds() >= self.config.channel_retry_seconds

    def _send_bark(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
        force: bool = False,
    ) -> None:
        if (
            self.dry_run
            or record.get("bark_status") == "sent"
            or not self.alerts_allowed(now=now)
            or int(record.get("bark_attempts") or 0)
            >= self.config.max_channel_attempts_per_incident
        ):
            return
        if not self._retry_due(
            record,
            "last_bark_attempt_at",
            now=now,
            force=force,
        ):
            return
        record["last_bark_attempt_at"] = iso_time(now)
        record["bark_attempts"] = int(record.get("bark_attempts") or 0) + 1
        result = self.bark.send(
            title=incident.title,
            message=incident.message,
            critical=incident.severity == "critical",
        )
        record["bark_status"] = result.detail
        if result.ok:
            record["bark_sent_at"] = iso_time(now)

    def _send_feishu_message(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
        force: bool = False,
    ) -> None:
        if (
            self.dry_run
            or record.get("feishu_message_status") == "sent"
            or not self.alerts_allowed(now=now)
            or int(record.get("feishu_message_attempts") or 0)
            >= self.config.max_channel_attempts_per_incident
        ):
            return
        if not self._retry_due(
            record,
            "last_feishu_message_attempt_at",
            now=now,
            force=force,
        ):
            return
        record["last_feishu_message_attempt_at"] = iso_time(now)
        record["feishu_message_attempts"] = int(
            record.get("feishu_message_attempts") or 0
        ) + 1
        result = self.feishu.send_message(
            title=incident.title,
            message=incident.message,
        )
        record["feishu_message_status"] = result.detail
        if result.ok:
            record["feishu_message_sent_at"] = iso_time(now)
            record["feishu_message_id"] = result.receipt

    def _send_feishu_phone(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
        force: bool = False,
    ) -> None:
        if (
            incident.severity != "critical"
            or self.dry_run
            or record.get("feishu_phone_status") == "sent"
            or not self.alerts_allowed(now=now)
        ):
            return
        message_id = str(record.get("feishu_message_id") or "")
        if not message_id:
            return
        attempts = int(record.get("feishu_phone_attempts") or 0)
        if attempts >= self.config.max_phone_attempts_per_incident:
            return
        if not self._retry_due(
            record,
            "last_feishu_phone_attempt_at",
            now=now,
            force=force,
        ):
            return
        record["feishu_phone_attempts"] = attempts + 1
        record["last_feishu_phone_attempt_at"] = iso_time(now)
        result = self.feishu.phone_urgent(message_id)
        record["feishu_phone_status"] = result.detail
        if result.ok:
            record["feishu_phone_sent_at"] = iso_time(now)

    def _deliver_incident(
        self,
        incident: Incident,
        record: dict[str, Any],
        *,
        now: datetime,
        force: bool = False,
    ) -> None:
        if (
            not self.alerts_allowed(now=now)
            or
            record.get("acknowledged_at")
            and record.get("acknowledged_signature")
            == record.get("incident_signature")
        ):
            return
        # Warnings are intentionally Telegram-only. The Feishu message is
        # reserved for critical events because it is also required by the
        # urgent-phone API.
        if incident.severity != "critical":
            return
        self._send_feishu_message(incident, record, now=now, force=force)
        self._send_feishu_phone(incident, record, now=now, force=force)
        # Bark is a last-resort path, not a third routine copy of the alert.
        # It is sent only when neither Telegram nor the Feishu phone path
        # delivered the critical incident.
        if (
            record.get("telegram_status") != "sent"
            and record.get("feishu_phone_status") != "sent"
        ):
            self._send_bark(incident, record, now=now, force=force)

    def _recover_incident(
        self,
        key: str,
        record: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        if not bool(record.get("notify_recovery", True)):
            return
        if not self.alerts_allowed(now=now):
            return
        message = "\n".join(
            [
                "[Var/Lighter] 风险监控已恢复",
                f"事件：{incident_key_cn(key)}",
                f"恢复时间：{iso_time(now)}",
            ]
        )
        self._notify_telegram(message)

    def run_once(
        self,
        *,
        synthetic: Incident | None = None,
        force_delivery: bool = False,
    ) -> list[Incident]:
        now = self.clock()
        self._poll_telegram_acknowledgements(now=now)
        state = read_json(self.state_path)
        risk_health = read_json(self.risk_health_path)
        events = self.metrics.read()
        pid_value = risk_health.get("pid")
        try:
            pid = int(pid_value) if pid_value not in (None, "") else None
        except (TypeError, ValueError):
            pid = None
        strategy_running = self.strategy_check(pid)
        incidents = [synthetic] if synthetic is not None else []
        if synthetic is None and self.monitor_strategy_enabled():
            incidents = evaluate_incidents(
                state=state,
                risk_health=risk_health,
                events=events,
                strategy_running=strategy_running,
                config=self.config,
                now=now,
            )
            rearm_until = self.memory["incident_rearm_until"]
            for key, value in list(rearm_until.items()):
                deadline = parse_time(value)
                if deadline is None or deadline <= now:
                    rearm_until.pop(key, None)
            incidents = [
                incident
                for incident in incidents
                if (
                    (deadline := parse_time(rearm_until.get(incident.key))) is None
                    or deadline <= now
                )
            ]
        active = self.memory["active_incidents"]
        if not any(item.severity == "critical" for item in incidents):
            promoted: list[Incident] = []
            for incident in incidents:
                record = active.get(incident.key)
                first_seen = parse_time(
                    record.get("first_seen_at")
                    if isinstance(record, dict)
                    else None
                )
                if (
                    incident.severity == "warning"
                    and incident.key.startswith("data_visibility:")
                    and first_seen is not None
                    and (now - first_seen).total_seconds()
                    >= self.config.data_unavailable_critical_seconds
                ):
                    incident = Incident(
                        key=incident.key,
                        severity="critical",
                        title="持仓期间双边账户数据持续失联",
                        message=incident.message
                        + " 数据持续不可用，请立即检查双边账户。",
                        alert_params=(
                            incident.alert_params[0]
                            if incident.alert_params
                            else "ETH",
                            "持仓期间账户数据持续失联",
                        ),
                    )
                promoted.append(incident)
            incidents = promoted
        current_keys = {item.key for item in incidents}
        for incident in incidents:
            signature = self._incident_signature(incident)
            record = active.get(incident.key)
            if not isinstance(record, dict):
                record = {
                    "first_seen_at": iso_time(now),
                    "severity": incident.severity,
                    "title": incident.title,
                    "message": incident.message,
                    "incident_signature": signature,
                    "acknowledgement_token": (
                        self._new_acknowledgement_token(incident, now=now)
                    ),
                    "rearm_seconds": incident.rearm_seconds,
                    "notify_recovery": incident.notify_recovery,
                }
                active[incident.key] = record
                self._send_new_incident(incident, record, now=now)
            previous_signature = record.get("incident_signature")
            if previous_signature and previous_signature != signature:
                self._reset_delivery_state(record)
                record["acknowledgement_token"] = (
                    self._new_acknowledgement_token(incident, now=now)
                )
                if incident.severity == "critical":
                    record["critical_since_at"] = iso_time(now)
                record["incident_signature"] = signature
                self._notify_telegram(
                    "\n".join(
                        [
                            "[Var/Lighter] 风险原因已变化",
                            incident.message,
                        ]
                    ),
                    acknowledgement_token=str(
                        record.get("acknowledgement_token") or ""
                    ),
                    record=record,
                )
                self._deliver_incident(
                    incident,
                    record,
                    now=now,
                    force=True,
                )
            elif not record.get("acknowledgement_token"):
                record["acknowledgement_token"] = (
                    self._new_acknowledgement_token(incident, now=now)
                )
                self._notify_telegram(
                    "\n".join(
                        [
                            "[Var/Lighter] 当前风险确认入口",
                            incident.message,
                        ]
                    ),
                    acknowledgement_token=str(
                        record["acknowledgement_token"]
                    ),
                    record=record,
                )
            if (
                incident.severity == "critical"
                and record.get("severity") != "critical"
            ):
                record["critical_since_at"] = iso_time(now)
            record["severity"] = incident.severity
            record["incident_signature"] = signature
            record["title"] = incident.title
            record["message"] = incident.message
            record["rearm_seconds"] = incident.rearm_seconds
            record["notify_recovery"] = incident.notify_recovery
            record["last_seen_at"] = iso_time(now)
            self._deliver_incident(
                incident,
                record,
                now=now,
                force=force_delivery,
            )
        for key in list(active):
            if key in current_keys:
                continue
            record = active.pop(key)
            if isinstance(record, dict):
                try:
                    rearm_seconds = max(0.0, float(record.get("rearm_seconds") or 0.0))
                except (TypeError, ValueError):
                    rearm_seconds = 0.0
                if rearm_seconds > 0:
                    deadline = iso_time(now + timedelta(seconds=rearm_seconds))
                    self.memory["incident_rearm_until"][key] = deadline
                    if key == "critical_account_risk":
                        # A feed-only critical event is aggregated under the
                        # generic critical key. Suppress its flat warning too
                        # while the same outage is in the rearm window.
                        self.memory["incident_rearm_until"][
                            "variational_reference_feed_stale"
                        ] = deadline
                self._recover_incident(key, record, now=now)
        self._persist(now=now)
        self._write_health(
            now=now,
            strategy_running=strategy_running,
            incidents=incidents,
        )
        self.remote_publisher.publish(
            build_heartbeat_payload(
                node_id=self.remote_publisher.node_id,
                state=state,
                risk_health=risk_health,
                strategy_running=strategy_running,
                watchdog_memory=self.memory,
                alert_control=read_alert_control(self.alert_control_path),
                sent_at=now,
            )
        )
        return incidents

    def run_forever(self) -> None:
        while not self.stop_requested:
            try:
                self.run_once()
            except Exception as exc:
                logging.getLogger("risk_wakeup_watchdog").exception(
                    "watchdog_cycle_failed error=%s",
                    type(exc).__name__,
                )
            time.sleep(self.config.poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Independent Var/Lighter account-risk wakeup watchdog",
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test-alert", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--silence-alerts", action="store_true")
    parser.add_argument("--resume-alerts", action="store_true")
    parser.add_argument("--ack-active-incidents", action="store_true")
    parser.add_argument("--silence-minutes", type=float, default=120.0)
    parser.add_argument("--maintenance-start", action="store_true")
    parser.add_argument("--maintenance-stop", action="store_true")
    monitor = parser.add_mutually_exclusive_group()
    monitor.add_argument("--enable-strategy-monitor", action="store_true")
    monitor.add_argument("--disable-strategy-monitor", action="store_true")
    return parser


def main() -> int:
    load_dotenv(ROOT / ".env")
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    config = WatchdogConfig.from_env()
    watchdog = RiskWakeupWatchdog(config=config, dry_run=args.dry_run)
    control_actions = sum(
        bool(value)
        for value in (
            args.silence_alerts,
            args.resume_alerts,
            args.ack_active_incidents,
            args.maintenance_start,
            args.maintenance_stop,
        )
    )
    if control_actions > 1:
        print("choose_only_one_alert_control_action")
        return 2
    if args.silence_alerts or args.maintenance_start:
        control = silence_alerts(
            watchdog.alert_control_path,
            minutes=args.silence_minutes,
            reason=(
                "planned_maintenance"
                if args.maintenance_start
                else "operator_silence"
            ),
            maintenance=args.maintenance_start,
        )
        print(
            "alerts_silenced_until="
            + str(control.get("silenced_until"))
            + " maintenance="
            + str(bool(control.get("maintenance_until")))
        )
        return 0
    if args.resume_alerts or args.maintenance_stop:
        resume_alerts(watchdog.alert_control_path)
        print("alerts=RESUMED")
        return 0
    if args.ack_active_incidents:
        now = utc_now()
        active = watchdog.memory.get("active_incidents")
        count = 0
        if isinstance(active, dict):
            for record in active.values():
                if not isinstance(record, dict):
                    continue
                record["acknowledged_at"] = iso_time(now)
                record["acknowledged_by"] = "operator_cli"
                record["acknowledged_signature"] = record.get(
                    "incident_signature"
                )
                count += 1
        watchdog._persist(now=now)
        print(f"acknowledged_incidents={count}")
        return 0
    if args.status:
        control = read_alert_control(watchdog.alert_control_path)
        print(f"alerts_allowed={notifications_allowed(control)}")
        print(f"suppression_reason={suppression_reason(control) or '-'}")
        print(f"silenced_until={control.get('silenced_until')}")
        print(f"maintenance_until={control.get('maintenance_until')}")
        print(f"active_incidents={len(watchdog.memory.get('active_incidents') or {})}")
        return 0
    if args.enable_strategy_monitor or args.disable_strategy_monitor:
        enabled = bool(args.enable_strategy_monitor)
        write_json_atomic(
            WATCHDOG_CONTROL_PATH,
            {
                "schema_version": 1,
                "updated_at": iso_time(utc_now()),
                "monitor_strategy": enabled,
            },
        )
        print(
            "strategy_monitor="
            + ("ENABLED" if enabled else "DISABLED_HEARTBEAT_ONLY")
        )
        return 0
    if args.check:
        errors = watchdog.configuration_errors()
        print(f"watchdog_enabled={config.enabled}")
        print(f"strategy_monitor={watchdog.monitor_strategy_enabled()}")
        print(f"bark_configured={watchdog.bark.enabled}")
        print(f"feishu_configured={watchdog.feishu.enabled}")
        print(f"telegram_configured={watchdog.telegram.enabled}")
        print(f"remote_backup_configured={watchdog.remote_publisher.enabled}")
        print(f"configuration_ready={not errors}")
        for error in errors:
            print(f"configuration_error={error}")
        return 0 if not errors else 1
    if not config.enabled and not args.test_alert and not args.dry_run:
        print("watchdog_disabled set RISK_WAKEUP_ENABLED=true")
        return 2
    if args.test_alert:
        incident = Incident(
            key=f"end_to_end_test:{int(time.time())}",
            severity="critical",
            title="套利风险叫醒测试",
            message="ETH：这是端到端测试，不代表真实账户风险。",
            alert_params=("ETH", "风险叫醒测试"),
        )
        with watchdog_process_lock():
            watchdog.run_once(synthetic=incident, force_delivery=True)
        if args.dry_run:
            print("test_alert=DRY_RUN")
            return 0
        record = watchdog.memory["active_incidents"].get(incident.key, {})
        bark_passed = record.get("bark_status") == "sent"
        print(
            "bark_test="
            + ("PASS" if bark_passed else "FAIL")
            + f" detail={record.get('bark_status') or 'not_attempted'}"
        )
        message_passed = record.get("feishu_message_status") == "sent"
        phone_passed = record.get("feishu_phone_status") == "sent"
        print(
            "feishu_message_test="
            + ("PASS" if message_passed else "FAIL")
            + f" detail={record.get('feishu_message_status') or 'not_attempted'}"
        )
        print(
            "feishu_phone_test="
            + ("PASS" if phone_passed else "FAIL")
            + f" detail={record.get('feishu_phone_status') or 'not_attempted'}"
        )
        return 0 if bark_passed and message_passed and phone_passed else 1
    if args.once:
        with watchdog_process_lock():
            incidents = watchdog.run_once()
        print(f"incidents={len(incidents)}")
        for incident in incidents:
            print(f"{incident.severity} {incident.key}")
        return 0

    configuration_errors = watchdog.configuration_errors()
    if configuration_errors:
        for error in configuration_errors:
            print(f"watchdog_configuration_error={error}")
        return 2

    def stop_handler(_signum: int, _frame: Any) -> None:
        watchdog.stop_requested = True

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    try:
        with watchdog_process_lock():
            watchdog.run_forever()
    except RuntimeError as exc:
        print(f"watchdog_start_failed={exc}")
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
