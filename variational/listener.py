"""Local WebSocket receiver for the Variational Chrome CDP forwarder."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import websockets


QUOTES_INDICATIVE_PATH = "/api/quotes/indicative"
ORDERS_V2_PATH = "/api/orders/v2"
POSITIONS_PATH = "/api/positions"
PORTFOLIO_PATH = "/api/portfolio"
WS_EVENTS_PATH = "/events"
WS_PORTFOLIO_PATH = "/portfolio"
WS_PRICES_PATH = "/prices"
TRADE_EVENT_KEYWORDS = ("trade", "fill", "filled", "order", "execution")
QUOTE_LOG_INTERVAL_SECONDS = 30
PORTFOLIO_LOG_INTERVAL_SECONDS = 300
HEARTBEAT_STALE_SECONDS = 11
HEARTBEAT_RECHECK_SECONDS = 10
HEARTBEAT_HOURLY_SECONDS = 3600


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class ListenerConfig:
    host: str = "127.0.0.1"
    ws_port: int = 8866
    rest_port: int = 8867
    command_port: int = 8868
    output_dir: Path | None = None
    quiet: bool = False
    monitor: bool = True
    trade_limit: int = 20
    snapshot_file: Path | None = None


@dataclass(slots=True)
class VariationalMonitor:
    trade_limit: int = 20
    snapshot_file: Path | None = None
    trade_event_limit: int = 2000
    rest_response_limit: int = 200
    quotes: dict[str, dict[str, Any]] = field(default_factory=dict)
    reference_quotes: dict[str, dict[str, Any]] = field(default_factory=dict)
    current_quote_asset: str | None = None
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    recent_trades: list[dict[str, Any]] = field(default_factory=list)
    trade_events: list[dict[str, Any]] = field(default_factory=list)
    portfolio_summary: dict[str, Any] = field(default_factory=dict)
    recent_rest_responses: list[dict[str, Any]] = field(default_factory=list)
    last_update_at: str | None = None
    last_heartbeat_iso: str | None = None
    _last_quote_log_ts: float | None = None
    _last_portfolio_log_ts: float | None = None
    _last_heartbeat_monotonic: float | None = None
    _next_heartbeat_check_ts: float = 0.0
    _stale_alert_sent: bool = False
    _last_hourly_alert_hour: int = 0
    _next_trade_event_seq: int = 1
    _seen_trade_event_keys: set[str] = field(default_factory=set)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def process_rest_event(self, payload: dict[str, Any]) -> list[str]:
        if payload.get("kind") != "rest_response":
            return []

        url = str(payload.get("url", ""))
        endpoint = classify_rest_endpoint(url)
        body = decode_response_body(payload)
        parsed = try_parse_json(body) if body is not None else None
        async with self._lock:
            self._remember_rest_response(payload=payload, endpoint=endpoint, parsed=parsed, body=body)
        if endpoint not in {
            QUOTES_INDICATIVE_PATH,
            ORDERS_V2_PATH,
            POSITIONS_PATH,
            PORTFOLIO_PATH,
        }:
            return []

        if body is None:
            return [f"[MONITOR] Failed to decode REST body for {url}"]

        if parsed is None:
            return [f"[MONITOR] REST body is not JSON for {url}"]

        async with self._lock:
            lines: list[str] = []
            now_ts = asyncio.get_running_loop().time()

            if endpoint == QUOTES_INDICATIVE_PATH:
                if isinstance(parsed, dict):
                    parsed = {
                        **parsed,
                        "__source_url": url,
                        "__source_endpoint": endpoint,
                    }
                self._update_quote(parsed)
                self._mark_heartbeat(now_ts, payload.get("timestamp"))
            elif endpoint == ORDERS_V2_PATH:
                for event in self._iter_rest_trade_messages(parsed):
                    trade_line = self._update_trade_event(event)
                    if trade_line:
                        lines.append(trade_line)
                self._mark_heartbeat(now_ts, payload.get("timestamp"))
            elif endpoint == POSITIONS_PATH:
                self._update_positions_from_rest(parsed)
                self._mark_heartbeat(now_ts, payload.get("timestamp"))
            elif endpoint == PORTFOLIO_PATH:
                self._update_portfolio_summary_from_rest(parsed)
                self._mark_heartbeat(now_ts, payload.get("timestamp"))

            self.last_update_at = utc_now()
            if self.snapshot_file is not None:
                await asyncio.to_thread(write_json_file, self.snapshot_file, self.snapshot())

        return lines

    def _remember_rest_response(
        self,
        *,
        payload: dict[str, Any],
        endpoint: str | None,
        parsed: Any,
        body: str | None,
    ) -> None:
        url = str(payload.get("url", ""))
        path = ""
        try:
            path = urlparse(url).path
        except ValueError:
            path = url
        summary: dict[str, Any] = {
            "timestamp": payload.get("timestamp"),
            "url": url,
            "path": path,
            "endpoint": endpoint,
            "status": payload.get("status"),
            "type": payload.get("type"),
            "matchedPattern": payload.get("matchedPattern"),
            "body_size": len(body or ""),
        }
        if isinstance(parsed, dict):
            summary["json_keys"] = list(parsed.keys())[:20]
            result = parsed.get("result")
            if isinstance(result, list):
                summary["result_len"] = len(result)
                if result and isinstance(result[0], dict):
                    summary["first_result_keys"] = list(result[0].keys())[:20]
            elif isinstance(result, dict):
                summary["result_keys"] = list(result.keys())[:20]
            elif isinstance(parsed.get("positions"), list):
                summary["positions_len"] = len(parsed.get("positions") or [])
        elif isinstance(parsed, list):
            summary["json_list_len"] = len(parsed)
            if parsed and isinstance(parsed[0], dict):
                summary["first_item_keys"] = list(parsed[0].keys())[:20]
        self.recent_rest_responses.insert(0, summary)
        self.recent_rest_responses = self.recent_rest_responses[: self.rest_response_limit]

    async def process_ws_event(self, payload: dict[str, Any]) -> list[str]:
        kind = str(payload.get("kind", ""))
        if kind != "ws_frame":
            return []
        if payload.get("direction") != "received":
            return []

        url = str(payload.get("url", ""))
        stream = classify_ws_stream(url)
        if stream is None:
            return []

        message_text = decode_ws_frame_payload(payload)
        if message_text is None:
            return [f"[MONITOR] Failed to decode WS frame for {url}"]

        parsed = try_parse_json(message_text)
        if parsed is None:
            return []

        async with self._lock:
            lines: list[str] = []
            now_ts = asyncio.get_running_loop().time()
            if stream == WS_EVENTS_PATH:
                for event in self._iter_event_messages(parsed):
                    self._update_heartbeat(event, now_ts)
                    trade_line = self._update_trade_event(event)
                    if trade_line:
                        lines.append(trade_line)
                        portfolio_line = self._format_portfolio_line()
                        if portfolio_line:
                            lines.append(f"{portfolio_line} trigger=trade")
                            self._last_portfolio_log_ts = now_ts
            elif stream == WS_PORTFOLIO_PATH:
                for event in self._iter_market_messages(parsed):
                    trade_line = self._update_trade_event(event)
                    if trade_line:
                        lines.append(trade_line)
                self._update_portfolio(parsed)
                self._mark_heartbeat(now_ts, payload.get("timestamp"))
            elif stream == WS_PRICES_PATH:
                updated_quote = False
                for event in self._iter_market_messages(parsed):
                    self._update_heartbeat(event, now_ts)
                    quote_event = {
                        **event,
                        "__source_url": url,
                        "__source_stream": stream,
                    }
                    if self._update_quote(quote_event):
                        updated_quote = True
                    trade_line = self._update_trade_event(event)
                    if trade_line:
                        lines.append(trade_line)
                if updated_quote or lines:
                    self._mark_heartbeat(now_ts, payload.get("timestamp"))

            if not lines and stream != WS_PORTFOLIO_PATH:
                if stream != WS_PRICES_PATH or not (self.quotes or self.reference_quotes):
                    return []

            self.last_update_at = utc_now()
            if self.snapshot_file is not None:
                await asyncio.to_thread(write_json_file, self.snapshot_file, self.snapshot())
            return lines

    def _iter_event_messages(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            out = [payload]
            events = payload.get("events")
            if isinstance(events, list):
                out.extend([item for item in events if isinstance(item, dict)])
            data = payload.get("data")
            if isinstance(data, list):
                out.extend([item for item in data if isinstance(item, dict) and "type" in item])
            return out

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        return []

    def _iter_market_messages(self, payload: Any) -> list[dict[str, Any]]:
        candidates = self._iter_event_messages(payload)
        out: list[dict[str, Any]] = []
        seen: set[int] = set()

        def add(item: Any) -> None:
            if not isinstance(item, dict):
                return
            marker = id(item)
            if marker in seen:
                return
            seen.add(marker)
            out.append(item)

        for item in candidates:
            add(item)
            pricing = item.get("pricing")
            channel = item.get("channel")
            if isinstance(pricing, dict):
                merged = dict(pricing)
                if isinstance(channel, str):
                    merged["channel"] = channel
                add(merged)
            for key in ("data", "payload", "quote", "quotes", "prices", "items", "results"):
                nested = item.get(key)
                if isinstance(nested, dict):
                    add(nested)
                elif isinstance(nested, list):
                    for subitem in nested:
                        add(subitem)

        if isinstance(payload, dict):
            for key in ("data", "payload", "quote", "quotes", "prices", "items", "results"):
                nested = payload.get(key)
                if isinstance(nested, dict):
                    add(nested)
                elif isinstance(nested, list):
                    for subitem in nested:
                        add(subitem)

        return out

    def _iter_rest_trade_messages(self, payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        result = payload.get("result")
        if not isinstance(result, list):
            return []

        out: list[dict[str, Any]] = []
        for item in result:
            if not isinstance(item, dict):
                continue
            if not self._looks_like_trade_event(item):
                continue
            out.append(item)
        return out

    async def emit_periodic_logs(self) -> tuple[list[str], list[str]]:
        lines: list[str] = []
        alerts: list[str] = []
        async with self._lock:
            now_ts = asyncio.get_running_loop().time()
            if self.quotes and self._should_log_quote(now_ts):
                quote_line = self._format_quote_line()
                if quote_line:
                    lines.append(quote_line)
                    self._last_quote_log_ts = now_ts

            if self.positions and self._should_log_portfolio(now_ts):
                portfolio_line = self._format_portfolio_line()
                if portfolio_line:
                    lines.append(f"{portfolio_line} trigger=interval")
                    self._last_portfolio_log_ts = now_ts

            alerts.extend(self._collect_heartbeat_alerts(now_ts))

        return lines, alerts

    @staticmethod
    def _source_timestamp_seconds(value: Any) -> float | None:
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    def _update_quote(self, payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False

        instrument = payload.get("instrument")
        asset = None
        if isinstance(instrument, dict):
            asset = instrument.get("underlying") or instrument.get("symbol") or instrument.get("asset")

        if not asset:
            asset = payload.get("underlying") or payload.get("symbol") or payload.get("asset") or payload.get("market")

        channel = payload.get("channel")
        if not asset and isinstance(channel, str) and channel.startswith("instrument_price:"):
            parts = channel.split(":", 1)
            if len(parts) == 2:
                market_parts = parts[1].split("-")
                if len(market_parts) >= 2 and market_parts[1]:
                    asset = market_parts[1]

        bid = payload.get("bid") or payload.get("best_bid") or payload.get("b")
        ask = payload.get("ask") or payload.get("best_ask") or payload.get("a")
        mark = payload.get("mark_price") or payload.get("price") or payload.get("mid")
        underlying_price = payload.get("underlying_price")
        ts = payload.get("timestamp") or payload.get("updated_at") or payload.get("created_at")
        received_at = utc_now()
        received_monotonic = time.monotonic()

        if mark is None and underlying_price is not None:
            mark = underlying_price

        if not asset:
            return False

        asset = str(asset).upper()

        # The /prices stream publishes a reference price, not executable
        # bid/ask. Keep it separate so it cannot be mistaken for an RFQ.
        is_reference_stream = (
            payload.get("__source_stream") == WS_PRICES_PATH
            or (
                isinstance(channel, str)
                and channel.startswith("instrument_price:")
            )
        )
        if is_reference_stream:
            if mark is None:
                return False
            previous = self.reference_quotes.get(asset)
            previous_ts = self._source_timestamp_seconds(
                previous.get("timestamp") if previous else None
            )
            current_ts = self._source_timestamp_seconds(ts)
            if (
                previous_ts is not None
                and current_ts is not None
                and current_ts < previous_ts
            ):
                return False
            if (
                previous_ts is not None
                and current_ts is not None
                and current_ts == previous_ts
                and previous is not None
                and previous.get("reference_price") == mark
            ):
                return False
            self.reference_quotes[asset] = {
                "asset": asset,
                "reference_price": mark,
                "mark_price": mark,
                "timestamp": ts,
                "received_at": received_at,
                "received_monotonic": received_monotonic,
                "quote_kind": "reference_price_only",
                "quote_source": "passive_browser_stream",
                "quote_semantics": "reference_price_only",
                "quote_latency_kind": "passive_stream_no_rfq",
                "raw": payload,
            }
            self.current_quote_asset = asset
            return True

        # Only a complete explicit bid/ask pair belongs in the executable
        # quote cache. A single price is never promoted to both sides.
        if bid is None or ask is None:
            return False
        previous = self.quotes.get(asset)
        previous_ts = self._source_timestamp_seconds(
            previous.get("timestamp") if previous else None
        )
        current_ts = self._source_timestamp_seconds(ts)
        if (
            previous_ts is not None
            and current_ts is not None
            and current_ts < previous_ts
        ):
            return False
        if (
            previous_ts is not None
            and current_ts is not None
            and current_ts == previous_ts
            and previous is not None
            and previous.get("bid") == bid
            and previous.get("ask") == ask
            and previous.get("mark_price") == mark
        ):
            return False

        self.quotes[asset] = {
            "asset": asset,
            "bid": bid,
            "ask": ask,
            "mark_price": mark,
            "timestamp": ts,
            "received_at": received_at,
            "received_monotonic": received_monotonic,
            "quote_kind": "indicative_bid_ask",
            "raw": payload,
        }
        self.current_quote_asset = asset
        return True

    def _update_trade_event(self, payload: Any) -> str | None:
        summary = self._extract_trade_summary(payload)
        if summary is None:
            return None

        trade_id = str(summary.get("trade_id", ""))
        dedupe_key = self._trade_event_dedupe_key(summary)
        if dedupe_key in self._seen_trade_event_keys:
            return None

        summary = {
            "timestamp": summary.get("timestamp") or "-",
            "trade_id": trade_id,
            "side": summary.get("side", "-"),
            "asset": summary.get("asset", "UNKNOWN"),
            "price": summary.get("price", "-"),
            "qty": summary.get("qty", "-"),
            "status": summary.get("status", "-"),
            "role": summary.get("role", "-"),
            "received_at": utc_now(),
            "raw": payload,
        }

        summary["event_seq"] = self._next_trade_event_seq
        self._next_trade_event_seq += 1
        self._seen_trade_event_keys.add(dedupe_key)
        if len(self._seen_trade_event_keys) > self.trade_event_limit * 4:
            self._rebuild_seen_trade_event_keys()

        if trade_id:
            self.recent_trades = [t for t in self.recent_trades if t.get("trade_id") != trade_id]
        self.recent_trades.insert(0, summary)
        self.recent_trades = self.recent_trades[: self.trade_limit]
        self.trade_events.append(summary)
        if len(self.trade_events) > self.trade_event_limit:
            self.trade_events = self.trade_events[-self.trade_event_limit:]

        trade_id_short = trade_id[:8] if trade_id else "-"
        return (
            f"[MONITOR] TRADE {summary['side']} {summary['qty']} {summary['asset']} "
            f"@{summary['price']} status={summary['status']} role={summary['role']} id={trade_id_short}"
        )

    @staticmethod
    def _trade_event_dedupe_key(summary: dict[str, Any]) -> str:
        trade_id = str(summary.get("trade_id", "")).strip()
        status = str(summary.get("status", "")).strip().lower()
        if trade_id:
            # Allow the same trade to progress from pending -> filled/cleared
            # without being collapsed into a single event.
            return f"id:{trade_id}|status:{status}"
        return "|".join(
            [
                str(summary.get("timestamp", "")).strip(),
                str(summary.get("side", "")).strip().lower(),
                str(summary.get("asset", "")).strip().upper(),
                str(summary.get("price", "")).strip(),
                str(summary.get("qty", "")).strip(),
                str(summary.get("status", "")).strip().lower(),
            ]
        )

    def _rebuild_seen_trade_event_keys(self) -> None:
        rebuilt: set[str] = set()
        for event in self.trade_events:
            rebuilt.add(self._trade_event_dedupe_key(event))
        self._seen_trade_event_keys = rebuilt

    @staticmethod
    def _looks_like_trade_event(payload: dict[str, Any]) -> bool:
        fields = [
            payload.get("type"),
            payload.get("event"),
            payload.get("event_type"),
            payload.get("topic"),
            payload.get("channel"),
            payload.get("status"),
            payload.get("state"),
            payload.get("order_status"),
            payload.get("execution_status"),
        ]
        for value in fields:
            text = str(value or "").strip().lower()
            if any(keyword in text for keyword in TRADE_EVENT_KEYWORDS):
                return True

        return any(
            key in payload
            for key in (
                "trade_id",
                "order_id",
                "execution_id",
                "fill_price",
                "filled_qty",
                "filled_quantity",
                "filled_size",
            )
        )

    @staticmethod
    def _extract_trade_summary(payload: Any) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        nested_data = payload.get("data")
        if isinstance(nested_data, dict) and VariationalMonitor._looks_like_trade_event(nested_data):
            data = nested_data
        else:
            nested_payload = payload.get("payload")
            if isinstance(nested_payload, dict) and VariationalMonitor._looks_like_trade_event(nested_payload):
                data = nested_payload
            else:
                data = payload

        if not VariationalMonitor._looks_like_trade_event(data) and not VariationalMonitor._looks_like_trade_event(payload):
            return None

        instrument = data.get("instrument")
        asset = None
        if isinstance(instrument, dict):
            asset = instrument.get("underlying") or instrument.get("symbol") or instrument.get("asset")
        if not asset:
            asset = data.get("underlying") or data.get("symbol") or data.get("asset") or data.get("market")

        channel = data.get("channel") or payload.get("channel")
        if not asset and isinstance(channel, str) and channel.startswith("instrument_price:"):
            parts = channel.split(":", 1)
            if len(parts) == 2:
                market_parts = parts[1].split("-")
                if len(market_parts) >= 2 and market_parts[1]:
                    asset = market_parts[1]

        event_type = str(
            data.get("type")
            or payload.get("type")
            or data.get("event_type")
            or payload.get("event_type")
            or ""
        ).strip().lower()
        status = (
            data.get("status")
            or data.get("state")
            or data.get("order_status")
            or data.get("execution_status")
            or payload.get("status")
            or payload.get("state")
            or "-"
        )
        if status == "-" and ("fill" in event_type or "execution" in event_type or event_type == "confirmed"):
            status = "filled"

        return {
            "timestamp": data.get("created_at") or data.get("updated_at") or payload.get("timestamp") or payload.get("published_at"),
            "trade_id": data.get("trade_id") or data.get("id") or data.get("order_id") or data.get("execution_id") or data.get("client_order_id") or "",
            "side": data.get("side") or data.get("direction") or data.get("order_side") or "-",
            "asset": str(asset).upper() if asset else "UNKNOWN",
            "price": data.get("price") or data.get("fill_price") or data.get("avg_price") or data.get("average_price") or data.get("execution_price") or "-",
            "qty": data.get("qty") or data.get("quantity") or data.get("filled_qty") or data.get("filled_quantity") or data.get("filled_size") or data.get("size") or data.get("amount") or data.get("base_amount") or "-",
            "status": status,
            "role": data.get("role") or data.get("liquidity_role") or data.get("maker_taker") or "-",
        }

    def _update_portfolio(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        positions_data = payload.get("positions")
        if not isinstance(positions_data, list):
            return

        next_positions: dict[str, dict[str, Any]] = {}
        for item in positions_data:
            if not isinstance(item, dict):
                continue
            position_info = item.get("position_info")
            if not isinstance(position_info, dict):
                continue
            instrument = position_info.get("instrument")
            if not isinstance(instrument, dict):
                continue

            asset = str(instrument.get("underlying", "UNKNOWN"))
            next_positions[asset] = {
                "asset": asset,
                "qty": position_info.get("qty"),
                "avg_entry_price": position_info.get("avg_entry_price"),
                "updated_at": position_info.get("updated_at"),
                "value": item.get("value"),
                "upnl": item.get("upnl"),
                "rpnl": item.get("rpnl"),
                "raw": item,
            }

        pool = payload.get("pool_portfolio_result")
        margin = {}
        if isinstance(pool, dict):
            margin_raw = pool.get("margin_usage")
            if isinstance(margin_raw, dict):
                margin = {
                    "initial_margin": margin_raw.get("initial_margin"),
                    "maintenance_margin": margin_raw.get("maintenance_margin"),
                }

        self.positions = next_positions
        self.portfolio_summary = {
            "balance": pool.get("balance") if isinstance(pool, dict) else None,
            "upnl": pool.get("upnl") if isinstance(pool, dict) else None,
            "margin_usage": margin,
            "published_at": payload.get("published_at"),
            "raw": pool if isinstance(pool, dict) else {},
        }

    def _update_positions_from_rest(self, payload: Any) -> None:
        if not isinstance(payload, list):
            return

        next_positions: dict[str, dict[str, Any]] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            position_info = item.get("position_info")
            if not isinstance(position_info, dict):
                continue
            instrument = position_info.get("instrument")
            if not isinstance(instrument, dict):
                continue

            asset = str(instrument.get("underlying", "UNKNOWN"))
            next_positions[asset] = {
                "asset": asset,
                "qty": position_info.get("qty"),
                "avg_entry_price": position_info.get("avg_entry_price"),
                "updated_at": position_info.get("updated_at"),
                "value": item.get("value"),
                "upnl": item.get("upnl"),
                "rpnl": item.get("rpnl"),
                "raw": item,
            }

        self.positions = next_positions

    def _update_portfolio_summary_from_rest(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        self.portfolio_summary = {
            "balance": payload.get("balance"),
            "upnl": payload.get("upnl"),
            "margin_usage": payload.get("margin_usage") if isinstance(payload.get("margin_usage"), dict) else {},
            "published_at": payload.get("updated_at") or utc_now(),
            "raw": payload,
        }

    def _should_log_quote(self, now_ts: float) -> bool:
        if self._last_quote_log_ts is None:
            return True
        return now_ts - self._last_quote_log_ts >= QUOTE_LOG_INTERVAL_SECONDS

    def _should_log_portfolio(self, now_ts: float) -> bool:
        if self._last_portfolio_log_ts is None:
            return True
        return now_ts - self._last_portfolio_log_ts >= PORTFOLIO_LOG_INTERVAL_SECONDS

    def _update_heartbeat(self, payload: Any, now_ts: float) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("type") != "heartbeat":
            return

        self._mark_heartbeat(now_ts, payload.get("timestamp"))

    def _mark_heartbeat(self, now_ts: float, timestamp: Any = None) -> None:
        
        self._last_heartbeat_monotonic = now_ts
        if isinstance(timestamp, str):
            self.last_heartbeat_iso = timestamp
        else:
            self.last_heartbeat_iso = utc_now()

        self._stale_alert_sent = False
        self._last_hourly_alert_hour = 0
        self._next_heartbeat_check_ts = now_ts + 1

    def _collect_heartbeat_alerts(self, now_ts: float) -> list[str]:
        if self._last_heartbeat_monotonic is None:
            return []
        if now_ts < self._next_heartbeat_check_ts:
            return []

        age_seconds = now_ts - self._last_heartbeat_monotonic
        if age_seconds <= HEARTBEAT_STALE_SECONDS:
            self._next_heartbeat_check_ts = now_ts + 1
            return []

        self._next_heartbeat_check_ts = now_ts + HEARTBEAT_RECHECK_SECONDS
        alerts: list[str] = []
        last_seen = self.last_heartbeat_iso or "unknown"
        if not self._stale_alert_sent:
            alerts.append(
                f"Heartbeat stale: last heartbeat {age_seconds:.1f}s ago (last_seen={last_seen})."
            )
            self._stale_alert_sent = True

        stale_hours = int(age_seconds // HEARTBEAT_HOURLY_SECONDS)
        if stale_hours >= 1 and stale_hours > self._last_hourly_alert_hour:
            alerts.append(
                f"Heartbeat still stale for {stale_hours}h (last_seen={last_seen})."
            )
            self._last_hourly_alert_hour = stale_hours

        return alerts

    def _format_quote_line(self) -> str | None:
        if not self.current_quote_asset:
            return None
        quote = self.quotes.get(self.current_quote_asset)
        if not quote:
            return None
        spread = compute_spread(quote.get("bid"), quote.get("ask"))
        spread_part = f" spread={spread}" if spread is not None else ""
        return (
            f"[MONITOR] QUOTE {self.current_quote_asset} bid={quote.get('bid')} "
            f"ask={quote.get('ask')}{spread_part} mark={quote.get('mark_price')}"
        )

    def _format_portfolio_line(self) -> str | None:
        if not self.current_quote_asset:
            return None
        row = self.positions.get(self.current_quote_asset)
        if row is None:
            position_part = f"{self.current_quote_asset} qty=0 upnl=0"
        else:
            position_part = (
                f"{self.current_quote_asset} qty={row.get('qty')} upnl={row.get('upnl')}"
            )
        return (
            f"[MONITOR] PORTFOLIO balance={self.portfolio_summary.get('balance')} "
            f"upnl={self.portfolio_summary.get('upnl')} asset={position_part}"
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "generated_at": utc_now(),
            "last_update_at": self.last_update_at,
            "current_quote_asset": self.current_quote_asset,
            "last_heartbeat_iso": self.last_heartbeat_iso,
            "quotes": self.quotes,
            "reference_quotes": self.reference_quotes,
            "positions": self.positions,
            "recent_trades": self.recent_trades,
            "trade_events": self.trade_events,
            "recent_rest_responses": self.recent_rest_responses,
            "portfolio_summary": self.portfolio_summary,
        }

    async def get_trading_state(self) -> dict[str, Any]:
        async with self._lock:
            now_ts = asyncio.get_running_loop().time()
            heartbeat_age: float | None = None
            if self._last_heartbeat_monotonic is not None:
                heartbeat_age = max(0.0, now_ts - self._last_heartbeat_monotonic)

            asset = self.current_quote_asset
            quote = self.quotes.get(asset) if asset else None
            reference_quote = self.reference_quotes.get(asset) if asset else None
            row = self.positions.get(asset) if asset else None
            qty = 0.0
            if isinstance(row, dict):
                qty_val = as_float(row.get("qty"))
                if qty_val is not None:
                    qty = qty_val

            return {
                "asset": asset,
                "position": qty,
                "position_row": row,
                "quote": quote,
                "has_quote": quote is not None,
                "reference_quote": reference_quote,
                "has_reference_quote": reference_quote is not None,
                "has_portfolio": bool(self.portfolio_summary),
                "last_update_at": self.last_update_at,
                "last_heartbeat_iso": self.last_heartbeat_iso,
                "heartbeat_age": heartbeat_age,
            }

    async def get_trade_events_since(
        self,
        min_event_seq: int,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            events = [event for event in self.trade_events if int(event.get("event_seq", 0)) > min_event_seq]
            if limit > 0:
                events = events[:limit]
            return events

    async def get_latest_trade_event_seq(self) -> int:
        async with self._lock:
            return self._next_trade_event_seq - 1


class EventSink:
    def __init__(
        self,
        output_dir: Path | None,
        quiet: bool = False,
        monitor: VariationalMonitor | None = None,
        raw_event_policy: str = "all",
    ) -> None:
        if raw_event_policy not in {"all", "trading", "none"}:
            raise ValueError("raw_event_policy must be all, trading, or none")
        self.output_dir = output_dir
        self.quiet = quiet
        self.monitor = monitor
        self.raw_event_policy = raw_event_policy
        self._write_lock = asyncio.Lock()
        if self.output_dir is not None:
            self.output_dir.mkdir(parents=True, exist_ok=True)

    async def handle(self, channel: str, raw_message: str) -> None:
        parsed: dict[str, Any] | str
        try:
            parsed = json.loads(raw_message)
        except json.JSONDecodeError:
            parsed = raw_message

        envelope = {
            "ingested_at": utc_now(),
            "channel": channel,
            "payload": parsed,
        }

        if self.monitor and isinstance(parsed, dict):
            lines: list[str] = []
            if channel == "rest":
                lines = await self.monitor.process_rest_event(parsed)
            elif channel == "ws":
                lines = await self.monitor.process_ws_event(parsed)
            if not self.quiet:
                for line in lines:
                    print(line, flush=True)

        if self.output_dir is not None and self._should_persist_raw_event(channel, parsed):
            file_name = "ws_events.jsonl" if channel == "ws" else "rest_events.jsonl"
            await self._append_jsonl(self.output_dir / file_name, envelope)

    def _should_persist_raw_event(self, channel: str, parsed: Any) -> bool:
        if self.raw_event_policy == "none":
            return False
        if self.raw_event_policy == "all":
            return True
        if not isinstance(parsed, dict):
            return False
        url = str(parsed.get("url") or "")
        if channel == "rest":
            return classify_rest_endpoint(url) == ORDERS_V2_PATH
        if channel != "ws" or classify_ws_stream(url) != WS_EVENTS_PATH:
            return False
        text = json.dumps(parsed, ensure_ascii=True).lower()
        return any(keyword in text for keyword in TRADE_EVENT_KEYWORDS)

    async def _append_jsonl(self, path: Path, obj: dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=True) + "\n"
        async with self._write_lock:
            await asyncio.to_thread(_append_line, path, line)


class CommandBroker:
    def __init__(self, quiet: bool = False) -> None:
        self.quiet = quiet
        self._lock = asyncio.Lock()
        self._roles: dict[websockets.ServerConnection, str] = {}
        self._extension: websockets.ServerConnection | None = None
        self._pending_requests: dict[str, websockets.ServerConnection] = {}
        self._pending_request_results: dict[str, str] = {}

    async def on_connect(self, websocket: websockets.ServerConnection) -> None:
        async with self._lock:
            self._roles[websocket] = "unknown"

    async def on_disconnect(self, websocket: websockets.ServerConnection) -> None:
        async with self._lock:
            role = self._roles.pop(websocket, "unknown")
            if websocket is self._extension:
                self._extension = None
                failures = list(self._pending_requests.items())
                self._pending_requests.clear()
                for request_id, requester in failures:
                    result_type = self._pending_request_results.pop(
                        request_id,
                        "ORDER_RESULT",
                    )
                    await self._send(
                        requester,
                        {
                            "type": result_type,
                            "requestId": request_id,
                            "ok": False,
                            "error": "Extension disconnected before order result.",
                            "timestamp": utc_now(),
                        },
                    )

            stale_request_ids = [req for req, requester in self._pending_requests.items() if requester is websocket]
            for req in stale_request_ids:
                self._pending_requests.pop(req, None)
                self._pending_request_results.pop(req, None)

            if not self.quiet:
                print(f"[COMMAND] disconnected role={role}", flush=True)

    async def handle_raw_message(self, websocket: websockets.ServerConnection, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await self._send(
                websocket,
                {
                    "type": "ERROR",
                    "ok": False,
                    "error": "Invalid JSON payload.",
                    "timestamp": utc_now(),
                },
            )
            return

        if not isinstance(payload, dict):
            await self._send(
                websocket,
                {
                    "type": "ERROR",
                    "ok": False,
                    "error": "Command payload must be an object.",
                    "timestamp": utc_now(),
                },
            )
            return

        msg_type = str(payload.get("type", "")).upper()
        if msg_type == "REGISTER":
            await self._handle_register(websocket, payload)
            return
        if msg_type == "PING":
            await self._send(websocket, {"type": "PONG", "timestamp": utc_now()})
            return
        if msg_type == "PAGE_PROBE":
            await self._handle_page_probe(websocket, payload)
            return
        if msg_type == "PLACE_ORDER_DRY_RUN":
            await self._handle_place_order_dry_run(websocket, payload)
            return
        if msg_type == "PREPARE_ORDER_DRY_RUN":
            await self._handle_prepare_order_dry_run(websocket, payload)
            return
        if msg_type == "PREPARE_ORDER_KEYBOARD_DRY_RUN":
            await self._handle_prepare_order_keyboard_dry_run(websocket, payload)
            return
        if msg_type == "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN":
            await self._handle_prepare_order_input_sweep_dry_run(websocket, payload)
            return
        if msg_type in {
            "VAR_API_POSITIONS",
            "VAR_API_READY",
            "VAR_API_STREAM_HEALTH",
            "VAR_API_PORTFOLIO",
            "VAR_API_ORDERS",
            "VAR_API_QUOTE",
            "VAR_API_ORDER",
            "VAR_API_RELOAD_PAGE",
        }:
            await self._handle_var_api_command(websocket, payload, msg_type, f"{msg_type}_RESULT")
            return
        if msg_type == "PLACE_ORDER":
            await self._handle_place_order(websocket, payload)
            return
        if msg_type in {
            "ORDER_RESULT",
            "PAGE_PROBE_RESULT",
            "PLACE_ORDER_DRY_RUN_RESULT",
            "PREPARE_ORDER_DRY_RUN_RESULT",
            "PREPARE_ORDER_KEYBOARD_DRY_RUN_RESULT",
            "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN_RESULT",
            "VAR_API_POSITIONS_RESULT",
            "VAR_API_READY_RESULT",
            "VAR_API_STREAM_HEALTH_RESULT",
            "VAR_API_PORTFOLIO_RESULT",
            "VAR_API_ORDERS_RESULT",
            "VAR_API_QUOTE_RESULT",
            "VAR_API_ORDER_RESULT",
            "VAR_API_RELOAD_PAGE_RESULT",
        }:
            await self._handle_command_result(payload)
            return

        await self._send(
            websocket,
            {
                "type": "ERROR",
                "ok": False,
                "error": f"Unsupported message type: {msg_type or 'UNKNOWN'}",
                "timestamp": utc_now(),
            },
        )

    async def _handle_register(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        role = str(payload.get("role", "")).strip().lower() or "unknown"
        async with self._lock:
            self._roles[websocket] = role
            if role == "extension":
                self._extension = websocket

        await self._send(
            websocket,
            {
                "type": "REGISTER_ACK",
                "ok": True,
                "role": role,
                "timestamp": utc_now(),
            },
        )
        if not self.quiet:
            print(f"[COMMAND] registered role={role}", flush=True)

    async def _handle_place_order(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        await self._forward_order_command(websocket, payload, "PLACE_ORDER", "ORDER_RESULT")

    async def _handle_page_probe(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())

        async with self._lock:
            extension = self._extension
            if extension is None:
                await self._send(
                    websocket,
                    {
                        "type": "PAGE_PROBE_RESULT",
                        "requestId": request_id,
                        "ok": False,
                        "error": "No extension command client connected.",
                        "timestamp": utc_now(),
                    },
                )
                return

            self._pending_requests[request_id] = websocket
            await self._send(
                extension,
                {
                    "type": "PAGE_PROBE",
                    "requestId": request_id,
                    "timestamp": utc_now(),
                },
            )

    async def _handle_place_order_dry_run(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        await self._forward_order_command(websocket, payload, "PLACE_ORDER_DRY_RUN", "PLACE_ORDER_DRY_RUN_RESULT")

    async def _handle_prepare_order_dry_run(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        await self._forward_order_command(websocket, payload, "PREPARE_ORDER_DRY_RUN", "PREPARE_ORDER_DRY_RUN_RESULT")

    async def _handle_prepare_order_keyboard_dry_run(
        self,
        websocket: websockets.ServerConnection,
        payload: dict[str, Any],
    ) -> None:
        await self._forward_order_command(
            websocket,
            payload,
            "PREPARE_ORDER_KEYBOARD_DRY_RUN",
            "PREPARE_ORDER_KEYBOARD_DRY_RUN_RESULT",
        )

    async def _handle_prepare_order_input_sweep_dry_run(
        self,
        websocket: websockets.ServerConnection,
        payload: dict[str, Any],
    ) -> None:
        await self._forward_order_command(
            websocket,
            payload,
            "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN",
            "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN_RESULT",
        )

    async def _handle_var_api_command(
        self,
        websocket: websockets.ServerConnection,
        payload: dict[str, Any],
        command_type: str,
        result_type: str,
    ) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        async with self._lock:
            extension = self._extension
            if extension is None:
                await self._send(
                    websocket,
                    {
                        "type": result_type,
                        "requestId": request_id,
                        "ok": False,
                        "error": "No extension command client connected.",
                        "timestamp": utc_now(),
                    },
                )
                return

            self._pending_requests[request_id] = websocket
            self._pending_request_results[request_id] = result_type
            forward_payload = dict(payload)
            forward_payload.update({"type": command_type, "requestId": request_id, "timestamp": utc_now()})
            await self._send(extension, forward_payload)

    async def _forward_order_command(
        self,
        websocket: websockets.ServerConnection,
        payload: dict[str, Any],
        command_type: str,
        result_type: str,
    ) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        side = str(payload.get("side", "")).upper()
        amount = str(payload.get("amount", "")).strip()

        if side not in {"BUY", "SELL"}:
            await self._send(
                websocket,
                {
                    "type": result_type,
                    "requestId": request_id,
                    "ok": False,
                    "error": "Invalid side. Use BUY or SELL.",
                    "timestamp": utc_now(),
                },
            )
            return
        try:
            if float(amount) <= 0:
                raise ValueError
        except ValueError:
            await self._send(
                websocket,
                {
                    "type": result_type,
                    "requestId": request_id,
                    "ok": False,
                    "error": "Invalid amount. Must be positive.",
                    "timestamp": utc_now(),
                },
            )
            return

        async with self._lock:
            extension = self._extension
            if extension is None:
                await self._send(
                    websocket,
                    {
                        "type": result_type,
                        "requestId": request_id,
                        "ok": False,
                        "error": "No extension command client connected.",
                        "timestamp": utc_now(),
                    },
                )
                return

            self._pending_requests[request_id] = websocket
            self._pending_request_results[request_id] = result_type
            forward_payload = dict(payload)
            forward_payload.update(
                {
                    "type": command_type,
                    "requestId": request_id,
                    "side": side,
                    "amount": amount,
                    "timestamp": utc_now(),
                }
            )
            await self._send(extension, forward_payload)

    async def _handle_command_result(self, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId", "")).strip()
        if not request_id:
            return
        async with self._lock:
            requester = self._pending_requests.pop(request_id, None)
            self._pending_request_results.pop(request_id, None)

        if requester is not None:
            await self._send(requester, payload)
            if not self.quiet:
                print(
                    f"[COMMAND] result type={payload.get('type')} requestId={request_id} ok={payload.get('ok')}",
                    flush=True,
                )

    async def _send(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        try:
            await websocket.send(json.dumps(payload, ensure_ascii=True))
        except Exception:
            return


def _append_line(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def write_json_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def classify_rest_endpoint(url: str) -> str | None:
    try:
        path = urlparse(url).path
    except ValueError:
        return None
    if path == QUOTES_INDICATIVE_PATH:
        return QUOTES_INDICATIVE_PATH
    if path == ORDERS_V2_PATH:
        return ORDERS_V2_PATH
    if path == POSITIONS_PATH:
        return POSITIONS_PATH
    if path == PORTFOLIO_PATH:
        return PORTFOLIO_PATH
    return None


def classify_ws_stream(url: str) -> str | None:
    try:
        path = urlparse(url).path
    except ValueError:
        return None
    lowered = path.lower()
    if lowered == WS_EVENTS_PATH or lowered.endswith(WS_EVENTS_PATH):
        return WS_EVENTS_PATH
    if lowered == WS_PORTFOLIO_PATH or lowered.endswith(WS_PORTFOLIO_PATH):
        return WS_PORTFOLIO_PATH
    if lowered == WS_PRICES_PATH or lowered.endswith(WS_PRICES_PATH) or "/prices" in lowered:
        return WS_PRICES_PATH
    return None


def decode_response_body(payload: dict[str, Any]) -> str | None:
    body = payload.get("body")
    if not isinstance(body, str):
        return None
    if payload.get("base64Encoded"):
        try:
            return base64.b64decode(body).decode("utf-8", errors="replace")
        except Exception:
            return None
    return body


def decode_ws_frame_payload(payload: dict[str, Any]) -> str | None:
    data = payload.get("payloadData")
    if not isinstance(data, str):
        return None

    opcode = payload.get("opcode")
    if opcode == 2:
        stripped = data.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            return data
        try:
            decoded = base64.b64decode(data)
            return decoded.decode("utf-8", errors="replace")
        except Exception:
            return data

    return data


def try_parse_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_spread(bid: Any, ask: Any) -> str | None:
    bid_val = as_float(bid)
    ask_val = as_float(ask)
    if bid_val is None or ask_val is None:
        return None
    return f"{ask_val - bid_val:.8f}"


async def run_receiver_server(
    channel: str,
    host: str,
    port: int,
    sink: EventSink,
) -> websockets.asyncio.server.Server:
    async def handler(websocket: websockets.ServerConnection) -> None:
        async for message in websocket:
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            await sink.handle(channel, message)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)


async def run_command_server(
    host: str,
    port: int,
    broker: CommandBroker,
) -> websockets.asyncio.server.Server:
    async def handler(websocket: websockets.ServerConnection) -> None:
        await broker.on_connect(websocket)
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="replace")
                await broker.handle_raw_message(websocket, message)
        finally:
            await broker.on_disconnect(websocket)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)


async def run(config: ListenerConfig) -> None:
    monitor = VariationalMonitor(trade_limit=config.trade_limit, snapshot_file=config.snapshot_file) if config.monitor else None
    sink = EventSink(config.output_dir, quiet=config.quiet, monitor=monitor)
    broker = CommandBroker(quiet=config.quiet)
    ws_server = await run_receiver_server("ws", config.host, config.ws_port, sink)
    rest_server = await run_receiver_server("rest", config.host, config.rest_port, sink)
    command_server = await run_command_server(config.host, config.command_port, broker)
    periodic_task: asyncio.Task[None] | None = None

    if monitor is not None:
        async def periodic_logger() -> None:
            while True:
                await asyncio.sleep(1)
                lines, alerts = await monitor.emit_periodic_logs()
                if not config.quiet:
                    for line in lines:
                        print(line, flush=True)
                for alert in alerts:
                    heartbeat_text = f"[HEARTBEAT_ALERT] {alert}"
                    if not config.quiet:
                        print(heartbeat_text, flush=True)

        periodic_task = asyncio.create_task(periodic_logger())

    print(
        f"Listening for Variational forwarder events on "
        f"ws://{config.host}:{config.ws_port} (WS) and "
        f"ws://{config.host}:{config.rest_port} (REST); "
        f"command broker ws://{config.host}:{config.command_port}",
        flush=True,
    )

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        if periodic_task is not None:
            periodic_task.cancel()
            await asyncio.gather(periodic_task, return_exceptions=True)
        command_server.close()
        ws_server.close()
        rest_server.close()
        await command_server.wait_closed()
        await ws_server.wait_closed()
        await rest_server.wait_closed()


def parse_args() -> ListenerConfig:
    parser = argparse.ArgumentParser(description="Run local receivers for Variational CDP forwarder events.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind receivers.")
    parser.add_argument("--ws-port", type=int, default=8866, help="Port for WebSocket frame events.")
    parser.add_argument("--rest-port", type=int, default=8867, help="Port for REST response events.")
    parser.add_argument("--command-port", type=int, default=8868, help="Port for PLACE_ORDER command broker.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for JSONL event files.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress all monitor logs in terminal (still writes to files when --output-dir is used).",
    )
    parser.add_argument(
        "--no-monitor",
        action="store_true",
        help="Disable live monitor parsing for quotes/trades/positions.",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=20,
        help="How many recent trade updates to keep in monitor state.",
    )
    parser.add_argument(
        "--snapshot-file",
        type=Path,
        default=None,
        help="Optional path for live monitor snapshot JSON.",
    )
    args = parser.parse_args()
    snapshot_file = args.snapshot_file
    if snapshot_file is None and args.output_dir is not None:
        snapshot_file = args.output_dir / "monitor_state.json"

    return ListenerConfig(
        host=args.host,
        ws_port=args.ws_port,
        rest_port=args.rest_port,
        command_port=args.command_port,
        output_dir=args.output_dir,
        quiet=args.quiet,
        monitor=not args.no_monitor,
        trade_limit=max(1, args.trade_limit),
        snapshot_file=snapshot_file,
    )


def main() -> None:
    config = parse_args()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        print("\nReceiver stopped.", flush=True)


if __name__ == "__main__":
    main()
