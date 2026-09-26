#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
import websockets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import BasisSampleStore, read_basis_samples  # noqa: E402
from variational.listener import CommandBroker, EventSink, run_command_server, run_receiver_server  # noqa: E402


ALLOWED_ASSETS = {"BTC", "ETH", "SOL"}
LIGHTER_REST_URL = "https://api.rh.lighter.xyz/api/v1/orderBooks"
LIGHTER_WS_URL = "wss://api.rh.lighter.xyz/stream"
STABLECOIN_URL = "https://api.binance.com/api/v3/ticker/price?symbol=USDCUSDT"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def percentile(values: list[Decimal], pct: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((Decimal(len(ordered) - 1) * pct / Decimal("100")).to_integral_value(rounding="ROUND_HALF_UP"))
    return ordered[max(0, min(index, len(ordered) - 1))]


def edge_bps(left: Decimal, right: Decimal) -> Decimal:
    return (left - right) / right * Decimal("10000")


def spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
    mid = (bid + ask) / Decimal("2")
    return (ask - bid) / mid * Decimal("10000")


def roundtrip_bps(
    direction: str,
    *,
    var_bid: Decimal,
    var_ask: Decimal,
    lighter_buy: Decimal,
    lighter_sell: Decimal,
) -> Decimal:
    if direction == "long_var_short_lighter":
        pnl = (var_bid - var_ask) + (lighter_sell - lighter_buy)
        return pnl / var_ask * Decimal("10000")
    pnl = (var_bid - var_ask) + (lighter_sell - lighter_buy)
    return pnl / var_bid * Decimal("10000")


def _extract_rate_limit_ms(value: Any) -> int | None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "rateLimitResetMs" and item is not None:
                with contextlib.suppress(TypeError, ValueError):
                    return max(0, int(item))
            nested = _extract_rate_limit_ms(item)
            if nested is not None:
                return nested
    return None


@dataclass
class MarketBook:
    asset: str
    market_id: int
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    nonce: int | None = None
    server_timestamp_ms: int | None = None
    received_monotonic: float | None = None
    ready: bool = False
    resyncing: bool = True
    cold_until: float = 0.0
    gaps: int = 0


class MultiLighterBooks:
    def __init__(self, assets: tuple[str, ...], logger: logging.Logger) -> None:
        self.assets = assets
        self.logger = logger
        self.books: dict[int, MarketBook] = {}
        self.by_asset: dict[str, MarketBook] = {}
        self.lock = asyncio.Lock()
        self.stop = False

    async def load_markets(self) -> None:
        response = await asyncio.to_thread(requests.get, LIGHTER_REST_URL, timeout=10)
        response.raise_for_status()
        markets = response.json().get("order_books", [])
        by_symbol = {str(item.get("symbol") or "").upper(): item for item in markets}
        for asset in self.assets:
            item = by_symbol.get(asset)
            if item is None:
                raise RuntimeError(f"Lighter market not found: {asset}")
            book = MarketBook(asset=asset, market_id=int(item["market_id"]))
            self.books[book.market_id] = book
            self.by_asset[asset] = book

    @staticmethod
    def _market_id(message: dict[str, Any]) -> int | None:
        channel = str(message.get("channel") or "")
        for separator in (":", "/"):
            if channel.startswith(f"order_book{separator}"):
                with contextlib.suppress(ValueError):
                    return int(channel.split(separator, 1)[1])
        return None

    @staticmethod
    def _apply_levels(target: dict[Decimal, Decimal], levels: list[Any]) -> None:
        for level in levels:
            if isinstance(level, dict):
                price = Decimal(str(level.get("price") or "0"))
                size = Decimal(str(level.get("size") or "0"))
            elif isinstance(level, list) and len(level) >= 2:
                price, size = Decimal(str(level[0])), Decimal(str(level[1]))
            else:
                continue
            if size > 0:
                target[price] = size
            else:
                target.pop(price, None)

    async def _subscribe_all(self, websocket: Any) -> None:
        for book in self.books.values():
            await websocket.send(json.dumps({"type": "subscribe", "channel": f"order_book/{book.market_id}"}))

    async def run(self) -> None:
        backoff = 1.0
        use_server_pings = os.getenv("LIGHTER_WS_SERVER_PINGS", "").strip().lower() in {"1", "true", "yes", "on"}
        websocket_url = f"{LIGHTER_WS_URL}?server_pings=true" if use_server_pings else LIGHTER_WS_URL
        while not self.stop:
            try:
                async with websockets.connect(websocket_url, ping_interval=30, ping_timeout=30, max_size=None) as websocket:
                    await self._subscribe_all(websocket)
                    backoff = 1.0
                    async for raw in websocket:
                        if self.stop:
                            return
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        message = json.loads(raw)
                        if message.get("type") == "ping":
                            await websocket.send(json.dumps({"type": "pong"}))
                            continue
                        market_id = self._market_id(message)
                        if market_id not in self.books:
                            continue
                        payload = message.get("order_book") or {}
                        begin_nonce = payload.get("begin_nonce")
                        nonce = payload.get("nonce")
                        message_type = str(message.get("type") or "")
                        async with self.lock:
                            book = self.books[market_id]
                            snapshot = not book.ready or message_type.startswith("subscribed/")
                            if not snapshot and begin_nonce is not None and book.nonce is not None and int(begin_nonce) != book.nonce:
                                book.gaps += 1
                                book.ready = False
                                book.resyncing = True
                                book.bids.clear()
                                book.asks.clear()
                                self.logger.warning(
                                    "lighter_sequence_gap asset=%s market_id=%s expected_begin_nonce=%s actual=%s",
                                    book.asset,
                                    market_id,
                                    book.nonce,
                                    begin_nonce,
                                )
                                await websocket.send(json.dumps({"type": "unsubscribe", "channel": f"order_book/{market_id}"}))
                                await websocket.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
                                continue
                            if snapshot:
                                book.bids.clear()
                                book.asks.clear()
                            self._apply_levels(book.bids, payload.get("bids", []))
                            self._apply_levels(book.asks, payload.get("asks", []))
                            book.nonce = int(nonce) if nonce is not None else book.nonce
                            timestamp = message.get("timestamp")
                            book.server_timestamp_ms = int(timestamp) if timestamp is not None else None
                            book.received_monotonic = time.monotonic()
                            book.ready = bool(book.bids and book.asks)
                            if book.ready and book.resyncing:
                                book.resyncing = False
                                book.cold_until = time.monotonic() + 5.0
                                self.logger.info("lighter_book_ready asset=%s market_id=%s", book.asset, market_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("lighter_ws_reconnect error=%s backoff=%.1f", exc, backoff)
                async with self.lock:
                    for book in self.books.values():
                        book.ready = False
                        book.resyncing = True
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    @staticmethod
    def _fill(levels: dict[Decimal, Decimal], *, side: str, quote_notional: Decimal) -> Decimal | None:
        ordered = sorted(levels.items(), reverse=side == "SELL")
        remaining_quote = quote_notional
        total_base = Decimal("0")
        total_quote = Decimal("0")
        for price, size in ordered:
            available_quote = price * size
            take_quote = min(available_quote, remaining_quote)
            if price <= 0:
                continue
            total_base += take_quote / price
            total_quote += take_quote
            remaining_quote -= take_quote
            if remaining_quote <= 0:
                break
        if remaining_quote > Decimal("0.000001") or total_base <= 0:
            return None
        return total_quote / total_base

    async def snapshot(self, asset: str, quote_notional: Decimal) -> dict[str, Any] | None:
        async with self.lock:
            book = self.by_asset[asset]
            if not book.ready or not book.bids or not book.asks:
                return None
            bid = max(book.bids)
            ask = min(book.asks)
            now_mono = time.monotonic()
            return {
                "bid": bid,
                "ask": ask,
                "sell_price": self._fill(book.bids, side="SELL", quote_notional=quote_notional),
                "buy_price": self._fill(book.asks, side="BUY", quote_notional=quote_notional),
                "nonce": book.nonce,
                "server_timestamp_ms": book.server_timestamp_ms,
                "book_age_seconds": None if book.received_monotonic is None else now_mono - book.received_monotonic,
                "continuity_ok": not book.resyncing,
                "cold": now_mono < book.cold_until,
                "sequence_gaps": book.gaps,
            }

    async def wait_ready(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            async with self.lock:
                if all(book.ready for book in self.books.values()):
                    return
            await asyncio.sleep(0.2)
        missing = [book.asset for book in self.books.values() if not book.ready]
        raise RuntimeError(f"Timed out waiting for Lighter books: {missing}")


class VariationalQuoteClient:
    def __init__(self, url: str, *, timeout: float, logger: logging.Logger) -> None:
        self.url = url
        self.timeout = timeout
        self.logger = logger
        self.websocket: Any = None

    async def close(self) -> None:
        if self.websocket is not None:
            with contextlib.suppress(Exception):
                await self.websocket.close()
            self.websocket = None

    async def quote(self, asset: str, amount: Decimal) -> tuple[dict[str, Any], float]:
        if self.websocket is None:
            self.websocket = await websockets.connect(self.url, ping_interval=20, ping_timeout=20)
        request_id = uuid.uuid4().hex
        payload = {
            "type": "VAR_API_QUOTE",
            "requestId": request_id,
            "market": asset,
            "amount": decimal_text(amount),
            "confirm": False,
        }
        started = time.monotonic()
        try:
            await self.websocket.send(json.dumps(payload, ensure_ascii=True))
            while True:
                raw = await asyncio.wait_for(self.websocket.recv(), timeout=self.timeout)
                message = json.loads(raw)
                if message.get("requestId") == request_id:
                    return message, (time.monotonic() - started) * 1000.0
        except Exception:
            await self.close()
            raise


class MultiAssetCollector:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.assets = tuple(args.assets)
        self.output_dir = Path(args.output_dir).resolve()
        self.sample_root = self.output_dir / "basis_samples"
        self.stop = False
        self.run_id = f"multibasis-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self.commit = self._git_commit()
        config_payload = {key: str(value) for key, value in sorted(vars(args).items())}
        self.config_hash = hashlib.sha256(json.dumps(config_payload, sort_keys=True).encode()).hexdigest()[:16]
        self.store = BasisSampleStore(self.sample_root, config_hash=self.config_hash, commit=self.commit)
        self.logger = logging.getLogger("basis_collector")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(self.output_dir / "basis_collector.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(handler)
        self.logger.addHandler(logging.StreamHandler())
        self.books = MultiLighterBooks(self.assets, self.logger)
        self.quote_client = VariationalQuoteClient(
            f"ws://{args.forwarder_host}:{args.forwarder_command_port}",
            timeout=args.quote_timeout_seconds,
            logger=self.logger,
        )
        self.histories: dict[str, dict[str, deque[Decimal]]] = {
            asset: {"long": deque(maxlen=8640), "short": deque(maxlen=8640)} for asset in self.assets
        }
        self.last_baseline: dict[str, float] = {asset: 0.0 for asset in self.assets}
        self.last_poll: dict[str, float] = {asset: 0.0 for asset in self.assets}
        self.last_basis: dict[str, Decimal | None] = {asset: None for asset in self.assets}
        self.burst_until: dict[str, float] = {asset: 0.0 for asset in self.assets}
        self.prebuffers: dict[str, deque[dict[str, Any]]] = {asset: deque() for asset in self.assets}
        self.errors: Counter[str] = Counter()
        self.samples: Counter[str] = Counter()
        self.extension_failures = 0
        self.rate_limit_until = 0.0
        self.stablecoin_cache: tuple[float, dict[str, Any]] = (0.0, {})
        self.servers: list[Any] = []

    @staticmethod
    def _git_commit() -> str:
        try:
            return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True).strip()
        except Exception:
            return "unknown"

    def request_stop(self, *_args: Any) -> None:
        self.stop = True

    @staticmethod
    def _normalize_error(error: Any) -> str:
        text = " ".join(str(error or "unknown_error").split())
        lowered = text.lower()
        if "<!doctype html" in lowered or "<html" in lowered:
            return "variational_html_response"
        if text.startswith("{") or text.startswith("["):
            return "variational_structured_error_response"
        status = re.search(r"\bHTTP\s+([45]\d\d)\b", text, flags=re.IGNORECASE)
        if status:
            return f"HTTP {status.group(1)}"
        if len(text) > 160:
            return text[:157] + "..."
        return text

    def _load_history(self) -> None:
        for asset in self.assets:
            rows = read_basis_samples(self.sample_root, limit=20000, asset_filter=asset)
            for row in rows:
                if str(row.get("sample_kind") or "baseline") != "baseline":
                    continue
                if str(row.get("sample_quality") or "valid") != "valid":
                    continue
                long_edge = row.get("long_edge_bps")
                short_edge = row.get("short_edge_bps")
                with contextlib.suppress(Exception):
                    self.histories[asset]["long"].append(Decimal(str(long_edge)))
                with contextlib.suppress(Exception):
                    self.histories[asset]["short"].append(Decimal(str(short_edge)))
            self.logger.info("history_loaded asset=%s baseline_rows=%s", asset, len(self.histories[asset]["long"]))

    async def _stablecoin(self) -> dict[str, Any]:
        now = time.monotonic()
        if now - self.stablecoin_cache[0] < 30.0:
            return self.stablecoin_cache[1]
        context: dict[str, Any] = {"stablecoin_rate_source": "binance_usdcusdt"}
        try:
            response = await asyncio.to_thread(requests.get, STABLECOIN_URL, timeout=3)
            response.raise_for_status()
            price = Decimal(str(response.json()["price"]))
            context.update(
                {
                    "stablecoin_rate_status": "ok",
                    "usdcusdt_price": decimal_text(price),
                    "stablecoin_basis_bps": decimal_text((price - Decimal("1")) * Decimal("10000")),
                    "stablecoin_rate_fetched_at": utc_now(),
                }
            )
        except Exception as exc:
            context.update({"stablecoin_rate_status": "unavailable", "stablecoin_rate_error": str(exc)})
        self.stablecoin_cache = (now, context)
        return context

    @staticmethod
    def _book_drift_bps(before: dict[str, Any], after: dict[str, Any]) -> Decimal:
        values: list[Decimal] = []
        for key in ("bid", "ask"):
            left = before.get(key)
            right = after.get(key)
            if isinstance(left, Decimal) and isinstance(right, Decimal) and left > 0:
                values.append(abs(right - left) / left * Decimal("10000"))
        return max(values) if values else Decimal("999")

    async def _build_row(self, asset: str) -> tuple[dict[str, Any] | None, str | None]:
        before = await self.books.snapshot(asset, Decimal(str(self.args.quote_notional_usd)))
        if before is None:
            return None, "lighter_book_unavailable"
        try:
            message, quote_ms = await self.quote_client.quote(asset, Decimal(str(self.args.quote_notional_usd)))
        except Exception as exc:
            return None, f"variational_quote_exception:{type(exc).__name__}:{exc}"
        after = await self.books.snapshot(asset, Decimal(str(self.args.quote_notional_usd)))
        if after is None:
            return None, "lighter_book_unavailable_after_quote"
        if not message.get("ok"):
            reset_ms = _extract_rate_limit_ms(message)
            if reset_ms is not None:
                self.rate_limit_until = max(self.rate_limit_until, time.monotonic() + max(1.0, reset_ms / 1000.0))
            return None, str(message.get("error") or message)
        quote = message.get("result") if isinstance(message.get("result"), dict) else message
        var_bid = Decimal(str(quote.get("bid")))
        var_ask = Decimal(str(quote.get("ask")))
        lighter_bid = after["bid"]
        lighter_ask = after["ask"]
        lighter_sell = after["sell_price"]
        lighter_buy = after["buy_price"]
        if lighter_sell is None or lighter_buy is None:
            return None, "lighter_depth_insufficient"
        now_dt = datetime.now(timezone.utc)
        quote_dt = parse_timestamp(quote.get("quoteTimestamp") or quote.get("timestamp"))
        quote_age = None if quote_dt is None else (now_dt - quote_dt).total_seconds()
        book_age = after.get("book_age_seconds")
        server_timestamp_ms = after.get("server_timestamp_ms")
        lighter_clock_drift = (
            None
            if server_timestamp_ms is None
            else abs(now_dt.timestamp() - float(server_timestamp_ms) / 1000.0)
        )
        drift = self._book_drift_bps(before, after)
        reasons: list[str] = []
        invalid = False
        if not after.get("continuity_ok"):
            invalid, reasons = True, ["lighter_sequence_not_continuous"]
        if quote_age is None or quote_age < -1 or quote_age > self.args.max_quote_age_seconds:
            invalid, reasons = True, [*reasons, "variational_quote_age_invalid"]
        if book_age is None or book_age > self.args.max_book_age_seconds:
            invalid, reasons = True, [*reasons, "lighter_book_age_invalid"]
        if lighter_clock_drift is not None and lighter_clock_drift > self.args.max_clock_drift_seconds:
            invalid, reasons = True, [*reasons, "lighter_clock_drift"]
        degraded = False
        if after.get("cold"):
            degraded, reasons = True, [*reasons, "lighter_resync_cooldown"]
        if quote_ms > self.args.max_quote_roundtrip_ms:
            degraded, reasons = True, [*reasons, "variational_quote_slow"]
        if drift > Decimal(str(self.args.max_pair_drift_bps)):
            degraded, reasons = True, [*reasons, "lighter_moved_during_var_quote"]
        quality = "invalid" if invalid else "degraded" if degraded else "valid"
        basis_mid = (var_bid + var_ask) / Decimal("2")
        lighter_mid = (lighter_bid + lighter_ask) / Decimal("2")
        basis = (basis_mid - lighter_mid) / lighter_mid * Decimal("10000")
        long_edge = edge_bps(lighter_sell, var_ask)
        short_edge = edge_bps(var_bid, lighter_buy)
        stablecoin = await self._stablecoin()
        rate = Decimal(str(stablecoin["usdcusdt_price"])) if stablecoin.get("stablecoin_rate_status") == "ok" else None
        normalized_var_bid = var_bid * rate if rate is not None else None
        normalized_var_ask = var_ask * rate if rate is not None else None
        previous_basis = self.last_basis[asset]
        move = None if previous_basis is None else abs(basis - previous_basis)
        self.last_basis[asset] = basis
        row = {
            "event": "live_inventory_basis_state",
            "logged_at": utc_now(),
            "sample_id": uuid.uuid4().hex,
            "sample_kind": "watch",
            "sample_quality": quality,
            "sample_quality_reasons": reasons,
            "record_kind": "basis_market_sample",
            "execution_mode": "collect_only",
            "run_id": self.run_id,
            "strategy_version": "basis-multi-collector-v1",
            "strategy_variant": "baseline-burst-executable-state",
            "asset": asset,
            "quote_id": quote.get("quoteId") or quote.get("quote_id"),
            "quote_timestamp": quote.get("quoteTimestamp") or quote.get("timestamp"),
            "quote_ms": f"{quote_ms:.3f}",
            "var_quote_age_seconds": None if quote_age is None else f"{quote_age:.6f}",
            "lighter_book_age_seconds": None if book_age is None else f"{book_age:.6f}",
            "lighter_clock_drift_seconds": None if lighter_clock_drift is None else f"{lighter_clock_drift:.6f}",
            "lighter_pair_drift_bps": decimal_text(drift),
            "lighter_nonce": after.get("nonce"),
            "lighter_sequence_gaps": after.get("sequence_gaps"),
            "lighter_continuity_ok": after.get("continuity_ok"),
            "var_bid": decimal_text(var_bid),
            "var_ask": decimal_text(var_ask),
            "lighter_bid": decimal_text(lighter_bid),
            "lighter_ask": decimal_text(lighter_ask),
            "lighter_buy_price": decimal_text(lighter_buy),
            "lighter_sell_price": decimal_text(lighter_sell),
            "var_spread_bps": decimal_text(spread_bps(var_bid, var_ask)),
            "lighter_spread_bps": decimal_text(spread_bps(lighter_bid, lighter_ask)),
            "basis_bps": decimal_text(basis),
            "basis_sample_move_bps": decimal_text(move),
            "basis_sample_move_ok": move is None or move <= Decimal("3"),
            "long_edge_bps": decimal_text(long_edge),
            "short_edge_bps": decimal_text(short_edge),
            "long_roundtrip_pnl_bps": decimal_text(roundtrip_bps("long_var_short_lighter", var_bid=var_bid, var_ask=var_ask, lighter_buy=lighter_buy, lighter_sell=lighter_sell)),
            "short_roundtrip_pnl_bps": decimal_text(roundtrip_bps("short_var_long_lighter", var_bid=var_bid, var_ask=var_ask, lighter_buy=lighter_buy, lighter_sell=lighter_sell)),
            "normalized_var_bid": decimal_text(normalized_var_bid),
            "normalized_var_ask": decimal_text(normalized_var_ask),
            "normalized_long_edge_bps": decimal_text(edge_bps(lighter_sell, normalized_var_ask)) if normalized_var_ask else None,
            "normalized_short_edge_bps": decimal_text(edge_bps(normalized_var_bid, lighter_buy)) if normalized_var_bid else None,
            "basis_collect_only": True,
            "fee_bps_per_leg": "0",
            "cost_model": "executable_bid_ask_plus_shortfall_reserve_no_fees",
            **stablecoin,
        }
        return row, None

    def _trigger_burst(self, asset: str, row: dict[str, Any]) -> bool:
        if row.get("sample_quality") != "valid":
            return False
        long_values = list(self.histories[asset]["long"])
        short_values = list(self.histories[asset]["short"])
        if len(long_values) < 60 or len(short_values) < 60:
            return False
        long_edge = Decimal(str(row["long_edge_bps"]))
        short_edge = Decimal(str(row["short_edge_bps"]))
        move = Decimal(str(row.get("basis_sample_move_bps") or "0"))
        return (
            long_edge > (percentile(long_values, Decimal("85")) or long_edge)
            or short_edge > (percentile(short_values, Decimal("85")) or short_edge)
            or move >= Decimal(str(self.args.burst_move_bps))
        )

    def _write_sample(self, row: dict[str, Any], kind: str) -> None:
        payload = {**row, "sample_kind": kind}
        self.store.append(payload)
        self.samples[f"{row['asset']}:{kind}"] += 1

    def _process_row(self, asset: str, row: dict[str, Any], now: float) -> None:
        baseline_due = now - self.last_baseline[asset] >= self.args.baseline_interval_seconds
        trigger = self._trigger_burst(asset, row)
        if trigger:
            self.burst_until[asset] = max(self.burst_until[asset], now + self.args.burst_post_seconds)
            while self.prebuffers[asset]:
                buffered = self.prebuffers[asset].popleft()
                self._write_sample(buffered, "burst")
        if baseline_due:
            self._write_sample(row, "baseline")
            self.last_baseline[asset] = now
            if row.get("sample_quality") == "valid":
                self.histories[asset]["long"].append(Decimal(str(row["long_edge_bps"])))
                self.histories[asset]["short"].append(Decimal(str(row["short_edge_bps"])))
        elif now <= self.burst_until[asset]:
            self._write_sample(row, "burst")
        else:
            self.prebuffers[asset].append(row)
        cutoff = datetime.now(timezone.utc).timestamp() - self.args.burst_pre_seconds
        while self.prebuffers[asset]:
            item_time = parse_timestamp(self.prebuffers[asset][0].get("logged_at"))
            if item_time is not None and item_time.timestamp() >= cutoff:
                break
            self.prebuffers[asset].popleft()

    def _health(self) -> dict[str, Any]:
        usage = shutil.disk_usage(self.output_dir)
        return {
            "event": "basis_collector_health",
            "updated_at": utc_now(),
            "run_id": self.run_id,
            "pid": os.getpid(),
            "assets": list(self.assets),
            "samples": dict(self.samples),
            "errors": dict(self.errors),
            "extension_consecutive_failures": self.extension_failures,
            "disk_free_bytes": usage.free,
            "disk_free_gb": round(usage.free / (1024**3), 3),
            "collector_config_hash": self.config_hash,
            "collector_commit": self.commit,
        }

    def _write_health(self) -> None:
        path = self.sample_root / "health.json"
        temporary = path.with_suffix(".json.tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(json.dumps(self._health(), ensure_ascii=True, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, path)

    async def _start_servers(self) -> None:
        sink = EventSink(output_dir=None, quiet=True, monitor=None)
        broker = CommandBroker(quiet=True)
        self.servers = [
            await run_receiver_server("ws", self.args.forwarder_host, self.args.forwarder_ws_port, sink),
            await run_receiver_server("rest", self.args.forwarder_host, self.args.forwarder_rest_port, sink),
            await run_command_server(self.args.forwarder_host, self.args.forwarder_command_port, broker),
        ]

    async def run(self) -> None:
        initial_free_gb = shutil.disk_usage(self.output_dir).free / (1024**3)
        if initial_free_gb < self.args.disk_stop_free_gb:
            raise RuntimeError(f"disk_free_below_stop_threshold free_gb={initial_free_gb:.3f}")
        self._load_history()
        self.store.rotate_closed_days()
        await self.books.load_markets()
        await self._start_servers()
        book_task = asyncio.create_task(self.books.run())
        try:
            await self.books.wait_ready()
            for asset_index, asset in enumerate(self.assets):
                deadline = time.monotonic() + 30.0
                while True:
                    message, elapsed = await self.quote_client.quote(asset, Decimal("0.00000001"))
                    if message.get("ok"):
                        self.logger.info("variational_api_command_client_preflight_passed asset=%s quote_ms=%.3f", asset, elapsed)
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"Variational preflight failed asset={asset}: {message.get('error') or message}")
                    await asyncio.sleep(1)
                if asset_index + 1 < len(self.assets):
                    await asyncio.sleep(self.args.global_poll_interval_seconds)
            self.logger.info(
                "basis_multi_collector_started assets=%s run_id=%s baseline_interval=%ss global_poll=%ss",
                ",".join(self.assets),
                self.run_id,
                self.args.baseline_interval_seconds,
                self.args.global_poll_interval_seconds,
            )
            last_health = 0.0
            while not self.stop:
                now = time.monotonic()
                if now < self.rate_limit_until:
                    await asyncio.sleep(min(1.0, self.rate_limit_until - now))
                    continue
                asset = min(self.assets, key=lambda item: self.last_poll[item])
                row, error = await self._build_row(asset)
                self.last_poll[asset] = time.monotonic()
                if error:
                    normalized_error = self._normalize_error(error)
                    self.errors[f"{asset}:{normalized_error}"] += 1
                    if "No extension command client connected" in error:
                        self.extension_failures += 1
                        if self.extension_failures >= self.args.extension_failure_limit:
                            raise RuntimeError("No extension command client connected (consecutive failure fuse)")
                    else:
                        self.extension_failures = 0
                elif row is not None:
                    self.extension_failures = 0
                    self._process_row(asset, row, time.monotonic())
                if time.monotonic() - last_health >= 60.0:
                    last_health = time.monotonic()
                    self.store.write_manifests()
                    self.store.rotate_closed_days()
                    self._write_health()
                    health = self._health()
                    self.logger.info(
                        "collector_health samples=%s error_total=%s error_types=%s "
                        "extension_failures=%s disk_free_gb=%s",
                        health["samples"],
                        sum(health["errors"].values()),
                        len(health["errors"]),
                        health["extension_consecutive_failures"],
                        health["disk_free_gb"],
                    )
                    free_gb = float(health["disk_free_gb"])
                    if free_gb < self.args.disk_stop_free_gb:
                        raise RuntimeError(f"disk_free_below_stop_threshold free_gb={free_gb}")
                    if free_gb < self.args.disk_warn_free_gb:
                        self.logger.warning("disk_free_below_warning_threshold free_gb=%s", free_gb)
                elapsed = time.monotonic() - self.last_poll[asset]
                await asyncio.sleep(max(0.0, self.args.global_poll_interval_seconds - elapsed))
        finally:
            self.stop = True
            self.books.stop = True
            book_task.cancel()
            await asyncio.gather(book_task, return_exceptions=True)
            await self.quote_client.close()
            self.store.write_manifests()
            self._write_health()
            for server in self.servers:
                server.close()
                await server.wait_closed()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Order-free multi-asset executable basis collector.")
    parser.add_argument("--assets", required=True, help="Comma-separated BTC,ETH,SOL subset.")
    parser.add_argument("--output-dir", default=str(ROOT / "log"))
    parser.add_argument("--forwarder-host", default="127.0.0.1")
    parser.add_argument("--forwarder-ws-port", type=int, default=8866)
    parser.add_argument("--forwarder-rest-port", type=int, default=8867)
    parser.add_argument("--forwarder-command-port", type=int, default=8868)
    parser.add_argument("--quote-notional-usd", type=float, default=20.0)
    parser.add_argument("--global-poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--baseline-interval-seconds", type=float, default=10.0)
    parser.add_argument("--burst-pre-seconds", type=float, default=60.0)
    parser.add_argument("--burst-post-seconds", type=float, default=60.0)
    parser.add_argument("--burst-move-bps", type=float, default=2.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-quote-age-seconds", type=float, default=2.0)
    parser.add_argument("--max-book-age-seconds", type=float, default=2.0)
    parser.add_argument("--max-clock-drift-seconds", type=float, default=5.0)
    parser.add_argument("--max-quote-roundtrip-ms", type=float, default=1500.0)
    parser.add_argument("--max-pair-drift-bps", type=float, default=1.5)
    parser.add_argument("--extension-failure-limit", type=int, default=3)
    parser.add_argument("--disk-warn-free-gb", type=float, default=5.0)
    parser.add_argument("--disk-stop-free-gb", type=float, default=3.0)
    args = parser.parse_args(argv)
    assets = tuple(dict.fromkeys(token.strip().upper() for token in args.assets.split(",") if token.strip()))
    if not assets or any(asset not in ALLOWED_ASSETS for asset in assets):
        parser.error(f"--assets must contain only {sorted(ALLOWED_ASSETS)}")
    if len(assets) < 2:
        parser.error("multi-asset collector requires at least two assets")
    if args.global_poll_interval_seconds <= 0 or args.baseline_interval_seconds <= 0:
        parser.error("poll and baseline intervals must be > 0")
    if args.disk_stop_free_gb <= 0 or args.disk_warn_free_gb <= args.disk_stop_free_gb:
        parser.error("disk warning threshold must exceed the positive stop threshold")
    args.assets = assets
    return args


async def _amain(args: argparse.Namespace) -> None:
    collector = MultiAssetCollector(args)
    loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signame, collector.request_stop)
    await collector.run()


def main() -> int:
    args = parse_args()
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"COLLECTOR_STOPPED reason={type(exc).__name__}:{exc}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
