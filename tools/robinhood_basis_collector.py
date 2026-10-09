#!/usr/bin/env python3
"""Read-only Variational/Lighter basis sidecar.

The sidecar consumes Variational quotes already persisted by the live process.
It never connects to the Variational extension and never imports trading keys.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import shutil
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.basis_store import BasisSampleStore  # noqa: E402


ROBINHOOD_LIGHTER_REST_URL = "https://api.rh.lighter.xyz/api/v1/orderBooks"
ROBINHOOD_LIGHTER_ORDER_BOOK_URL = (
    "https://api.rh.lighter.xyz/api/v1/orderBookOrders"
)
LIGHTER_VENUE_HOSTS = {
    "robinhood_chain_lighter": "api.rh.lighter.xyz",
    "mainnet_lighter": "mainnet.zklighter.elliot.ai",
}
DEFAULT_NOTIONALS = (Decimal("20"), Decimal("40"), Decimal("60"))
TRADE_EVENT_SNAPSHOT_EVENTS = {
    "live_inventory_entry_shadow_candidate",
    "live_inventory_entered",
    "live_inventory_v4_exit_fast_refresh_confirmed",
    "live_inventory_final_pnl",
}


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


def edge_bps(left: Decimal, right: Decimal) -> Decimal:
    return (left - right) / right * Decimal("10000")


def spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
    mid = (bid + ask) / Decimal("2")
    return (ask - bid) / mid * Decimal("10000")


def source_identity(row: dict[str, Any]) -> str:
    return str(
        row.get("sample_id")
        or f"{row.get('run_id')}:{row.get('sample_index')}:{row.get('logged_at')}"
    )


def event_identity(row: dict[str, Any]) -> str:
    return ":".join(
        str(value or "-")
        for value in (
            row.get("run_id"),
            row.get("event"),
            row.get("lot_id"),
            row.get("logged_at"),
        )
    )


def infer_venue(rest_url: str, orders_url: str) -> str:
    rest_host = (urlparse(rest_url).hostname or "").lower()
    orders_host = (urlparse(orders_url).hostname or "").lower()
    if rest_host != orders_host:
        raise ValueError("market and order-book endpoints must use the same host")
    for venue, host in LIGHTER_VENUE_HOSTS.items():
        if rest_host == host:
            return venue
    raise ValueError(f"unsupported Lighter market-data host: {rest_host or '-'}")


class FixedJsonlFollower:
    """Follow appended complete JSON lines from one file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.position = 0
        self.buffer = b""

    def read_new(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        size = self.path.stat().st_size
        if size < self.position:
            self.position = 0
            self.buffer = b""
        with self.path.open("rb") as handle:
            handle.seek(self.position)
            chunk = handle.read()
            self.position = handle.tell()
        if not chunk:
            return []
        payload = self.buffer + chunk
        lines = payload.split(b"\n")
        self.buffer = lines.pop()
        rows: list[dict[str, Any]] = []
        for line in lines:
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
        return rows

    def seek_to_end(self) -> None:
        self.position = self.path.stat().st_size if self.path.exists() else 0
        self.buffer = b""


class CurrentDayJsonlFollower:
    """Follow the live process's current UTC-day basis sample file."""

    def __init__(self, root: Path, asset: str) -> None:
        self.root = root
        self.asset = asset.upper()
        self.path: Path | None = None
        self.position = 0
        self.buffer = b""

    @staticmethod
    def _day() -> str:
        return datetime.now(timezone.utc).date().isoformat()

    def current_path(self) -> Path:
        return self.root / self.asset / f"{self._day()}.jsonl"

    def read_new(self) -> list[dict[str, Any]]:
        path = self.current_path()
        if path != self.path:
            self.path = path
            self.position = 0
            self.buffer = b""
        if not path.exists():
            return []
        size = path.stat().st_size
        if size < self.position:
            self.position = 0
            self.buffer = b""
        with path.open("rb") as handle:
            handle.seek(self.position)
            chunk = handle.read()
            self.position = handle.tell()
        if not chunk:
            return []
        payload = self.buffer + chunk
        lines = payload.split(b"\n")
        self.buffer = lines.pop()
        rows: list[dict[str, Any]] = []
        for line in lines:
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
        return rows

    def seek_to_end(self) -> None:
        path = self.current_path()
        self.path = path
        self.position = path.stat().st_size if path.exists() else 0
        self.buffer = b""


@dataclass
class RestMarket:
    asset: str
    market_id: int
    ready: bool = False
    resyncing: bool = True
    received_monotonic: float | None = None
    gaps: int = 0


class RobinhoodRestBooks:
    """Low-rate public REST snapshots aligned to persisted Variational quotes."""

    def __init__(
        self,
        asset: str,
        logger: logging.Logger,
        *,
        markets_url: str,
        orders_url: str,
        order_limit: int,
        cache_seconds: float = 0.25,
    ) -> None:
        self.asset = asset
        self.assets = (asset,)
        self.logger = logger
        self.markets_url = markets_url
        self.orders_url = orders_url
        self.order_limit = order_limit
        self.cache_seconds = cache_seconds
        self.by_asset: dict[str, RestMarket] = {}
        self.stop = False
        self.lock = asyncio.Lock()
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.received_monotonic: float | None = None

    async def load_markets(self) -> None:
        response = await asyncio.to_thread(
            requests.get,
            self.markets_url,
            timeout=10,
        )
        response.raise_for_status()
        markets = response.json().get("order_books", [])
        market = next(
            (
                item
                for item in markets
                if str(item.get("symbol") or "").upper() == self.asset
            ),
            None,
        )
        if market is None:
            raise RuntimeError(f"Lighter market not found: {self.asset}")
        self.by_asset[self.asset] = RestMarket(
            asset=self.asset,
            market_id=int(market["market_id"]),
        )

    @staticmethod
    def _levels(rows: list[Any]) -> dict[Decimal, Decimal]:
        levels: dict[Decimal, Decimal] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = Decimal(str(row.get("price") or "0"))
            size = Decimal(
                str(
                    row.get("remaining_base_amount")
                    or row.get("size")
                    or "0"
                )
            )
            if price > 0 and size > 0:
                levels[price] = levels.get(price, Decimal("0")) + size
        return levels

    @staticmethod
    def _fill(
        levels: dict[Decimal, Decimal],
        *,
        side: str,
        quote_notional: Decimal,
    ) -> Decimal | None:
        ordered = sorted(levels.items(), reverse=side == "SELL")
        remaining_quote = quote_notional
        total_base = Decimal("0")
        total_quote = Decimal("0")
        for price, size in ordered:
            if price <= 0:
                continue
            take_quote = min(price * size, remaining_quote)
            total_base += take_quote / price
            total_quote += take_quote
            remaining_quote -= take_quote
            if remaining_quote <= 0:
                break
        if remaining_quote > Decimal("0.000001") or total_base <= 0:
            return None
        return total_quote / total_base

    async def _refresh(self, *, force: bool = False) -> None:
        async with self.lock:
            now = time.monotonic()
            if (
                not force
                and self.received_monotonic is not None
                and now - self.received_monotonic <= self.cache_seconds
            ):
                return
            market = self.by_asset[self.asset]
            response = await asyncio.to_thread(
                requests.get,
                self.orders_url,
                params={"market_id": market.market_id, "limit": self.order_limit},
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            bids = self._levels(payload.get("bids", []))
            asks = self._levels(payload.get("asks", []))
            if not bids or not asks:
                raise RuntimeError("Lighter REST book is empty")
            self.bids = bids
            self.asks = asks
            self.received_monotonic = time.monotonic()
            market.ready = True
            market.resyncing = False
            market.received_monotonic = self.received_monotonic

    async def run(self) -> None:
        while not self.stop:
            await asyncio.sleep(1.0)

    async def wait_ready(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                await self._refresh()
                return
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(1.0)
        raise RuntimeError(
            f"Timed out waiting for Lighter REST book: {last_error}"
        )

    async def snapshot(
        self,
        asset: str,
        quote_notional: Decimal,
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any] | None:
        if asset != self.asset:
            return None
        await self._refresh(force=force_refresh)
        async with self.lock:
            if not self.bids or not self.asks:
                return None
            now = time.monotonic()
            return {
                "bid": max(self.bids),
                "ask": min(self.asks),
                "sell_price": self._fill(
                    self.bids,
                    side="SELL",
                    quote_notional=quote_notional,
                ),
                "buy_price": self._fill(
                    self.asks,
                    side="BUY",
                    quote_notional=quote_notional,
                ),
                "nonce": None,
                "server_timestamp_ms": None,
                "book_age_seconds": (
                    None
                    if self.received_monotonic is None
                    else now - self.received_monotonic
                ),
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
                "transport": "rest_snapshot",
            }


class RobinhoodBasisCollector:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.asset = args.asset
        self.venue = args.venue
        self.venue_prefix = (
            "robinhood_lighter"
            if self.venue == "robinhood_chain_lighter"
            else "mainnet_lighter"
        )
        self.notional_ladder = tuple(args.notional_ladder)
        self.primary_notional = self.notional_ladder[0]
        self.output_dir = Path(args.output_dir).resolve()
        self.sample_root = self.output_dir / "robinhood_basis_samples"
        self.health_path = self.output_dir / "robinhood_basis_health.json"
        run_prefix = "rhbasis" if self.venue == "robinhood_chain_lighter" else "mainnetbasis"
        self.run_id = (
            f"{run_prefix}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-"
            f"{uuid.uuid4().hex[:8]}"
        )
        self.stop = False
        self.started_monotonic = time.monotonic()
        self.last_sample_at: str | None = None
        self.last_source_id: str | None = None
        self.last_event_snapshot_at: str | None = None
        self.last_event_id: str | None = None
        self.samples = 0
        self.baseline_samples = 0
        self.event_snapshots = 0
        self.skipped = 0
        self.errors: dict[str, int] = {}
        self.commit = self._git_commit()
        config_payload = {key: str(value) for key, value in sorted(vars(args).items())}
        self.config_hash = hashlib.sha256(
            json.dumps(config_payload, sort_keys=True).encode()
        ).hexdigest()[:16]
        self.store = BasisSampleStore(
            self.sample_root,
            config_hash=self.config_hash,
            commit=self.commit,
        )
        self.logger = self._logger()
        self.books = RobinhoodRestBooks(
            self.asset,
            self.logger,
            markets_url=args.rest_url,
            orders_url=args.order_book_orders_url,
            order_limit=args.order_limit,
        )
        self.follower = CurrentDayJsonlFollower(
            Path(args.source_root).resolve(),
            self.asset,
        )
        self.event_follower = FixedJsonlFollower(
            Path(args.event_source_path).resolve()
        )

    def _logger(self) -> logging.Logger:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("robinhood_basis_collector")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        handler = RotatingFileHandler(
            self.output_dir / "robinhood_basis_collector.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler())
        return logger

    @staticmethod
    def _git_commit() -> str:
        try:
            return subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=ROOT,
                text=True,
            ).strip()
        except Exception:
            return "unknown"

    def request_stop(self, *_args: Any) -> None:
        self.stop = True

    def _count_error(self, reason: str) -> None:
        self.errors[reason] = self.errors.get(reason, 0) + 1

    def _health(self) -> dict[str, Any]:
        usage = shutil.disk_usage(self.output_dir)
        book = self.books.by_asset.get(self.asset)
        return {
            "status": "stopping" if self.stop else "running",
            "updated_at": utc_now(),
            "run_id": self.run_id,
            "execution_mode": "collect_only",
            "venue": self.venue,
            "venue_rest_host": urlparse(self.args.rest_url).hostname,
            "asset": self.asset,
            "samples": self.samples,
            "baseline_samples": self.baseline_samples,
            "event_snapshots": self.event_snapshots,
            "skipped": self.skipped,
            "errors": self.errors,
            "last_sample_at": self.last_sample_at,
            "last_source_id": self.last_source_id,
            "last_event_snapshot_at": self.last_event_snapshot_at,
            "last_event_id": self.last_event_id,
            "source_path": str(self.follower.current_path()),
            "event_source_path": str(self.event_follower.path),
            "book_ready": bool(book and book.ready),
            "book_sequence_gaps": None if book is None else book.gaps,
            "uptime_seconds": round(time.monotonic() - self.started_monotonic, 3),
            "disk_free_gb": round(usage.free / 1024**3, 3),
        }

    def _write_health(self) -> None:
        temporary = self.health_path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(self._health(), ensure_ascii=True, sort_keys=True, indent=2)
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.health_path)

    @staticmethod
    def _source_is_eligible(row: dict[str, Any], asset: str) -> bool:
        return (
            str(row.get("event")) == "live_inventory_basis_state"
            and str(row.get("asset") or "").upper() == asset
            and str(row.get("sample_kind") or "baseline") == "baseline"
            and str(row.get("sample_quality") or "valid") == "valid"
            and row.get("var_bid") is not None
            and row.get("var_ask") is not None
        )

    @staticmethod
    def _event_is_eligible(row: dict[str, Any], asset: str) -> bool:
        event = str(row.get("event"))
        if event not in TRADE_EVENT_SNAPSHOT_EVENTS:
            return False
        if str(row.get("asset") or "").upper() != asset:
            return False
        if event == "live_inventory_entry_shadow_candidate":
            return row.get("shadow_status") == "passed"
        return True

    @staticmethod
    def _first_decimal(source: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
        for key in keys:
            value = source.get(key)
            if value is None:
                continue
            try:
                parsed = Decimal(str(value))
            except Exception:
                continue
            if parsed > 0:
                return parsed
        return None

    async def _book_snapshots(
        self,
        *,
        force_refresh: bool,
    ) -> tuple[dict[str, dict[str, Any]] | None, str | None]:
        snapshots: dict[str, dict[str, Any]] = {}
        for index, notional in enumerate(self.notional_ladder):
            snapshot = await self.books.snapshot(
                self.asset,
                notional,
                force_refresh=force_refresh and index == 0,
            )
            if snapshot is None:
                return None, f"{self.venue_prefix}_book_unavailable"
            if snapshot.get("sell_price") is None or snapshot.get("buy_price") is None:
                return None, (
                    f"{self.venue_prefix}_depth_insufficient_"
                    f"{decimal_text(notional)}"
                )
            snapshots[decimal_text(notional) or "0"] = snapshot
        primary = snapshots[decimal_text(self.primary_notional) or "0"]
        book_age = primary.get("book_age_seconds")
        if book_age is None or book_age > self.args.max_book_age_seconds:
            return None, f"{self.venue_prefix}_book_stale"
        if not primary.get("continuity_ok") or primary.get("cold"):
            return None, f"{self.venue_prefix}_book_not_continuous"
        return snapshots, None

    async def build_row(
        self,
        source: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        now = now or datetime.now(timezone.utc)
        source_time = parse_timestamp(source.get("logged_at"))
        if source_time is None:
            return None, "source_timestamp_invalid"
        source_age = (now - source_time).total_seconds()
        if source_age < -1 or source_age > self.args.max_source_age_seconds:
            return None, "source_sample_stale"
        capture_started_monotonic = time.monotonic()
        snapshots, snapshot_error = await self._book_snapshots(force_refresh=False)
        if snapshots is None:
            return None, snapshot_error
        capture_elapsed_seconds = max(
            0.0, time.monotonic() - capture_started_monotonic
        )
        capture_completed_at = now + timedelta(seconds=capture_elapsed_seconds)
        source_age += capture_elapsed_seconds
        if source_age > self.args.max_source_age_seconds:
            return None, "source_sample_stale_at_capture"
        primary = snapshots[decimal_text(self.primary_notional) or "0"]
        book_age = primary.get("book_age_seconds")
        book_received_at = capture_completed_at - timedelta(seconds=float(book_age))
        var_bid = Decimal(str(source["var_bid"]))
        var_ask = Decimal(str(source["var_ask"]))
        normalized_var_bid = source.get("normalized_var_bid")
        normalized_var_ask = source.get("normalized_var_ask")
        ladder: list[dict[str, Any]] = []
        for notional in self.notional_ladder:
            snapshot = snapshots[decimal_text(notional) or "0"]
            buy_price = snapshot["buy_price"]
            sell_price = snapshot["sell_price"]
            ladder.append(
                {
                    "notional_usd": decimal_text(notional),
                    "buy_price": decimal_text(buy_price),
                    "sell_price": decimal_text(sell_price),
                    "short_var_long_lighter_edge_bps": decimal_text(
                        edge_bps(var_bid, buy_price)
                    ),
                    "long_var_short_lighter_edge_bps": decimal_text(
                        edge_bps(sell_price, var_ask)
                    ),
                }
            )
        lighter_bid = primary["bid"]
        lighter_ask = primary["ask"]
        lighter_buy = primary["buy_price"]
        lighter_sell = primary["sell_price"]
        row = {
            "event": (
                "robinhood_lighter_basis_state"
                if self.venue == "robinhood_chain_lighter"
                else "mainnet_lighter_basis_state"
            ),
            "logged_at": capture_completed_at.isoformat(),
            "venue_capture_started_at": now.isoformat(),
            "venue_capture_completed_at": capture_completed_at.isoformat(),
            "venue_book_received_at": book_received_at.isoformat(),
            "venue_capture_duration_ms": f"{capture_elapsed_seconds * 1000:.3f}",
            "sample_id": uuid.uuid4().hex,
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "record_kind": "basis_market_sample",
            "execution_mode": "collect_only",
            "run_id": self.run_id,
            "strategy_version": "lighter-basis-sidecar-v3",
            "asset": self.asset,
            "venue": self.venue,
            "source_sample_id": source_identity(source),
            "source_run_id": source.get("run_id"),
            "source_logged_at": source.get("logged_at"),
            "source_sample_index": source.get("sample_index"),
            "source_age_seconds": f"{source_age:.6f}",
            "source_quote_source": source.get("quote_source"),
            "source_quote_received_at": source.get("quote_received_at"),
            "source_reference_price": source.get("reference_price"),
            "source_sample_quality_version": source.get(
                "sample_quality_version"
            ),
            "source_sample_pair_valid": source.get("sample_pair_valid"),
            "source_quote_size_mode": source.get("quote_size_mode"),
            "source_var_quote_age_seconds": source.get("var_quote_age_seconds"),
            "var_bid": decimal_text(var_bid),
            "var_ask": decimal_text(var_ask),
            "normalized_var_bid": normalized_var_bid,
            "normalized_var_ask": normalized_var_ask,
            "lighter_market_id": self.books.by_asset[self.asset].market_id,
            "lighter_primary_notional_usd": decimal_text(self.primary_notional),
            "lighter_bid": decimal_text(lighter_bid),
            "lighter_ask": decimal_text(lighter_ask),
            "lighter_buy_price": decimal_text(lighter_buy),
            "lighter_sell_price": decimal_text(lighter_sell),
            "lighter_spread_bps": decimal_text(
                spread_bps(lighter_bid, lighter_ask)
            ),
            "lighter_book_age_seconds": f"{book_age:.6f}",
            "lighter_market_data_transport": primary.get("transport"),
            f"{self.venue_prefix}_market_id": self.books.by_asset[self.asset].market_id,
            f"{self.venue_prefix}_primary_notional_usd": decimal_text(
                self.primary_notional
            ),
            f"{self.venue_prefix}_bid": decimal_text(lighter_bid),
            f"{self.venue_prefix}_ask": decimal_text(lighter_ask),
            f"{self.venue_prefix}_buy_price": decimal_text(lighter_buy),
            f"{self.venue_prefix}_sell_price": decimal_text(lighter_sell),
            f"{self.venue_prefix}_spread_bps": decimal_text(
                spread_bps(lighter_bid, lighter_ask)
            ),
            f"{self.venue_prefix}_book_age_seconds": f"{book_age:.6f}",
            f"{self.venue_prefix}_nonce": primary.get("nonce"),
            f"{self.venue_prefix}_sequence_gaps": primary.get("sequence_gaps"),
            f"{self.venue_prefix}_continuity_ok": primary.get("continuity_ok"),
            f"{self.venue_prefix}_market_data_transport": primary.get("transport"),
            "basis_bps": decimal_text(
                ((var_bid + var_ask) / Decimal("2") - (lighter_bid + lighter_ask) / Decimal("2"))
                / ((lighter_bid + lighter_ask) / Decimal("2"))
                * Decimal("10000")
            ),
            "short_edge_bps": decimal_text(edge_bps(var_bid, lighter_buy)),
            "long_edge_bps": decimal_text(edge_bps(lighter_sell, var_ask)),
            "normalized_short_edge_bps": (
                decimal_text(edge_bps(Decimal(str(normalized_var_bid)), lighter_buy))
                if normalized_var_bid is not None
                else None
            ),
            "normalized_long_edge_bps": (
                decimal_text(edge_bps(lighter_sell, Decimal(str(normalized_var_ask))))
                if normalized_var_ask is not None
                else None
            ),
            "depth_ladder": ladder,
            "basis_collect_only": True,
            "private_credentials_loaded": False,
        }
        return row, None

    async def build_event_row(
        self,
        source: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        now = now or datetime.now(timezone.utc)
        source_time = parse_timestamp(source.get("logged_at"))
        if source_time is None:
            return None, "event_timestamp_invalid"
        source_age = (now - source_time).total_seconds()
        if source_age < -1 or source_age > self.args.max_event_age_seconds:
            return None, "trade_event_stale"
        capture_started_monotonic = time.monotonic()
        snapshots, snapshot_error = await self._book_snapshots(force_refresh=True)
        if snapshots is None:
            return None, snapshot_error
        capture_elapsed_seconds = max(
            0.0, time.monotonic() - capture_started_monotonic
        )
        capture_completed_at = now + timedelta(seconds=capture_elapsed_seconds)
        source_age += capture_elapsed_seconds
        if source_age > self.args.max_event_age_seconds:
            return None, "trade_event_stale_at_capture"
        primary = snapshots[decimal_text(self.primary_notional) or "0"]
        book_received_at = capture_completed_at - timedelta(
            seconds=float(primary["book_age_seconds"])
        )
        var_bid = self._first_decimal(
            source,
            ("var_bid", "entry_var_price", "final_var_fill_price", "exit_var_price"),
        )
        var_ask = self._first_decimal(
            source,
            ("var_ask", "exit_var_price", "final_var_fill_price", "entry_var_price"),
        )
        source_qty = self._first_decimal(
            source, ("qty", "quantity", "lot_qty", "final_qty")
        )
        ladder: list[dict[str, Any]] = []
        for notional in self.notional_ladder:
            snapshot = snapshots[decimal_text(notional) or "0"]
            buy_price = snapshot["buy_price"]
            sell_price = snapshot["sell_price"]
            ladder.append(
                {
                    "notional_usd": decimal_text(notional),
                    "buy_price": decimal_text(buy_price),
                    "sell_price": decimal_text(sell_price),
                    "short_var_long_lighter_edge_bps": (
                        decimal_text(edge_bps(var_bid, buy_price))
                        if var_bid is not None
                        else None
                    ),
                    "long_var_short_lighter_edge_bps": (
                        decimal_text(edge_bps(sell_price, var_ask))
                        if var_ask is not None
                        else None
                    ),
                }
            )
        lighter_bid = primary["bid"]
        lighter_ask = primary["ask"]
        lighter_buy = primary["buy_price"]
        lighter_sell = primary["sell_price"]
        return {
            "event": (
                "robinhood_lighter_trade_event_snapshot"
                if self.venue == "robinhood_chain_lighter"
                else "mainnet_lighter_trade_event_snapshot"
            ),
            "logged_at": capture_completed_at.isoformat(),
            "venue_capture_started_at": now.isoformat(),
            "venue_capture_completed_at": capture_completed_at.isoformat(),
            "venue_book_received_at": book_received_at.isoformat(),
            "venue_capture_duration_ms": f"{capture_elapsed_seconds * 1000:.3f}",
            "sample_id": uuid.uuid4().hex,
            "sample_kind": "trade_event",
            "sample_quality": "valid",
            "record_kind": "basis_market_sample",
            "execution_mode": "collect_only",
            "run_id": self.run_id,
            "strategy_version": "lighter-basis-sidecar-v3",
            "asset": self.asset,
            "venue": self.venue,
            "source_event_id": event_identity(source),
            "source_event": source.get("event"),
            "source_run_id": source.get("run_id"),
            "source_logged_at": source.get("logged_at"),
            "source_event_age_seconds": f"{source_age:.6f}",
            "source_lot_id": source.get("lot_id"),
            "source_qty": (
                decimal_text(source_qty)
            ),
            "source_episode_id": source.get("episode_id"),
            "source_direction": source.get("direction"),
            "source_edge_bps": source.get("edge_bps") or source.get("entry_edge_bps"),
            "source_pnl_bps": source.get("final_pnl_bps") or source.get("executable_pnl_bps"),
            "source_exit_confirmation_mode": source.get("exit_confirmation_mode"),
            "var_bid": decimal_text(var_bid),
            "var_ask": decimal_text(var_ask),
            "lighter_market_id": self.books.by_asset[self.asset].market_id,
            "lighter_bid": decimal_text(lighter_bid),
            "lighter_ask": decimal_text(lighter_ask),
            "lighter_buy_price": decimal_text(lighter_buy),
            "lighter_sell_price": decimal_text(lighter_sell),
            "lighter_spread_bps": decimal_text(
                spread_bps(lighter_bid, lighter_ask)
            ),
            "lighter_book_age_seconds": f"{primary['book_age_seconds']:.6f}",
            "lighter_market_data_transport": primary.get("transport"),
            "source_quote_notional_usd": (
                decimal_text(
                    source_qty
                    * ((var_bid + var_ask) / Decimal("2"))
                )
                if source_qty is not None
                and var_bid is not None
                and var_ask is not None
                else None
            ),
            f"{self.venue_prefix}_market_id": self.books.by_asset[self.asset].market_id,
            f"{self.venue_prefix}_bid": decimal_text(lighter_bid),
            f"{self.venue_prefix}_ask": decimal_text(lighter_ask),
            f"{self.venue_prefix}_buy_price": decimal_text(lighter_buy),
            f"{self.venue_prefix}_sell_price": decimal_text(lighter_sell),
            f"{self.venue_prefix}_spread_bps": decimal_text(
                spread_bps(lighter_bid, lighter_ask)
            ),
            f"{self.venue_prefix}_book_age_seconds": f"{primary['book_age_seconds']:.6f}",
            f"{self.venue_prefix}_market_data_transport": primary.get("transport"),
            "basis_bps": (
                decimal_text(
                    ((var_bid + var_ask) / Decimal("2") - (lighter_bid + lighter_ask) / Decimal("2"))
                    / ((lighter_bid + lighter_ask) / Decimal("2"))
                    * Decimal("10000")
                )
                if var_bid is not None and var_ask is not None
                else None
            ),
            "short_edge_bps": (
                decimal_text(edge_bps(var_bid, lighter_buy))
                if var_bid is not None
                else None
            ),
            "long_edge_bps": (
                decimal_text(edge_bps(lighter_sell, var_ask))
                if var_ask is not None
                else None
            ),
            "depth_ladder": ladder,
            "basis_collect_only": True,
            "private_credentials_loaded": False,
        }, None

    async def run(self) -> None:
        await self.books.load_markets()
        book_task = asyncio.create_task(self.books.run())
        try:
            await self.books.wait_ready(timeout=self.args.ready_timeout_seconds)
            if not self.args.replay_current_file:
                self.follower.seek_to_end()
            self.event_follower.seek_to_end()
            self.logger.info(
                "collector_started run_id=%s venue=%s asset=%s source=%s notionals=%s",
                self.run_id,
                self.venue,
                self.asset,
                self.follower.current_path(),
                ",".join(decimal_text(value) or "0" for value in self.notional_ladder),
            )
            last_health = 0.0
            while not self.stop:
                for source in self.follower.read_new():
                    if not self._source_is_eligible(source, self.asset):
                        continue
                    identity = source_identity(source)
                    if identity == self.last_source_id:
                        continue
                    row, reason = await self.build_row(source)
                    if row is None:
                        self.skipped += 1
                        self._count_error(reason or "unknown")
                        self.logger.warning(
                            "sample_skipped source_id=%s reason=%s",
                            identity,
                            reason,
                        )
                    else:
                        self.store.append(row)
                        self.samples += 1
                        self.baseline_samples += 1
                        self.last_sample_at = row["logged_at"]
                    self.last_source_id = identity
                for source in self.event_follower.read_new():
                    if not self._event_is_eligible(source, self.asset):
                        continue
                    identity = event_identity(source)
                    if identity == self.last_event_id:
                        continue
                    row, reason = await self.build_event_row(source)
                    if row is None:
                        self.skipped += 1
                        self._count_error(reason or "unknown")
                        self.logger.warning(
                            "event_snapshot_skipped event_id=%s reason=%s",
                            identity,
                            reason,
                        )
                    else:
                        self.store.append(row)
                        self.samples += 1
                        self.event_snapshots += 1
                        self.last_sample_at = row["logged_at"]
                        self.last_event_snapshot_at = row["logged_at"]
                    self.last_event_id = identity
                now_mono = time.monotonic()
                if now_mono - last_health >= self.args.health_interval_seconds:
                    last_health = now_mono
                    self.store.write_manifests()
                    self.store.rotate_closed_days()
                    self._write_health()
                    free_gb = self._health()["disk_free_gb"]
                    self.logger.info(
                        "collector_health samples=%s skipped=%s errors=%s disk_free_gb=%s",
                        self.samples,
                        self.skipped,
                        self.errors,
                        free_gb,
                    )
                    if float(free_gb) < self.args.disk_stop_free_gb:
                        raise RuntimeError(
                            f"disk_free_below_stop_threshold free_gb={free_gb}"
                        )
                await asyncio.sleep(self.args.poll_interval_seconds)
        finally:
            self.stop = True
            self.books.stop = True
            book_task.cancel()
            await asyncio.gather(book_task, return_exceptions=True)
            self.store.write_manifests()
            self._write_health()


def parse_notional_ladder(value: str) -> tuple[Decimal, ...]:
    try:
        notionals = tuple(
            dict.fromkeys(
                Decimal(token.strip())
                for token in value.split(",")
                if token.strip()
            )
        )
    except Exception as exc:
        raise argparse.ArgumentTypeError("notionals must be positive decimals") from exc
    if not notionals or any(value <= 0 for value in notionals):
        raise argparse.ArgumentTypeError("notionals must be positive decimals")
    return notionals


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only Variational/Lighter basis sidecar."
    )
    parser.add_argument("--asset", default="ETH", choices=("ETH",))
    parser.add_argument(
        "--venue",
        choices=("auto", *LIGHTER_VENUE_HOSTS),
        default="auto",
        help="Venue label inferred from the market-data endpoint when set to auto.",
    )
    parser.add_argument("--source-root", default=str(ROOT / "log" / "basis_samples"))
    parser.add_argument("--output-dir", default=str(ROOT / "log"))
    parser.add_argument(
        "--event-source-path",
        default=str(ROOT / "log" / "order_metrics.jsonl"),
    )
    parser.add_argument("--notional-ladder", type=parse_notional_ladder, default=DEFAULT_NOTIONALS)
    parser.add_argument("--rest-url", default=ROBINHOOD_LIGHTER_REST_URL)
    parser.add_argument(
        "--order-book-orders-url",
        default=ROBINHOOD_LIGHTER_ORDER_BOOK_URL,
    )
    parser.add_argument("--order-limit", type=int, default=100)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5)
    parser.add_argument("--max-source-age-seconds", type=float, default=90.0)
    parser.add_argument("--max-event-age-seconds", type=float, default=90.0)
    parser.add_argument("--max-book-age-seconds", type=float, default=2.0)
    parser.add_argument("--ready-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--health-interval-seconds", type=float, default=60.0)
    parser.add_argument("--disk-stop-free-gb", type=float, default=3.0)
    parser.add_argument(
        "--replay-current-file",
        action="store_true",
        help="Replay fresh rows from today's source file; default follows only new rows.",
    )
    args = parser.parse_args(argv)
    try:
        endpoint_venue = infer_venue(args.rest_url, args.order_book_orders_url)
    except ValueError as exc:
        parser.error(str(exc))
    if args.venue == "auto":
        args.venue = endpoint_venue
    elif args.venue != endpoint_venue:
        parser.error(
            f"venue {args.venue} does not match endpoint host "
            f"{urlparse(args.rest_url).hostname}"
        )
    if args.poll_interval_seconds <= 0 or args.health_interval_seconds <= 0:
        parser.error("poll and health intervals must be > 0")
    if (
        args.max_source_age_seconds <= 0
        or args.max_event_age_seconds <= 0
        or args.max_book_age_seconds <= 0
    ):
        parser.error("age limits must be > 0")
    if args.order_limit <= 0:
        parser.error("order limit must be > 0")
    return args


async def _amain(args: argparse.Namespace) -> None:
    collector = RobinhoodBasisCollector(args)
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
        print(f"ROBINHOOD_COLLECTOR_STOPPED reason={type(exc).__name__}:{exc}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
