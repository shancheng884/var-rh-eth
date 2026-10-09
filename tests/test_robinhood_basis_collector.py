import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tools.robinhood_basis_collector import (
    CurrentDayJsonlFollower,
    FixedJsonlFollower,
    RobinhoodBasisCollector,
    parse_args,
)


def test_follower_waits_for_complete_json_lines(tmp_path) -> None:
    follower = CurrentDayJsonlFollower(tmp_path, "ETH")
    path = follower.current_path()
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"event":"first"}\n{"event":"second"')

    assert follower.read_new() == [{"event": "first"}]

    with path.open("ab") as handle:
        handle.write(b"}\n")

    assert follower.read_new() == [{"event": "second"}]


def test_follower_seek_to_end_only_emits_new_rows(tmp_path) -> None:
    follower = CurrentDayJsonlFollower(tmp_path, "ETH")
    path = follower.current_path()
    path.parent.mkdir(parents=True)
    path.write_text('{"event":"old"}\n', encoding="utf-8")
    follower.seek_to_end()

    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"event":"new"}\n')

    assert follower.read_new() == [{"event": "new"}]


def test_fixed_follower_only_emits_complete_appended_rows(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"event":"old"}\n', encoding="utf-8")
    follower = FixedJsonlFollower(path)
    follower.seek_to_end()
    with path.open("ab") as handle:
        handle.write(b'{"event":"new"')

    assert follower.read_new() == []

    with path.open("ab") as handle:
        handle.write(b'}\n')

    assert follower.read_new() == [{"event": "new"}]


def test_collector_builds_depth_ladder_without_credentials(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(
            [
                "--source-root",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )
        collector = RobinhoodBasisCollector(args)
        collector.books.by_asset["ETH"] = SimpleNamespace(market_id=0)

        async def fake_snapshot(_asset, notional, *, force_refresh=False):
            price_offset = notional / Decimal("10000")
            return {
                "bid": Decimal("1999.9"),
                "ask": Decimal("2000.1"),
                "sell_price": Decimal("1999.9") - price_offset,
                "buy_price": Decimal("2000.1") + price_offset,
                "nonce": 10,
                "book_age_seconds": 0.1,
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
            }

        collector.books.snapshot = fake_snapshot
        now = datetime(2026, 8, 25, 0, 0, 1, tzinfo=timezone.utc)
        source = {
            "event": "live_inventory_basis_state",
            "sample_id": "source-1",
            "sample_kind": "baseline",
            "sample_quality": "valid",
            "logged_at": "2026-08-25T00:00:00+00:00",
            "asset": "ETH",
            "run_id": "live-run",
            "sample_index": 1,
            "var_bid": "2001",
            "var_ask": "2002",
            "normalized_var_bid": "2001.2",
            "normalized_var_ask": "2002.2",
        }

        row, error = await collector.build_row(source, now=now)

        assert error is None
        assert row is not None
        assert row["event"] == "robinhood_lighter_basis_state"
        assert row["venue"] == "robinhood_chain_lighter"
        assert row["robinhood_lighter_market_id"] == 0
        assert row["robinhood_lighter_primary_notional_usd"] == "20"
        assert row["source_sample_id"] == "source-1"
        assert row["source_quote_source"] is None
        assert row["private_credentials_loaded"] is False
        assert [item["notional_usd"] for item in row["depth_ladder"]] == [
            "20",
            "40",
            "60",
        ]
        assert Decimal(row["short_edge_bps"]) > 0

    asyncio.run(run())


def test_collector_infers_mainnet_venue_from_matching_endpoints(tmp_path) -> None:
    args = parse_args(
        [
            "--source-root",
            str(tmp_path / "source"),
            "--output-dir",
            str(tmp_path / "output"),
            "--rest-url",
            "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks",
            "--order-book-orders-url",
            "https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders",
        ]
    )

    assert args.venue == "mainnet_lighter"


def test_mainnet_collector_row_has_venue_and_capture_timing_metadata(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(
            [
                "--source-root",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "output"),
                "--rest-url",
                "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks",
                "--order-book-orders-url",
                "https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders",
            ]
        )
        collector = RobinhoodBasisCollector(args)
        collector.books.by_asset["ETH"] = SimpleNamespace(market_id=1)

        async def fake_snapshot(_asset, notional, *, force_refresh=False):
            return {
                "bid": Decimal("1999.9"),
                "ask": Decimal("2000.1"),
                "sell_price": Decimal("1999.8") - notional / Decimal("10000"),
                "buy_price": Decimal("2000.2") + notional / Decimal("10000"),
                "nonce": 10,
                "book_age_seconds": 0.1,
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
            }

        collector.books.snapshot = fake_snapshot
        now = datetime(2026, 8, 25, 0, 0, 1, tzinfo=timezone.utc)
        row, error = await collector.build_row(
            {
                "event": "live_inventory_basis_state",
                "sample_id": "source-mainnet-1",
                "sample_kind": "baseline",
                "sample_quality": "valid",
                "sample_pair_valid": True,
                "logged_at": "2026-08-25T00:00:00.900000+00:00",
                "quote_received_at": "2026-08-25T00:00:00.800000+00:00",
                "quote_size_mode": "market",
                "var_quote_age_seconds": "0.1",
                "asset": "ETH",
                "run_id": "live-run",
                "sample_index": 1,
                "var_bid": "2001",
                "var_ask": "2002",
            },
            now=now,
        )

        assert error is None
        assert row is not None
        assert row["event"] == "mainnet_lighter_basis_state"
        assert row["venue"] == "mainnet_lighter"
        assert row["mainnet_lighter_market_id"] == 1
        assert row["source_sample_pair_valid"] is True
        assert row["venue_capture_completed_at"] >= row["venue_capture_started_at"]
        assert row["venue_book_received_at"] <= row["venue_capture_completed_at"]
        assert row["venue_capture_duration_ms"] is not None

    asyncio.run(run())


def test_collector_rejects_venue_or_endpoint_host_mismatch(tmp_path) -> None:
    import pytest

    common = ["--source-root", str(tmp_path / "source"), "--output-dir", str(tmp_path / "output")]
    with pytest.raises(SystemExit):
        parse_args(
            [
                *common,
                "--venue",
                "mainnet_lighter",
            ]
        )

    with pytest.raises(SystemExit):
        parse_args(
            [
                *common,
                "--rest-url",
                "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks",
                "--order-book-orders-url",
                "https://api.rh.lighter.xyz/api/v1/orderBookOrders",
            ]
        )


def test_collector_builds_forced_trade_event_snapshot(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(
            [
                "--source-root",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )
        collector = RobinhoodBasisCollector(args)
        collector.books.by_asset["ETH"] = SimpleNamespace(market_id=0)
        force_values = []

        async def fake_snapshot(_asset, notional, *, force_refresh=False):
            force_values.append(force_refresh)
            return {
                "bid": Decimal("1999.9"),
                "ask": Decimal("2000.1"),
                "sell_price": Decimal("1999.9") - notional / Decimal("10000"),
                "buy_price": Decimal("2000.1") + notional / Decimal("10000"),
                "nonce": None,
                "book_age_seconds": 0.01,
                "continuity_ok": True,
                "cold": False,
                "sequence_gaps": 0,
                "transport": "rest_snapshot",
            }

        collector.books.snapshot = fake_snapshot
        row, error = await collector.build_event_row(
            {
                "event": "live_inventory_entered",
                "logged_at": "2026-08-25T00:00:00+00:00",
                "run_id": "live-run",
                "asset": "ETH",
                "lot_id": 1,
                "entry_var_price": "2001",
                "entry_lighter_price": "2000",
            },
            now=datetime(2026, 8, 25, 0, 0, 1, tzinfo=timezone.utc),
        )

        assert error is None
        assert row is not None
        assert row["sample_kind"] == "trade_event"
        assert row["source_event"] == "live_inventory_entered"
        assert row["source_lot_id"] == 1
        assert row["private_credentials_loaded"] is False
        assert Decimal(row["short_edge_bps"]) > 0
        assert force_values == [True, False, False]

    asyncio.run(run())


def test_trade_event_filter_skips_blocked_entry_candidates(tmp_path) -> None:
    args = parse_args(
        [
            "--source-root",
            str(tmp_path / "source"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )
    collector = RobinhoodBasisCollector(args)

    assert collector._event_is_eligible(
        {
            "event": "live_inventory_entry_shadow_candidate",
            "asset": "ETH",
            "shadow_status": "passed",
        },
        "ETH",
    )
    assert not collector._event_is_eligible(
        {
            "event": "live_inventory_entry_shadow_candidate",
            "asset": "ETH",
            "shadow_status": "blocked",
        },
        "ETH",
    )


def test_collector_rejects_stale_source_sample(tmp_path) -> None:
    async def run() -> None:
        args = parse_args(
            [
                "--source-root",
                str(tmp_path / "source"),
                "--output-dir",
                str(tmp_path / "output"),
                "--max-source-age-seconds",
                "30",
            ]
        )
        collector = RobinhoodBasisCollector(args)
        source = {
            "logged_at": "2026-08-25T00:00:00+00:00",
            "var_bid": "2000",
            "var_ask": "2001",
        }

        row, error = await collector.build_row(
            source,
            now=datetime(2026, 8, 25, 0, 1, 0, tzinfo=timezone.utc),
        )

        assert row is None
        assert error == "source_sample_stale"

    asyncio.run(run())


def test_health_file_is_collect_only_and_separate(tmp_path) -> None:
    args = parse_args(
        [
            "--source-root",
            str(tmp_path / "source"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )
    collector = RobinhoodBasisCollector(args)
    collector._write_health()

    health = json.loads(collector.health_path.read_text(encoding="utf-8"))
    assert health["execution_mode"] == "collect_only"
    assert health["venue"] == "robinhood_chain_lighter"
    assert collector.health_path.name == "robinhood_basis_health.json"
