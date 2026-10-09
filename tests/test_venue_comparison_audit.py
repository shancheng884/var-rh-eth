from datetime import datetime, timezone
from decimal import Decimal

from tools.venue_comparison_audit import (
    build_pairs,
    opportunity_episodes,
    summarize_direction,
)


def venue_row(
    venue: str,
    *,
    identity: str,
    at: str,
    capture_at: str,
    book_at: str,
    edge: str,
) -> dict:
    return {
        "asset": "ETH",
        "venue": venue,
        "sample_kind": "baseline",
        "sample_quality": "valid",
        "source_sample_pair_valid": True,
        "source_sample_id": identity,
        "source_logged_at": at,
        "source_quote_received_at": "2026-10-09T00:00:00+00:00",
        "source_quote_size_mode": "market",
        "source_var_quote_age_seconds": "0.2",
        "venue_capture_completed_at": capture_at,
        "venue_book_received_at": book_at,
        "lighter_book_age_seconds": "0.1",
        "var_bid": "2500",
        "var_ask": "2501",
        "depth_ladder": [
            {
                "notional_usd": "20",
                "short_var_long_lighter_edge_bps": edge,
                "long_var_short_lighter_edge_bps": "-1",
            }
        ],
    }


def paired_rows(identity: str, at: str, rh_edge: str, mainnet_edge: str):
    rh = venue_row(
        "robinhood_chain_lighter",
        identity=identity,
        at=at,
        capture_at=at,
        book_at=at,
        edge=rh_edge,
    )
    mainnet = venue_row(
        "mainnet_lighter",
        identity=identity,
        at=at,
        capture_at=at,
        book_at=at,
        edge=mainnet_edge,
    )
    return rh, mainnet


def test_build_pairs_requires_strict_quote_and_capture_alignment() -> None:
    rh_ok, mainnet_ok = paired_rows(
        "same-1", "2026-10-09T00:00:00+00:00", "2", "4"
    )
    rh_capture_skew, mainnet_capture_skew = paired_rows(
        "same-2", "2026-10-09T00:01:00+00:00", "2", "4"
    )
    mainnet_capture_skew["venue_capture_completed_at"] = (
        "2026-10-09T00:01:00.600000+00:00"
    )
    rh_book_skew, mainnet_book_skew = paired_rows(
        "same-3", "2026-10-09T00:02:00+00:00", "2", "4"
    )
    mainnet_book_skew["venue_book_received_at"] = (
        "2026-10-09T00:02:00.600000+00:00"
    )

    pairs, rejected = build_pairs(
        {
            "same-1": rh_ok,
            "same-2": rh_capture_skew,
            "same-3": rh_book_skew,
        },
        {
            "same-1": mainnet_ok,
            "same-2": mainnet_capture_skew,
            "same-3": mainnet_book_skew,
        },
        max_capture_skew_seconds=Decimal("0.5"),
        max_source_quote_age_seconds=Decimal("1.5"),
        max_book_age_seconds=Decimal("2"),
    )

    assert [pair[0] for pair in pairs] == ["same-1"]
    assert rejected["capture_skew_over_limit"] == 1
    assert rejected["book_receive_skew_over_limit"] == 1


def test_summarize_direction_and_opportunity_episodes() -> None:
    samples = [
        paired_rows("a", "2026-10-09T00:00:00+00:00", "2", "3"),
        paired_rows("b", "2026-10-09T00:00:30+00:00", "4", "5"),
        paired_rows("c", "2026-10-09T00:03:20+00:00", "1", "1"),
    ]
    pairs = [(str(index), rh, mainnet) for index, (rh, mainnet) in enumerate(samples)]

    summary = summarize_direction(
        pairs, direction_field="short_var_long_lighter_edge_bps"
    )
    assert summary[0]["n"] == 3
    assert summary[0]["rh_median"] == Decimal("2")
    assert summary[0]["mainnet_median"] == Decimal("3")
    assert summary[0]["mainnet_minus_rh_median"] == Decimal("1")

    episodes = opportunity_episodes(
        pairs,
        venue_index=1,
        direction_field="short_var_long_lighter_edge_bps",
        notional=Decimal("20"),
        threshold_bps=Decimal("2"),
        max_gap_seconds=90,
    )
    assert episodes["qualifying_samples"] == 2
    assert episodes["episodes"] == 1
    assert episodes["multi_sample_episodes"] == 1
    assert episodes["median_duration_seconds"] == 30
