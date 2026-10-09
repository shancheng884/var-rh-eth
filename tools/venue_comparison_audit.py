#!/usr/bin/env python3
"""Compare read-only Lighter books against the same Variational quote sample."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BEIJING = timezone(timedelta(hours=8))
MAX_LINE_BYTES = 1024 * 1024
MAX_ROWS = 200_000


def decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return number if number.is_finite() else None


def parse_time(value: Any) -> datetime | None:
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


def source_id(row: dict[str, Any]) -> str | None:
    identity = row.get("source_sample_id")
    if identity:
        return str(identity)
    run_id = row.get("source_run_id")
    sample_index = row.get("source_sample_index")
    logged_at = row.get("source_logged_at")
    if run_id and sample_index is not None and logged_at:
        return f"{run_id}:{sample_index}:{logged_at}"
    return None


def load_venue_rows(
    root: Path,
    *,
    expected_venue: str,
    since: datetime,
) -> tuple[dict[str, dict[str, Any]], Counter[str], int, int]:
    rows: dict[str, dict[str, Any]] = {}
    counts: Counter[str] = Counter()
    scanned = 0
    oversized = 0
    paths = (
        sorted([*root.rglob("*.jsonl"), *root.rglob("*.jsonl.gz")])
        if root.exists()
        else []
    )
    for path in paths:
        open_file = gzip.open if path.name.endswith(".gz") else open
        with open_file(path, "rb") as handle:
            while True:
                line = handle.readline(MAX_LINE_BYTES + 1)
                if not line:
                    break
                scanned += 1
                if len(line) > MAX_LINE_BYTES:
                    oversized += 1
                    while line and not line.endswith(b"\n"):
                        line = handle.readline(MAX_LINE_BYTES + 1)
                    continue
                try:
                    row = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    counts["invalid_json"] += 1
                    continue
                if not isinstance(row, dict):
                    continue
                if str(row.get("asset") or "").upper() != "ETH":
                    continue
                at = parse_time(row.get("source_logged_at"))
                if at is None or at < since:
                    continue
                if row.get("venue") != expected_venue:
                    counts["wrong_venue_tag"] += 1
                    continue
                if row.get("sample_kind") != "baseline" or row.get(
                    "sample_quality"
                ) != "valid":
                    continue
                if row.get("source_sample_pair_valid") is not True:
                    counts["invalid_var_source_pair"] += 1
                    continue
                identity = source_id(row)
                if not identity:
                    counts["missing_source_identity"] += 1
                    continue
                prior = rows.get(identity)
                capture_at = parse_time(row.get("venue_capture_completed_at"))
                prior_capture_at = (
                    parse_time(prior.get("venue_capture_completed_at"))
                    if prior
                    else None
                )
                if prior is None or (
                    capture_at is not None
                    and (prior_capture_at is None or capture_at > prior_capture_at)
                ):
                    rows[identity] = row
                else:
                    counts["duplicate_source_identity"] += 1
                if len(rows) > MAX_ROWS:
                    raise RuntimeError(
                        f"row limit exceeded in {root}; narrow the date range"
                    )
    return rows, counts, scanned, oversized


def depth_by_notional(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    ladder = row.get("depth_ladder")
    if not isinstance(ladder, list):
        return result
    for item in ladder:
        if isinstance(item, dict) and item.get("notional_usd") is not None:
            result[str(item["notional_usd"])] = item
    return result


def build_pairs(
    rh_rows: dict[str, dict[str, Any]],
    mainnet_rows: dict[str, dict[str, Any]],
    *,
    max_capture_skew_seconds: Decimal,
    max_source_quote_age_seconds: Decimal,
    max_book_age_seconds: Decimal,
) -> tuple[list[tuple[str, dict[str, Any], dict[str, Any]]], Counter[str]]:
    pairs = []
    rejected: Counter[str] = Counter()
    common_ids = rh_rows.keys() & mainnet_rows.keys()
    for identity in common_ids:
        rh = rh_rows[identity]
        mainnet = mainnet_rows[identity]
        rh_capture = parse_time(rh.get("venue_capture_completed_at"))
        mainnet_capture = parse_time(mainnet.get("venue_capture_completed_at"))
        if rh_capture is None or mainnet_capture is None:
            rejected["capture_time_missing"] += 1
            continue
        capture_skew = Decimal(
            str(abs((rh_capture - mainnet_capture).total_seconds()))
        )
        if capture_skew > max_capture_skew_seconds:
            rejected["capture_skew_over_limit"] += 1
            continue
        rh_book_received = parse_time(rh.get("venue_book_received_at"))
        mainnet_book_received = parse_time(mainnet.get("venue_book_received_at"))
        if rh_book_received is None or mainnet_book_received is None:
            rejected["book_receive_time_missing"] += 1
            continue
        book_receive_skew = Decimal(
            str(abs((rh_book_received - mainnet_book_received).total_seconds()))
        )
        if book_receive_skew > max_capture_skew_seconds:
            rejected["book_receive_skew_over_limit"] += 1
            continue
        if (
            decimal(rh.get("var_bid")) != decimal(mainnet.get("var_bid"))
            or decimal(rh.get("var_ask")) != decimal(mainnet.get("var_ask"))
        ):
            rejected["var_quote_mismatch"] += 1
            continue
        if (
            rh.get("source_quote_received_at")
            != mainnet.get("source_quote_received_at")
            or rh.get("source_quote_size_mode")
            != mainnet.get("source_quote_size_mode")
        ):
            rejected["var_quote_metadata_mismatch"] += 1
            continue
        var_age = decimal(rh.get("source_var_quote_age_seconds"))
        if var_age is None or var_age > max_source_quote_age_seconds:
            rejected["var_quote_stale_or_unknown"] += 1
            continue
        if var_age != decimal(mainnet.get("source_var_quote_age_seconds")):
            rejected["var_quote_age_mismatch"] += 1
            continue
        rh_age = decimal(rh.get("lighter_book_age_seconds"))
        mainnet_age = decimal(mainnet.get("lighter_book_age_seconds"))
        if (
            rh_age is None
            or mainnet_age is None
            or rh_age > max_book_age_seconds
            or mainnet_age > max_book_age_seconds
        ):
            rejected["book_stale_or_unknown"] += 1
            continue
        pairs.append((identity, rh, mainnet))
    return pairs, rejected


def summarize_direction(
    pairs: list[tuple[str, dict[str, Any], dict[str, Any]]],
    *,
    direction_field: str,
) -> list[dict[str, Any]]:
    notionals = sorted(
        {
            key
            for _, rh, mainnet in pairs
            for key in (*depth_by_notional(rh), *depth_by_notional(mainnet))
        },
        key=lambda value: decimal(value) or Decimal("0"),
    )
    results = []
    for notional in notionals:
        diffs: list[Decimal] = []
        rh_values: list[Decimal] = []
        mainnet_values: list[Decimal] = []
        for _, rh, mainnet in pairs:
            rh_depth = depth_by_notional(rh).get(notional) or {}
            mainnet_depth = depth_by_notional(mainnet).get(notional) or {}
            rh_edge = decimal(rh_depth.get(direction_field))
            mainnet_edge = decimal(mainnet_depth.get(direction_field))
            if rh_edge is None or mainnet_edge is None:
                continue
            rh_values.append(rh_edge)
            mainnet_values.append(mainnet_edge)
            diffs.append(mainnet_edge - rh_edge)
        if not diffs:
            continue
        results.append(
            {
                "notional": notional,
                "n": len(diffs),
                "rh_median": median(rh_values),
                "mainnet_median": median(mainnet_values),
                "mainnet_minus_rh_median": median(diffs),
                "mainnet_better": sum(value > 0 for value in diffs),
            }
        )
    return results


def opportunity_episodes(
    pairs: list[tuple[str, dict[str, Any], dict[str, Any]]],
    *,
    venue_index: int,
    direction_field: str,
    notional: Decimal,
    threshold_bps: Decimal,
    max_gap_seconds: float,
) -> dict[str, Any]:
    observations = []
    notional_key = format(notional.normalize(), "f")
    for _, rh, mainnet in pairs:
        row = (rh, mainnet)[venue_index]
        depth = depth_by_notional(row).get(notional_key) or {}
        edge = decimal(depth.get(direction_field))
        at = parse_time(row.get("source_logged_at"))
        if edge is not None and at is not None:
            observations.append((at.timestamp(), edge))
    observations.sort()
    qualifying = 0
    episode_durations: list[float] = []
    active_first: float | None = None
    active_last: float | None = None

    def finish() -> None:
        nonlocal active_first, active_last
        if active_first is not None and active_last is not None:
            episode_durations.append(active_last - active_first)
        active_first = active_last = None

    for at, edge in observations:
        if active_last is not None and at - active_last > max_gap_seconds:
            finish()
        if edge < threshold_bps:
            finish()
            continue
        qualifying += 1
        if active_first is None:
            active_first = at
        active_last = at
    finish()
    return {
        "qualifying_samples": qualifying,
        "episodes": len(episode_durations),
        "multi_sample_episodes": sum(duration > 0 for duration in episode_durations),
        "median_duration_seconds": (
            median(episode_durations) if episode_durations else None
        ),
    }


def parse_decimal_list(value: str) -> tuple[Decimal, ...]:
    try:
        values = tuple(Decimal(item.strip()) for item in value.split(",") if item.strip())
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("thresholds must be comma-separated decimals") from exc
    if not values or any(not item.is_finite() or item <= 0 for item in values):
        raise argparse.ArgumentTypeError("thresholds must be positive finite decimals")
    return values


def parse_positive_decimal(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("value must be a decimal") from exc
    if not number.is_finite() or number <= 0:
        raise argparse.ArgumentTypeError("value must be a positive finite decimal")
    return number


def main() -> int:
    default_day = (datetime.now(timezone.utc).astimezone(BEIJING)).date().isoformat()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-beijing", default=default_day)
    parser.add_argument(
        "--rh-root", default=str(ROOT / "log" / "robinhood_basis_samples")
    )
    parser.add_argument(
        "--mainnet-root",
        default=str(
            ROOT / "log" / "research_mainnet_shadow" / "robinhood_basis_samples"
        ),
    )
    parser.add_argument(
        "--max-capture-skew-seconds", type=Decimal, default=Decimal("0.5")
    )
    parser.add_argument(
        "--max-source-quote-age-seconds", type=Decimal, default=Decimal("1.5")
    )
    parser.add_argument(
        "--max-book-age-seconds", type=Decimal, default=Decimal("2")
    )
    parser.add_argument(
        "--primary-notional-usd",
        type=parse_positive_decimal,
        default=Decimal("20"),
    )
    parser.add_argument(
        "--opportunity-thresholds-bps",
        type=parse_decimal_list,
        default=(Decimal("3"), Decimal("5"), Decimal("7")),
    )
    parser.add_argument("--max-episode-gap-seconds", type=float, default=90.0)
    args = parser.parse_args()
    try:
        day = datetime.fromisoformat(args.since_beijing).date()
    except ValueError:
        parser.error("--since-beijing must be YYYY-MM-DD")
    since = datetime.combine(day, datetime.min.time(), tzinfo=BEIJING).astimezone(
        timezone.utc
    )
    age_limits = (
        args.max_capture_skew_seconds,
        args.max_source_quote_age_seconds,
        args.max_book_age_seconds,
        args.primary_notional_usd,
    )
    if (
        any(not value.is_finite() or value <= 0 for value in age_limits)
        or not math.isfinite(args.max_episode_gap_seconds)
        or args.max_episode_gap_seconds <= 0
    ):
        parser.error("age and skew limits must be positive")

    try:
        rh_rows, rh_counts, rh_scanned, rh_oversized = load_venue_rows(
            Path(args.rh_root), expected_venue="robinhood_chain_lighter", since=since
        )
        (
            mainnet_rows,
            mainnet_counts,
            mainnet_scanned,
            mainnet_oversized,
        ) = load_venue_rows(
            Path(args.mainnet_root),
            expected_venue="mainnet_lighter",
            since=since,
        )
        pairs, rejected = build_pairs(
            rh_rows,
            mainnet_rows,
            max_capture_skew_seconds=args.max_capture_skew_seconds,
            max_source_quote_age_seconds=args.max_source_quote_age_seconds,
            max_book_age_seconds=args.max_book_age_seconds,
        )
    except (OSError, RuntimeError) as exc:
        print(f"comparison=REFUSED reason={exc}", file=sys.stderr)
        return 2

    print(f"window_start_utc={since.isoformat()} asset=ETH")
    print(
        f"rh_rows={len(rh_rows)} mainnet_rows={len(mainnet_rows)} "
        f"strict_pairs={len(pairs)}"
    )
    print(
        f"scanned_lines rh={rh_scanned} mainnet={mainnet_scanned} "
        f"oversized_lines rh={rh_oversized} mainnet={mainnet_oversized}"
    )
    print(f"rh_rejections={dict(rh_counts)}")
    print(f"mainnet_rejections={dict(mainnet_counts)}")
    print(f"pair_rejections={dict(rejected)}")
    for label, field in (
        ("short_var_long_lighter", "short_var_long_lighter_edge_bps"),
        ("long_var_short_lighter", "long_var_short_lighter_edge_bps"),
    ):
        for result in summarize_direction(pairs, direction_field=field):
            print(
                f"direction={label} notional={result['notional']}U "
                f"n={result['n']} rh_median_bps={result['rh_median']:.3f} "
                f"mainnet_median_bps={result['mainnet_median']:.3f} "
                f"mainnet_minus_rh_median_bps="
                f"{result['mainnet_minus_rh_median']:.3f} "
                f"mainnet_better={result['mainnet_better']}/{result['n']}"
            )
        for venue_index, venue in ((0, "rh"), (1, "mainnet")):
            for threshold in args.opportunity_thresholds_bps:
                stats = opportunity_episodes(
                    pairs,
                    venue_index=venue_index,
                    direction_field=field,
                    notional=args.primary_notional_usd,
                    threshold_bps=threshold,
                    max_gap_seconds=args.max_episode_gap_seconds,
                )
                print(
                    f"opportunities venue={venue} direction={label} "
                    f"notional={args.primary_notional_usd}U "
                    f"threshold_bps={threshold} "
                    f"samples={stats['qualifying_samples']} "
                    f"episodes={stats['episodes']} "
                    f"episodes_2plus_samples={stats['multi_sample_episodes']} "
                    f"median_duration_seconds="
                    f"{stats['median_duration_seconds']}"
                )
    print(
        "comparison_uses_same_var_sample_and_depth; opportunity_episodes_are_not_fills; "
        "realized_pnl_fees_funding_and_execution_latency_not_inferred"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
