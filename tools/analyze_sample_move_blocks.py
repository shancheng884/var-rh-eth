#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
import gzip
import json
import os
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (
    ORDER_METRICS,
    avg,
    fmt_decimal,
    parse_time,
    rotated_jsonl_paths,
    to_decimal,
)

DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"
HORIZONS_SECONDS = (5, 10, 30, 60)

COMPACT_FIELDS = {
    "asset",
    "basis_sample_move_bps",
    "direction",
    "dynamic_entry_threshold_bps",
    "edge_bps",
    "event",
    "lighter_book_age_seconds",
    "lighter_buy_price",
    "lighter_sell_price",
    "logged_at",
    "long_edge_bps",
    "min_abs_entry_bps",
    "min_entry_edge_bps",
    "reason",
    "run_id",
    "sample_index",
    "short_edge_bps",
    "v4_entry_threshold_bps",
    "var_ask",
    "var_bid",
    "var_quote_age_seconds",
}


def _compact_relevant_row(
    row: dict[str, Any], *, asset: str | None
) -> dict[str, Any] | None:
    event = row.get("event")
    if event == "live_inventory_entry_blocked":
        if row.get("reason") != "basis_sample_move_too_large":
            return None
    elif event != "live_inventory_basis_state":
        return None
    if asset and str(row.get("asset") or "").upper() != asset.upper():
        return None
    return {key: row.get(key) for key in COMPACT_FIELDS if key in row}


def _reverse_plain_lines(path: Path, *, block_size: int = 1024 * 1024):
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        remainder = b""
        while position > 0:
            size = min(block_size, position)
            position -= size
            handle.seek(position)
            parts = (handle.read(size) + remainder).split(b"\n")
            remainder = parts[0]
            for line in reversed(parts[1:]):
                if line:
                    yield line
        if remainder:
            yield remainder


def _reverse_parsed_rows(
    path: Path,
    *,
    limit: int,
    asset: str | None,
):
    if path.suffix != ".gz":
        for raw in _reverse_plain_lines(path):
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            yield parse_time(row.get("logged_at")), _compact_relevant_row(
                row, asset=asset
            )
        return

    # Gzip files cannot be read backward. Keep only compact metadata for the
    # last requested lines rather than retaining the large source payloads.
    buffered = deque(maxlen=max(1, limit))
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            buffered.append(
                (
                    parse_time(row.get("logged_at")),
                    _compact_relevant_row(row, asset=asset),
                )
            )
    yield from reversed(buffered)


def load_recent_compact_rows(
    log_file: Path,
    *,
    asset: str | None,
    hours: float,
    limit: int,
    include_rotated: bool,
) -> list[dict[str, Any]]:
    paths = (
        rotated_jsonl_paths(log_file)
        if include_rotated
        else ([log_file] if log_file.exists() else [])
    )
    newest_first = list(reversed(paths))
    compact_rows: list[dict[str, Any]] = []
    latest = None
    cutoff = None
    observed_lines = 0
    for path in newest_first:
        remaining = max(1, limit - observed_lines)
        for logged_at, compact in _reverse_parsed_rows(
            path,
            limit=remaining,
            asset=asset,
        ):
            observed_lines += 1
            if latest is None and logged_at is not None:
                latest = logged_at
                if hours > 0:
                    cutoff = latest - timedelta(hours=hours)
            if cutoff is not None and logged_at is not None and logged_at < cutoff:
                return list(reversed(compact_rows))
            if compact is not None:
                compact_rows.append(compact)
            if observed_lines >= max(1, limit):
                return list(reversed(compact_rows))
    return list(reversed(compact_rows))


def _required_edge_bps(row: dict[str, Any]) -> Decimal | None:
    values = [
        to_decimal(row.get("v4_entry_threshold_bps")),
        to_decimal(row.get("min_entry_edge_bps")),
        to_decimal(row.get("min_abs_entry_bps")),
        to_decimal(row.get("dynamic_entry_threshold_bps")),
    ]
    available = [value for value in values if value is not None]
    return max(available) if available else None


def _direction_edge_bps(row: dict[str, Any], direction: str) -> Decimal | None:
    field = "long_edge_bps" if direction == DIRECTION_LONG else "short_edge_bps"
    return to_decimal(row.get(field))


def _quotes_fresh(
    row: dict[str, Any],
    *,
    max_var_quote_age_ms: Decimal,
    max_lighter_book_age_seconds: Decimal,
) -> bool:
    var_age = to_decimal(row.get("var_quote_age_seconds"))
    lighter_age = to_decimal(row.get("lighter_book_age_seconds"))
    return bool(
        var_age is not None
        and lighter_age is not None
        and var_age * Decimal("1000") <= max_var_quote_age_ms
        and lighter_age <= max_lighter_book_age_seconds
    )


def _shadow_pnl_bps(
    candidate: dict[str, Any],
    followup: dict[str, Any],
    direction: str,
) -> Decimal | None:
    if direction == DIRECTION_LONG:
        entry_var = to_decimal(candidate.get("var_ask"))
        entry_lighter = to_decimal(candidate.get("lighter_sell_price"))
        exit_var = to_decimal(followup.get("var_bid"))
        exit_lighter = to_decimal(followup.get("lighter_buy_price"))
        if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
            return None
        assert entry_var is not None
        assert entry_lighter is not None
        assert exit_var is not None
        assert exit_lighter is not None
        pnl_per_unit = exit_var - entry_var + entry_lighter - exit_lighter
    elif direction == DIRECTION_SHORT:
        entry_var = to_decimal(candidate.get("var_bid"))
        entry_lighter = to_decimal(candidate.get("lighter_buy_price"))
        exit_var = to_decimal(followup.get("var_ask"))
        exit_lighter = to_decimal(followup.get("lighter_sell_price"))
        if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
            return None
        assert entry_var is not None
        assert entry_lighter is not None
        assert exit_var is not None
        assert exit_lighter is not None
        pnl_per_unit = entry_var - exit_var + exit_lighter - entry_lighter
    else:
        return None
    if entry_var <= 0:
        return None
    return pnl_per_unit / entry_var * Decimal("10000")


def analyze_sample_move_blocks(
    rows: list[dict[str, Any]],
    *,
    asset: str | None = None,
    horizons: tuple[int, ...] = HORIZONS_SECONDS,
    max_var_quote_age_ms: Decimal = Decimal("1500"),
    max_lighter_book_age_seconds: Decimal = Decimal("2"),
) -> list[dict[str, Any]]:
    timed_rows = [
        (parsed, row)
        for row in rows
        if (parsed := parse_time(row.get("logged_at"))) is not None
    ]
    timed_rows.sort(key=lambda item: item[0])
    states: dict[tuple[str, str], list[tuple[Any, dict[str, Any]]]] = defaultdict(list)
    for logged_at, row in timed_rows:
        if row.get("event") != "live_inventory_basis_state":
            continue
        key = (str(row.get("run_id") or ""), str(row.get("asset") or "").upper())
        states[key].append((logged_at, row))

    results: list[dict[str, Any]] = []
    for blocked_at, candidate in timed_rows:
        if candidate.get("event") != "live_inventory_entry_blocked":
            continue
        if candidate.get("reason") != "basis_sample_move_too_large":
            continue
        candidate_asset = str(candidate.get("asset") or "").upper()
        if asset and candidate_asset != asset.upper():
            continue
        direction = str(candidate.get("direction") or "")
        if direction not in {DIRECTION_LONG, DIRECTION_SHORT}:
            continue
        required_edge = _required_edge_bps(candidate)
        key = (str(candidate.get("run_id") or ""), candidate_asset)
        later_states = [
            (logged_at, row)
            for logged_at, row in states.get(key, [])
            if logged_at > blocked_at
        ]
        lost_point = None
        for logged_at, row in later_states:
            edge = _direction_edge_bps(row, direction)
            fresh = _quotes_fresh(
                row,
                max_var_quote_age_ms=max_var_quote_age_ms,
                max_lighter_book_age_seconds=max_lighter_book_age_seconds,
            )
            if not fresh or (
                required_edge is not None
                and edge is not None
                and edge < required_edge
            ):
                lost_point = (logged_at, row)
                break
        horizon_results: dict[int, dict[str, Any] | None] = {}
        for horizon in horizons:
            target = blocked_at + timedelta(seconds=horizon)
            if lost_point is not None and lost_point[0] <= target:
                # Once the edge or quote freshness is lost, the candidate did
                # not remain continuously executable through this horizon.
                point = lost_point
            else:
                point = next(
                    (
                        (logged_at, row)
                        for logged_at, row in later_states
                        if target <= logged_at <= target + timedelta(seconds=7)
                    ),
                    None,
                )
            if point is None:
                horizon_results[horizon] = None
                continue
            point_at, row = point
            edge = _direction_edge_bps(row, direction)
            fresh = _quotes_fresh(
                row,
                max_var_quote_age_ms=max_var_quote_age_ms,
                max_lighter_book_age_seconds=max_lighter_book_age_seconds,
            )
            retained = bool(
                fresh
                and edge is not None
                and required_edge is not None
                and edge >= required_edge
            )
            horizon_results[horizon] = {
                "observed_after_seconds": Decimal(str((point_at - blocked_at).total_seconds())),
                "edge_bps": edge,
                "quotes_fresh": fresh,
                "edge_retained": retained,
                "shadow_pnl_bps": _shadow_pnl_bps(candidate, row, direction) if fresh else None,
            }

        lost_after_seconds = (
            None
            if lost_point is None
            else Decimal(str((lost_point[0] - blocked_at).total_seconds()))
        )
        results.append(
            {
                "logged_at": blocked_at,
                "run_id": key[0],
                "asset": candidate_asset,
                "direction": direction,
                "sample_index": candidate.get("sample_index"),
                "initial_edge_bps": to_decimal(candidate.get("edge_bps")),
                "required_edge_bps": required_edge,
                "sample_move_bps": to_decimal(candidate.get("basis_sample_move_bps")),
                "lost_after_seconds": lost_after_seconds,
                "horizons": horizon_results,
            }
        )
    return results


def _print_report(results: list[dict[str, Any]]) -> None:
    print("=== SAMPLE MOVE BLOCK FOLLOW-UP ===")
    print(f"candidates={len(results)}")
    if not results:
        print("status=WAITING_FOR_SAMPLE_MOVE_BLOCKS")
        return
    for horizon in HORIZONS_SECONDS:
        points = [
            result["horizons"].get(horizon)
            for result in results
            if result["horizons"].get(horizon) is not None
        ]
        retained = [point for point in points if point["edge_retained"]]
        pnl_values = [
            point["shadow_pnl_bps"]
            for point in points
            if point["shadow_pnl_bps"] is not None
        ]
        rate = Decimal(len(retained)) / Decimal(len(points)) * Decimal("100") if points else None
        print(
            f"horizon_seconds={horizon} observed={len(points)} retained={len(retained)} "
            f"retained_pct={fmt_decimal(rate)} avg_shadow_pnl_bps={fmt_decimal(avg(pnl_values))}"
        )
    print("\n=== RECENT CANDIDATES ===")
    for result in results[-20:]:
        followups = []
        for horizon in HORIZONS_SECONDS:
            point = result["horizons"].get(horizon)
            if point is None:
                followups.append(f"{horizon}s=pending")
            else:
                followups.append(
                    f"{horizon}s(retained={point['edge_retained']},"
                    f"edge={fmt_decimal(point['edge_bps'])},"
                    f"pnl={fmt_decimal(point['shadow_pnl_bps'])})"
                )
        print(
            f"time={result['logged_at'].isoformat()} direction={result['direction']} "
            f"edge={fmt_decimal(result['initial_edge_bps'])} "
            f"required={fmt_decimal(result['required_edge_bps'])} "
            f"move={fmt_decimal(result['sample_move_bps'])} "
            f"lost_after_seconds={fmt_decimal(result['lost_after_seconds'])} "
            + " ".join(followups)
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure whether sample-move-blocked entry opportunities persisted."
    )
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--tail", type=int, default=200000)
    parser.add_argument("--include-rotated", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--current-only", action="store_true")
    parser.add_argument("--log-file", type=Path, default=ORDER_METRICS)
    args = parser.parse_args()

    rows = load_recent_compact_rows(
        args.log_file,
        asset=args.asset,
        hours=args.hours,
        limit=args.tail,
        include_rotated=not args.current_only,
    )
    results = analyze_sample_move_blocks(rows, asset=args.asset)
    _print_report(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
