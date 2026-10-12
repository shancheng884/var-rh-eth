#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
import gzip
import json
from pathlib import Path
import sys
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.lib.runtime_files import (
    ORDER_METRICS,
    fmt_decimal,
    parse_time,
    rotated_jsonl_paths,
    to_decimal,
)


DIRECTION_LONG = "long_var_short_lighter"
DIRECTION_SHORT = "short_var_long_lighter"
DIRECTIONS = (DIRECTION_LONG, DIRECTION_SHORT)
COMPACT_FIELDS = {
    "asset",
    "event",
    "lighter_book_age_seconds",
    "lighter_buy_price",
    "lighter_sell_price",
    "logged_at",
    "open_lots_total",
    "run_id",
    "sample_index",
    "v4_direction_edges_bps",
    "v4_direction_thresholds_bps",
    "v4_entry_direction",
    "v4_health_ready",
    "v4_real_gradient_active_tier",
    "v4_real_gradient_market_tier",
    "var_ask",
    "var_bid",
    "var_quote_age_seconds",
}


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    for source_path in rotated_jsonl_paths(path):
        opener = gzip.open if source_path.suffix == ".gz" else open
        with opener(source_path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    yield value


def load_recent_basis_states(
    path: Path,
    *,
    asset: str,
    hours: float,
) -> list[dict[str, Any]]:
    rows: deque[tuple[datetime, dict[str, Any]]] = deque()
    window = timedelta(hours=max(0.0, hours))
    for value in _iter_jsonl(path):
        if value.get("event") != "live_inventory_basis_state":
            continue
        if str(value.get("asset") or "").upper() != asset.upper():
            continue
        observed_at = parse_time(value.get("logged_at"))
        if observed_at is None:
            continue
        compact = {key: value.get(key) for key in COMPACT_FIELDS if key in value}
        compact["_observed_at"] = observed_at
        rows.append((observed_at, compact))
        if hours > 0:
            cutoff = observed_at - window
            while rows and rows[0][0] < cutoff:
                rows.popleft()
    return [row for _, row in rows]


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _direction_context(
    row: dict[str, Any],
) -> tuple[str, Decimal, Decimal] | None:
    edges = _mapping(row.get("v4_direction_edges_bps"))
    thresholds = _mapping(row.get("v4_direction_thresholds_bps"))
    direction = str(row.get("v4_entry_direction") or "")
    if direction not in DIRECTIONS:
        candidates: list[tuple[Decimal, str, Decimal, Decimal]] = []
        for item in DIRECTIONS:
            edge = to_decimal(edges.get(item))
            threshold = to_decimal(thresholds.get(item))
            if edge is not None and threshold is not None:
                candidates.append((edge - threshold, item, edge, threshold))
        if not candidates:
            return None
        _, direction, edge, threshold = max(candidates)
        return direction, edge, threshold
    edge = to_decimal(edges.get(direction))
    threshold = to_decimal(thresholds.get(direction))
    if edge is None or threshold is None:
        return None
    return direction, edge, threshold


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


def _executable_pnl_bps(
    entry: dict[str, Any],
    future: dict[str, Any],
    direction: str,
) -> Decimal | None:
    if direction == DIRECTION_LONG:
        entry_var = to_decimal(entry.get("var_ask"))
        entry_lighter = to_decimal(entry.get("lighter_sell_price"))
        exit_var = to_decimal(future.get("var_bid"))
        exit_lighter = to_decimal(future.get("lighter_buy_price"))
        if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
            return None
        assert entry_var is not None
        assert entry_lighter is not None
        assert exit_var is not None
        assert exit_lighter is not None
        pnl_per_unit = exit_var - entry_var + entry_lighter - exit_lighter
    else:
        entry_var = to_decimal(entry.get("var_bid"))
        entry_lighter = to_decimal(entry.get("lighter_buy_price"))
        exit_var = to_decimal(future.get("var_ask"))
        exit_lighter = to_decimal(future.get("lighter_sell_price"))
        if None in {entry_var, entry_lighter, exit_var, exit_lighter}:
            return None
        assert entry_var is not None
        assert entry_lighter is not None
        assert exit_var is not None
        assert exit_lighter is not None
        pnl_per_unit = entry_var - exit_var + exit_lighter - entry_lighter
    if entry_var <= 0:
        return None
    return pnl_per_unit / entry_var * Decimal("10000")


def find_unconfirmed_single_sample_episodes(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    episodes: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def finish() -> None:
        nonlocal current
        if current is not None and not current["confirmed"]:
            episodes.append(current)
        current = None

    for index, row in enumerate(rows):
        context = _direction_context(row)
        raw_tier = int(row.get("v4_real_gradient_market_tier") or 0)
        active_tier = int(row.get("v4_real_gradient_active_tier") or 0)
        flat = int(row.get("open_lots_total") or 0) == 0
        health_ready = row.get("v4_health_ready") is not False
        crossed = bool(
            context is not None
            and raw_tier > 0
            and context[1] >= context[2]
        )
        direction = context[0] if context is not None else None

        if current is not None and (
            not crossed
            or direction != current["direction"]
            or not flat
            or not health_ready
        ):
            finish()
        if not crossed or not flat or not health_ready or context is None:
            continue
        if current is None:
            current = {
                "entry_index": index,
                "direction": direction,
                "rows": [],
                "confirmed": False,
                "max_market_tier": raw_tier,
                "max_active_tier": active_tier,
            }
        current["rows"].append(row)
        current["max_market_tier"] = max(current["max_market_tier"], raw_tier)
        current["max_active_tier"] = max(current["max_active_tier"], active_tier)
        if active_tier > 0:
            current["confirmed"] = True
    finish()
    return [episode for episode in episodes if len(episode["rows"]) == 1]


def evaluate_single_sample_episodes(
    rows: list[dict[str, Any]],
    *,
    target_bps: Decimal = Decimal("3"),
    shortfall_reserve_bps: Decimal = Decimal("0.5"),
    max_horizon_seconds: int = 3600,
    max_sample_gap_seconds: int = 90,
    max_var_quote_age_ms: Decimal = Decimal("1500"),
    max_lighter_book_age_seconds: Decimal = Decimal("2"),
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for episode in find_unconfirmed_single_sample_episodes(rows):
        entry_index = int(episode["entry_index"])
        entry = rows[entry_index]
        entry_at = entry["_observed_at"]
        direction = str(episode["direction"])
        context = _direction_context(entry)
        if context is None or not _quotes_fresh(
            entry,
            max_var_quote_age_ms=max_var_quote_age_ms,
            max_lighter_book_age_seconds=max_lighter_book_age_seconds,
        ):
            continue
        mfe: Decimal | None = None
        mae: Decimal | None = None
        target_at: datetime | None = None
        last_at = entry_at
        path_complete = True
        reached_horizon = False
        observed = 0
        for future in rows[entry_index + 1 :]:
            future_at = future["_observed_at"]
            if str(future.get("run_id") or "") != str(entry.get("run_id") or ""):
                break
            elapsed = (future_at - entry_at).total_seconds()
            if elapsed > max_horizon_seconds:
                remaining_gap = (
                    entry_at
                    + timedelta(seconds=max_horizon_seconds)
                    - last_at
                ).total_seconds()
                if remaining_gap > max_sample_gap_seconds:
                    path_complete = False
                else:
                    reached_horizon = True
                break
            if (future_at - last_at).total_seconds() > max_sample_gap_seconds:
                path_complete = False
                break
            last_at = future_at
            if not _quotes_fresh(
                future,
                max_var_quote_age_ms=max_var_quote_age_ms,
                max_lighter_book_age_seconds=max_lighter_book_age_seconds,
            ):
                continue
            pnl = _executable_pnl_bps(entry, future, direction)
            if pnl is None:
                continue
            observed += 1
            net_pnl = pnl - shortfall_reserve_bps
            mfe = net_pnl if mfe is None else max(mfe, net_pnl)
            mae = net_pnl if mae is None else min(mae, net_pnl)
            if target_at is None and net_pnl >= target_bps:
                target_at = future_at
                break
            if elapsed >= max_horizon_seconds:
                reached_horizon = True
                break
        path_complete = bool(
            path_complete and (target_at is not None or reached_horizon)
        )
        results.append(
            {
                "logged_at": entry_at,
                "direction": direction,
                "market_tier": episode["max_market_tier"],
                "edge_bps": context[1],
                "threshold_bps": context[2],
                "excess_bps": context[1] - context[2],
                "observed_followups": observed,
                "path_complete": path_complete,
                "net_mfe_bps": mfe,
                "net_mae_bps": mae,
                "target_hit": target_at is not None,
                "target_after_seconds": (
                    None
                    if target_at is None
                    else Decimal(str((target_at - entry_at).total_seconds()))
                ),
            }
        )
    return results


def _print_report(results: list[dict[str, Any]], *, target_bps: Decimal) -> None:
    print("=== ENTRY CONFIRMATION COUNTERFACTUAL ===")
    print("model=logged_executable_quotes_minus_shortfall_reserve")
    print(f"unconfirmed_single_sample_episodes={len(results)}")
    complete = [item for item in results if item["path_complete"]]
    hits = [item for item in complete if item["target_hit"]]
    print(f"complete_paths={len(complete)}")
    print(f"target_bps={target_bps}")
    print(f"target_hits={len(hits)}")
    hit_rate = (
        Decimal(len(hits)) / Decimal(len(complete)) * Decimal("100")
        if complete
        else None
    )
    print(f"target_hit_rate_pct={fmt_decimal(hit_rate)}")
    for index, item in enumerate(results, start=1):
        print(
            f"episode={index} time={item['logged_at'].isoformat()} "
            f"direction={item['direction']} tier={item['market_tier']} "
            f"edge_bps={item['edge_bps']} threshold_bps={item['threshold_bps']} "
            f"excess_bps={item['excess_bps']} path_complete={item['path_complete']} "
            f"followups={item['observed_followups']} net_mfe_bps={item['net_mfe_bps']} "
            f"net_mae_bps={item['net_mae_bps']} target_hit={item['target_hit']} "
            f"target_after_seconds={item['target_after_seconds']}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate unconfirmed one-sample V4 entries from compact log data."
    )
    parser.add_argument("--source", type=Path, default=ORDER_METRICS)
    parser.add_argument("--asset", default="ETH")
    parser.add_argument("--hours", type=float, default=12)
    parser.add_argument("--target-bps", type=Decimal, default=Decimal("3"))
    parser.add_argument(
        "--shortfall-reserve-bps", type=Decimal, default=Decimal("0.5")
    )
    parser.add_argument("--max-horizon-seconds", type=int, default=3600)
    parser.add_argument("--max-sample-gap-seconds", type=int, default=90)
    args = parser.parse_args()
    if not args.source.exists():
        raise SystemExit(f"source_not_found={args.source}")
    rows = load_recent_basis_states(
        args.source,
        asset=args.asset,
        hours=args.hours,
    )
    results = evaluate_single_sample_episodes(
        rows,
        target_bps=args.target_bps,
        shortfall_reserve_bps=args.shortfall_reserve_bps,
        max_horizon_seconds=args.max_horizon_seconds,
        max_sample_gap_seconds=args.max_sample_gap_seconds,
    )
    print(f"source={args.source}")
    print(f"basis_states={len(rows)}")
    _print_report(results, target_bps=args.target_bps)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
