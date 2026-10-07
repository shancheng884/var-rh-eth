#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.lib.account_equity_ledger import load_account_equity_state  # noqa: E402
from tools.lib.platform_pnl import (  # noqa: E402
    DEFAULT_LEDGER_PATH,
    DEFAULT_START,
    aggregate_period,
    load_platform_ledger,
    platform_source_completeness,
    sync_platform_ledger,
    weighted_capital,
)
from tools.lib.pnl_baseline import BEIJING_TIMEZONE, parse_timestamp  # noqa: E402


def period_boundary(value: str, *, inclusive_date_end: bool = False) -> datetime:
    try:
        if len(value) == 10:
            parsed_date = date.fromisoformat(value)
            if inclusive_date_end:
                parsed_date += timedelta(days=1)
            return datetime.combine(parsed_date, time.min, tzinfo=BEIJING_TIMEZONE).astimezone(timezone.utc)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use YYYY-MM-DD or ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BEIJING_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.000001'))} U"


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Var/RH realized PnL for any time range.")
    parser.add_argument("--start", default=DEFAULT_START, help="inclusive Beijing date or ISO timestamp")
    parser.add_argument("--end", help="inclusive Beijing date or exclusive ISO timestamp; defaults to now")
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--no-sync", action="store_true", help="use the last saved ledger without querying RH")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    start = period_boundary(args.start)
    end = period_boundary(args.end, inclusive_date_end=True) if args.end else datetime.now(timezone.utc)
    if end <= start:
        raise SystemExit("REFUSE: --end must be after --start")
    if not args.no_sync:
        try:
            sync_platform_ledger(path=args.ledger_path)
        except Exception as exc:
            raise SystemExit(f"SYNC_FAILED: {type(exc).__name__}: {exc}") from exc
    ledger = load_platform_ledger(args.ledger_path)
    selected = aggregate_period(ledger, start, end)
    baseline_start = parse_timestamp(ledger.get("statistics_start") or DEFAULT_START)
    if baseline_start is None:
        raise SystemExit("REFUSE: invalid persisted statistics start")
    if start < baseline_start:
        raise SystemExit(
            f"REFUSE: --start precedes the configured statistics start {baseline_start.isoformat()}"
        )
    cumulative = aggregate_period(ledger, baseline_start, end)
    avg_capital, current_capital = weighted_capital(ledger, start, end)
    days = Decimal(str((end - start).total_seconds())) / Decimal("86400")
    complete, source_detail = platform_source_completeness(
        ledger, required_through=end
    )
    annualized = (
        selected["net_pnl_usd"] / avg_capital * Decimal("100") * Decimal("365") / days
        if complete and avg_capital > 0 and days > 0 else None
    )

    sources = ledger.get("source_status") or {}
    var = sources.get("variational") or {}
    rh = sources.get("rh") or {}

    equity = load_account_equity_state(ROOT / "log" / "account_equity_daily_state.json")
    print(f"状态={'完整' if complete else '部分时段'}")
    print(f"统计区间UTC={start.isoformat()} 至 {end.isoformat()}")
    print(f"成交笔数={selected['trade_count']}")
    print(f"双平台总成交量={money(selected['volume_usd'])}")
    print(f"平仓收益={money(selected['realized_pnl_usd'])}")
    print(f"已结算资金费={money(selected['funding_usd'])}")
    print(f"区间双平台总盈亏={money(selected['net_pnl_usd'])}")
    print(f"区间累计简单年化={annualized.quantize(Decimal('0.0001')) if annualized is not None else '不可用'}%")
    print(f"累计成交量={money(cumulative['volume_usd'])}")
    print(f"累计总盈亏={money(cumulative['net_pnl_usd'])}")
    print(f"统计起始日={baseline_start.astimezone(BEIJING_TIMEZONE).date().isoformat()}")
    print(f"期初本金={money(Decimal(str(ledger.get('starting_capital_usd') or '0')))}")
    print(f"区间末统计本金（按净充提调整）={money(current_capital)}")
    print(f"区间净充提={money(selected['cashflow_usd'])}")
    print(f"最新双边权益={equity.get('latest_combined_equity_usd') or '-'} U")
    print(f"Variational权益={equity.get('latest_variational_equity_usd') or '-'} U")
    print(f"RH权益={equity.get('latest_lighter_equity_usd') or '-'} U")
    print(f"来源 Var={var.get('status', 'missing')}（导出新鲜={source_detail['variational_export_fresh']}，覆盖已确认={source_detail['variational_coverage_covers_report']}，疑似截断={source_detail['variational_export_may_be_truncated']}） RH={rh.get('status', 'missing')}")
    if source_detail.get("reasons"):
        print("部分原因=" + ",".join(source_detail["reasons"]))
    return 0 if complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
