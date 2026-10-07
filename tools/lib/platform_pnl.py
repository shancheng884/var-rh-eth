from __future__ import annotations

import csv
import hashlib
import inspect
import json
import os
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

import requests

from tools.lib.pnl_baseline import BEIJING_TIMEZONE, parse_timestamp


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LEDGER_PATH = ROOT / "log" / "platform_pnl_ledger.json"
DEFAULT_VARIATIONAL_EXPORT_DIR = ROOT / "log" / "platform_pnl_imports"
DEFAULT_VARIATIONAL_COVERAGE_FILE = DEFAULT_VARIATIONAL_EXPORT_DIR / "coverage.json"
DEFAULT_START = "2026-10-05T00:00:00+08:00"
DEFAULT_CAPITAL_USD = Decimal("241.774564")
LIGHTER_API_URL = "https://api.rh.lighter.xyz"
LIGHTER_CHAIN_ID = 466324
SCHEMA_VERSION = 1
USD_ASSET_SYMBOLS = {"USD", "USDC", "USDT", "USDC.E"}


def decimal_value(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value)) if value not in (None, "") else None
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result is not None and result.is_finite() else None


def as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    for method_name, kwargs in (("to_dict", {}), ("model_dump", {"mode": "json"})):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                result = method(**kwargs)
            except TypeError:
                result = method()
            if isinstance(result, dict):
                return result
    return {}


def timestamp_iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    parsed = parse_timestamp(value)
    if parsed is None:
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return None
        if numeric > 10**12:
            numeric /= 1000
        try:
            parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    return parsed.isoformat()


def _load_ledger(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "statistics_start": DEFAULT_START,
            "starting_capital_usd": str(DEFAULT_CAPITAL_USD),
            "events": [],
            "source_status": {},
        }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"platform PnL ledger unreadable: {path}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("events"), list):
        raise RuntimeError(f"platform PnL ledger has invalid structure: {path}")
    return value


def load_platform_ledger(path: Path = DEFAULT_LEDGER_PATH) -> dict[str, Any]:
    return _load_ledger(path)


def _save_ledger(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def _event_key(event: dict[str, Any]) -> str:
    return f"{event['venue']}:{event['kind']}:{event['event_id']}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _merge_events(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = {_event_key(item): item for item in existing if isinstance(item, dict) and item.get("event_id")}
    for event in incoming:
        if event.get("event_id"):
            merged[_event_key(event)] = event
    return sorted(merged.values(), key=lambda item: (str(item.get("timestamp") or ""), _event_key(item)))


def _instrument_symbol(row: dict[str, Any]) -> str:
    for key in ("underlying", "market", "market_symbol", "symbol", "instrument"):
        value = str(row.get(key) or "").strip().upper()
        if value:
            return value
    return ""


def _is_eth_instrument(value: str) -> bool:
    normalized = re.sub(r"[^A-Z0-9]", "", value.upper())
    return normalized in {"ETH", "ETHUSD", "ETHUSDC", "ETHPERP", "ETHUSDT"}


def _is_usd_asset(value: Any) -> bool:
    return str(value or "").strip().upper() in USD_ASSET_SYMBOLS


def import_variational_exports(export_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events: list[dict[str, Any]] = []
    files = sorted(export_dir.glob("*.csv")) if export_dir.exists() else []
    trade_files = [p for p in files if "trade" in p.stem.lower()]
    transfer_files = [p for p in files if "transfer" in p.stem.lower() or "fund" in p.stem.lower()]
    parsed_counts = {"trades": 0, "realized_pnl": 0, "funding": 0, "cashflows": 0, "fees": 0}
    unclassified_rows = 0
    latest_mtime = 0.0
    hashes = {
        path.name: _sha256_file(path)
        for path in files
    }
    coverage: dict[str, Any] = {}
    coverage_path = export_dir / "coverage.json"
    if coverage_path.exists():
        try:
            candidate = json.loads(coverage_path.read_text(encoding="utf-8"))
            if isinstance(candidate, dict):
                coverage = candidate
        except (OSError, json.JSONDecodeError):
            coverage = {}
    coverage_files_match = (
        coverage.get("confirmed_complete") is True
        and coverage.get("files_sha256") == hashes
        and bool(trade_files)
        and bool(transfer_files)
    )

    for kind, paths in (("trade", trade_files), ("transfer", transfer_files)):
        for path in paths:
            latest_mtime = max(latest_mtime, path.stat().st_mtime)
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    row = {str(k or "").strip().lower(): v for k, v in row.items()}
                    transfer_type = str(row.get("transfer_type") or row.get("type") or "").strip().lower()
                    if kind == "trade":
                        instrument = _instrument_symbol(row)
                        if not instrument and "eth" in path.stem.lower():
                            instrument = "ETH"
                        if not instrument:
                            unclassified_rows += 1
                            continue
                        if not _is_eth_instrument(instrument):
                            continue
                    elif transfer_type not in {"deposit", "withdrawal", "funding", "realized_pnl", "fee"}:
                        instrument = _instrument_symbol(row)
                        if not instrument and "eth" in path.stem.lower():
                            instrument = "ETH"
                        if not instrument or _is_eth_instrument(instrument):
                            unclassified_rows += 1
                        continue
                    elif transfer_type in {"funding", "realized_pnl", "fee"}:
                        instrument = _instrument_symbol(row)
                        if not instrument and "eth" in path.stem.lower():
                            instrument = "ETH"
                        if not instrument:
                            unclassified_rows += 1
                            continue
                        if not _is_eth_instrument(instrument):
                            continue
                    status = str(row.get("status") or "").strip().lower()
                    if status in {"failed", "rejected", "cancelled", "canceled", "expired"}:
                        continue
                    if status not in {"confirmed", "complete", "completed", "success", "successful"}:
                        unclassified_rows += 1
                        continue
                    event_id = str(row.get("id") or row.get("trade_id") or "").strip()
                    at = timestamp_iso(row.get("created_at") or row.get("timestamp"))
                    if not event_id or not at:
                        unclassified_rows += 1
                        continue
                    if kind == "trade":
                        qty = decimal_value(row.get("qty") or row.get("quantity"))
                        price = decimal_value(row.get("price"))
                        if qty is None or price is None:
                            unclassified_rows += 1
                            continue
                        events.append({
                            "venue": "variational", "kind": "trade", "event_id": event_id,
                            "timestamp": at, "asset": "ETH", "notional_usd": str(abs(qty * price)),
                            "side": str(row.get("side") or "").lower(), "realized_pnl_usd": "0",
                        })
                        parsed_counts["trades"] += 1
                        continue
                    amount = decimal_value(row.get("qty") or row.get("amount"))
                    if amount is None:
                        unclassified_rows += 1
                        continue
                    if transfer_type == "funding":
                        event_kind = "funding"
                        parsed_counts["funding"] += 1
                    elif transfer_type == "realized_pnl":
                        event_kind = "realized_pnl"
                        parsed_counts["realized_pnl"] += 1
                    elif transfer_type == "fee":
                        event_kind = "fee"
                        parsed_counts["fees"] += 1
                    elif transfer_type in {"deposit", "withdrawal"}:
                        if not _is_usd_asset(row.get("asset") or row.get("currency")):
                            unclassified_rows += 1
                            continue
                        event_kind = "cashflow"
                        if transfer_type == "deposit":
                            amount = abs(amount)
                        else:
                            amount = -abs(amount)
                        parsed_counts["cashflows"] += 1
                    else:
                        continue
                    events.append({
                        "venue": "variational", "kind": event_kind, "event_id": event_id,
                        "timestamp": at, "asset": "ETH" if event_kind not in {"cashflow"} else str(row.get("asset") or row.get("currency") or "USD"),
                        "amount_usd": str(amount),
                    })

    final_files = sorted(export_dir.glob("*.csv"))
    hashes_unchanged = hashes == {
        path.name: _sha256_file(path)
        for path in final_files
    }
    status = "available" if trade_files and transfer_files and unclassified_rows == 0 and hashes_unchanged else (
        "missing_exports" if not trade_files or not transfer_files else "incomplete"
    )
    return events, {
        "status": status,
        "trade_files": len(trade_files), "transfer_files": len(transfer_files),
        "counts": parsed_counts,
        "unclassified_rows": unclassified_rows,
        "last_export_mtime_utc": datetime.fromtimestamp(latest_mtime, timezone.utc).isoformat() if latest_mtime else None,
        "export_snapshot_at_utc": datetime.fromtimestamp(latest_mtime, timezone.utc).isoformat() if latest_mtime else None,
        "coverage_confirmed": coverage_files_match,
        "coverage_from": coverage.get("complete_from") if coverage_files_match else None,
        "coverage_through": coverage.get("complete_through") if coverage_files_match else None,
        "coverage_confirmed_at": coverage.get("confirmed_at_utc") if coverage_files_match else None,
        "coverage_manifest_valid": coverage_files_match and hashes_unchanged,
    }


def _call_with_supported_kwargs(method: Callable[..., Any], positional: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    try:
        signature = inspect.signature(method)
        accepts_extra = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values())
        supported = kwargs if accepts_extra else {k: v for k, v in kwargs.items() if k in signature.parameters}
    except (TypeError, ValueError):
        supported = kwargs
    return method(*positional, **supported)


async def fetch_lighter_activity(start_at: str, *, base_url: str = LIGHTER_API_URL) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    account_index = int(os.environ["LIGHTER_ACCOUNT_INDEX"])
    api_key_index = int(os.environ["LIGHTER_API_KEY_INDEX"])
    private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or os.environ["LIGHTER_PRIVATE_KEY"]
    from lighter import AccountApi, OrderApi, TransactionApi
    from lighter.signer_client import SignerClient

    signer = SignerClient(
        url=base_url, account_index=account_index,
        api_private_keys={api_key_index: private_key}, chain_id=LIGHTER_CHAIN_ID,
    )
    try:
        check_error = signer.check_client()
        if check_error is not None:
            raise RuntimeError(f"Lighter client check failed: {check_error}")
        authorization, auth_error = signer.create_auth_token_with_expiry(api_key_index=api_key_index)
        if auth_error is not None or not authorization:
            raise RuntimeError(f"Lighter read authorization failed: {auth_error or 'empty token'}")
        api = signer.api_client
        order_api, account_api, transaction_api = OrderApi(api), AccountApi(api), TransactionApi(api)
        start_time = parse_timestamp(start_at)
        if start_time is None:
            raise ValueError("invalid statistics start timestamp")
        order_books = requests.get(f"{base_url}/api/v1/orderBooks", timeout=15)
        order_books.raise_for_status()
        eth_markets = [int(x["market_id"]) for x in order_books.json().get("order_books", []) if str(x.get("symbol", "")).upper() == "ETH"]
        if not eth_markets:
            raise RuntimeError("RH Lighter ETH market id not found")
        market_id = eth_markets[0]

        def api_rows(response: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
            for key in keys:
                rows = response.get(key)
                if isinstance(rows, list):
                    return [as_dict(row) for row in rows]
            return []

        async def call_api(method: Callable[..., Any], positional: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
            result = _call_with_supported_kwargs(method, positional, kwargs)
            if inspect.isawaitable(result):
                result = await result
            response = as_dict(result)
            code = response.get("code")
            if code not in (None, 0, 200):
                raise RuntimeError(f"Lighter API returned code={code} message={response.get('message')}")
            return response

        async def collect_pages(
            method: Callable[..., Any],
            positional: tuple[Any, ...],
            kwargs: dict[str, Any],
            *,
            row_fields: tuple[str, ...],
            page_limit: int | None,
            stop_at_start: bool,
        ) -> tuple[list[dict[str, Any]], bool, int]:
            all_rows: list[dict[str, Any]] = []
            cursor = None
            seen_cursors: set[str] = set()
            complete = True
            pages = 0
            for _ in range(1000):
                response = await call_api(method, positional, {**kwargs, "cursor": cursor})
                pages += 1
                rows = api_rows(response, *row_fields)
                if not rows:
                    break
                all_rows.extend(rows)
                old_page = False
                if stop_at_start:
                    for row in rows:
                        at = timestamp_iso(row.get("timestamp") or row.get("created_at") or row.get("transaction_time"))
                        parsed = parse_timestamp(at)
                        if parsed is not None and parsed < start_time:
                            old_page = True
                            break
                next_cursor = response.get("next_cursor") or response.get("cursor")
                if old_page:
                    break
                if not next_cursor:
                    if page_limit is not None and len(rows) >= page_limit:
                        complete = False
                    break
                cursor_key = str(next_cursor)
                if cursor_key in seen_cursors:
                    complete = False
                    break
                cursor = cursor_key
                seen_cursors.add(cursor_key)
            else:
                complete = False
            return all_rows, complete, pages

        # Resolve the L1 address and asset symbols from public read-only account metadata.
        account_rows: list[dict[str, Any]] = []
        l1_address = ""
        account_method = getattr(account_api, "account", None)
        if callable(account_method):
            account_response = await call_api(account_method, ("index", str(account_index)), {})
            account_rows = api_rows(account_response, "accounts")
            if account_rows:
                l1_address = str(account_rows[0].get("l1_address") or "").strip()

        asset_symbols: dict[str, str] = {}
        asset_method = getattr(order_api, "asset_details", None)
        if callable(asset_method):
            asset_response = await call_api(asset_method, (), {})
            for asset in api_rows(asset_response, "asset_details"):
                if asset.get("asset_id") is not None and asset.get("symbol"):
                    asset_symbols[str(asset["asset_id"])] = str(asset["symbol"]).upper()

        events: list[dict[str, Any]] = []
        counts = {"trades": 0, "funding": 0, "cashflows": 0, "fees": 0}
        page_status: dict[str, Any] = {}
        unclassified_rows = 0
        unsupported_fee_rows = 0

        for method_name, target, positional, kwargs, fields, limit, stop_at_start in (
            ("trades", order_api, ("timestamp", 100),
             {"authorization": authorization, "account_index": account_index, "market_id": market_id,
              "sort_dir": "desc", "aggregate": False}, ("trades",), 100, True),
            ("position_funding", account_api, (account_index, 100),
             {"authorization": authorization, "market_ids": [market_id]},
             ("position_fundings",), 100, False),
        ):
            method = getattr(target, method_name, None)
            if not callable(method):
                raise RuntimeError(f"installed Lighter SDK lacks read-only {method_name} API")
            rows, page_complete, page_count = await collect_pages(
                method, positional, kwargs, row_fields=fields, page_limit=limit,
                stop_at_start=stop_at_start,
            )
            page_status[method_name] = {"complete": page_complete, "pages": page_count}
            for row in rows:
                at = timestamp_iso(row.get("timestamp") or row.get("transaction_time"))
                parsed_at = parse_timestamp(at)
                if not at or parsed_at is None:
                    unclassified_rows += 1
                    continue
                if parsed_at < start_time:
                    continue
                try:
                    row_market_id = int(row.get("market_id"))
                except (TypeError, ValueError):
                    unclassified_rows += 1
                    continue
                if row_market_id != market_id:
                    continue
                event_id = str(row.get("trade_id_str") or row.get("trade_id") or row.get("funding_id") or "").strip()
                if not event_id:
                    unclassified_rows += 1
                    continue
                if method_name == "trades":
                    try:
                        ask_account = int(row.get("ask_account_id"))
                        bid_account = int(row.get("bid_account_id"))
                    except (TypeError, ValueError):
                        unclassified_rows += 1
                        continue
                    if ask_account == account_index:
                        realized = decimal_value(row.get("ask_account_pnl"))
                    elif bid_account == account_index:
                        realized = decimal_value(row.get("bid_account_pnl"))
                    else:
                        unclassified_rows += 1
                        continue
                    if realized is None:
                        unclassified_rows += 1
                        realized = Decimal(0)
                    notional = decimal_value(row.get("usd_amount"))
                    if notional is None:
                        qty = decimal_value(row.get("size"))
                        price = decimal_value(row.get("price"))
                        if qty is None or price is None:
                            unclassified_rows += 1
                            continue
                        notional = abs(qty * price)
                    if any(decimal_value(row.get(key)) not in (None, Decimal(0)) for key in ("maker_fee", "taker_fee")):
                        unsupported_fee_rows += 1
                    events.append({"venue": "rh", "kind": "trade", "event_id": event_id, "timestamp": at,
                                   "asset": "ETH", "notional_usd": str(abs(notional)), "realized_pnl_usd": str(realized)})
                    counts["trades"] += 1
                else:
                    amount = decimal_value(row.get("change"))
                    if amount is None:
                        unclassified_rows += 1
                        continue
                    events.append({"venue": "rh", "kind": "funding", "event_id": event_id, "timestamp": at,
                                   "asset": "ETH", "amount_usd": str(amount)})
                    counts["funding"] += 1

        if not l1_address:
            unclassified_rows += 1
            page_status["account_l1_address"] = {"complete": False}
        else:
            page_status["account_l1_address"] = {"complete": True}

        # Internal account transfers are separate from external deposit/withdraw records.
        transfer_method = getattr(transaction_api, "transfer_history", None)
        if not callable(transfer_method):
            raise RuntimeError("installed Lighter SDK lacks read-only transfer_history API")
        transfers, transfer_complete, transfer_pages = await collect_pages(
            transfer_method, (account_index,), {"authorization": authorization},
            row_fields=("transfers",), page_limit=None, stop_at_start=True,
        )
        page_status["transfer_history"] = {"complete": transfer_complete, "pages": transfer_pages}
        for row in transfers:
            at = timestamp_iso(row.get("timestamp"))
            parsed_at = parse_timestamp(at)
            if parsed_at is None or parsed_at < start_time:
                continue
            symbol = asset_symbols.get(str(row.get("asset_id")), "")
            transfer_type = str(row.get("type") or "").lower()
            try:
                amount = decimal_value(row.get("amount"))
                from_index = row.get("from_account_index")
                to_index = row.get("to_account_index")
                if str(to_index) == str(account_index):
                    amount = abs(amount) if amount is not None else None
                elif str(from_index) == str(account_index):
                    amount = -abs(amount) if amount is not None else None
                else:
                    continue
            except (TypeError, ValueError):
                amount = None
            if amount is None or not _is_usd_asset(symbol):
                unclassified_rows += 1
                continue
            event_id = str(row.get("id") or row.get("tx_hash") or "").strip()
            if not event_id:
                unclassified_rows += 1
                continue
            events.append({"venue": "rh", "kind": "cashflow", "event_id": event_id, "timestamp": at,
                           "asset": symbol, "amount_usd": str(amount), "transfer_type": transfer_type})
            counts["cashflows"] += 1

        # External deposits and withdrawals use dedicated history endpoints.
        for method_name, field_name, kind, positional in (
            ("deposit_history", "deposits", "deposit", (authorization, account_index, l1_address)),
            ("withdraw_history", "withdraws", "withdrawal", (authorization, account_index)),
        ):
            if not l1_address and kind == "deposit":
                page_status[method_name] = {"complete": False, "pages": 0}
                continue
            method = getattr(transaction_api, method_name, None)
            if not callable(method):
                raise RuntimeError(f"installed Lighter SDK lacks read-only {method_name} API")
            rows, page_complete, page_count = await collect_pages(
                method, positional, {}, row_fields=(field_name,), page_limit=None, stop_at_start=True
            )
            page_status[method_name] = {"complete": page_complete, "pages": page_count}
            for row in rows:
                at = timestamp_iso(row.get("timestamp"))
                parsed_at = parse_timestamp(at)
                if parsed_at is None or parsed_at < start_time:
                    continue
                status = str(row.get("status") or "").strip().lower()
                accepted_statuses = {"confirmed", "complete", "completed", "success", "successful", "processed", "claimable"}
                if status not in accepted_statuses:
                    if status not in {"failed", "rejected", "cancelled", "canceled", "expired"}:
                        unclassified_rows += 1
                    continue
                symbol = asset_symbols.get(str(row.get("asset_id")), "")
                amount = decimal_value(row.get("amount"))
                if amount is None or not _is_usd_asset(symbol):
                    unclassified_rows += 1
                    continue
                event_id = str(row.get("id") or row.get("l1_tx_hash") or "").strip()
                if not event_id:
                    unclassified_rows += 1
                    continue
                if kind == "withdrawal":
                    amount = -abs(amount)
                else:
                    amount = abs(amount)
                events.append({"venue": "rh", "kind": "cashflow", "event_id": event_id, "timestamp": at,
                               "asset": symbol, "amount_usd": str(amount), "transfer_type": kind})
                counts["cashflows"] += 1

        pages_complete = all(bool(value.get("complete")) for value in page_status.values())
        status = "available" if pages_complete and unclassified_rows == 0 and unsupported_fee_rows == 0 else "incomplete"
        return events, {"status": status, "market_id": market_id, "account_index": account_index, "counts": counts,
                        "page_status": page_status, "unclassified_rows": unclassified_rows,
                        "unsupported_fee_rows": unsupported_fee_rows,
                        "usd_cashflow_assets": sorted(USD_ASSET_SYMBOLS),
                        "synced_at_utc": datetime.now(timezone.utc).isoformat()}
    finally:
        close = getattr(signer, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result


def sync_platform_ledger(
    path: Path = DEFAULT_LEDGER_PATH,
    export_dir: Path = DEFAULT_VARIATIONAL_EXPORT_DIR,
    *,
    start_at: str = DEFAULT_START,
    fetch_rh: bool = True,
) -> dict[str, Any]:
    ledger = _load_ledger(path)
    if str(ledger.get("statistics_start") or DEFAULT_START) != start_at:
        raise RuntimeError("statistics start differs from persisted ledger; use a deliberate ledger migration")
    incoming: list[dict[str, Any]] = []
    try:
        var_events, var_status = import_variational_exports(export_dir)
        incoming.extend(var_events)
    except Exception as exc:
        var_status = {"status": "sync_failed", "error_type": type(exc).__name__}
    rh_status: dict[str, Any]
    if fetch_rh:
        import asyncio
        try:
            rh_events, rh_status = asyncio.run(fetch_lighter_activity(start_at))
            incoming.extend(rh_events)
        except Exception as exc:
            rh_status = {
                "status": "sync_failed",
                "error_type": type(exc).__name__,
                "synced_at_utc": datetime.now(timezone.utc).isoformat(),
            }
    else:
        rh_status = {"status": "not_synced"}
    ledger["events"] = _merge_events(ledger.get("events", []), incoming)
    ledger["source_status"] = {"variational": var_status, "rh": rh_status,
                                "last_sync_at_utc": datetime.now(timezone.utc).isoformat()}
    ledger["statistics_start"] = start_at
    ledger["starting_capital_usd"] = str(ledger.get("starting_capital_usd") or DEFAULT_CAPITAL_USD)
    _save_ledger(path, ledger)
    return ledger


def aggregate_period(ledger: dict[str, Any], start: datetime, end: datetime) -> dict[str, Any]:
    if end <= start:
        raise ValueError("period end must be later than start")
    totals = {"trade_count": 0, "volume_usd": Decimal(0), "realized_pnl_usd": Decimal(0),
              "funding_usd": Decimal(0), "cashflow_usd": Decimal(0), "fee_usd": Decimal(0)}
    for event in ledger.get("events", []):
        at = parse_timestamp(event.get("timestamp"))
        if at is None or not (start <= at < end):
            continue
        kind = event.get("kind")
        if kind == "trade":
            totals["trade_count"] += 1
            totals["volume_usd"] += decimal_value(event.get("notional_usd")) or Decimal(0)
            totals["realized_pnl_usd"] += decimal_value(event.get("realized_pnl_usd")) or Decimal(0)
        elif kind == "realized_pnl":
            totals["realized_pnl_usd"] += decimal_value(event.get("amount_usd")) or Decimal(0)
        elif kind == "funding":
            totals["funding_usd"] += decimal_value(event.get("amount_usd")) or Decimal(0)
        elif kind == "cashflow":
            totals["cashflow_usd"] += decimal_value(event.get("amount_usd")) or Decimal(0)
        elif kind == "fee":
            totals["fee_usd"] += abs(decimal_value(event.get("amount_usd")) or Decimal(0))
    totals["net_pnl_usd"] = totals["realized_pnl_usd"] + totals["funding_usd"] - totals["fee_usd"]
    return totals


def platform_source_completeness(
    ledger: dict[str, Any], *, now: datetime | None = None,
    required_through: datetime | None = None, max_export_age_hours: int = 30
) -> tuple[bool, dict[str, Any]]:
    observed = now or datetime.now(timezone.utc)
    sources = ledger.get("source_status") or {}
    var = sources.get("variational") or {}
    rh = sources.get("rh") or {}
    export_at = parse_timestamp(var.get("last_export_mtime_utc"))
    export_age_seconds = (observed - export_at).total_seconds() if export_at else None
    var_fresh = export_age_seconds is not None and 0 <= export_age_seconds <= max_export_age_hours * 3600
    counts = var.get("counts") or {}
    var_truncated = any(
        int(counts.get(key) or 0) >= 10_000
        for key in ("trades", "realized_pnl", "funding", "cashflows")
    )
    statistics_start = parse_timestamp(ledger.get("statistics_start") or DEFAULT_START)
    coverage_from = parse_timestamp(var.get("coverage_from"))
    coverage_through = parse_timestamp(var.get("coverage_through"))
    required_end = required_through or observed
    coverage_range_valid = bool(
        var.get("coverage_manifest_valid")
        and statistics_start
        and coverage_from
        and coverage_through
        and coverage_from <= statistics_start
        and coverage_through >= required_end
    )
    rh_pages = rh.get("page_status") or {}
    rh_pages_complete = bool(rh_pages) and all(
        bool(item.get("complete")) for item in rh_pages.values() if isinstance(item, dict)
    )
    rh_complete = rh.get("status") == "available" and rh_pages_complete
    details = {
        "variational": var.get("status", "missing"),
        "variational_export_fresh": var_fresh,
        "variational_export_age_seconds": export_age_seconds,
        "variational_export_may_be_truncated": var_truncated,
        "variational_coverage_confirmed": bool(var.get("coverage_manifest_valid")),
        "variational_coverage_from": var.get("coverage_from"),
        "variational_coverage_through": var.get("coverage_through"),
        "variational_coverage_covers_report": coverage_range_valid,
        "variational_unclassified_rows": int(var.get("unclassified_rows") or 0),
        "rh": rh.get("status", "missing"),
        "rh_pages_complete": rh_pages_complete,
        "rh_unclassified_rows": int(rh.get("unclassified_rows") or 0),
        "rh_unsupported_fee_rows": int(rh.get("unsupported_fee_rows") or 0),
    }
    complete = (
        var.get("status") == "available"
        and var_fresh
        and not var_truncated
        and int(var.get("unclassified_rows") or 0) == 0
        and coverage_range_valid
        and rh_complete
        and int(rh.get("unclassified_rows") or 0) == 0
        and int(rh.get("unsupported_fee_rows") or 0) == 0
    )
    reasons: list[str] = []
    if var.get("status") != "available":
        reasons.append("variational_export_missing_or_incomplete")
    if not var_fresh:
        reasons.append("variational_export_stale_or_timestamp_missing")
    if var_truncated:
        reasons.append("variational_export_may_be_truncated")
    if not var.get("coverage_manifest_valid"):
        reasons.append("variational_coverage_not_confirmed_or_export_changed")
    elif not coverage_range_valid:
        reasons.append("variational_coverage_does_not_cover_report_period")
    if int(var.get("unclassified_rows") or 0):
        reasons.append("variational_rows_unclassified")
    if not rh_complete:
        reasons.append("rh_history_sync_or_pagination_incomplete")
    if int(rh.get("unclassified_rows") or 0):
        reasons.append("rh_rows_unclassified")
    if int(rh.get("unsupported_fee_rows") or 0):
        reasons.append("rh_nonzero_fees_need_currency_normalization")
    details["reasons"] = reasons
    return complete, details


def weighted_capital(ledger: dict[str, Any], start: datetime, end: datetime) -> tuple[Decimal, Decimal]:
    initial = decimal_value(ledger.get("starting_capital_usd"))
    if initial is None or initial <= 0:
        raise ValueError("starting capital is missing or invalid")
    base = parse_timestamp(ledger.get("statistics_start"))
    if base is None:
        raise ValueError("statistics start is invalid")
    prior_flows = Decimal(0)
    interval_flows: list[tuple[datetime, Decimal]] = []
    for event in ledger.get("events", []):
        if event.get("kind") != "cashflow":
            continue
        at = parse_timestamp(event.get("timestamp"))
        amount = decimal_value(event.get("amount_usd"))
        if at is None or amount is None:
            continue
        if base <= at < start:
            prior_flows += amount
        elif start <= at < end:
            interval_flows.append((at, amount))
    capital_at_start = initial + prior_flows
    duration = Decimal(str((end - start).total_seconds()))
    if duration <= 0:
        raise ValueError("period duration must be positive")
    dollar_seconds = capital_at_start * duration
    for at, amount in interval_flows:
        remaining = Decimal(str((end - at).total_seconds()))
        dollar_seconds += amount * remaining
    average_capital = dollar_seconds / duration
    latest_capital = capital_at_start + sum((x[1] for x in interval_flows), Decimal(0))
    return average_capital, latest_capital
