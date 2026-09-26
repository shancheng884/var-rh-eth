#!/usr/bin/env python3
"""Safely probe Variational XAU instrument support through the local extension.

The default mode is passive-only. With ``--allow-rfq`` the tool sends at most
one indicative quote request for each instrument type. It never sends orders.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COMMAND_URL = "ws://127.0.0.1:8868"
DEFAULT_OUTPUT = ROOT / "log" / "xau_market_probe.json"
INSTRUMENTS = (
    {
        "label": "XAU-PERP",
        "instrument_type": "perpetual_future",
        "funding_interval_s": 3600,
    },
    {
        "label": "XAU-SWAP",
        "instrument_type": "swap",
        "funding_interval_s": 86400,
    },
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def validate_qty(value: str) -> str:
    try:
        qty = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError("--qty must be a positive decimal") from exc
    if not qty.is_finite() or qty <= 0:
        raise argparse.ArgumentTypeError("--qty must be a positive decimal")
    return decimal_text(qty)


def _result_payload(message: dict[str, Any]) -> dict[str, Any]:
    result = message.get("result")
    return result if isinstance(result, dict) else {}


def summarize_command_result(
    message: dict[str, Any],
    *,
    instrument: dict[str, Any] | None,
    elapsed_ms: float,
) -> dict[str, Any]:
    result = _result_payload(message)
    ok = bool(message.get("ok")) and bool(result.get("ok", message.get("ok")))
    return {
        "ok": ok,
        "step": result.get("step"),
        "http_status": result.get("httpStatus"),
        "address_used": result.get("addressUsed"),
        "timed_out": bool(result.get("timedOut")),
        "error": message.get("error") or result.get("error") or None,
        "elapsed_ms": round(elapsed_ms, 3),
        "instrument_type": instrument.get("instrument_type") if instrument else None,
        "quote_id_present": bool(result.get("quoteId")),
        "bid": result.get("bid"),
        "ask": result.get("ask"),
        "mark_price": result.get("markPrice"),
        "index_price": result.get("indexPrice"),
        "quote_timestamp": result.get("quoteTimestamp"),
        "rate_limit_reset_ms": result.get("rateLimitResetMs"),
    }


def passive_price_evidence(path: Path, asset: str, *, max_bytes: int) -> dict[str, Any]:
    """Find lightweight evidence that the forwarded passive price stream saw asset."""

    evidence: dict[str, Any] = {
        "source_file": str(path),
        "available": path.exists(),
        "asset_mentions": 0,
        "latest_ingested_at": None,
    }
    if not path.exists():
        return evidence

    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            raw = handle.read().decode("utf-8", errors="replace")
    except OSError as exc:
        evidence["error"] = str(exc)
        return evidence

    asset_token = asset.upper()
    for line in raw.splitlines():
        if asset_token not in line.upper() or '"channel": "ws"' not in line:
            continue
        try:
            envelope = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = envelope.get("payload") if isinstance(envelope, dict) else None
        if not isinstance(payload, dict):
            continue
        if "/prices" not in str(payload.get("url") or ""):
            continue
        evidence["asset_mentions"] += 1
        evidence["latest_ingested_at"] = envelope.get("ingested_at")
    return evidence


class CommandClient:
    def __init__(self, url: str, timeout_seconds: float) -> None:
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.websocket: Any = None

    async def __aenter__(self) -> "CommandClient":
        try:
            import websockets
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "websockets is required to connect to the local command broker"
            ) from exc
        self.websocket = await websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=min(5.0, self.timeout_seconds),
        )
        return self

    async def __aexit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        if self.websocket is not None:
            await self.websocket.close()
            self.websocket = None

    async def request(self, payload: dict[str, Any]) -> tuple[dict[str, Any], float]:
        if self.websocket is None:
            raise RuntimeError("command client is not connected")
        request_id = str(payload.setdefault("requestId", uuid.uuid4().hex))
        started = time.monotonic()
        await self.websocket.send(json.dumps(payload, ensure_ascii=True))
        while True:
            raw = await asyncio.wait_for(
                self.websocket.recv(), timeout=self.timeout_seconds
            )
            message = json.loads(raw)
            if isinstance(message, dict) and str(message.get("requestId")) == request_id:
                return message, (time.monotonic() - started) * 1000.0


async def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    output: dict[str, Any] = {
        "schema_version": 1,
        "probe": "variational_xau_market_capability",
        "checked_at": utc_now(),
        "command_url": args.command_url,
        "asset": "XAU",
        "qty": args.qty,
        "rfq_allowed": bool(args.allow_rfq),
        "orders_attempted": False,
        "rfq_requests": 0,
        "passive_price_evidence": passive_price_evidence(
            args.passive_file,
            "XAU",
            max_bytes=args.passive_max_bytes,
        ),
        "ready": None,
        "instruments": [],
        "errors": [],
    }

    try:
        async with CommandClient(args.command_url, args.timeout_seconds) as client:
            ready_message, ready_elapsed_ms = await client.request(
                {
                    "type": "VAR_API_READY",
                    "requestTimeoutMs": int(args.timeout_seconds * 1000),
                }
            )
            ready_result = summarize_command_result(
                ready_message,
                instrument=None,
                elapsed_ms=ready_elapsed_ms,
            )
            ready_result["ready"] = bool(
                ready_result["ok"] and _result_payload(ready_message).get("ready", True)
            )
            output["ready"] = ready_result
            if not ready_result["ok"]:
                output["errors"].append("Variational extension is not ready")
                return output

            for instrument in INSTRUMENTS:
                record = {
                    "label": instrument["label"],
                    "instrument_type": instrument["instrument_type"],
                    "funding_interval_s": instrument["funding_interval_s"],
                    "rfq_attempted": bool(args.allow_rfq),
                }
                if not args.allow_rfq:
                    record.update({"status": "skipped", "reason": "rfq_disabled"})
                    output["instruments"].append(record)
                    continue

                message, elapsed_ms = await client.request(
                    {
                        "type": "VAR_API_QUOTE",
                        "market": "XAU",
                        "amount": args.qty,
                        "instrumentType": instrument["instrument_type"],
                        "settlementAsset": "USDC",
                        "fundingIntervalS": instrument["funding_interval_s"],
                        "confirm": False,
                        "requestTimeoutMs": int(args.timeout_seconds * 1000),
                        "operationTimeoutMs": int(args.timeout_seconds * 1000),
                    }
                )
                record.update(
                    summarize_command_result(
                        message,
                        instrument=instrument,
                        elapsed_ms=elapsed_ms,
                    )
                )
                record["status"] = "ok" if record["ok"] else "failed"
                output["rfq_requests"] += 1
                output["instruments"].append(record)
    except Exception as exc:
        output["errors"].append(f"{type(exc).__name__}: {exc}")
    return output


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only Variational XAU capability probe; never sends orders."
    )
    parser.add_argument("--command-url", default=DEFAULT_COMMAND_URL)
    parser.add_argument(
        "--qty",
        default="0.004",
        type=validate_qty,
        help="Base quantity for optional indicative RFQs; no order is sent.",
    )
    parser.add_argument(
        "--allow-rfq",
        action="store_true",
        help="Explicitly allow one indicative RFQ per XAU instrument type.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--passive-file",
        type=Path,
        default=ROOT / "log" / "ws_events.jsonl",
        help="Forwarded WebSocket event file used for passive-price evidence.",
    )
    parser.add_argument("--passive-max-bytes", type=int, default=4 * 1024 * 1024)
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.passive_max_bytes <= 0:
        parser.error("--passive-max-bytes must be positive")
    return args


def main() -> int:
    args = parse_args()
    payload = asyncio.run(run_probe(args))
    write_json_atomic(args.output, payload)
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    if payload["errors"]:
        return 1
    if args.allow_rfq and any(item.get("status") != "ok" for item in payload["instruments"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
