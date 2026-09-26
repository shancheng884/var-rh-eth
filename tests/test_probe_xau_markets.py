import argparse
import asyncio
import json
from pathlib import Path

import tools.probe_xau_markets as probe
from tools.probe_xau_markets import passive_price_evidence, summarize_command_result


def test_probe_summarizes_only_safe_quote_fields() -> None:
    result = summarize_command_result(
        {
            "ok": True,
            "result": {
                "ok": True,
                "quoteId": "quote-1",
                "bid": "4400.00",
                "ask": "4401.00",
                "raw": {"secret_like": "must not be persisted"},
            },
        },
        instrument={"instrument_type": "swap"},
        elapsed_ms=12.3456,
    )

    assert result["ok"] is True
    assert result["instrument_type"] == "swap"
    assert result["quote_id_present"] is True
    assert result["bid"] == "4400.00"
    assert "raw" not in result
    assert "secret_like" not in json.dumps(result)


def test_passive_evidence_reads_only_prices_events(tmp_path: Path) -> None:
    path = tmp_path / "ws_events.jsonl"
    path.write_text(
        json.dumps(
            {
                "ingested_at": "2026-09-05T00:00:00+00:00",
                "channel": "ws",
                "payload": {
                    "url": "wss://omni.variational.io/prices",
                    "payloadData": '{"instrument_price:XAU": {"price": "4400"}}',
                },
            }
        )
        + "\n"
        + json.dumps(
            {
                "ingested_at": "2026-09-05T00:00:01+00:00",
                "channel": "ws",
                "payload": {"url": "wss://omni.variational.io/events", "payloadData": "XAU"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    evidence = passive_price_evidence(path, "XAU", max_bytes=10000)

    assert evidence["asset_mentions"] == 1
    assert evidence["latest_ingested_at"] == "2026-09-05T00:00:00+00:00"


def _probe_args(tmp_path: Path, *, allow_rfq: bool) -> argparse.Namespace:
    return argparse.Namespace(
        command_url="ws://127.0.0.1:8868",
        qty="0.004",
        allow_rfq=allow_rfq,
        timeout_seconds=1.0,
        passive_file=tmp_path / "missing.jsonl",
        passive_max_bytes=1000,
    )


def test_probe_default_mode_sends_no_rfq(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, _url, _timeout_seconds):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, payload):
            calls.append(payload)
            return {"ok": True, "result": {"ok": True, "ready": True}}, 1.0

    monkeypatch.setattr(probe, "CommandClient", FakeClient)
    result = asyncio.run(probe.run_probe(_probe_args(tmp_path, allow_rfq=False)))

    assert result["rfq_requests"] == 0
    assert [call["type"] for call in calls] == ["VAR_API_READY"]
    assert all(call["type"] != "VAR_API_ORDER" for call in calls)


def test_probe_rfq_mode_is_hard_capped_at_one_per_instrument(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, _url, _timeout_seconds):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, payload):
            calls.append(payload)
            if payload["type"] == "VAR_API_READY":
                return {"ok": True, "result": {"ok": True, "ready": True}}, 1.0
            return {
                "ok": True,
                "result": {
                    "ok": True,
                    "quoteId": "probe-only",
                    "bid": "4400.00",
                    "ask": "4401.00",
                },
            }, 1.0

    monkeypatch.setattr(probe, "CommandClient", FakeClient)
    result = asyncio.run(probe.run_probe(_probe_args(tmp_path, allow_rfq=True)))

    assert result["rfq_requests"] == 2
    assert [call["type"] for call in calls] == [
        "VAR_API_READY",
        "VAR_API_QUOTE",
        "VAR_API_QUOTE",
    ]
    assert [call["instrumentType"] for call in calls[1:]] == [
        "perpetual_future",
        "swap",
    ]
