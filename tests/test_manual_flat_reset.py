import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from main import VariationalToLighterRuntime


def test_state_only_reset_rejects_lighter_response_without_account_rows() -> None:
    assert (
        VariationalToLighterRuntime.extract_all_lighter_position_qtys(
            {"code": 200, "accounts": []}, asset="ETH"
        )
        is None
    )


def _reset_runtime(state_path: Path, *, var_qty: str = "0", lighter_qty: str = "0"):
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.live_inventory_state_file = state_path
    runtime.live_allowed_assets = {"ETH"}
    runtime.live_inventory_flat_reset_confirmation = "FLAT:ETH"
    runtime.ticker = None
    runtime.lighter_market_index = 0
    runtime.base_amount_multiplier = 0
    runtime.price_multiplier = 0
    runtime.lighter_min_base_amount = None
    runtime.lighter_min_quote_amount = None

    def get_market_config():
        assert runtime.ticker == "ETH"
        return 9, 1000, 100, None, None

    runtime.get_lighter_market_config = get_market_config

    async def fetch_variational_positions():
        return {
            "ok": True,
            "result": {
                "positions": (
                    [{"instrument": {"underlying": "ETH"}, "qty": var_qty}]
                    if var_qty != "0"
                    else []
                )
            },
        }

    async def fetch_variational_orders(**_kwargs):
        return {"ok": True, "orders": {"result": []}}

    async def fetch_lighter_account():
        positions = (
            [{"symbol": "ETH", "position": lighter_qty, "sign": 1}]
            if lighter_qty != "0"
            else []
        )
        return {"code": 200, "accounts": [{"positions": positions}]}

    async def fetch_lighter_active_orders():
        assert runtime.lighter_market_index == 9
        return []

    async def append_log(*_args, **_kwargs):
        return None

    runtime.fetch_variational_positions = fetch_variational_positions
    runtime.fetch_variational_orders = fetch_variational_orders
    runtime.fetch_lighter_account = fetch_lighter_account
    runtime.fetch_lighter_active_orders = fetch_lighter_active_orders
    runtime.append_live_inventory_log = append_log
    runtime.sync_live_inventory_memory_from_state = lambda: None
    return runtime


def test_lighter_active_order_request_includes_resolved_market_id(monkeypatch) -> None:
    captured = {}

    class FakeOrderApi:
        def __init__(self, api_client):
            assert api_client == "api-client"

        async def account_active_orders(self, **kwargs):
            captured.update(kwargs)
            return {"code": 200, "orders": []}

    class FakeClient:
        api_client = "api-client"

        def create_auth_token_with_expiry(self, *, api_key_index):
            assert api_key_index == 3
            return "read-token", None

    monkeypatch.setattr("lighter.OrderApi", FakeOrderApi)
    runtime = VariationalToLighterRuntime.__new__(VariationalToLighterRuntime)
    runtime.account_index = 12
    runtime.api_key_index = 3
    runtime.lighter_market_index = 9
    runtime.initialize_lighter_client = lambda: FakeClient()

    async def run() -> None:
        assert await runtime.fetch_lighter_active_orders() == []

    asyncio.run(run())
    assert captured["account_index"] == 12
    assert captured["market_id"] == 9
    assert captured["authorization"] == "read-token"


def test_state_only_reset_requires_verified_flat_and_creates_backup(tmp_path) -> None:
    async def run() -> None:
        state_path = tmp_path / "live_inventory_state.json"
        original = {
            "status": "open",
            "asset": "ETH",
            "run_id": "old-run",
            "open_lots": [{"asset": "ETH", "lot_id": 1, "qty": "0.01"}],
            "pending_actions": [],
        }
        state_path.write_text(json.dumps(original), encoding="utf-8")
        runtime = _reset_runtime(state_path)

        await runtime.reset_live_inventory_state_only_after_verified_flat(asset="ETH")

        result = json.loads(state_path.read_text(encoding="utf-8"))
        backups = list(tmp_path.glob("live_inventory_state.json.before_manual_flat_reset.*.bak"))
        assert result["status"] == "flat"
        assert result["open_lots"] == []
        assert result["pending_actions"] == []
        assert result["reason"] == "manual_flat_state_reset_exchange_verified"
        assert result["exchange_flat_verification"]["snapshot_count"] == 2
        assert result["reset_source_run_id"] == "old-run"
        assert len(backups) == 1
        assert json.loads(backups[0].read_text(encoding="utf-8")) == original

    asyncio.run(run())


def test_state_only_reset_refuses_nonzero_exchange_position_without_mutating_state(
    tmp_path,
) -> None:
    async def run() -> None:
        state_path = tmp_path / "live_inventory_state.json"
        original = {
            "status": "open",
            "asset": "ETH",
            "open_lots": [{"asset": "ETH", "lot_id": 1, "qty": "0.01"}],
            "pending_actions": [],
        }
        original_bytes = json.dumps(original).encode()
        state_path.write_bytes(original_bytes)
        runtime = _reset_runtime(state_path, lighter_qty="0.001")

        try:
            await runtime.reset_live_inventory_state_only_after_verified_flat(asset="ETH")
        except RuntimeError as exc:
            assert "Exchange position is not flat" in str(exc)
        else:
            raise AssertionError("non-zero position must prevent local-state reset")

        assert state_path.read_bytes() == original_bytes
        assert list(tmp_path.glob("*.bak")) == []

    asyncio.run(run())


def test_state_only_reset_refuses_active_order_without_mutating_state(tmp_path) -> None:
    async def run() -> None:
        state_path = tmp_path / "live_inventory_state.json"
        original = {
            "status": "open",
            "asset": "ETH",
            "open_lots": [{"asset": "ETH", "lot_id": 1, "qty": "0.01"}],
            "pending_actions": [],
        }
        original_bytes = json.dumps(original).encode()
        state_path.write_bytes(original_bytes)
        runtime = _reset_runtime(state_path)

        async def active_orders():
            return [{"order_index": 7}]

        runtime.fetch_lighter_active_orders = active_orders

        try:
            await runtime.reset_live_inventory_state_only_after_verified_flat(asset="ETH")
        except RuntimeError as exc:
            assert "Open orders remain" in str(exc)
        else:
            raise AssertionError("active order must prevent local-state reset")

        assert state_path.read_bytes() == original_bytes
        assert list(tmp_path.glob("*.bak")) == []

    asyncio.run(run())


def test_state_only_reset_checks_every_matching_variational_position(tmp_path) -> None:
    async def run() -> None:
        state_path = tmp_path / "live_inventory_state.json"
        original_bytes = json.dumps(
            {
                "status": "open",
                "asset": "ETH",
                "open_lots": [{"asset": "ETH", "lot_id": 1, "qty": "0.01"}],
                "pending_actions": [],
            }
        ).encode()
        state_path.write_bytes(original_bytes)
        runtime = _reset_runtime(state_path)

        async def positions():
            return {
                "ok": True,
                "result": {
                    "positions": [
                        {"instrument": {"underlying": "ETH"}, "qty": "0"},
                        {"instrument": {"underlying": "ETH"}, "qty": "0.01"},
                    ]
                },
            }

        runtime.fetch_variational_positions = positions
        try:
            await runtime.reset_live_inventory_state_only_after_verified_flat(asset="ETH")
        except RuntimeError as exc:
            assert "Exchange position is not flat" in str(exc)
        else:
            raise AssertionError("every matching position must be checked")

        assert state_path.read_bytes() == original_bytes
        assert list(tmp_path.glob("*.bak")) == []

    asyncio.run(run())
