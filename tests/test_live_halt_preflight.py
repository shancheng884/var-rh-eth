import json

from tools import live


def test_v4_stop_loss_halt_requires_explicit_flat_reset(monkeypatch, tmp_path) -> None:
    path = tmp_path / "live_inventory_state.json"
    path.write_text(json.dumps({
        "status": "flat", "asset": "ETH", "open_lots": [],
        "pending_actions": [], "completed_cycles": 2,
        "v4_batch_halted_reason": "v4_batch_halted_after_stop_loss",
    }), encoding="utf-8")
    monkeypatch.setattr(live, "LIVE_STATE", path)
    config = live.LiveConfig(v4_live_mode=True, max_cycles=0)

    allowed, message = live.validate_state(config)
    assert not allowed
    assert "v4_batch_halted_after_stop_loss" in message
    assert "reset_local_state_only" in message

    reset_allowed, reset_message = live.validate_state(
        config, reset_state_only_after_manual_flat=True,
    )
    assert reset_allowed
    assert "exchange_position_and_order_verification_required" in reset_message

    acknowledged, _ = live.validate_state(
        config, reset_state_after_manual_flat=True,
    )
    assert acknowledged
