from tools.lib.account_equity_ledger import (
    complete_beijing_equity_day,
    record_account_equity_sample,
)


def test_full_day_requires_boundary_samples_and_known_continuity() -> None:
    record = {
        "first_sample_at": "2026-10-04T16:05:00+00:00",
        "latest_sample_at": "2026-10-05T15:55:00+00:00",
        "sample_count": 288,
        "max_sample_gap_seconds": "300",
    }
    assert complete_beijing_equity_day(record, "2026-10-05")
    assert not complete_beijing_equity_day(
        {**record, "max_sample_gap_seconds": None, "coverage_complete": True},
        "2026-10-05",
    )
    assert not complete_beijing_equity_day(
        {**record, "max_sample_gap_seconds": "1800"}, "2026-10-05"
    )
    assert not complete_beijing_equity_day(
        {**record, "first_sample_at": "2026-10-05T06:55:00+00:00"},
        "2026-10-05",
    )


def test_sample_ledger_records_gaps_without_promoting_partial_day(tmp_path) -> None:
    path = tmp_path / "account_equity_daily_state.json"
    first = record_account_equity_sample(
        path,
        {
            "captured_at": "2026-10-04T16:05:00+00:00",
            "variational_equity_usd": "125",
            "lighter_equity_usd": "115",
            "combined_equity_usd": "240",
        },
    )
    assert first["daily_history"]["2026-10-05"]["max_sample_gap_seconds"] == "0"
    last = record_account_equity_sample(
        path,
        {
            "captured_at": "2026-10-05T15:55:00+00:00",
            "variational_equity_usd": "126",
            "lighter_equity_usd": "115",
            "combined_equity_usd": "241",
        },
    )
    record = last["daily_history"]["2026-10-05"]
    assert record["max_sample_gap_seconds"] == "85800.0"
    assert record["coverage_complete"] is False
