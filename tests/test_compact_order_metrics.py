from __future__ import annotations

import gzip
import json
from pathlib import Path

from tools.compact_order_metrics import compact


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_compact_archives_all_events_and_resets_active_file(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    rows = [
        {
            "event": "live_inventory_negative_direction_shadow_candidate",
            "sample_index": index,
        }
        for index in range(5)
    ]
    rows.extend(
        [
            {"event": "live_inventory_entered", "lot_id": 1},
            {"event": "live_inventory_actual_pnl", "actual_pnl_bps": "1.2"},
            {"event": "live_inventory_final_pnl", "final_pnl_bps": "1.1"},
        ]
    )
    write_rows(path, rows)

    archive, source_lines, retained_lines = compact(path)

    assert source_lines == 8
    assert retained_lines == source_lines
    assert path.read_text(encoding="utf-8") == ""
    with gzip.open(archive, "rt", encoding="utf-8") as handle:
        archived = [json.loads(line) for line in handle]
    assert archived == rows
    assert ".segment-" in archive.name
