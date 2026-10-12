from datetime import datetime, timedelta, timezone
import gzip
import json
import os
from pathlib import Path

import pytest

from tools.lib.order_metrics_store import (
    append_order_metrics_line,
    compress_closed_order_metrics,
)
from tools.lib.basis_store import BasisSampleStore, read_basis_samples
from tools.lib.jsonl_follower import FixedJsonlFollower
from tools.lib.runtime_files import iter_jsonl_lines, rotated_jsonl_paths


def test_order_metrics_rotates_before_size_limit_and_keeps_order(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    first = '{"event":"first"}\n'
    second = '{"event":"second"}\n'
    append_order_metrics_line(path, first, rotate_bytes=100)

    rotated = append_order_metrics_line(
        path, second, rotate_bytes=path.stat().st_size + len(second) - 1
    )

    assert rotated is not None
    assert rotated.read_text(encoding="utf-8") == first
    assert path.read_text(encoding="utf-8") == second
    assert [line.strip() for line in iter_jsonl_lines(path)] == [
        first.strip(),
        second.strip(),
    ]


def test_order_metrics_daily_rotation_uses_utc_mtime(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    append_order_metrics_line(path, '{"event":"before"}\n')
    previous_day = datetime.now(timezone.utc) - timedelta(days=1)
    import os

    os.utime(path, (previous_day.timestamp(), previous_day.timestamp()))

    rotated = append_order_metrics_line(
        path,
        '{"event":"after"}\n',
        now=datetime.now(timezone.utc),
    )

    assert rotated is not None
    assert '"before"' in rotated.read_text(encoding="utf-8")
    assert '"after"' in path.read_text(encoding="utf-8")


def test_closed_segment_compresses_only_after_grace_and_reads_once(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)
    append_order_metrics_line(
        path, '{"event":"archived"}\n', now=now, rotate_bytes=1
    )
    rotated = append_order_metrics_line(
        path, '{"event":"active"}\n', now=now, rotate_bytes=1
    )
    assert rotated is not None

    assert compress_closed_order_metrics(
        path,
        now=now + timedelta(hours=23),
        minimum_free_bytes=0,
    ) == []
    archives = compress_closed_order_metrics(
        path,
        now=now + timedelta(hours=25),
        minimum_free_bytes=0,
    )

    assert len(archives) == 1
    assert not rotated.exists()
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        assert handle.read() == '{"event":"archived"}\n'
    assert [line.strip() for line in iter_jsonl_lines(path)] == [
        '{"event":"archived"}',
        '{"event":"active"}',
    ]
    assert rotated_jsonl_paths(path)[-1] == path


def test_rotated_reader_orders_legacy_and_segment_archives_once(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    legacy = tmp_path / "order_metrics.jsonl.full-20261001T000000Z.gz"
    segment = tmp_path / (
        "order_metrics.jsonl.segment-20261001T000001000000Z.jsonl"
    )
    with gzip.open(legacy, "wt", encoding="utf-8") as handle:
        handle.write('{"event":"legacy"}\n')
    segment.write_text('{"event":"segment"}\n', encoding="utf-8")
    with gzip.open(Path(str(segment) + ".gz"), "wt", encoding="utf-8") as handle:
        handle.write('{"event":"segment"}\n')
    path.write_text('{"event":"active"}\n', encoding="utf-8")

    sources = rotated_jsonl_paths(path)
    assert sources == [legacy, Path(str(segment) + ".gz"), path]
    assert [json.loads(line)["event"] for line in iter_jsonl_lines(path)] == [
        "legacy",
        "segment",
        "active",
    ]


@pytest.mark.skipif(os.name == "nt", reason="open-file rename semantics require POSIX")
def test_follower_reads_unread_tail_across_rotation(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    path.write_text('{"event":"old"}\n', encoding="utf-8")
    follower = FixedJsonlFollower(path)
    follower.seek_to_end()

    first_new = '{"event":"before_rotation"}\n'
    append_order_metrics_line(path, first_new, rotate_bytes=10_000)
    second_new = '{"event":"after_rotation"}\n'
    append_order_metrics_line(
        path,
        second_new,
        rotate_bytes=path.stat().st_size + len(second_new) - 1,
    )

    assert [row["event"] for row in follower.read_new()] == [
        "before_rotation",
        "after_rotation",
    ]


@pytest.mark.skipif(os.name == "nt", reason="open-file unlink semantics require POSIX")
def test_follower_does_not_replay_raw_segment_when_compressed(tmp_path) -> None:
    path = tmp_path / "order_metrics.jsonl"
    now = datetime(2026, 10, 1, tzinfo=timezone.utc)
    follower = FixedJsonlFollower(path)
    follower.seek_to_end()
    append_order_metrics_line(path, '{"event":"before"}\n', now=now)
    rotated = append_order_metrics_line(
        path,
        '{"event":"after"}\n',
        now=now,
        rotate_bytes=path.stat().st_size + len('{"event":"after"}\n') - 1,
    )
    assert rotated is not None
    assert [row["event"] for row in follower.read_new()] == ["before", "after"]

    compress_closed_order_metrics(
        path,
        now=now + timedelta(hours=25),
        minimum_free_bytes=0,
    )
    append_order_metrics_line(path, '{"event":"latest"}\n', now=now)

    assert [row["event"] for row in follower.read_new()] == ["latest"]


def test_late_basis_rows_do_not_overwrite_existing_compressed_day(tmp_path) -> None:
    store = BasisSampleStore(tmp_path, config_hash="config", commit="commit")
    store.append(
        {
            "event": "sample",
            "logged_at": "2026-10-01T10:00:00+00:00",
            "asset": "ETH",
        }
    )
    store.rotate_closed_days(current_day="2026-10-02")

    late_store = BasisSampleStore(tmp_path, config_hash="config", commit="commit")
    late_store.append(
        {
            "event": "late_sample",
            "logged_at": "2026-10-01T11:00:00+00:00",
            "asset": "ETH",
        }
    )
    late_store.rotate_closed_days(current_day="2026-10-02")

    assert (tmp_path / "ETH" / "2026-10-01.jsonl.gz").exists()
    assert (tmp_path / "ETH" / "2026-10-01.jsonl").exists()
    assert [row["event"] for row in read_basis_samples(tmp_path, limit=10)] == [
        "sample",
        "late_sample",
    ]
