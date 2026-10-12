from __future__ import annotations

import gzip
import json
import os
from collections import deque
from pathlib import Path
from typing import Any

from tools.lib.runtime_files import rotated_jsonl_paths


class FixedJsonlFollower:
    """Follow an active JSONL file across atomic rotations and compression."""

    MAX_READ_BYTES = 8 * 1024 * 1024

    def __init__(self, path: Path) -> None:
        self.path = path
        self.position = 0
        self.buffer = b""
        self._handles_by_key: dict[str, Any] = {}
        self._handles_by_inode: dict[tuple[int, int], tuple[str, Any]] = {}
        self._buffers: dict[str, bytes] = {}
        self._finished: set[str] = set()

    def _sources(self) -> list[Path]:
        return [
            source
            for source in rotated_jsonl_paths(self.path)
            if source == self.path
            or ".segment-" in source.name
            or source.name.startswith(self.path.name + ".")
        ]

    def _key(self, source: Path) -> str:
        return "active" if source == self.path else source.name.removesuffix(".gz")

    @staticmethod
    def _inode(source: Path) -> tuple[int, int]:
        stat = source.stat()
        return stat.st_dev, stat.st_ino

    def _handle_for(self, source: Path, key: str) -> Any:
        inode = self._inode(source)
        known = self._handles_by_inode.get(inode)
        if known is not None:
            old_key, handle = known
            if old_key != key:
                self._buffers[key] = self._buffers.pop(old_key, b"")
                if self._handles_by_key.get(old_key) is handle:
                    self._handles_by_key.pop(old_key, None)
                self._handles_by_key[key] = handle
                self._handles_by_inode[inode] = (key, handle)
            return handle

        if key != "active":
            active_handle = self._handles_by_key.get("active")
            if active_handle is not None and not active_handle.closed:
                active_stat = os.fstat(active_handle.fileno())
                try:
                    current_inode = self._inode(self.path)
                except FileNotFoundError:
                    current_inode = None
                if current_inode != (active_stat.st_dev, active_stat.st_ino):
                    active_inode = (active_stat.st_dev, active_stat.st_ino)
                    self._buffers[key] = self._buffers.pop("active", b"")
                    self._handles_by_key.pop("active", None)
                    self._handles_by_key[key] = active_handle
                    self._handles_by_inode[active_inode] = (key, active_handle)
                    return active_handle

        handle = self._handles_by_key.get(key)
        if handle is not None and not handle.closed:
            old_stat = os.fstat(handle.fileno())
            if (old_stat.st_dev, old_stat.st_ino) == inode:
                return handle
            if source.suffix == ".gz":
                # Keep reading the original inode if it was compressed after open.
                return handle
            self._forget_handle(key, handle)

        opener = gzip.open if source.suffix == ".gz" else open
        handle = opener(source, "rb")
        self._handles_by_key[key] = handle
        self._handles_by_inode[inode] = (key, handle)
        return handle

    def _forget_handle(self, key: str, handle: Any) -> None:
        try:
            stat = os.fstat(handle.fileno())
            self._handles_by_inode.pop((stat.st_dev, stat.st_ino), None)
        except OSError:
            pass
        if self._handles_by_key.get(key) is handle:
            self._handles_by_key.pop(key, None)
        handle.close()

    def _read_source(self, source: Path, key: str) -> list[dict[str, Any]]:
        if key in self._finished:
            return []
        handle = self._handle_for(source, key)
        chunk = handle.read(self.MAX_READ_BYTES)
        if key == "active":
            self.position = handle.tell()
        if not chunk:
            if key != "active":
                self._finished.add(key)
                self._buffers.pop(key, None)
                self._forget_handle(key, handle)
            return []

        payload = self._buffers.get(key, b"") + chunk
        lines = payload.split(b"\n")
        self._buffers[key] = lines.pop()
        if key == "active":
            self.buffer = self._buffers[key]
        rows: list[dict[str, Any]] = []
        for line in lines:
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
        return rows

    def read_new(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for source in self._sources():
            key = self._key(source)
            try:
                rows.extend(self._read_source(source, key))
            except FileNotFoundError:
                continue
        return rows

    def seek_to_end(self) -> None:
        for handle in set(self._handles_by_key.values()):
            handle.close()
        self._handles_by_key.clear()
        self._handles_by_inode.clear()
        self._buffers.clear()
        self._finished.clear()
        active_exists = False
        for source in self._sources():
            key = self._key(source)
            if source != self.path:
                self._finished.add(key)
                continue
            active_exists = True
            try:
                handle = source.open("rb")
                handle.seek(0, os.SEEK_END)
                inode = self._inode(source)
                self._handles_by_key[key] = handle
                self._handles_by_inode[inode] = (key, handle)
                self.position = handle.tell()
            except FileNotFoundError:
                self.position = 0
        if not active_exists:
            self.position = 0
        self.buffer = b""

    def read_recent_rows(self, max_bytes: int) -> list[dict[str, Any]]:
        remaining = max(1, max_bytes)
        newest_chunks: list[bytes] = []
        for source in reversed(self._sources()):
            if remaining <= 0:
                break
            opener = gzip.open if source.suffix == ".gz" else open
            if source.suffix == ".gz":
                tail: deque[bytes] = deque()
                size = 0
                with opener(source, "rb") as handle:
                    for line in handle:
                        tail.append(line)
                        size += len(line)
                        while size > remaining and tail:
                            size -= len(tail.popleft())
                chunk = b"".join(tail)
            else:
                try:
                    file_size = source.stat().st_size
                    start = max(0, file_size - remaining)
                    with opener(source, "rb") as handle:
                        handle.seek(start)
                        if start:
                            handle.readline()
                        chunk = handle.read()
                except FileNotFoundError:
                    continue
            newest_chunks.append(chunk)
            remaining -= len(chunk)

        rows: list[dict[str, Any]] = []
        for line in b"".join(reversed(newest_chunks)).splitlines():
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
        return rows
