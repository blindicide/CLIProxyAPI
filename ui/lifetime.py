"""Archived history for window=all: records that archiver.py moved out of requests.jsonl.

Archives are immutable, so they are aggregated once per set of archive files, streaming
(one line + an id set + the latency samples in memory), through the same Accumulator as live
records, and cached. Costs are computed at read time with the current price table. Records
present both in an archive and in the live file (only possible after a manual restore) and
duplicates across archives count once.
"""
from __future__ import annotations

import gzip
import json
import threading
from pathlib import Path
from typing import Any, Iterator

from analytics import Accumulator, Derived
from ingest import iso_utc
from tools import datastore


def iter_archived_records(ui: Path, live_ids: set) -> Iterator[dict[str, Any]]:
    """Each archived record once, oldest archive first, skipping ids present in the live file."""
    seen: set = set()
    for path in datastore.archive_files(ui):
        with gzip.open(path, "rb") as handle:
            for line in datastore.iter_lines(handle):
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                rid = record.get("id") if isinstance(record, dict) else None
                if not rid or rid in seen or rid in live_ids:
                    continue
                seen.add(rid)
                yield record


def build(ui: Path, live_ids: set) -> tuple[Accumulator, dict[str, Any]]:
    acc = Accumulator(hourly=False)
    first = last = None
    for record in iter_archived_records(ui, live_ids):
        d = Derived(record)
        acc.add(d, archived=True)
        if d.ts:
            first = d.ts if first is None or d.ts < first else first
            last = d.ts if last is None or d.ts > last else last
    info = {
        "archived_records": acc.total["requests"],
        "archives": len(datastore.archive_files(ui)),
        "archived_from": iso_utc(first) if first else None,
        "archived_until": iso_utc(last) if last else None,
    }
    return acc, info


class ArchiveCache:
    """Archive aggregation keyed by the archive files' (name, size, mtime) signature."""

    def __init__(self, ui: Path) -> None:
        self.ui = ui
        self._key: tuple | None = None
        self._value: tuple[Accumulator, dict[str, Any]] | None = None
        self._lock = threading.Lock()

    def _signature(self) -> tuple:
        return tuple((p.name, p.stat().st_size, p.stat().st_mtime_ns) for p in datastore.archive_files(self.ui))

    def get(self, live_ids: set) -> tuple[Accumulator, dict[str, Any]]:
        with self._lock:
            key = self._signature()
            if key != self._key or self._value is None:
                self._value = build(self.ui, live_ids)
                self._key = key
            return self._value
