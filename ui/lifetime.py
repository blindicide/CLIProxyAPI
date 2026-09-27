"""All-time totals: live history plus archived records (archiver.py moves old records out).

Archives are immutable, so their totals are computed once per set of archive files, streaming
(memory: one line plus a set of ids), and cached. Costs are computed at read time with the
current price table, like everything else. Records present both in an archive and in the live
file (only possible after a manual restore) and duplicates across archives count once.
"""
from __future__ import annotations

import gzip
import json
import threading
from pathlib import Path
from typing import Any

from ingest import iso_utc, parse_ts
from pricing import record_cost
from tools import datastore

FIELDS = ("requests", "success", "failed", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens",
          "reasoning_tokens", "priced_requests", "unpriced_requests")


def _empty() -> dict[str, Any]:
    return {**{f: 0 for f in FIELDS}, "estimated_cost_usd": 0.0, "first_timestamp": None, "last_timestamp": None}


def archive_totals(ui: Path, live_ids: set) -> dict[str, Any]:
    totals = _empty()
    seen: set = set()
    first = last = None
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
                cost = record_cost(record)
                usage = cost["usage"]
                totals["requests"] += 1
                totals["failed" if record.get("failed") else "success"] += 1
                totals["input_tokens"] += usage["input_total"]
                totals["output_tokens"] += usage["output"]
                totals["cache_read_tokens"] += usage["cache_read"]
                totals["cache_write_tokens"] += usage["cache_write"]
                totals["reasoning_tokens"] += usage["reasoning"]
                if cost["cost_usd"] is None:
                    totals["unpriced_requests"] += 1
                else:
                    totals["priced_requests"] += 1
                    totals["estimated_cost_usd"] += cost["cost_usd"]
                ts = parse_ts(record.get("timestamp"))
                if ts:
                    first = ts if first is None or ts < first else first
                    last = ts if last is None or ts > last else last
    totals["estimated_cost_usd"] = round(totals["estimated_cost_usd"], 8)
    totals["first_timestamp"] = iso_utc(first) if first else None
    totals["last_timestamp"] = iso_utc(last) if last else None
    totals["archives"] = len(datastore.archive_files(ui))
    return totals


class LifetimeCache:
    """Archive totals keyed by the archive files' (name, size, mtime) signature."""

    def __init__(self, ui: Path) -> None:
        self.ui = ui
        self._key: tuple | None = None
        self._value: dict[str, Any] | None = None
        self._lock = threading.Lock()

    def _signature(self) -> tuple:
        return tuple((p.name, p.stat().st_size, p.stat().st_mtime_ns) for p in datastore.archive_files(self.ui))

    def archived(self, live_ids: set) -> dict[str, Any]:
        with self._lock:
            key = self._signature()
            if key != self._key or self._value is None:
                self._value = archive_totals(self.ui, live_ids)
                self._key = key
            return self._value


def combine(live_summary: dict[str, Any], archived: dict[str, Any]) -> dict[str, Any]:
    out = {f: live_summary[f] + archived[f] for f in FIELDS}
    live_cost = live_summary["estimated_cost_usd"]
    out["estimated_cost_usd"] = round((live_cost or 0.0) + archived["estimated_cost_usd"], 8) if out["priced_requests"] else (0.0 if out["requests"] == 0 else None)
    out["live_requests"] = live_summary["requests"]
    out["archived_requests"] = archived["requests"]
    out["archived"] = archived
    out["basis"] = "live requests.jsonl + data/archive (records older than the archive threshold)"
    return out
