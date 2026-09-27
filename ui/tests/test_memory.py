"""In-memory record representation: compact, interned, and identical to what a restart loads."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime

from analytics import aggregate, recent
from ingest import IN_MEMORY_DROP, RecordStore, normalize_record

NOW = datetime(2026, 9, 27, 13, 0, tzinfo=UTC)


def _raws(sample, n):
    out = []
    for i in range(n):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"mem-{i}"
        out.append(raw)
    return out


def _store(tmp_path):
    return RecordStore(tmp_path / "requests.jsonl", tmp_path / "data" / "state.json")


def test_memory_matches_restart_and_disk_keeps_everything(tmp_path, sample):
    store = _store(tmp_path)
    normalised = [normalize_record(r, {}) for r in _raws(sample, 6)]
    assert store.append(normalised) == 6
    reloaded = _store(tmp_path)
    assert store.records == reloaded.records
    assert all(not (IN_MEMORY_DROP & record.keys()) for record in store.records)
    on_disk = [json.loads(line) for line in (tmp_path / "requests.jsonl").read_text().splitlines()]
    assert on_disk == normalised  # nothing is dropped from the durable copy
    assert all("ratelimit" in row for row in on_disk)


def test_repeated_values_are_shared_objects(tmp_path, sample):
    store = _store(tmp_path)
    store.append([normalize_record(r, {}) for r in _raws(sample, 4)])
    a, _, c, _ = _store(tmp_path).records
    assert a["model"] is c["model"] and a["user_agent"] is c["user_agent"]
    assert next(iter(a)) is next(iter(c))  # keys are interned too
    assert a["execution_id"] is not c["execution_id"]


def test_analytics_unchanged_by_compaction(tmp_path, sample):
    normalised = [normalize_record(r, {}) for r in _raws(sample, 8)]
    store = _store(tmp_path)
    store.append(copy.deepcopy(normalised))
    for window in ("24h", "7d", "30d", "all"):
        assert aggregate(store.records, window, NOW) == aggregate(normalised, window, NOW)
    assert recent(store.records, 5) == recent(normalised, 5)
