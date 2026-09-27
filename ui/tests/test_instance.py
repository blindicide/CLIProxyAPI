"""One instance per data dir, startup repair under the lock, counters reconciled with disk."""
from __future__ import annotations

import copy
import json

import pytest

from ingest import normalize_record


async def test_second_instance_on_the_same_data_dir_refuses(make_app):
    first, second = make_app(), make_app()
    async with first.router.lifespan_context(first):
        with pytest.raises(RuntimeError, match="already running"):
            async with second.router.lifespan_context(second):
                pass
    # Released on shutdown: a later start works.
    async with second.router.lifespan_context(second):
        assert second.state.instance_lock is not None
    assert second.state.instance_lock is None


async def test_counter_lag_after_a_crash_is_reconciled(make_app, tmp_path, sample):
    (tmp_path / "data").mkdir()
    records = [normalize_record(copy.deepcopy(sample[i % 2]) | {"execution_id": f"lag-{i}"}, {}) for i in range(5)]
    (tmp_path / "requests.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
    # SIGKILL landed after the fsync but before the state save: the counter says 3.
    (tmp_path / "data" / "ingest_state.json").write_text(json.dumps({"total_ingested": 3, "archived_total": 7}))
    app = make_app()
    async with app.router.lifespan_context(app):
        pass
    state = json.loads((tmp_path / "data" / "ingest_state.json").read_text())
    assert state["total_ingested"] == 5 and state["archived_total"] == 0 and state["reconcile_missing"] == 0


async def test_records_missing_from_disk_are_reported_not_hidden(make_app, tmp_path, sample):
    (tmp_path / "data").mkdir()
    (tmp_path / "requests.jsonl").write_text(json.dumps(normalize_record(sample[0], {})) + "\n")
    (tmp_path / "data" / "ingest_state.json").write_text(json.dumps({"total_ingested": 4}))
    app = make_app()
    async with app.router.lifespan_context(app):
        pass
    state = json.loads((tmp_path / "data" / "ingest_state.json").read_text())
    assert state["total_ingested"] == 4 and state["reconcile_missing"] == 3  # never lowered silently
