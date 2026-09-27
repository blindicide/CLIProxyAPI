"""Store durability: torn writes, corrupt lines and restarts must never lose good records."""
from __future__ import annotations

import copy
import json

from ingest import RecordStore, normalize_record


def _store(tmp_path):
    return RecordStore(tmp_path / "requests.jsonl", tmp_path / "data" / "ingest_state.json")


def test_torn_tail_does_not_swallow_next_record(tmp_path, sample):
    first = normalize_record(sample[0], {})
    path = tmp_path / "requests.jsonl"
    # Simulate a crash mid-write: one complete line, then a partial one without newline.
    path.write_text(json.dumps(first) + "\n" + '{"id":"torn","model":"claude-opu', encoding="utf-8")

    store = _store(tmp_path)
    assert [r["id"] for r in store.records] == [first["id"]]
    assert store.corrupt_lines == 1
    assert store.append([normalize_record(sample[1], {})]) == 1

    reloaded = _store(tmp_path)
    assert [r["id"] for r in reloaded.records] == [first["id"], normalize_record(sample[1], {})["id"]]
    assert reloaded.corrupt_lines == 1
    # The torn fragment is preserved on disk (never delete data), just isolated on its own line.
    assert '{"id":"torn","model":"claude-opu' in path.read_text().splitlines()


def test_blank_and_non_object_lines_are_ignored(tmp_path, sample):
    rec = normalize_record(sample[0], {})
    (tmp_path / "requests.jsonl").write_text("\n[1,2]\n" + json.dumps(rec) + "\n\n", encoding="utf-8")
    store = _store(tmp_path)
    assert [r["id"] for r in store.records] == [rec["id"]]
    assert store.corrupt_lines == 1


def test_duplicate_lines_on_disk_load_once(tmp_path, sample):
    rec = normalize_record(sample[0], {})
    (tmp_path / "requests.jsonl").write_text((json.dumps(rec) + "\n") * 3, encoding="utf-8")
    assert len(_store(tmp_path).records) == 1


def test_corrupt_state_file_falls_back_to_defaults(tmp_path, sample):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "ingest_state.json").write_text("{not json", encoding="utf-8")
    store = _store(tmp_path)
    assert store.state["total_ingested"] == 0
    store.append([normalize_record(copy.deepcopy(sample[0]), {})])
    store.save_state()
    saved = json.loads((tmp_path / "data" / "ingest_state.json").read_text())
    assert saved["total_ingested"] == 1 and saved["records_in_file"] == 1
    assert not (tmp_path / "data" / "ingest_state.tmp").exists()


async def test_health_reports_corrupt_lines(make_app, api_client, tmp_path, sample):
    (tmp_path / "requests.jsonl").write_text(json.dumps(normalize_record(sample[0], {})) + "\n{broken", encoding="utf-8")
    async with api_client(make_app()) as client:
        ingest = (await client.get("/api/health")).json()["ingest"]
    assert ingest["records"] == 1 and ingest["corrupt_lines"] == 1


class _FailingHandle:
    """Writes a partial line to the real file, then fails like a full disk."""

    def __init__(self, real):
        self.real = real

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.real.close()
        return False

    def write(self, text):
        self.real.write(text[: len(text) // 2])
        self.real.flush()
        raise OSError(28, "No space left on device")


def test_failed_write_keeps_records_pending_and_retries(tmp_path, sample, monkeypatch):
    store = _store(tmp_path)
    first, second = (normalize_record(r, {}) for r in sample)
    real_open = type(store.requests_path).open
    monkeypatch.setattr(type(store.requests_path), "open", lambda self, *a, **k: _FailingHandle(real_open(self, *a, **k)))

    assert store.append([first]) == 0
    assert [r["id"] for r in store.pending] == [first["id"]]
    assert store.records == [] and first["id"] not in store.ids
    assert store.state["last_write_error"] == "OSError: No space left on device"
    # A replay of the same record while it is pending is still a duplicate.
    assert store.append([copy.deepcopy(first)]) == 0 and len(store.pending) == 1

    monkeypatch.setattr(type(store.requests_path), "open", real_open)
    assert store.append([second]) == 2
    assert store.pending == [] and store.state["last_write_error"] is None
    assert store.state["total_ingested"] == 2

    reloaded = _store(tmp_path)
    assert [r["id"] for r in reloaded.records] == [first["id"], second["id"]]
    # The half-written line was rolled back, not left behind: no corrupt line, no duplicate.
    assert reloaded.corrupt_lines == 0
    assert len((tmp_path / "requests.jsonl").read_text().splitlines()) == 2


async def test_health_degraded_while_writes_pending(make_app, api_client, sample):
    app = make_app()
    app.state.store.pending.append(normalize_record(sample[0], {}))
    app.state.store.state["last_ok_at"] = app.state.store.state["last_drain_at"] = None
    async with api_client(app) as client:
        health = (await client.get("/api/health")).json()
    assert health["status"] == "degraded"
    assert health["ingest"]["pending_writes"] == 1


def test_state_save_failure_is_not_fatal(tmp_path, monkeypatch, caplog):
    store = _store(tmp_path)

    def boom(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("ingest.os.replace", boom)
    store.save_state()  # must not raise
    assert "could not save ingest state" in caplog.text


def test_partial_batch_write_is_rolled_back_not_duplicated(tmp_path, sample, monkeypatch):
    """ENOSPC after some complete lines: the retry must not write those lines twice."""
    store = _store(tmp_path)
    batch = [normalize_record(copy.deepcopy(sample[i % 2]) | {"execution_id": f"p-{i}"}, {}) for i in range(4)]
    real_open = type(store.requests_path).open

    class TwoLinesThenFull:
        def __init__(self, real):
            self.real, self.lines = real, 0

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.real.close()
            return False

        def write(self, text):
            if self.lines == 2:
                raise OSError(28, "No space left on device")
            self.lines += 1
            self.real.write(text)
            self.real.flush()

    monkeypatch.setattr(type(store.requests_path), "open", lambda self, *a, **k: TwoLinesThenFull(real_open(self, *a, **k)))
    assert store.append(batch) == 0 and len(store.pending) == 4
    with open(tmp_path / "requests.jsonl") as handle:  # builtin open: Path.open is patched here
        assert handle.read() == ""  # the two complete lines were rolled back
    monkeypatch.setattr(type(store.requests_path), "open", real_open)
    assert store.append([]) == 4
    ids = [json.loads(line)["id"] for line in (tmp_path / "requests.jsonl").read_text().splitlines()]
    assert sorted(ids) == sorted(r["id"] for r in batch) and len(ids) == len(set(ids))


def test_rollback_deferred_when_it_cannot_run_yet(tmp_path, sample, monkeypatch):
    store = _store(tmp_path)
    first = normalize_record(sample[0], {})
    assert store.append([first]) == 1
    size = (tmp_path / "requests.jsonl").stat().st_size
    with open(tmp_path / "requests.jsonl", "a") as handle:
        handle.write('{"id":"partial')  # what a failed write left behind
    store._truncate_to = size  # rollback could not run (e.g. read-only at the time)
    assert store.append([normalize_record(sample[1], {})]) == 1
    lines = (tmp_path / "requests.jsonl").read_text().splitlines()
    assert len(lines) == 2 and all(json.loads(line)["id"] for line in lines)
