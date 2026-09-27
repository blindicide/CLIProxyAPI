"""Automatic archiving: records move to verified archives and are never lost."""
from __future__ import annotations

import copy
import gzip
import json
import random
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

import archiver
from ingest import iso_utc, normalize_record
from tools import datastore

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)  # 14:00 local, after ARCHIVE_HOUR


def _raws(sample, ages_days, prefix):
    out = []
    for i, age in enumerate(ages_days):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"{prefix}-{i}"
        raw["timestamp"] = iso_utc(NOW - timedelta(days=age, minutes=i))
        out.append(raw)
    return out


async def _ingest(app, mgmt, raws):
    mgmt.queue_responses = [raws, []]
    await app.state.drain_once(app)


def _archive_ids(tmp_path):
    ids = []
    for path in datastore.archive_files(tmp_path):
        with gzip.open(path, "rb") as handle:
            ids += list(datastore.scan(handle)[1])
    return ids


def _live_ids(tmp_path):
    return datastore.record_ids((tmp_path / "requests.jsonl").read_bytes())


def _assert_conserved(app, tmp_path, ingested_ids):
    archived, live = _archive_ids(tmp_path), _live_ids(tmp_path)
    assert len(archived) + len(live) == len(ingested_ids) == app.state.store.state["total_ingested"]
    assert set(archived).isdisjoint(live)
    assert set(archived) | set(live) == set(ingested_ids)
    audit = datastore.audit(tmp_path)
    assert audit["ok"] and audit["unique_records"] == len(ingested_ids) and audit["in_live_and_archive"] == 0


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
async def test_no_record_can_disappear(make_app, mgmt, sample, tmp_path, seed):
    """archive + live == everything ingested, across random histories and repeated archive runs."""
    rng = random.Random(seed)
    app = make_app()
    ingested = []
    now = NOW
    for cycle in range(3):
        ages = [rng.choice([rng.uniform(0, 170), rng.uniform(185, 900)]) for _ in range(rng.randint(5, 40))]
        raws = _raws(sample, ages, f"s{seed}c{cycle}")
        await _ingest(app, mgmt, raws)
        ingested += [f"{r['execution_id']}:{r['request_id']}" for r in raws]
        result = await app.state.archive_once(app, now)
        assert "error" not in result
        _assert_conserved(app, tmp_path, ingested)
        # In memory: only live records remain, and they match the file.
        assert sorted(r["id"] for r in app.state.store.records) == sorted(_live_ids(tmp_path))
        now += timedelta(days=rng.randint(0, 120))  # later runs archive more of the same history
    assert app.state.store.state["archived_total"] == len(_archive_ids(tmp_path)) > 0


async def test_archive_moves_only_old_records_and_reports(make_app, mgmt, sample, tmp_path, api_client):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [1, 10, 179, 181, 400], "a"))
    result = await app.state.archive_once(app, NOW)
    assert result["archived"] == 2 and result["kept"] == 3
    assert sorted(_archive_ids(tmp_path)) == sorted(f"a-{i}:{sample[i % 2]['request_id']}" for i in (3, 4))
    async with api_client(app) as client:
        health = (await client.get("/api/health")).json()
        analytics = (await client.get("/api/analytics?window=all")).json()
    archive = health["ingest"]["archive"]
    assert archive["archived_total"] == 2 and archive["archives"] == 1 and archive["last_error"] is None
    assert archive["after_days"] == 180 and health["ingest"]["records"] == 3
    # window=all spans the archive too, and says so.
    assert analytics["summary"]["requests"] == 5 and analytics["summary"]["archived_requests"] == 2
    assert analytics["coverage"]["live_records"] == 3 and analytics["coverage"]["archived_records"] == 2
    # A fresh verified backup of the full history was taken first.
    assert datastore.verify(max((tmp_path / "data" / "backups").glob("*.tar.gz")))["records"] == 5


async def test_nothing_old_is_a_cheap_noop(make_app, mgmt, sample, tmp_path):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [1, 2], "n"))
    before = (tmp_path / "requests.jsonl").read_bytes()
    assert await app.state.archive_once(app, NOW) == {"archived": 0}
    assert (tmp_path / "requests.jsonl").read_bytes() == before
    assert not (tmp_path / "data" / "backups").exists()  # no backup needed when nothing moves


async def test_records_appended_during_archiving_are_kept(make_app, mgmt, sample, tmp_path, monkeypatch):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [300, 5], "p"))
    late = [normalize_record(r, {}) for r in _raws(sample, [0, 0], "late")]
    real = archiver.prepare

    def prepare_then_ingest(*args, **kwargs):
        plan = real(*args, **kwargs)
        app.state.store.append(late)  # the poller keeps writing while the archive is prepared
        return plan

    monkeypatch.setattr(archiver, "prepare", prepare_then_ingest)
    result = await app.state.archive_once(app, NOW)
    assert result["archived"] == 1
    live = _live_ids(tmp_path)
    assert {r["id"] for r in late} <= set(live) and len(live) == 3
    _assert_conserved(app, tmp_path, [f"p-{i}:{sample[i % 2]['request_id']}" for i in (0, 1)] + [r["id"] for r in late])


def _corrupt_archive_on_fsync(monkeypatch):
    real = datastore._fsync

    def corrupting(path):
        if path.name.endswith(".jsonl.tmp"):
            data = path.read_bytes()
            path.write_bytes(data[: len(data) // 2])  # torn gzip
        return real(path)

    monkeypatch.setattr(datastore, "_fsync", corrupting)


@pytest.mark.parametrize("failure", ["archive_readback", "backup_unverified", "backup_missing_ids", "commit_mismatch"])
async def test_failed_verification_keeps_everything_and_warns(make_app, mgmt, sample, tmp_path, api_client, monkeypatch, failure):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [1, 300, 400], "f"))
    before = (tmp_path / "requests.jsonl").read_bytes()
    if failure == "archive_readback":
        _corrupt_archive_on_fsync(monkeypatch)
    elif failure == "backup_unverified":
        real_backup = datastore.backup
        monkeypatch.setattr(datastore, "backup", lambda ui, keep=30: {**real_backup(ui, keep), "verified": False})
    elif failure == "backup_missing_ids":
        real_scan = datastore._scan_backup
        monkeypatch.setattr(datastore, "_scan_backup", lambda path: (*real_scan(path)[:2], set()))
    else:
        real_scan = datastore.scan
        calls = {"n": 0}

        def scan(handle, limit=None):
            sha, ids, lines = real_scan(handle, limit)
            if getattr(handle, "name", "").endswith(archiver.REWRITE):
                ids = ids | {"phantom"}
            return sha, ids, lines

        monkeypatch.setattr(datastore, "scan", scan)
    result = await app.state.archive_once(app, NOW)
    assert result["archived"] == 0 and result["error"]
    assert (tmp_path / "requests.jsonl").read_bytes() == before  # never truncated
    assert len(app.state.store.records) == 3
    assert not (tmp_path / "data" / archiver.REWRITE).exists() and not (tmp_path / "data" / archiver.REWRITE_OK).exists()
    assert not list((tmp_path / "data" / "archive").glob("*.tmp")) if (tmp_path / "data" / "archive").exists() else True
    if failure != "commit_mismatch":
        assert datastore.archive_files(tmp_path) == []  # no archive claimed without verification
    async with api_client(app) as client:
        health = (await client.get("/api/health")).json()
    assert health["ingest"]["archive"]["last_error"]
    audit = datastore.audit(tmp_path)
    assert audit["ok"] and audit["unique_records"] == 3


async def test_skips_while_writes_are_pending(make_app, mgmt, sample, tmp_path):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [300], "w"))
    app.state.store.pending.append({"id": "stuck"})
    result = await app.state.archive_once(app, NOW)
    assert result == {"archived": 0, "skipped": "pending writes"}
    assert len(_live_ids(tmp_path)) == 1


async def test_interrupted_rewrite_is_completed_on_start(make_app, mgmt, sample, tmp_path, monkeypatch):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [1, 2, 300], "c"))
    real_copy = archiver._copy_in_place

    def crash_midway(source, live):
        with open(live, "r+b") as dst:  # half the new content lands, then the process dies
            dst.write(source.read_bytes()[:40])
        raise KeyboardInterrupt("simulated crash")

    monkeypatch.setattr(archiver, "_copy_in_place", crash_midway)
    with pytest.raises(KeyboardInterrupt):
        await app.state.archive_once(app, NOW)
    assert (tmp_path / "data" / archiver.REWRITE_OK).exists()
    monkeypatch.setattr(archiver, "_copy_in_place", real_copy)
    restarted = make_app()
    async with restarted.router.lifespan_context(restarted):  # service start re-applies the journal
        pass
    assert not (tmp_path / "data" / archiver.REWRITE_OK).exists()
    assert len(restarted.state.store.records) == 2 and restarted.state.store.corrupt_lines == 0
    audit = datastore.audit(tmp_path)
    assert audit["ok"] and audit["live_records"] == 2 and audit["archived_records"] == 1


def test_unverified_journal_is_discarded_and_live_untouched(tmp_path, sample):
    (tmp_path / "data").mkdir()
    live = tmp_path / "requests.jsonl"
    live.write_text(json.dumps(normalize_record(sample[0], {})) + "\n")
    before = live.read_bytes()
    (tmp_path / "data" / archiver.REWRITE).write_text("partial")
    assert archiver.recover_interrupted_rewrite(tmp_path) == "discarded"
    assert live.read_bytes() == before and not (tmp_path / "data" / archiver.REWRITE).exists()
    assert archiver.recover_interrupted_rewrite(tmp_path) is None


def test_archive_schedule(make_app):
    app = make_app()
    due = app.state.archive_due
    assert due(datetime(2026, 9, 27, 12, 0, tzinfo=UTC))  # 14:00 local, never attempted
    assert not due(datetime(2026, 9, 27, 1, 0, tzinfo=UTC))  # 03:00 local, before the backup timer
    app.state.store.state["archive_last_attempt_at"] = "2026-09-27T02:30:00Z"
    assert not due(datetime(2026, 9, 27, 12, 0, tzinfo=UTC))  # 9.5 h later
    assert due(datetime(2026, 9, 28, 2, 31, tzinfo=UTC))  # 24 h later, 04:31 local


def test_backup_waits_for_an_archive_rewrite(tmp_path, sample):
    (tmp_path / "data").mkdir()
    (tmp_path / "requests.jsonl").write_text(json.dumps(normalize_record(sample[0], {})) + "\n")
    finished = threading.Event()
    with datastore.history_lock(tmp_path, exclusive=True):
        worker = threading.Thread(target=lambda: (datastore.backup(tmp_path), finished.set()))
        worker.start()
        assert not finished.wait(0.3)  # blocked while the rewrite holds the lock
    worker.join(10)
    assert finished.is_set()


async def test_lock_hold_time_is_measured_and_warned(make_app, mgmt, sample, tmp_path, api_client):
    ticks = [100.0, 101.0, 200.0, 230.0]  # prepare 1 s; commit holds the lock 30 s

    def clock():
        return ticks.pop(0) if ticks else 230.0

    app = make_app(clock=clock)
    await _ingest(app, mgmt, _raws(sample, [1, 300], "t"))
    result = await app.state.archive_once(app, NOW)
    assert result["prepare_s"] == 1.0 and result["lock_held_s"] == 30.0
    async with api_client(app) as client:
        archive = (await client.get("/api/health")).json()["ingest"]["archive"]
    assert archive["last_result"]["lock_held_s"] == 30.0
    assert "held the write lock 30.0 s (> 20 s" in archive["lock_held_warning"]


async def test_fast_archive_has_no_lock_warning(make_app, mgmt, sample, api_client):
    app = make_app()
    await _ingest(app, mgmt, _raws(sample, [1, 300], "q"))
    await app.state.archive_once(app, NOW)
    async with api_client(app) as client:
        archive = (await client.get("/api/health")).json()["ingest"]["archive"]
    assert archive["lock_held_warning"] is None and archive["last_result"]["lock_held_s"] < 5
