"""Detection of usage records pruned unread by cproxy's in-memory queue retention."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from ingest import MAX_LOSS_WINDOWS, RecordStore, iso_utc, loss_window, retention_from_config, utc_now

T0 = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def test_loss_window_math():
    assert loss_window(None, T0, 60) is None
    assert loss_window(T0, T0 + timedelta(seconds=59), 60) is None
    assert loss_window(T0, T0 + timedelta(seconds=60), 60) is None  # nothing could have expired yet
    window = loss_window(T0, T0 + timedelta(seconds=300), 60)
    assert window == {
        "from": "2026-09-27T12:00:00Z",
        "to": "2026-09-27T12:04:00Z",
        "unobserved_s": 240.0,
        "retention_s": 60,
        "detected_at": "2026-09-27T12:05:00Z",
    }


def test_retention_from_config_reads_only_that_field():
    assert retention_from_config({"redis-usage-queue-retention-seconds": 60, "api-keys": ["secret"]}) == 60.0
    assert retention_from_config({"redis-usage-queue-retention-seconds": 99999}) == 3600.0
    assert retention_from_config({"redis-usage-queue-retention-seconds": 0}) == 60.0
    assert retention_from_config({}) is None
    assert retention_from_config(["not", "a", "dict"]) is None


async def test_gap_longer_than_retention_is_recorded(make_app, mgmt, sample, api_client, tmp_path, raw_key):
    mgmt.config = {"redis-usage-queue-retention-seconds": 90, "api-keys": [raw_key]}
    mgmt.queue_responses = [[sample[0]], []]
    app = make_app()
    await app.state.drain_once(app)
    assert app.state.queue_retention_s == 90.0
    assert app.state.store.state["loss_windows"] == []

    # The process was down for 5 minutes: persisted last_ok_at is old, and a new process starts.
    app.state.store.state["last_ok_at"] = iso_utc(utc_now() - timedelta(minutes=5))
    app.state.store.save_state()
    app = make_app()
    await app.state.drain_once(app)
    windows = app.state.store.state["loss_windows"]
    assert len(windows) == 1
    assert windows[0]["retention_s"] == 90.0
    assert windows[0]["unobserved_s"] == pytest.approx(300 - 90, abs=5)

    # An immediate follow-up drain is within retention: no new window.
    await app.state.drain_once(app)
    assert app.state.store.state["loss_windows_total"] == 1

    async with api_client(app) as client:
        health = await client.get("/api/health")
    ingest = health.json()["ingest"]
    assert ingest["loss_windows_total"] == 1 and ingest["queue_retention_s"] == 90.0
    assert ingest["queue_retention_source"] == "cproxy config"
    assert raw_key not in health.text  # nothing but the retention value comes from /config

    # Persisted: a restart still knows about the loss.
    saved = json.loads((tmp_path / "data" / "ingest_state.json").read_text())
    assert saved["loss_windows_total"] == 1 and len(saved["loss_windows"]) == 1


async def test_unreadable_config_falls_back_to_default(make_app, mgmt, sample):
    mgmt.queue_responses = [[sample[0]], []]
    app = make_app()
    result = await app.state.drain_once(app)  # /config answers 404 in the mock
    assert result["stored"] == 1
    assert app.state.queue_retention_s == 60.0 and app.state.queue_retention_source == "default"


def test_loss_windows_are_capped(tmp_path):
    store = RecordStore(tmp_path / "requests.jsonl", tmp_path / "data" / "state.json")
    for i in range(MAX_LOSS_WINDOWS + 7):
        start = T0 + timedelta(hours=i)
        store.note_loss_window(loss_window(start, start + timedelta(minutes=10), 60))
    assert len(store.state["loss_windows"]) == MAX_LOSS_WINDOWS
    assert store.state["loss_windows_total"] == MAX_LOSS_WINDOWS + 7
    assert store.state["loss_windows"][-1]["from"] == iso_utc(T0 + timedelta(hours=MAX_LOSS_WINDOWS + 6))


async def test_wall_clock_jump_within_a_process_is_not_an_outage(make_app, mgmt, sample, monkeypatch):
    import app as app_module
    import ingest

    mgmt.queue_responses = [[sample[0]], []]
    app = make_app()
    await app.state.drain_once(app)
    real = ingest.utc_now
    for jump in (timedelta(hours=2), -timedelta(hours=3), timedelta(days=400)):
        monkeypatch.setattr(app_module, "utc_now", lambda jump=jump: real() + jump)
        monkeypatch.setattr(ingest, "utc_now", lambda jump=jump: real() + jump)
        await app.state.drain_once(app)
    assert app.state.store.state["loss_windows_total"] == 0
