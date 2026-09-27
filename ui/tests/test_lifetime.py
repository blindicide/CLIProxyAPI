"""window=all spans live history and archives; other windows are live only. Verified against
an in-memory aggregation of everything and the independent oracle."""
from __future__ import annotations

import copy
import csv
import gzip
import importlib
import importlib.util
import io
import json
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

import archiver
import lifetime
from analytics import aggregate
from ingest import iso_utc, normalize_record, utc_now
from tools import datastore

_SPEC = importlib.util.spec_from_file_location("verify_totals", Path(__file__).resolve().parents[1] / "tools" / "verify_totals.py")
verify = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(verify)

AGES = [1, 3, 200, 250, 400, 400.5]


def _raws(sample, ages, prefix):
    out = []
    for i, age in enumerate(ages):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"{prefix}-{i}"
        raw["timestamp"] = iso_utc(utc_now() - timedelta(days=age, minutes=i))
        raw["latency_ms"] = 100 * (i + 1)
        raw["failed"] = i == 4
        out.append(raw)
    return out


@pytest.fixture
async def archived(make_app, mgmt, sample):
    raws = _raws(sample, AGES, "life")
    app = make_app()
    mgmt.queue_responses = [raws, []]
    await app.state.drain_once(app)
    assert (await app.state.archive_once(app))["archived"] == 4
    return app, [normalize_record(r, app.state.key_names) for r in raws]


def _strip(obj):
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if k not in ("archived_requests", "generated_at", "generated_at_local", "window_minutes", "requests_per_min", "coverage", "version", "management", "ingest", "pricing")}
    if isinstance(obj, list):
        return [_strip(v) for v in obj]
    return obj


async def test_window_all_equals_aggregating_everything(archived, api_client):
    app, everything = archived
    async with api_client(app) as client:
        body = (await client.get("/api/analytics?window=all")).json()
    expected = aggregate(everything, "all", utc_now())
    assert _strip(body) == _strip(expected)  # same numbers as if nothing had been archived
    assert body["summary"]["requests"] == 6 and body["summary"]["archived_requests"] == 4
    assert body["coverage"] == {"includes_archives": True, "live_records": 2, "archive_after_days": 180, "archived_records": 4,
                                "archives": 1, "archived_from": body["coverage"]["archived_from"], "archived_until": body["coverage"]["archived_until"]}
    archive_days = [row for row in body["per_day"] if row["archived_requests"]]
    assert sum(row["archived_requests"] for row in archive_days) == 4
    assert all(row["archived_requests"] == row["requests"] for row in archive_days)
    assert sum(row["requests"] for row in body["series"]) == 6


async def test_short_windows_stay_live_only(archived, api_client):
    app, _ = archived
    async with api_client(app) as client:
        week = (await client.get("/api/analytics?window=7d")).json()
    assert week["summary"]["requests"] == 2 and week["summary"]["archived_requests"] == 0
    assert week["coverage"]["includes_archives"] is False and week["coverage"]["archived_records"] == 0


async def test_oracle_matches_every_window_across_archives(archived, api_client, tmp_path):
    app, _ = archived
    records = verify.load_lifetime(tmp_path / "requests.jsonl")
    assert len(records) == 6
    async with api_client(app) as client:
        for window in ("24h", "7d", "30d", "all"):
            body = (await client.get(f"/api/analytics?window={window}")).json()
            _, problems = verify.compare(body, records)
            assert problems == [], (window, problems)


async def test_export_all_includes_archived_rows_oldest_first(archived, api_client):
    app, everything = archived
    async with api_client(app) as client:
        rows = list(csv.DictReader(io.StringIO((await client.get("/api/export.csv?window=all")).text)))
        week = list(csv.DictReader(io.StringIO((await client.get("/api/export.csv?window=7d")).text)))
    assert sorted(r["id"] for r in rows) == sorted(r["id"] for r in everything)
    # Archived rows first, in archive (ingestion) order; then live rows oldest first.
    archived_ids = [r["id"] for r in everything if r["execution_id"] in {f"life-{i}" for i in (2, 3, 4, 5)}]
    assert [r["id"] for r in rows[:4]] == archived_ids
    assert [r["timestamp_utc"] for r in rows[4:]] == sorted(r["timestamp_utc"] for r in rows[4:])
    assert len(week) == 2


async def test_restored_overlap_counts_once(archived, api_client, tmp_path):
    with gzip.open(datastore.archive_files(tmp_path)[0], "rb") as handle:
        first = handle.readline()
    with open(tmp_path / "requests.jsonl", "ab") as handle:  # a manual restore, service stopped
        handle.write(first)
    from app import create_app

    restarted = create_app(management_key="k", client=httpx.AsyncClient(), data_dir=tmp_path, start_poller=False)
    async with api_client(restarted) as client:
        body = (await client.get("/api/analytics?window=all")).json()
    assert body["summary"]["requests"] == 6
    assert body["coverage"]["live_records"] == 3 and body["coverage"]["archived_records"] == 3


async def test_archive_aggregation_is_cached_until_archives_change(archived, monkeypatch, mgmt, sample):
    app, _ = archived
    calls = {"n": 0}
    real = lifetime.build

    def counting(*args):
        calls["n"] += 1
        return real(*args)

    monkeypatch.setattr(lifetime, "build", counting)
    cache = app.state.archive_cache
    cache.get(set())
    cache.get(set())
    assert calls["n"] == 1
    mgmt.queue_responses = [_raws(sample, [300], "later"), []]
    await app.state.drain_once(app)
    await app.state.archive_once(app, utc_now() + timedelta(days=1))
    assert cache.get(set())[1]["archived_records"] == 5 and calls["n"] == 2


def test_archive_threshold_has_a_floor(monkeypatch):
    monkeypatch.setenv("CPROXY_UI_ARCHIVE_AFTER_DAYS", "7")
    try:
        assert importlib.reload(archiver).ARCHIVE_AFTER_DAYS == 31
    finally:
        monkeypatch.delenv("CPROXY_UI_ARCHIVE_AFTER_DAYS")
        importlib.reload(archiver)
    assert archiver.ARCHIVE_AFTER_DAYS == 180
