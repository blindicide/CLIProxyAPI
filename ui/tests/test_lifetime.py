"""All-time totals across the live file and archives, verified by the independent oracle."""
from __future__ import annotations

import copy
import gzip
import importlib.util
from datetime import timedelta
from pathlib import Path

import pytest

import lifetime
from ingest import iso_utc, utc_now
from tools import datastore

_SPEC = importlib.util.spec_from_file_location("verify_totals", Path(__file__).resolve().parents[1] / "tools" / "verify_totals.py")
verify = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(verify)


def _raws(sample, ages, prefix):
    out = []
    for i, age in enumerate(ages):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"{prefix}-{i}"
        raw["timestamp"] = iso_utc(utc_now() - timedelta(days=age, minutes=i))
        out.append(raw)
    return out


@pytest.fixture
async def archived_app(make_app, mgmt, sample):
    app = make_app()
    mgmt.queue_responses = [_raws(sample, [1, 3, 200, 250, 400], "life"), []]
    await app.state.drain_once(app)
    assert (await app.state.archive_once(app))["archived"] == 3
    return app


async def test_lifetime_covers_live_and_archived(archived_app, api_client, tmp_path):
    async with api_client(archived_app) as client:
        body = (await client.get("/api/analytics?window=all")).json()
        other = (await client.get("/api/analytics?window=7d")).json()
    life = body["lifetime"]
    assert body["summary"]["requests"] == 2 and life["live_requests"] == 2
    assert life["archived_requests"] == 3 and life["requests"] == 5 == archived_app.state.store.state["total_ingested"]
    assert life["archived"]["archives"] == 1 and life["archived"]["first_timestamp"] < life["archived"]["last_timestamp"]
    assert "lifetime" not in other
    # Independent recomputation over live + archives agrees field by field.
    mine, problems = verify.compare_lifetime(body, verify.load_lifetime(tmp_path / "requests.jsonl"))
    assert problems == [] and mine["requests"] == 5


async def test_restored_overlap_counts_once(archived_app, api_client, tmp_path):
    archive = datastore.archive_files(tmp_path)[0]
    with gzip.open(archive, "rb") as handle:
        first = handle.readline()
    # Simulate a manual restore that put an archived record back into the live file.
    with open(tmp_path / "requests.jsonl", "ab") as handle:
        handle.write(first)
    import httpx

    from app import create_app  # a restore happens with the service stopped: fresh process

    restarted_app = create_app(management_key="k", client=httpx.AsyncClient(), data_dir=tmp_path, start_poller=False)
    async with api_client(restarted_app) as client:
        life = (await client.get("/api/analytics?window=all")).json()["lifetime"]
    assert life["live_requests"] == 3 and life["archived_requests"] == 2 and life["requests"] == 5


async def test_archive_totals_are_cached_until_archives_change(archived_app, monkeypatch, tmp_path, mgmt, sample):
    calls = {"n": 0}
    real = lifetime.archive_totals

    def counting(*args):
        calls["n"] += 1
        return real(*args)

    monkeypatch.setattr(lifetime, "archive_totals", counting)
    cache = archived_app.state.lifetime
    cache.archived(set())
    cache.archived(set())
    assert calls["n"] == 1
    mgmt.queue_responses = [_raws(sample, [300], "later"), []]
    await archived_app.state.drain_once(archived_app)
    await archived_app.state.archive_once(archived_app, utc_now() + timedelta(days=1))
    assert cache.archived(set())["requests"] == 4 and calls["n"] == 2


def test_no_archives_lifetime_equals_live(tmp_path):
    archived = lifetime.archive_totals(tmp_path, set())
    live = {f: 1 for f in lifetime.FIELDS} | {"estimated_cost_usd": 0.5}
    combined = lifetime.combine(live, archived)
    assert combined["requests"] == 1 and combined["archived_requests"] == 0 and combined["estimated_cost_usd"] == 0.5
    assert archived["archives"] == 0 and archived["first_timestamp"] is None
