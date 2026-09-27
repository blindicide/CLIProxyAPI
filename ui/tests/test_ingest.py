"""Drain loop, dedupe, restart-resume and key masking."""
from __future__ import annotations

import copy
import json

import pytest

from app import ManagementError
from ingest import RecordStore, drain_queue, mask_key


async def test_drain_loops_until_empty_and_dedupes(make_app, mgmt, sample, tmp_path):
    rec_a, rec_b = sample
    mgmt.queue_responses = [[rec_a], [rec_b], []]
    app = make_app()
    result = await app.state.drain_once(app)
    assert result["pops"] == 3 and result["stored"] == 2
    assert mgmt.calls.count("usage-queue") == 3
    lines = (tmp_path / "requests.jsonl").read_text().splitlines()
    assert len(lines) == 2
    assert {json.loads(line)["execution_id"] for line in lines} == {rec_a["execution_id"], rec_b["execution_id"]}
    assert all("ingested_at" in json.loads(line) for line in lines)

    # A re-drain of the same records (e.g. replay after a crash) must not double-count.
    mgmt.queue_responses = [[copy.deepcopy(rec_a)], [copy.deepcopy(rec_b)], []]
    result = await app.state.drain_once(app)
    assert result["stored"] == 0
    assert len((tmp_path / "requests.jsonl").read_text().splitlines()) == 2
    assert app.state.store.state["duplicates_skipped"] == 2

    # Restart: a fresh app over the same data dir resumes ids and state.
    mgmt.queue_responses = [[copy.deepcopy(rec_a), copy.deepcopy(rec_b)], []]
    restarted = make_app()
    assert len(restarted.state.store.records) == 2
    assert restarted.state.store.state["total_ingested"] == 2
    result = await restarted.state.drain_once(restarted)
    assert result["stored"] == 0
    assert len((tmp_path / "requests.jsonl").read_text().splitlines()) == 2


async def test_drain_is_bounded(tmp_path, sample):
    store = RecordStore(tmp_path / "requests.jsonl", tmp_path / "data" / "state.json")
    counter = {"n": 0}

    async def endless(path, **params):
        counter["n"] += 1
        rec = copy.deepcopy(sample[0])
        rec["execution_id"] = f"x-{counter['n']}"
        return [rec]

    result = await drain_queue(endless, store, {}, max_pops=7)
    assert result["pops"] == 7 and result["stored"] == 7 and result["exhausted"]


async def test_malformed_and_refresh_items(tmp_path, sample):
    store = RecordStore(tmp_path / "requests.jsonl", tmp_path / "data" / "state.json")
    responses = [[42, {"refresh": True}, json.dumps(sample[0])], []]

    async def fetch(path, **params):
        return responses.pop(0)

    result = await drain_queue(fetch, store, {})
    assert result["stored"] == 1
    assert store.state["malformed_skipped"] == 1


async def test_management_down_is_reported(make_app, mgmt, api_client):
    mgmt.down = True
    app = make_app()
    with pytest.raises(ManagementError):
        await app.state.drain_once(app)
    async with api_client(app) as client:
        health = (await client.get("/api/health")).json()
        analytics = (await client.get("/api/analytics?window=24h")).json()
        quota = await client.get("/api/quota")
    assert health["status"] == "degraded"
    assert health["management"]["reachable"] is False
    assert "unreachable" in health["management"]["last_error"]
    assert analytics["management"]["reachable"] is False
    assert quota.status_code == 503 and quota.json()["available"] is False


def test_mask_key_format(raw_key):
    masked = mask_key(raw_key)
    assert masked.startswith("sha256:") and masked.endswith(raw_key[-4:])
    assert raw_key not in masked
    assert mask_key(None) is None


async def test_raw_key_never_leaks(make_app, mgmt, sample, raw_key, tmp_path, api_client):
    leaky = copy.deepcopy(sample[0])
    leaky["execution_id"] = "leaky"
    leaky["failed"] = True
    leaky["fail"] = {"status_code": 401, "body": f"invalid api key {raw_key}"}
    mgmt.queue_responses = [[sample[0]], [sample[1]], [leaky], []]
    app = make_app()
    await app.state.drain_once(app)

    stored = (tmp_path / "requests.jsonl").read_text()
    assert raw_key not in stored
    assert "conftest" not in stored
    rows = [json.loads(line) for line in stored.splitlines()]
    assert {row["api_key_name"] for row in rows} == {"key-2"}
    assert all(row["api_key_masked"] == mask_key(raw_key) for row in rows)
    assert "api_key" not in rows[0]
    state = (tmp_path / "data" / "ingest_state.json").read_text()
    assert raw_key not in state

    async with api_client(app) as client:
        paths = ["/", "/api/health", "/api/pricing", "/api/quota", "/api/models", "/api/requests?limit=1000"]
        paths += [f"/api/analytics?window={w}" for w in ("24h", "7d", "30d", "all")]
        for path in paths:
            response = await client.get(path)
            assert response.status_code == 200, path
            assert raw_key not in response.text, path
            assert raw_key[:16] not in response.text, path
        quota = (await client.get("/api/quota")).json()
    # The configured client keys come back masked, never raw.
    assert [k["name"] for k in quota["client_keys"]] == ["key-1", "key-2"]
    assert quota["client_keys"][1]["key_masked"] == mask_key(raw_key)
