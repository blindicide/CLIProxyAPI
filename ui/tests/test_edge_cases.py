"""Edge cases found by branch coverage: secret masking, live quota shape, backlog, schema drift."""
from __future__ import annotations

import asyncio
import copy
import json

import httpx

from analytics import aggregate, recent
from ingest import normalize_record, parse_ts, record_id
from pricing import record_cost

UPSTREAM_KEY = "sk-ant-api03-upstream-test-key-000000000000000000000000000000"


async def test_upstream_key_usage_is_masked(make_app, mgmt, api_client):
    original = mgmt.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-key-usage"):
            return httpx.Response(200, json={"claude": {f"https://api.anthropic.com|{UPSTREAM_KEY}": {"success": 7, "failed": 1, "recent_requests": []}, "not-a-dict": 3}})
        return original(request)

    mgmt.handler = handler
    async with api_client(make_app()) as client:
        response = await client.get("/api/quota")
    assert UPSTREAM_KEY not in response.text
    assert UPSTREAM_KEY[:20] not in response.text
    (row,) = response.json()["upstream_key_usage"]
    assert row["provider"] == "claude" and row["base_url"] == "https://api.anthropic.com"
    assert row["key_masked"].startswith("sha256:") and row["key_masked"].endswith(UPSTREAM_KEY[-4:])
    assert (row["success"], row["failed"]) == (7, 1)


async def test_live_quota_shape_prefers_top_level_signals(make_app, mgmt, api_client):
    """The live auth-files carries a top-level `quota` (latest) besides per-model quotas."""
    cred = mgmt.auth_files["files"][0]
    latest = dict(cred["model_quotas"]["claude-opus-5"]["signals"])
    latest["Anthropic-Ratelimit-Unified-5h-Utilization"] = "0.13"
    latest["Anthropic-Ratelimit-Unified-7d-Utilization"] = "0.74"
    cred["quota"] = {"observed_at": "2026-09-27T15:05:07+02:00", "signals": latest}
    mgmt.auth_files["files"].insert(0, "garbage-entry")
    async with api_client(make_app()) as client:
        body = (await client.get("/api/quota")).json()
    (only,) = body["credentials"]  # the non-dict entry is skipped
    assert only["limits"]["windows"]["5h"]["used_pct"] == 13.0
    assert only["limits"]["windows"]["7d"]["used_pct"] == 74.0
    assert only["quota_observed_at"] == "2026-09-27T15:05:07+02:00"


async def test_backlog_beyond_one_cycle_drains_without_sleeping(make_app, mgmt, sample, monkeypatch):
    import ingest

    monkeypatch.setattr(ingest, "MAX_POPS_PER_CYCLE", 3)
    batches = []
    for i in range(7):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"backlog-{i}"
        batches.append([raw])
    mgmt.queue_responses = batches + [[]]
    # A very long poll interval: finishing quickly proves exhausted cycles do not sleep.
    app = make_app(poll_interval=3600, start_poller=True)

    async with app.router.lifespan_context(app):
        async def wait():
            while len(app.state.store.records) < 7:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(wait(), 5)
    assert len(app.state.store.records) == 7


def test_record_id_without_ids_is_stable_and_key_free(sample, raw_key):
    a = copy.deepcopy(sample[0])
    for field in ("execution_id", "request_id"):
        a.pop(field)
    b = copy.deepcopy(a)
    b["api_key"] = "sk-different-key"
    assert record_id(a) == record_id(b)  # the key never feeds the id
    assert record_id(a).startswith("sha256:") and raw_key not in record_id(a)
    a["latency_ms"] += 1
    assert record_id(a) != record_id(b)


def test_schema_drift_never_crashes_or_invents(sample, raw_key):
    hostile = copy.deepcopy(sample[0])
    hostile.update(
        timestamp="not-a-time",
        latency_ms="fast",
        ttft_ms=None,
        fail={"status_code": "teapot", "body": 42},
        tokens={"input_tokens": "lots", "output_tokens": -5},
        token_breakdown={"quality": "complete", "input": "bad", "output": None},
        response_headers={"Anthropic-Ratelimit-Unified-5h-Utilization": [], "Anthropic-Ratelimit-Unified-7d-Reset": ["soon"]},
        stream="yes",
        model=None,
        alias=None,
    )
    record = normalize_record(hostile, {})
    assert record["timestamp"] is None and record["status_code"] is None
    assert record["model"] == "unknown" and record["fail_body"] == "42"
    assert record["ratelimit"]["windows"]["5h"]["utilization"] is None
    assert record["ratelimit"]["windows"]["7d"]["reset_epoch"] is None
    cost = record_cost(record)
    assert cost["cost_usd"] is None and cost["usage"]["input_total"] == 0 and cost["usage"]["output"] == 0
    result = aggregate([record], "all")
    assert result["summary"]["requests"] == 1
    assert result["summary"]["avg_latency_ms"] is None and result["summary"]["estimated_cost_usd"] is None
    assert result["per_day"] == []  # no timestamp, no day bucket
    assert raw_key not in json.dumps(recent([record], 1))
    # A timestamp without zone is taken as UTC rather than rejected.
    assert parse_ts("2026-09-27T12:00:00").isoformat() == "2026-09-27T12:00:00+00:00"


async def test_invalid_json_from_management_is_a_clean_error(make_app, mgmt, api_client):
    original = mgmt.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/auth-files"):
            return httpx.Response(200, content=b"<html>proxy error</html>")
        return original(request)

    mgmt.handler = handler
    async with api_client(make_app()) as client:
        response = await client.get("/api/quota")
    assert response.status_code == 503
    assert response.json()["error"] == "management API auth-files returned invalid JSON"
