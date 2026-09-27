"""Poller lifecycle and management-API failure modes."""
from __future__ import annotations

import asyncio

import httpx
import pytest


async def test_api_keys_failure_does_not_block_draining(make_app, mgmt, sample):
    """A broken /api-keys must not stop records from being drained before cproxy prunes them."""
    original = mgmt.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-keys"):
            return httpx.Response(500, json={"error": "boom"})
        return original(request)

    mgmt.handler = handler
    mgmt.queue_responses = [[sample[0]], []]
    app = make_app()
    result = await app.state.drain_once(app)
    assert result["stored"] == 1
    assert app.state.store.records[0]["api_key_name"] == "unknown-key"
    assert app.state.store.records[0]["api_key_masked"].startswith("sha256:")


async def test_key_names_error_is_reported_and_retried_on_ttl(make_app, mgmt, sample, api_client):
    original = mgmt.handler
    state = {"fail": True, "api_keys_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-keys"):
            state["api_keys_calls"] += 1
            if state["fail"]:
                return httpx.Response(503, json={})
        return original(request)

    mgmt.handler = handler
    app = make_app()
    await app.state.drain_once(app)
    await app.state.drain_once(app)
    assert state["api_keys_calls"] == 1  # not hammered every 2 s
    async with api_client(app) as client:
        health = (await client.get("/api/health")).json()
    assert health["status"] == "ok"  # draining works; naming is degraded, not ingest
    assert "HTTP 503" in health["management"]["key_names_error"]

    state["fail"] = False
    app.state.key_names_at = 0.0  # TTL elapsed
    mgmt.queue_responses = [[sample[0]], []]
    await app.state.drain_once(app)
    assert app.state.key_names_error is None
    assert app.state.store.records[-1]["api_key_name"] == "key-2"


async def _run_until(app, predicate, timeout=5.0):
    async with app.router.lifespan_context(app):
        async def wait():
            while not predicate():
                await asyncio.sleep(0.005)
        await asyncio.wait_for(wait(), timeout)


async def test_poller_drains_in_background_and_stops_cleanly(make_app, mgmt, sample):
    mgmt.queue_responses = [[sample[0]], [sample[1]], []]
    app = make_app(poll_interval=0.01, start_poller=True)
    await _run_until(app, lambda: len(app.state.store.records) == 2)
    assert app.state.store.state["last_error"] is None
    # After shutdown nothing polls any more.
    calls = len(mgmt.calls)
    await asyncio.sleep(0.05)
    assert len(mgmt.calls) == calls


async def test_poller_survives_outage_and_recovers(make_app, mgmt, sample):
    original = mgmt.handler
    failures = {"left": 3}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/usage-queue") and failures["left"]:
            failures["left"] -= 1
            raise httpx.ConnectError("connection refused", request=request)
        return original(request)

    mgmt.handler = handler
    mgmt.queue_responses = [[sample[0]], []]
    app = make_app(poll_interval=0.01, start_poller=True)
    await _run_until(app, lambda: len(app.state.store.records) == 1)
    assert failures["left"] == 0
    assert app.state.store.state["last_error"] is None and app.state.store.state["last_ok_at"]


async def test_poller_survives_unexpected_exception(make_app, mgmt, sample, monkeypatch):
    import app as app_module

    real = app_module.drain_queue
    boom = {"left": 1}

    async def flaky(*args, **kwargs):
        if boom["left"]:
            boom["left"] -= 1
            raise RuntimeError("bug")
        return await real(*args, **kwargs)

    monkeypatch.setattr(app_module, "drain_queue", flaky)
    mgmt.queue_responses = [[sample[0]], []]
    application = make_app(poll_interval=0.01, start_poller=True)
    await _run_until(application, lambda: len(application.state.store.records) == 1)


@pytest.fixture
async def client(make_app, api_client):
    async with api_client(make_app()) as c:
        yield c


async def test_wrong_management_key_is_explained(make_app, api_client):
    from app import create_app

    app = create_app(management_url="http://mgmt.test/v0/management", management_key="wrong", client=make_app().state.client, start_poller=False, data_dir=make_app().state.store.requests_path.parent)
    async with api_client(app) as c:
        quota = await c.get("/api/quota")
        models = (await c.get("/api/models")).json()
    assert quota.status_code == 503 and "HTTP 401" in quota.json()["error"]
    assert models["available"] is False and "HTTP 401" in models["error"] and models["models"] == []


async def test_missing_management_key_is_explained(make_app, api_client, tmp_path, monkeypatch):
    from app import create_app

    monkeypatch.delenv("CPROXY_MANAGEMENT_KEY", raising=False)
    app = create_app(management_url="http://mgmt.test/v0/management", management_key=None, client=make_app().state.client, start_poller=False, data_dir=tmp_path)
    async with api_client(app) as c:
        quota = (await c.get("/api/quota")).json()
    assert quota["error"] == "CPROXY_MANAGEMENT_KEY is not configured"


async def test_quota_partial_failures_degrade_to_no_data(make_app, mgmt, api_client):
    original = mgmt.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(("/api-key-usage", "/quota/providers", "/api-keys")):
            return httpx.Response(500, json={"error": "x"})
        return original(request)

    mgmt.handler = handler
    async with api_client(make_app()) as c:
        body = (await c.get("/api/quota")).json()
    assert body["available"] is True and body["has_data"] is True
    assert body["upstream_key_usage"] == [] and body["quota_providers"] == [] and body["client_keys"] == []


async def test_quota_credential_states(make_app, mgmt, api_client):
    cred = mgmt.auth_files["files"][0]
    cred.update(disabled=True, cooldowns=[{"model": "claude-opus-5", "until": "2026-09-27T16:00:00+02:00"}], model_quotas={})
    mgmt.auth_files["files"].append({"id": "second.json", "label": "second", "provider": "claude", "failed": 0})
    async with api_client(make_app()) as c:
        body = (await c.get("/api/quota")).json()
    first, second = body["credentials"]
    assert first["disabled"] is True and first["cooldowns"][0]["model"] == "claude-opus-5"
    assert first["limits"] is None and second["limits"] is None
    assert body["has_data"] is False  # "no data", never invented numbers
