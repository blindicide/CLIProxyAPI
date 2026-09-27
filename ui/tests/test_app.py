"""Smoke test: every endpoint answers 200 with the expected JSON keys."""
from __future__ import annotations

import pytest

from version import VERSION


@pytest.fixture
async def loaded(make_app, mgmt, sample, api_client):
    mgmt.queue_responses = [[sample[0]], [sample[1]], []]
    app = make_app()
    await app.state.drain_once(app)
    async with api_client(app) as client:
        yield client


async def test_dashboard_html(loaded):
    response = await loaded.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert '<meta name="cproxy-ui" content="dashboard">' in html
    assert f"v{VERSION}" in html and "__CPROXY_UI_VERSION__" not in html
    assert "https://" not in html.split("<script>")[0].split("</style>")[0]  # no CDN assets
    assert "<script src=" not in html and "<link rel=\"stylesheet\"" not in html


async def test_health(loaded):
    body = (await loaded.get("/api/health")).json()
    assert body["status"] == "ok"
    assert body["version"] == VERSION
    assert {"service", "build_date", "management", "ingest", "pricing", "uptime_s"} <= body.keys()
    assert body["ingest"]["records"] == 2
    assert body["ingest"]["last_ingest_at"]


@pytest.mark.parametrize("window", ["24h", "7d", "30d", "all"])
async def test_analytics(loaded, window):
    response = await loaded.get(f"/api/analytics?window={window}")
    assert response.status_code == 200
    body = response.json()
    assert {"window", "summary", "per_model", "per_key", "per_endpoint", "per_client_ip", "per_user_agent", "per_day", "series", "management", "ingest", "pricing", "version"} <= body.keys()
    assert {"requests", "success", "failed", "estimated_cost_usd", "p50_latency_ms", "p95_latency_ms", "avg_ttft_ms", "requests_per_min", "stream_requests", "non_stream_requests"} <= body["summary"].keys()
    if window == "all":
        assert body["summary"]["requests"] == 2
        assert body["summary"]["estimated_cost_usd"] == pytest.approx(0.004451)


async def test_analytics_bad_window(loaded):
    assert (await loaded.get("/api/analytics?window=1y")).status_code == 400


async def test_requests(loaded):
    body = (await loaded.get("/api/requests?limit=1")).json()
    assert body["total"] == 2 and len(body["requests"]) == 1
    row = body["requests"][0]
    assert row["model"] == "claude-opus-5"  # newest first
    assert {"timestamp", "timestamp_local", "cost_usd", "cost_quality", "usage", "api_key_masked", "api_key_name", "status_code", "latency_ms"} <= row.keys()


async def test_quota(loaded):
    body = (await loaded.get("/api/quota")).json()
    assert body["available"] is True and body["has_data"] is True
    cred = body["credentials"][0]
    assert cred["failed"] == 1 and cred["disabled"] is False and cred["cooldowns"] == []
    five, seven = cred["limits"]["windows"]["5h"], cred["limits"]["windows"]["7d"]
    assert five["used_pct"] == 0.0 and seven["used_pct"] == 72.0
    assert seven["reset"]["epoch"] == 1790535600
    assert seven["reset"]["utc"] == "2026-09-27T19:00:00Z"
    assert seven["reset"]["local"].startswith("2026-09-27T21:00:00+02:00")
    assert cred["limits"]["overage_disabled_reason"] == "org_level_disabled"
    assert body["upstream_key_usage"] == [] and body["quota_providers"] == []


async def test_models(loaded):
    body = (await loaded.get("/api/models")).json()
    assert body["available"] is True and body["count"] == 2
    rows = {row["id"]: row for row in body["models"]}
    assert rows["claude-opus-5"]["price"]["input"] == 5.00
    assert rows["claude-3-7-sonnet-20250219"]["price_status"] == "unpriced_legacy"
