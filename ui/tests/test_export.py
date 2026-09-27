"""CSV export: correctness against analytics, formula-injection safety, no key leaks."""
from __future__ import annotations

import copy
import csv
import io

import pytest

from export import COLUMNS, safe_cell


async def _export(app, api_client, window="all"):
    async with api_client(app) as client:
        response = await client.get(f"/api/export.csv?window={window}")
        analytics = (await client.get(f"/api/analytics?window={window}")).json()
    return response, analytics


def _rows(text):
    return list(csv.DictReader(io.StringIO(text)))


async def test_export_matches_analytics(make_app, mgmt, sample, api_client, raw_key):
    mgmt.queue_responses = [[sample[0]], [sample[1]], []]
    app = make_app()
    await app.state.drain_once(app)
    response, analytics = await _export(app, api_client)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert 'filename="cproxy-usage-all-' in response.headers["content-disposition"]
    assert raw_key not in response.text
    rows = _rows(response.text)
    assert list(rows[0].keys()) == list(COLUMNS)
    assert [r["model"] for r in rows] == ["claude-sonnet-5", "claude-opus-5"]  # oldest first
    assert rows[0]["cost_usd"] == "0.00034600" and rows[1]["cost_usd"] == "0.00410500"
    summary = analytics["summary"]
    assert sum(float(r["cost_usd"]) for r in rows) == pytest.approx(summary["estimated_cost_usd"])
    assert sum(int(r["input_tokens"]) for r in rows) == summary["input_tokens"]
    assert sum(int(r["output_tokens"]) for r in rows) == summary["output_tokens"]
    assert {r["api_key_name"] for r in rows} == {"key-2"}


async def test_unpriced_rows_have_empty_cost(make_app, mgmt, sample, api_client):
    legacy = copy.deepcopy(sample[0])
    legacy["model"] = "claude-3-7-sonnet-20250219"
    mgmt.queue_responses = [[legacy], []]
    app = make_app()
    await app.state.drain_once(app)
    row = _rows((await _export(app, api_client))[0].text)[0]
    assert row["cost_usd"] == "" and row["price_status"] == "unpriced_legacy"


async def test_formula_injection_is_neutralised(make_app, mgmt, sample, api_client):
    evil = copy.deepcopy(sample[0])
    evil["user_agent"] = '=HYPERLINK("http://evil.example","x")'
    mgmt.queue_responses = [[evil], []]
    app = make_app()
    await app.state.drain_once(app)
    row = _rows((await _export(app, api_client))[0].text)[0]
    assert row["user_agent"] == "'" + evil["user_agent"]


@pytest.mark.parametrize("value", ["=1+1", "+1", "-1", "@SUM(A1)", "\tx", "\rx"])
def test_safe_cell_prefixes(value):
    assert safe_cell(value) == "'" + value


def test_safe_cell_leaves_normal_values():
    assert safe_cell("curl/8.5.0") == "curl/8.5.0"
    assert safe_cell(-5) == -5  # numbers are not text formulas
    assert safe_cell(None) is None


async def test_export_window_and_validation(make_app, mgmt, sample, api_client):
    mgmt.queue_responses = [[sample[0]], []]
    app = make_app()
    await app.state.drain_once(app)
    async with api_client(app) as client:
        assert (await client.get("/api/export.csv?window=1y")).status_code == 400
        # The fixture records are from 2026-09-27; relative to "now" they may fall outside 24h.
        text = (await client.get("/api/export.csv?window=24h")).text
    assert text.splitlines()[0].startswith("timestamp_utc,")


async def test_large_export_streams_all_rows(make_app, mgmt, sample, api_client):
    batch = []
    for i in range(1203):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"bulk-{i}"
        batch.append(raw)
    mgmt.queue_responses = [batch, []]
    app = make_app()
    await app.state.drain_once(app)
    rows = _rows((await _export(app, api_client))[0].text)
    assert len(rows) == 1203 and len({r["id"] for r in rows}) == 1203
