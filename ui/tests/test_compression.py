"""Response compression for dashboard polling."""
from __future__ import annotations

import csv
import gzip
import io
import json

import pytest


@pytest.fixture
async def loaded(make_app, mgmt, sample, api_client):
    mgmt.queue_responses = [sample, []]
    app = make_app()
    await app.state.drain_once(app)
    async with api_client(app) as client:
        yield client


async def _raw(client, path, encoding):
    # httpx decodes gzip transparently; read the wire bytes via a streamed response.
    async with client.stream("GET", path, headers={"Accept-Encoding": encoding}) as response:
        return response, b"".join([chunk async for chunk in response.aiter_raw()])


# (path, byte-identical across calls?) - analytics embeds generation time and poll ages.
@pytest.mark.parametrize(
    ("path", "stable"),
    [("/", True), ("/api/requests?limit=100", True), ("/api/export.csv?window=all", False), ("/api/analytics?window=all", False)],
)
async def test_large_responses_are_gzipped(loaded, path, stable):
    response, wire = await _raw(loaded, path, "gzip")
    assert response.headers["content-encoding"] == "gzip"
    assert "accept-encoding" in response.headers["vary"].lower()
    plain, plain_wire = await _raw(loaded, path, "identity")
    assert "content-encoding" not in plain.headers
    if stable:
        assert gzip.decompress(wire) == plain_wire
    assert len(wire) < len(plain_wire)
    # Security headers survive compression.
    assert response.headers["x-content-type-options"] == "nosniff"


async def test_json_and_csv_decode_after_compression(loaded):
    response, wire = await _raw(loaded, "/api/requests?limit=100", "gzip")
    assert json.loads(gzip.decompress(wire))["total"] == 2
    response, wire = await _raw(loaded, "/api/export.csv?window=all", "gzip")
    assert len(list(csv.DictReader(io.StringIO(gzip.decompress(wire).decode())))) == 2


async def test_small_responses_are_not_compressed(loaded):
    response, wire = await _raw(loaded, "/api/analytics?window=bogus", "gzip")
    assert response.status_code == 400 and len(wire) < 1024
    assert "content-encoding" not in response.headers
