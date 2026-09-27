"""HTTP hardening: CSP pinned to the dashboard's inline script, baseline headers everywhere."""
from __future__ import annotations

import base64
import hashlib
import re

import pytest


@pytest.fixture
async def client(make_app, api_client):
    async with api_client(make_app()) as c:
        yield c


async def test_dashboard_csp_pins_the_inline_script(client):
    response = await client.get("/")
    csp = response.headers["content-security-policy"]
    scripts = re.findall(r"<script>(.*?)</script>", response.text, flags=re.DOTALL)
    assert len(scripts) == 1
    digest = base64.b64encode(hashlib.sha256(scripts[0].encode("utf-8")).digest()).decode()
    directives = dict(d.strip().split(" ", 1) for d in csp.split(";"))
    assert directives["script-src"] == f"'sha256-{digest}'"
    assert "unsafe-inline" not in directives["script-src"] and "unsafe-eval" not in csp
    assert directives["default-src"] == "'none'"
    assert directives["connect-src"] == "'self'"
    assert directives["frame-ancestors"] == "'none'"
    # No external origins at all: the page must render with no internet.
    assert "http" not in csp
    assert "<script src" not in response.text


def test_csp_without_scripts_blocks_all_scripts():
    from app import dashboard_csp

    assert "script-src 'none'" in dashboard_csp("<html><body>no scripts</body></html>")


@pytest.mark.parametrize("path", ["/", "/api/health", "/api/pricing", "/api/analytics?window=all", "/api/requests", "/api/nope"])
async def test_baseline_headers(client, path):
    headers = (await client.get(path)).headers
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["x-frame-options"] == "DENY"
    assert headers["referrer-policy"] == "no-referrer"
    assert "camera=()" in headers["permissions-policy"]


async def test_api_responses_are_not_cacheable(client):
    assert (await client.get("/api/analytics?window=24h")).headers["cache-control"] == "no-store"
    assert (await client.get("/")).headers["cache-control"] == "no-cache"


@pytest.mark.parametrize("path", ["/", "/api/health"])
async def test_head_is_supported_for_monitors(client, path):
    head = await client.head(path)
    assert head.status_code == 200 and head.content == b""
    assert head.headers["x-content-type-options"] == "nosniff"
