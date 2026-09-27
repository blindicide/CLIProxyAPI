"""Operator acceptance check (tools/smoke.py) and strict health for monitors."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
import asyncio

import httpx

_SPEC = importlib.util.spec_from_file_location("smoke", Path(__file__).resolve().parents[1] / "tools" / "smoke.py")
smoke = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(smoke)

CLIENT_KEY = "sk-smoke-test-client-key-0000000000000000"


async def test_strict_health_is_503_only_when_degraded(make_app, mgmt, api_client):
    app = make_app()
    await app.state.drain_once(app)
    async with api_client(app) as client:
        assert (await client.get("/api/health?strict=1")).status_code == 200
        mgmt.down = True
        app.state.store.state["last_ok_at"] = None
        assert (await client.get("/api/health")).status_code == 200  # dashboard still reads it
        degraded = await client.get("/api/health?strict=1")
    assert degraded.status_code == 503 and degraded.json()["status"] == "degraded"


def _fetcher(app, models: int = 17, seen_keys: list | None = None):
    async def call(method, target, headers, body):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://ui.test") as client:
            response = await client.request(method, target, headers=headers, content=body)
            return response.status_code, response.text

    def fetch(method, url, headers=None, body=None):
        path = urlsplit(url)
        target = path.path + (f"?{path.query}" if path.query else "")
        if target.startswith("/v1/"):
            if seen_keys is not None:
                seen_keys.append((headers or {}).get("Authorization"))
            if target == "/v1/models":
                return 200, json.dumps({"data": [{"id": f"m{i}"} for i in range(models)]})
            return 200, json.dumps({"choices": [{"message": {"content": "smoke ok"}}]})
        return asyncio.run(call(method, target, headers, body))

    return fetch


@pytest.fixture
def args(tmp_path):
    env = tmp_path / "cproxy.env"
    env.write_text(f"# test\nCPROXY_API_KEY={CLIENT_KEY}\nCPROXY_MANAGEMENT_KEY=other\n")
    return SimpleNamespace(base="https://example.test", local_ui="http://ui.test", ui=str(tmp_path), env_file=str(env),
                           completion=True, no_systemd=False, json=False)


@pytest.fixture
async def ready_app(make_app, mgmt, sample):
    mgmt.queue_responses = [sample, []]
    app = make_app()
    await app.state.drain_once(app)
    return app


def _pass_local(monkeypatch):
    monkeypatch.setattr(smoke, "check_totals", lambda local, path: (True, "all windows MATCH"))


def test_all_checks_pass(ready_app, args, monkeypatch):
    _pass_local(monkeypatch)
    keys: list = []
    results = smoke.run(args, fetch=_fetcher(ready_app, seen_keys=keys), active=lambda unit: True)
    assert [r["check"] for r in results] == ["services", "health", "pricing", "analytics", "dashboard", "models", "totals", "audit", "completion"]
    assert all(r["ok"] for r in results), results
    assert keys == [f"Bearer {CLIENT_KEY}"] * 2  # the key goes to the relay only
    assert CLIENT_KEY not in json.dumps(results)


@pytest.mark.parametrize(
    ("setup", "failing"),
    [
        ({"models": 16}, "models"),
        ({"inactive": "cproxy"}, "services"),
        ({"no_key": True}, "models"),
        ({"version": "9.9.9"}, "dashboard"),
    ],
)
def test_failures_are_reported_not_crashed(ready_app, args, monkeypatch, setup, failing, tmp_path):
    _pass_local(monkeypatch)
    if setup.get("no_key"):
        Path(args.env_file).write_text("CPROXY_MANAGEMENT_KEY=other\n")
    fetch = _fetcher(ready_app, models=setup.get("models", 17))
    if "version" in setup:
        real = fetch

        def fetch(method, url, headers=None, body=None):  # noqa: F811 - wrap to fake a stale page
            status, text = real(method, url, headers, body)
            if urlsplit(url).path == "/":
                text = text.replace('content="0.3.0"', f'content="{setup["version"]}"')
            return status, text

    results = smoke.run(args, fetch=fetch, active=lambda unit: unit != setup.get("inactive"))
    failed = {r["check"] for r in results if not r["ok"]}
    assert failing in failed


def test_a_crashing_check_is_a_failure(ready_app, args, monkeypatch):
    _pass_local(monkeypatch)
    monkeypatch.setattr(smoke, "check_pricing", lambda fetch, base: 1 / 0)
    results = {r["check"]: r for r in smoke.run(args, fetch=_fetcher(ready_app), active=lambda unit: True)}
    assert results["pricing"]["ok"] is False and "ZeroDivisionError" in results["pricing"]["detail"]
    assert results["health"]["ok"] is True


@pytest.mark.parametrize("fmt", ["--json", None])
def test_main_output_never_contains_the_key(ready_app, args, monkeypatch, capsys, fmt):
    _pass_local(monkeypatch)
    argv = ["--env-file", args.env_file, "--ui", args.ui, "--completion"] + ([fmt] if fmt else [])
    code = smoke.main(argv, fetch=_fetcher(ready_app), active=lambda unit: True)
    out = capsys.readouterr().out
    assert code == 0 and CLIENT_KEY not in out
    assert ("ALL PASS" in out) if fmt is None else json.loads(out)["ok"]
