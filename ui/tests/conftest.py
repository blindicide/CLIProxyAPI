"""Shared fixtures: the real captured usage-queue sample and a mocked management API."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Callable

import httpx
import pytest

FIXTURES = Path(__file__).parent / "fixtures"
# The raw capture (local only, gitignored) carries the live client key; the committed copy is
# byte-identical except for a synthetic key of the same length.
RAW_SAMPLE = FIXTURES / "usage_queue_sample.json"
REDACTED_SAMPLE = FIXTURES / "usage_queue_sample.redacted.json"

MGMT_URL = "http://mgmt.test/v0/management"
MGMT_KEY = "test-management-key"
OTHER_CLIENT_KEY = "sk-second-client-key-for-tests"


def load_sample() -> list[dict[str, Any]]:
    path = RAW_SAMPLE if RAW_SAMPLE.exists() else REDACTED_SAMPLE
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def sample() -> list[dict[str, Any]]:
    return copy.deepcopy(load_sample())


@pytest.fixture
def raw_key(sample: list[dict[str, Any]]) -> str:
    key = sample[0]["api_key"]
    assert key and all(r["api_key"] == key for r in sample)
    return key


def auth_files_payload() -> dict[str, Any]:
    """Shape captured from the live /v0/management/auth-files (values from the fixture headers)."""
    signals = {
        "Anthropic-Ratelimit-Unified-5h-Reset": "1790531400",
        "Anthropic-Ratelimit-Unified-5h-Status": "allowed",
        "Anthropic-Ratelimit-Unified-5h-Utilization": "0.0",
        "Anthropic-Ratelimit-Unified-7d-Reset": "1790535600",
        "Anthropic-Ratelimit-Unified-7d-Status": "allowed",
        "Anthropic-Ratelimit-Unified-7d-Utilization": "0.72",
        "Anthropic-Ratelimit-Unified-Fallback-Percentage": "0.5",
        "Anthropic-Ratelimit-Unified-Overage-Disabled-Reason": "org_level_disabled",
        "Anthropic-Ratelimit-Unified-Overage-Status": "rejected",
        "Anthropic-Ratelimit-Unified-Representative-Claim": "five_hour",
        "Anthropic-Ratelimit-Unified-Reset": "1790531400",
        "Anthropic-Ratelimit-Unified-Status": "allowed",
    }
    return {
        "files": [
            {
                "id": "claude-test.json",
                "label": "fixture@example.com",
                "provider": "claude",
                "account_type": "oauth",
                "auth_index": "73201639ccbcc4b2",
                "disabled": False,
                "failed": 1,
                "cooldowns": [],
                "model_quotas": {"claude-opus-5": {"observed_at": "2026-09-27T14:50:52+02:00", "signals": signals}},
            }
        ]
    }


class MockManagement:
    """Programmable stand-in for the cproxy management API behind an httpx.MockTransport."""

    def __init__(self, client_key: str, queue: list[Any] | None = None) -> None:
        self.client_key = client_key
        # Each entry is one usage-queue response body (a list); an exhausted script returns [].
        self.queue_responses: list[list[Any]] = list(queue or [])
        self.calls: list[str] = []
        self.down = False
        self.auth_files = auth_files_payload()
        # None -> /config answers 404 (exercises the default-retention fallback).
        self.config: dict[str, Any] | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/v0/management/")
        self.calls.append(path)
        if self.down:
            raise httpx.ConnectError("connection refused", request=request)
        if request.headers.get("authorization") != f"Bearer {MGMT_KEY}":
            return httpx.Response(401, json={"error": "invalid management key"})
        if path == "usage-queue":
            return httpx.Response(200, json=self.queue_responses.pop(0) if self.queue_responses else [])
        if path == "api-keys":
            return httpx.Response(200, json={"api-keys": [OTHER_CLIENT_KEY, self.client_key]})
        if path == "config" and self.config is not None:
            return httpx.Response(200, json=self.config)
        if path == "auth-files":
            return httpx.Response(200, json=self.auth_files)
        if path == "api-key-usage":
            return httpx.Response(200, json={})
        if path == "quota/providers":
            return httpx.Response(200, json={"providers": []})
        if path == "model-definitions/claude":
            return httpx.Response(200, json={"channel": "claude", "models": [{"id": "claude-opus-5", "display_name": "Claude Opus 5"}, {"id": "claude-3-7-sonnet-20250219", "display_name": "Claude Sonnet 3.7"}]})
        return httpx.Response(404, json={"error": "not found"})


@pytest.fixture
def mgmt(raw_key: str) -> MockManagement:
    return MockManagement(raw_key)


@pytest.fixture
def make_app(tmp_path: Path, mgmt: MockManagement) -> Callable[..., Any]:
    from app import create_app

    def factory(data_dir: Path | None = None, **kwargs: Any) -> Any:
        client = httpx.AsyncClient(transport=httpx.MockTransport(mgmt.handler))
        kwargs.setdefault("start_poller", False)
        return create_app(management_url=MGMT_URL, management_key=MGMT_KEY, client=client, data_dir=data_dir or tmp_path, **kwargs)

    return factory


@pytest.fixture
def api_client() -> Callable[[Any], httpx.AsyncClient]:
    def factory(app: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://ui.test")

    return factory
