"""Caching behaviour of the analytics path (deterministic: call counts and a fake clock, no timing)."""
from __future__ import annotations

import copy

import analytics
from analytics import DerivedCache, aggregate
from ingest import normalize_record


def _records(sample, n):
    out = []
    for i in range(n):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"perf-{i}"
        out.append(normalize_record(raw, {}))
    return out


def test_cached_aggregate_matches_uncached(sample):
    records = _records(sample, 40)
    cache = DerivedCache()
    now = analytics.utc_now()
    for window in ("24h", "7d", "30d", "all"):
        assert aggregate(records, window, now, cache) == aggregate(records, window, now)


def test_records_are_priced_once_across_requests(sample, monkeypatch):
    records = _records(sample, 25)
    calls = {"n": 0}
    real = analytics.record_cost

    def counting(record):
        calls["n"] += 1
        return real(record)

    monkeypatch.setattr(analytics, "record_cost", counting)
    cache = DerivedCache()
    for window in ("24h", "7d", "30d", "all", "all"):
        aggregate(records, window, cache=cache)
    assert calls["n"] == 25
    assert len(cache) == 25


def test_derived_cache_is_identity_safe(sample):
    cache = DerivedCache()
    a = normalize_record(sample[0], {})
    b = copy.deepcopy(a)
    b["model"] = "claude-opus-5"
    assert cache.get(a).model == "claude-sonnet-5"
    assert cache.get(b).model == "claude-opus-5"  # equal id() of a dead object can never alias
    assert cache.get(a) is cache.get(a)


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


async def test_analytics_result_cache(make_app, mgmt, sample, api_client, monkeypatch):
    import app as app_module

    clock = FakeClock()
    mgmt.queue_responses = [[sample[0]], []]
    application = make_app(clock=clock)
    await application.state.drain_once(application)
    calls = {"n": 0}
    real = app_module.aggregate

    def counting(*args, **kwargs):
        calls["n"] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(app_module, "aggregate", counting)
    async with api_client(application) as client:
        first = (await client.get("/api/analytics?window=all")).json()
        second = (await client.get("/api/analytics?window=all")).json()
        assert calls["n"] == 1 and first["summary"] == second["summary"]
        # Health fields are always fresh even on a cache hit.
        assert "management" in second and "ingest" in second

        await client.get("/api/analytics?window=24h")
        assert calls["n"] == 2  # windows are cached separately

        clock.t += app_module.ANALYTICS_TTL_SECONDS
        await client.get("/api/analytics?window=all")
        assert calls["n"] == 3  # TTL expired

        mgmt.queue_responses = [[sample[1]], []]
        await application.state.drain_once(application)
        fresh = (await client.get("/api/analytics?window=all")).json()
        assert calls["n"] == 4  # new records bypass the TTL
        assert fresh["summary"]["requests"] == 2
