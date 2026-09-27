"""Shutdown must not drop records whose queue pop is already in flight."""
from __future__ import annotations

import asyncio


async def test_inflight_pop_is_persisted_on_shutdown(make_app, sample):
    app = make_app(poll_interval=0.01, start_poller=True)
    popped = asyncio.Event()
    release = asyncio.Event()
    served = {"n": 0}

    async def slow_get(path, **params):
        if path == "usage-queue":
            served["n"] += 1
            if served["n"] == 1:
                # cproxy has already removed the record from its queue at this point.
                popped.set()
                await release.wait()
                return [sample[0]]
            return []
        return {}

    app.state.management.get = slow_get
    ctx = app.router.lifespan_context(app)
    await ctx.__aenter__()
    await asyncio.wait_for(popped.wait(), 5)
    shutdown = asyncio.create_task(ctx.__aexit__(None, None, None))
    await asyncio.sleep(0.02)  # let shutdown begin while the pop is still in flight
    release.set()
    await asyncio.wait_for(shutdown, 5)
    assert [r["execution_id"] for r in app.state.store.records] == [sample[0]["execution_id"]]
    assert (app.state.store.requests_path).read_text().count("\n") == 1


async def test_shutdown_does_not_hang_on_a_stuck_pop(make_app, monkeypatch):
    import app as app_module

    monkeypatch.setattr(app_module, "SHUTDOWN_GRACE_SECONDS", 0.05)
    app = make_app(poll_interval=0.01, start_poller=True)
    started = asyncio.Event()

    async def stuck_get(path, **params):
        if path == "usage-queue":
            started.set()
            await asyncio.Event().wait()  # never answers
        return {}

    app.state.management.get = stuck_get
    ctx = app.router.lifespan_context(app)
    await ctx.__aenter__()
    await asyncio.wait_for(started.wait(), 5)
    await asyncio.wait_for(ctx.__aexit__(None, None, None), 5)  # grace expires, then cancel


async def test_drain_starts_no_new_pop_after_stop(tmp_path, sample):
    from ingest import RecordStore, drain_queue

    store = RecordStore(tmp_path / "requests.jsonl", tmp_path / "data" / "state.json")
    flag = {"stop": True}
    calls = {"n": 0}

    async def fetch(path, **params):
        calls["n"] += 1
        flag["stop"] = True
        return [sample[calls["n"] % 2]]

    assert (await drain_queue(fetch, store, {}, stop=lambda: flag["stop"]))["pops"] == 0
    flag["stop"] = False
    result = await drain_queue(fetch, store, {}, stop=lambda: flag["stop"])
    assert result["pops"] == 1 and result["stored"] == 1 and not result["exhausted"]
