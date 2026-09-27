"""The app's totals must match an independent recomputation straight from requests.jsonl."""
from __future__ import annotations

import copy
import importlib.util
import json
from datetime import timedelta
from pathlib import Path

import pytest

from ingest import iso_utc, utc_now
from pricing import PRICING

_SPEC = importlib.util.spec_from_file_location("verify_totals", Path(__file__).resolve().parents[1] / "tools" / "verify_totals.py")
verify = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(verify)

WINDOWS = ("24h", "7d", "30d", "all")


def _variant(raw, i, *, age, model=None, uncached=0, read=0, write=0, out=0, reasoning=0, quality="complete", unclassified=0, failed=False):
    r = copy.deepcopy(raw)
    r["execution_id"] = f"oracle-{i}"
    r["timestamp"] = iso_utc(utc_now() - age)
    if model:
        r["model"] = r["alias"] = model
    r["failed"] = failed
    r["fail"] = {"status_code": 400 if failed else 200, "body": "bad" if failed else ""}
    r["token_breakdown"] = {
        "schema_version": 2, "quality": quality, "total_tokens": uncached + read + write + out + unclassified,
        "input": {"total_tokens": uncached + read + write, "uncached_tokens": uncached, "cache_read_tokens": read, "cache_write_tokens": write},
        "output": {"total_tokens": out, "non_reasoning_tokens": out - reasoning, "reasoning_tokens": reasoning},
        "unclassified_tokens": unclassified,
    }
    r["tokens"] = {"input_tokens": uncached, "output_tokens": out, "reasoning_tokens": reasoning, "cached_tokens": read,
                   "cache_read_tokens": read, "cache_read_tokens_present": True, "cache_creation_tokens": write,
                   "total_tokens": uncached + read + write + out}
    return r


@pytest.fixture
async def drained(make_app, mgmt, sample, tmp_path):
    raw = sample[0]
    rows = [
        _variant(raw, 0, age=timedelta(hours=1), model="claude-opus-5-5", uncached=1200, read=34000, write=5600, out=1000, reasoning=300),
        _variant(raw, 1, age=timedelta(hours=2), model="claude-3-7-sonnet-20250219", uncached=500, out=50),
        _variant(raw, 2, age=timedelta(hours=3), model="claude-mystery-9", uncached=10, out=1),
        _variant(raw, 3, age=timedelta(hours=5), model="claude-sonnet-4-6", uncached=900, out=20, quality="unclassified", unclassified=77),
        _variant(raw, 4, age=timedelta(days=2), model="claude-haiku-4-5", uncached=300, read=100, out=40, failed=True),
        _variant(raw, 5, age=timedelta(days=12), model="claude-fable-5-1", uncached=2000, write=900, out=600, reasoning=200),
        _variant(raw, 6, age=timedelta(days=45), model="claude-opus-4-1", uncached=50, out=5),
        copy.deepcopy(sample[1]),
    ]
    mgmt.queue_responses = [rows[:3], rows[3:], []]
    app = make_app()
    await app.state.drain_once(app)
    return app, tmp_path / "requests.jsonl"


@pytest.mark.parametrize("window", WINDOWS)
async def test_app_matches_independent_recomputation(drained, api_client, window):
    app, path = drained
    async with api_client(app) as client:
        analytics = (await client.get(f"/api/analytics?window={window}")).json()
    mine, problems = verify.compare(analytics, verify.load(path))
    assert problems == []
    assert mine["requests"] == analytics["summary"]["requests"] > 0


async def test_tampered_file_is_detected(drained, api_client):
    app, path = drained
    async with api_client(app) as client:
        analytics = (await client.get("/api/analytics?window=all")).json()
    lines = path.read_text().splitlines()
    first = json.loads(lines[0])
    first["token_breakdown"]["output"]["total_tokens"] += 1000
    path.write_text("\n".join([json.dumps(first)] + lines[1:]) + "\n")
    _, problems = verify.compare(analytics, verify.load(path))
    fields = {p[0] for p in problems}
    assert "output_tokens" in fields and "estimated_cost_usd" in fields


def test_price_tables_agree():
    """Two independent encodings of the same official table."""
    for model_id, row in PRICING.items():
        oracle = verify.PRICES[model_id]
        if row["price_status"] == "unpriced_legacy":
            assert oracle is None
        else:
            assert oracle == (row["input"], row["output"], row["cache_write_5m"], row["cache_read"])
    assert set(verify.PRICES) == set(PRICING)


def test_cli_exit_codes(drained, monkeypatch, capsys):
    app, path = drained
    good = {"window": "24h", "generated_at": iso_utc(utc_now()), "summary": {}, "per_model": [], "per_day": []}
    mine, _ = verify.recompute(verify.load(path), "24h", utc_now())
    good["summary"] = {k: mine[k] for k in ("requests", "success", "failed", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "reasoning_tokens", "priced_requests", "unpriced_requests")}
    good["summary"]["estimated_cost_usd"] = round(mine["cost"], 8)
    _, per_model = verify.recompute(verify.load(path), "24h", utc_now())
    good["per_model"] = [{"model": m, "requests": v["requests"], "cost_usd": v["cost"] if v["priced"] else None} for m, v in per_model.items()]
    good["per_day"] = [{"cost_usd": mine["cost"]}]
    monkeypatch.setattr(verify, "fetch", lambda url: good)
    assert verify.main(["--file", str(path), "--window", "24h"]) == 0
    bad = copy.deepcopy(good)
    bad["summary"]["requests"] += 1
    monkeypatch.setattr(verify, "fetch", lambda url: bad)
    assert verify.main(["--file", str(path), "--window", "24h", "--json"]) == 1
    assert '"ok": false' in capsys.readouterr().out
