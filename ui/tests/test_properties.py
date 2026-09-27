"""Property-based fuzzing (Hypothesis) of the ingest and cost paths.

Random and malformed usage records must never crash the drain, never lose a record (each is
stored or quarantined, exactly once), never leak the raw api_key, never put NaN/Infinity into
an API response, and never produce a cost outside what the pricing table allows.
"""
from __future__ import annotations

import copy
import json
import math
from datetime import UTC, datetime

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from analytics import aggregate, enrich
from export import row
from ingest import RecordStore, drain_queue, mask_key, normalize_record, parse_ts, ratelimit_from_signals
from pricing import PRICED, PRICING, record_cost
from tests.conftest import load_sample

SETTINGS = settings(max_examples=150, deadline=None, derandomize=True, suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture])
BASE = load_sample()
NOW = datetime(2026, 9, 27, 13, 0, tzinfo=UTC)

scalars = st.one_of(
    st.none(), st.booleans(), st.integers(min_value=-(10**30), max_value=10**30),
    st.floats(allow_nan=True, allow_infinity=True), st.text(max_size=40),
)
json_values = st.recursive(scalars, lambda inner: st.one_of(st.lists(inner, max_size=4), st.dictionaries(st.text(max_size=12), inner, max_size=4)), max_leaves=12)
token_numbers = st.one_of(st.integers(min_value=-(10**20), max_value=10**20), st.floats(allow_nan=True, allow_infinity=True), st.text(max_size=8), st.none())
FIELDS = list(BASE[0].keys())


@st.composite
def records(draw):
    """A real captured record with a random subset of fields replaced by garbage."""
    record = copy.deepcopy(draw(st.sampled_from(BASE)))
    for key in draw(st.lists(st.sampled_from(FIELDS), max_size=8, unique=True)):
        record[key] = draw(json_values)
    if draw(st.booleans()):
        record["tokens"] = draw(st.dictionaries(st.sampled_from(["input_tokens", "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_creation_tokens"]), token_numbers))
    if draw(st.booleans()):
        record["token_breakdown"] = {
            "quality": draw(st.sampled_from(["complete", "unclassified", "inconsistent", None])),
            "input": draw(st.one_of(json_values, st.dictionaries(st.sampled_from(["uncached_tokens", "cache_read_tokens", "cache_write_tokens"]), token_numbers))),
            "output": draw(st.one_of(json_values, st.dictionaries(st.sampled_from(["total_tokens", "reasoning_tokens"]), token_numbers))),
            "unclassified_tokens": draw(token_numbers),
        }
    if draw(st.booleans()):
        record["model"] = draw(st.one_of(st.sampled_from(list(PRICING)), st.text(max_size=30)))
    if draw(st.booleans()):
        record["api_key"] = draw(st.text(min_size=0, max_size=64))
    record["execution_id"] = draw(st.one_of(st.uuids().map(str), json_values))
    return record


def _finite_json(obj) -> str:
    return json.dumps(obj, allow_nan=False, default=str)


def _with_key(key):
    record = copy.deepcopy(BASE[0])
    record["api_key"] = key
    return record


@SETTINGS
@given(records())
# Keys that corrupted the old whole-JSON-text scrub (found while writing these properties).
@example(_with_key('"'))
@example(_with_key(","))
@example(_with_key("1"))
@example(_with_key("id"))
@example(_with_key(":"))
@example(_with_key('abc"def,ghi:jkl'))
def test_normalize_never_crashes_and_never_leaks_the_key(raw):
    record = normalize_record(raw, {})
    assert isinstance(record, dict) and isinstance(record["id"], str) and record["id"]
    text = json.dumps(record, default=str)
    key = raw.get("api_key")
    if isinstance(key, str) and len(key) >= 8:
        assert key not in text
    json.loads(json.dumps(record, default=str))  # round-trips as stored


@SETTINGS
@given(records())
def test_cost_is_always_within_the_pricing_table(raw):
    record = normalize_record(raw, {})
    result = record_cost(record)
    cost, usage = result["cost_usd"], result["usage"]
    for value in usage.values():
        if isinstance(value, (int, float)):
            assert math.isfinite(value) and value >= 0
    if cost is None:
        assert result["price_status"] != PRICED
        return
    assert math.isfinite(cost) and cost >= 0
    row_ = PRICING[result["price_model"]]
    assert row_["price_status"] == PRICED
    rates = [row_[k] for k in ("input", "output", "cache_write_5m", "cache_read")]
    billed = usage["uncached_input"] + usage["unclassified"] + usage["cache_read"] + usage["cache_write"] + usage["output"]
    assert min(rates) * billed / 1e6 * (1 - 1e-9) <= cost <= max(rates) * billed / 1e6 * (1 + 1e-9) + 1e-12


@SETTINGS
@given(st.lists(records(), max_size=12))
def test_analytics_and_export_never_crash_or_emit_nan(raws):
    normalised = [normalize_record(r, {}) for r in raws]
    for window in ("24h", "all"):
        _finite_json(aggregate(normalised, window, NOW))  # Starlette refuses NaN/Infinity
    for record in normalised:
        _finite_json(enrich(record))
        row(record)


item = st.one_of(records(), st.integers(), st.text(max_size=20), st.none(), st.lists(st.integers(), max_size=2), json_values)


@SETTINGS
@given(st.lists(st.lists(item, max_size=6), max_size=5))
def test_drain_never_crashes_and_never_loses_a_record(tmp_path_factory, batches):
    tmp = tmp_path_factory.mktemp("drain")
    store = RecordStore(tmp / "requests.jsonl", tmp / "data" / "state.json")
    responses = [copy.deepcopy(b) for b in batches] + [[]]

    async def fetch(path, **params):
        return responses.pop(0) if responses else []

    import asyncio

    asyncio.run(drain_queue(fetch, store, {}))
    # The drain stops at the first empty response; later batches were never popped (still queued).
    handed_out = []
    for batch in batches:
        if not batch:
            break
        handed_out.append(batch)
    dict_items = [i for b in handed_out for i in b if isinstance(i, dict) and not (i.get("refresh") or i.get("support_refresh"))]
    expected_ids = []
    for raw in dict_items:
        try:
            expected_ids.append(normalize_record(copy.deepcopy(raw), {})["id"])
        except Exception:  # noqa: BLE001 - quarantined instead
            expected_ids.append(None)
    stored = {r["id"] for r in store.records}
    unique_ok = {i for i in expected_ids if i}
    assert unique_ok <= stored  # every normalisable record is stored
    quarantine = tmp / "data" / "quarantine.jsonl"
    quarantined = len(quarantine.read_text().splitlines()) if quarantine.exists() else 0
    assert quarantined == store.state.get("quarantined", 0) == expected_ids.count(None)  # the rest are kept, not dropped
    raw_lines = (tmp / "requests.jsonl").read_bytes().split(b"\n")[:-1] if (tmp / "requests.jsonl").exists() else []
    assert all(line.isascii() for line in raw_lines)  # one record per physical line for any reader
    assert len((tmp / "requests.jsonl").read_text().splitlines()) == len(raw_lines) if raw_lines else True
    on_disk = [json.loads(line)["id"] for line in raw_lines]
    assert len(on_disk) == len(set(on_disk)) and set(on_disk) == stored
    reloaded = RecordStore(tmp / "requests.jsonl", tmp / "data" / "state.json")
    assert {r["id"] for r in reloaded.records} == stored


@SETTINGS
@given(st.text(max_size=60))
def test_parsers_never_crash(text):
    parse_ts(text)
    ratelimit_from_signals({text: text, "anthropic-ratelimit-unified-5h-utilization": text, "anthropic-ratelimit-unified-7d-reset": [text]})
    masked = mask_key(text)
    if len(text) >= 12:
        assert text not in masked
