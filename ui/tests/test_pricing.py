"""Cost estimation and the /api/pricing regression against mandate section 4."""
from __future__ import annotations

import copy

import pytest

from analytics import aggregate
from ingest import normalize_record
from pricing import AS_OF, CACHE_WRITE_TTL_ASSUMPTION, PRICING, SOURCE_URL, record_cost, resolve_model

# Mandate section 4, verbatim: input, output, cache write 5m, cache write 1h, cache read (USD / 1M).
EXPECTED = {
    "claude-fable-5-1": (10.00, 50.00, 12.50, 20.00, 0.25),
    "claude-fable-5": (10.00, 50.00, 12.50, 20.00, 1.00),
    "claude-opus-5-5": (4.00, 20.00, 5.00, 8.00, 0.20),
    "claude-opus-5": (5.00, 25.00, 6.25, 10.00, 0.50),
    "claude-opus-4-8": (5.00, 25.00, 6.25, 10.00, 0.50),
    "claude-opus-4-7": (5.00, 25.00, 6.25, 10.00, 0.50),
    "claude-opus-4-6": (5.00, 25.00, 6.25, 10.00, 0.50),
    "claude-opus-4-5-20251101": (5.00, 25.00, 6.25, 10.00, 0.50),
    "claude-opus-4-1-20250805": (15.00, 75.00, 18.75, 30.00, 1.50),
    "claude-opus-4-20250514": (15.00, 75.00, 18.75, 30.00, 1.50),
    "claude-sonnet-5": (2.00, 10.00, 2.50, 4.00, 0.20),
    "claude-sonnet-4-6": (3.00, 15.00, 3.75, 6.00, 0.30),
    "claude-sonnet-4-5-20250929": (3.00, 15.00, 3.75, 6.00, 0.30),
    "claude-sonnet-4-20250514": (3.00, 15.00, 3.75, 6.00, 0.30),
    "claude-haiku-4-5-20251001": (1.00, 5.00, 1.25, 2.00, 0.10),
    "claude-3-5-haiku-20241022": (0.80, 4.00, 1.00, 1.60, 0.08),
}
UNPRICED = "claude-3-7-sonnet-20250219"
FIELDS = ("input", "output", "cache_write_5m", "cache_write_1h", "cache_read")


def with_usage(record: dict, model: str, *, uncached: int, read: int, write: int, non_reasoning: int, reasoning: int) -> dict:
    """A copy of a real captured record with its model and token buckets replaced."""
    record = copy.deepcopy(record)
    record["model"] = record["alias"] = model
    total_in = uncached + read + write
    total_out = non_reasoning + reasoning
    record["token_breakdown"] = {
        "schema_version": 2,
        "quality": "complete",
        "total_tokens": total_in + total_out,
        "input": {"total_tokens": total_in, "uncached_tokens": uncached, "cache_read_tokens": read, "cache_write_tokens": write},
        "output": {"total_tokens": total_out, "non_reasoning_tokens": non_reasoning, "reasoning_tokens": reasoning},
        "unclassified_tokens": 0,
    }
    record["tokens"] = {
        "input_tokens": uncached,
        "output_tokens": total_out,
        "reasoning_tokens": reasoning,
        "cached_tokens": read,
        "cache_read_tokens": read,
        "cache_read_tokens_present": True,
        "cache_creation_tokens": write,
        "total_tokens": total_in + total_out,
    }
    return record


def test_table_matches_mandate_exactly():
    priced = {k: v for k, v in PRICING.items() if v["price_status"] == "priced"}
    assert set(priced) == set(EXPECTED)
    for model_id, values in EXPECTED.items():
        assert tuple(PRICING[model_id][f] for f in FIELDS) == values, model_id
    assert PRICING[UNPRICED]["price_status"] == "unpriced_legacy"
    assert all(PRICING[UNPRICED][f] is None for f in FIELDS)
    assert not any(k.startswith("claude-mythos") for k in PRICING)
    assert len(PRICING) == 17


@pytest.mark.parametrize("model_id", sorted(EXPECTED))
def test_cost_each_priced_model(sample, model_id):
    record = with_usage(sample[0], model_id, uncached=1200, read=34000, write=5600, non_reasoning=700, reasoning=300)
    inp, out, cw5, _cw1, cr = EXPECTED[model_id]
    expected = (1200 * inp + 34000 * cr + 5600 * cw5 + (700 + 300) * out) / 1e6
    result = record_cost(record)
    assert result["cost_usd"] == pytest.approx(expected, rel=1e-12)
    assert result["cost_quality"] == "complete"
    assert result["usage"]["output"] == 1000  # reasoning is billed as output
    assert result["usage"]["reasoning"] == 300


def test_fixture_records_hand_computed(sample):
    # Real captured records: sonnet-5 138 in / 7 out, opus-5 511 in / 62 out, no cache.
    assert record_cost(sample[0])["cost_usd"] == pytest.approx((138 * 2.00 + 7 * 10.00) / 1e6)  # $0.000346
    assert record_cost(sample[1])["cost_usd"] == pytest.approx((511 * 5.00 + 62 * 25.00) / 1e6)  # $0.004105


def test_cache_write_uses_5m_rate(sample):
    record = with_usage(sample[0], "claude-opus-5", uncached=0, read=0, write=1_000_000, non_reasoning=0, reasoning=0)
    assert CACHE_WRITE_TTL_ASSUMPTION == "5m"
    assert record_cost(record)["cost_usd"] == pytest.approx(6.25)


def test_unpriced_legacy_is_none_and_excluded(sample):
    legacy = with_usage(sample[0], UNPRICED, uncached=1000, read=0, write=0, non_reasoning=100, reasoning=0)
    result = record_cost(legacy)
    assert result["cost_usd"] is None
    assert result["price_status"] == "unpriced_legacy"
    records = [normalize_record(legacy, {}), normalize_record(sample[1], {})]
    summary = aggregate(records, "all")["summary"]
    assert summary["unpriced_requests"] == 1
    assert summary["unpriced_models"] == {UNPRICED: 1}
    assert summary["estimated_cost_usd"] == pytest.approx(0.004105)
    per_model = {row["model"]: row for row in aggregate(records, "all")["per_model"]}
    assert per_model[UNPRICED]["cost_usd"] is None


def test_unknown_model_is_unpriced(sample):
    record = copy.deepcopy(sample[0])
    record["model"] = "claude-mystery-9"
    assert record_cost(record)["cost_usd"] is None
    assert record_cost(record)["price_status"] == "unknown_model"


def test_date_suffixed_ids_map_to_family_row():
    assert resolve_model("claude-opus-4-5") == "claude-opus-4-5-20251101"
    assert resolve_model("claude-sonnet-4-6-20260101") == "claude-sonnet-4-6"
    assert resolve_model("claude-haiku-4-5") == "claude-haiku-4-5-20251001"
    assert resolve_model("claude-opus-4-1") == "claude-opus-4-1-20250805"
    assert resolve_model("claude-3-7-sonnet-latest") == UNPRICED
    assert resolve_model("gpt-5") is None


def test_fallback_to_tokens_when_breakdown_not_complete(sample):
    record = copy.deepcopy(sample[1])  # opus-5, 511 in / 62 out
    record["token_breakdown"]["quality"] = "unclassified"
    record["token_breakdown"]["unclassified_tokens"] = 100
    result = record_cost(record)
    assert result["cost_quality"] == "fallback_tokens:unclassified"
    assert result["usage"]["unclassified"] == 100
    assert result["cost_usd"] == pytest.approx(((511 + 100) * 5.00 + 62 * 25.00) / 1e6)

    record.pop("token_breakdown")
    result = record_cost(record)
    assert result["cost_quality"] == "fallback_tokens:no_breakdown"
    assert result["cost_usd"] == pytest.approx((511 * 5.00 + 62 * 25.00) / 1e6)


def test_fallback_flag_surfaces_in_aggregate(sample):
    record = copy.deepcopy(sample[1])
    record["token_breakdown"]["quality"] = "inconsistent"
    summary = aggregate([normalize_record(record, {}), normalize_record(sample[0], {})], "all")["summary"]
    assert summary["cost_quality"] == {"fallback_tokens:inconsistent": 1, "complete": 1}


async def test_api_pricing_regression(make_app, api_client):
    async with api_client(make_app()) as client:
        body = (await client.get("/api/pricing")).json()
    assert body["source_url"] == SOURCE_URL == "https://platform.claude.com/docs/en/about-claude/pricing"
    assert body["as_of"] == AS_OF == "2026-09-27"
    assert body["cache_write_ttl_assumption"] == "5m"
    rows = {row["id"]: row for row in body["models"]}
    for model_id, values in EXPECTED.items():
        assert tuple(rows[model_id][f] for f in FIELDS) == values, model_id
        assert rows[model_id]["price_status"] == "priced"
    assert rows[UNPRICED]["price_status"] == "unpriced_legacy"
    assert all(rows[UNPRICED][f] is None for f in FIELDS)
