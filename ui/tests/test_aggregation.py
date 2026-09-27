"""Window filtering and grouping over normalised real records."""
from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta

import pytest

from analytics import aggregate, filter_window, percentile, short_user_agent
from ingest import key_hash, normalize_record, parse_ts

NOW = datetime(2026, 9, 27, 13, 0, tzinfo=UTC)


def shifted(record: dict, age: timedelta, suffix: str) -> dict:
    record = copy.deepcopy(record)
    record["timestamp"] = (NOW - age).isoformat()
    record["execution_id"] = f"{record['execution_id']}-{suffix}"
    return record


@pytest.fixture
def records(sample, raw_key):
    names = {key_hash(raw_key): "key-2"}
    sonnet, opus = sample
    failed = copy.deepcopy(sonnet)
    failed.update(failed=True, fail={"status_code": 400, "body": "unknown model"}, model="claude-nope", alias="claude-nope")
    failed["token_breakdown"]["input"].update(total_tokens=0, uncached_tokens=0)
    failed["token_breakdown"]["output"].update(total_tokens=0, non_reasoning_tokens=0)
    raws = [
        shifted(sonnet, timedelta(hours=1), "a"),
        shifted(opus, timedelta(hours=2), "b"),
        shifted(failed, timedelta(hours=3), "c"),
        shifted(opus, timedelta(days=3), "d"),
        shifted(sonnet, timedelta(days=20), "e"),
        shifted(opus, timedelta(days=60), "f"),
    ]
    return [normalize_record(r, names) for r in raws]


def test_parse_go_nanosecond_timestamp(sample):
    ts = parse_ts(sample[0]["timestamp"])  # 2026-09-27T14:50:37.541863648+02:00
    assert ts == datetime(2026, 9, 27, 12, 50, 37, 541863, tzinfo=UTC)


@pytest.mark.parametrize(("window", "count"), [("24h", 3), ("7d", 4), ("30d", 5), ("all", 6)])
def test_window_filtering(records, window, count):
    assert len(filter_window(records, window, NOW)) == count
    assert aggregate(records, window, NOW)["summary"]["requests"] == count


def test_unknown_window_rejected(records):
    with pytest.raises(ValueError):
        filter_window(records, "1y", NOW)


def test_success_failed_and_groups(records):
    result = aggregate(records, "24h", NOW)
    summary = result["summary"]
    assert (summary["success"], summary["failed"]) == (2, 1)
    assert summary["failed_pct"] == pytest.approx(33.33, abs=0.01)
    assert summary["status_codes"] == {"200": 2, "400": 1}
    per_model = {row["model"]: row for row in result["per_model"]}
    assert per_model["claude-sonnet-5"]["requests"] == 1
    assert per_model["claude-opus-5"]["input_tokens"] == 511
    assert per_model["claude-opus-5"]["output_tokens"] == 62
    assert per_model["claude-opus-5"]["cost_usd"] == pytest.approx(0.004105)
    assert per_model["claude-nope"]["failed"] == 1
    assert per_model["claude-nope"]["cost_usd"] is None
    assert summary["estimated_cost_usd"] == pytest.approx(0.004105 + 0.000346)
    assert [row["key_name"] for row in result["per_key"]] == ["key-2"]
    assert result["per_key"][0]["requests"] == 3
    assert result["per_key"][0]["key_masked"].startswith("sha256:")
    assert [row["endpoint"] for row in result["per_endpoint"]] == ["/v1/chat/completions"]
    assert result["per_client_ip"][0]["client_ip"] == "127.0.0.1"
    assert result["per_user_agent"][0]["user_agent"] == "curl/8.5.0"
    assert sum(row["share_pct"] for row in result["per_model"]) == pytest.approx(100, abs=0.05)


def test_per_day_and_series(records):
    result = aggregate(records, "all", NOW)
    days = {row["day"]: row["requests"] for row in result["per_day"]}
    assert days["2026-09-27"] == 3
    assert days["2026-09-24"] == 1
    assert sum(days.values()) == 6
    assert result["series_granularity"] == "day"
    hourly = aggregate(records, "24h", NOW)
    assert hourly["series_granularity"] == "hour"
    assert [row["bucket"] for row in hourly["series"]] == ["2026-09-27T12:00", "2026-09-27T13:00", "2026-09-27T14:00"]
    day_cost = sum(row["cost_usd"] or 0 for row in result["per_day"])
    assert day_cost == pytest.approx(result["summary"]["estimated_cost_usd"])


def test_latency_stats_and_rate(records):
    summary = aggregate(records, "24h", NOW)["summary"]
    assert summary["p50_latency_ms"] == 1429
    assert summary["p95_latency_ms"] == 2356
    assert summary["avg_ttft_ms"] == pytest.approx((964 + 1396 + 964) / 3, abs=0.1)
    assert summary["stream_requests"] == 2 and summary["non_stream_requests"] == 1
    assert summary["requests_per_min"] == pytest.approx(3 / 1440, abs=1e-4)


def test_empty_window_has_no_invented_numbers():
    summary = aggregate([], "24h", NOW)["summary"]
    assert summary["requests"] == 0
    assert summary["avg_latency_ms"] is None and summary["p95_latency_ms"] is None
    assert summary["success_pct"] is None
    assert summary["estimated_cost_usd"] == 0.0


def test_helpers():
    assert percentile([], 50) is None
    assert percentile([5, 1, 3], 50) == 3
    assert short_user_agent("OpenAI/Python 1.2.3") == "OpenAI/Python"
    assert short_user_agent("Mozilla/5.0 (X11; Linux)") == "Mozilla/5.0"
    assert short_user_agent(None) == "(none)"
