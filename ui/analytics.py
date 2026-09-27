"""Aggregation of persisted usage records into dashboard analytics."""
from __future__ import annotations

import heapq
import math
import re
from datetime import datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from ingest import iso_utc, parse_ts, utc_now
from pricing import record_cost

LOCAL_TZ = ZoneInfo("Europe/Amsterdam")
WINDOWS: dict[str, timedelta | None] = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "all": None,
}


def local_iso(dt: datetime | None) -> str | None:
    return dt.astimezone(LOCAL_TZ).isoformat() if dt else None


class Derived:
    """Per-record fields the aggregation needs, computed once (timestamps, cost, group keys)."""

    __slots__ = ("ts", "day", "hour", "cost", "usage", "model", "key_name", "key_masked", "endpoint", "ip", "ua", "failed", "stream", "latency", "ttft", "status")

    def __init__(self, record: dict[str, Any]) -> None:
        self.ts = parse_ts(record.get("timestamp"))
        local = self.ts.astimezone(LOCAL_TZ) if self.ts else None
        self.day = local.strftime("%Y-%m-%d") if local else None
        self.hour = local.strftime("%Y-%m-%dT%H:00") if local else None
        self.cost = record_cost(record)
        self.usage = self.cost["usage"]
        self.model = str(record.get("model") or "unknown")
        self.key_name = str(record.get("api_key_name") or "no-key")
        self.key_masked = record.get("api_key_masked")
        self.endpoint = str(record.get("endpoint_path") or record.get("endpoint") or "unknown")
        self.ip = str(record.get("resolved_client_ip") or record.get("client_ip") or "unknown")
        self.ua = short_user_agent(record.get("user_agent"))
        self.failed = bool(record.get("failed"))
        self.stream = bool(record.get("stream"))
        self.latency = _num(record.get("latency_ms"))
        self.ttft = _num(record.get("ttft_ms"))
        self.status = str(record.get("status_code") if record.get("status_code") is not None else "unknown")


class DerivedCache:
    """Memoises ``Derived`` per record object.

    Keyed by object identity and holding a reference to the record, so an id can never be
    reused by a different object while its entry is alive. Stored records are never mutated,
    which keeps the cached values valid for the life of the process.
    """

    def __init__(self) -> None:
        self._items: dict[int, tuple[dict[str, Any], Derived]] = {}

    def get(self, record: dict[str, Any]) -> Derived:
        hit = self._items.get(id(record))
        if hit is not None and hit[0] is record:
            return hit[1]
        derived = Derived(record)
        self._items[id(record)] = (record, derived)
        return derived

    def __len__(self) -> int:
        return len(self._items)


def _derive(records: Iterable[dict[str, Any]], cache: DerivedCache | None) -> list[tuple[dict[str, Any], Derived]]:
    get = cache.get if cache is not None else Derived
    return [(record, get(record)) for record in records]


def filter_window(records: Iterable[dict[str, Any]], window: str, now: datetime | None = None, cache: DerivedCache | None = None) -> list[dict[str, Any]]:
    return [record for record, _ in _select(_derive(records, cache), window, now or utc_now())]


def _select(pairs: list[tuple[dict[str, Any], Derived]], window: str, now: datetime) -> list[tuple[dict[str, Any], Derived]]:
    if window not in WINDOWS:
        raise ValueError(f"unknown window {window!r}; expected one of {', '.join(WINDOWS)}")
    span = WINDOWS[window]
    if span is None:
        return pairs
    cutoff = now - span
    return [pair for pair in pairs if pair[1].ts and pair[1].ts >= cutoff]


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def short_user_agent(ua: str | None) -> str:
    if not ua:
        return "(none)"
    first = re.split(r"[\s(]", ua.strip(), maxsplit=1)[0]
    return (first or ua)[:48]


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _new_group(key: str) -> dict[str, Any]:
    return {
        "key": key,
        "requests": 0,
        "success": 0,
        "failed": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "reasoning_tokens": 0,
        "cost_usd": 0.0,
        "priced_requests": 0,
        "unpriced_requests": 0,
        "_latency_sum": 0.0,
        "_latency_n": 0,
    }


def _add(group: dict[str, Any], d: Derived) -> None:
    usage = d.usage
    group["requests"] += 1
    if d.failed:
        group["failed"] += 1
    else:
        group["success"] += 1
    group["input_tokens"] += usage["input_total"]
    group["output_tokens"] += usage["output"]
    group["cache_read_tokens"] += usage["cache_read"]
    group["cache_write_tokens"] += usage["cache_write"]
    group["reasoning_tokens"] += usage["reasoning"]
    cost = d.cost["cost_usd"]
    if cost is None:
        group["unpriced_requests"] += 1
    else:
        group["priced_requests"] += 1
        group["cost_usd"] += cost
    if d.latency is not None:
        group["_latency_sum"] += d.latency
        group["_latency_n"] += 1


def _finish(groups: dict[str, dict[str, Any]], total_requests: int, key_name: str) -> list[dict[str, Any]]:
    rows = []
    for group in groups.values():
        latency_sum, latency_n = group.pop("_latency_sum"), group.pop("_latency_n")
        group["avg_latency_ms"] = round(latency_sum / latency_n, 1) if latency_n else None
        group["share_pct"] = round(100 * group["requests"] / total_requests, 2) if total_requests else 0.0
        # A group with only unpriced traffic has no cost at all, not a zero cost.
        if group["priced_requests"] == 0:
            group["cost_usd"] = None
        group[key_name] = group.pop("key")
        rows.append(group)
    rows.sort(key=lambda row: (-row["requests"], str(row[key_name])))
    return rows


def enrich(record: dict[str, Any]) -> dict[str, Any]:
    """A record as returned by the API: stored fields plus computed cost and local time."""
    cost = record_cost(record)
    ts = parse_ts(record.get("timestamp"))
    out = {k: v for k, v in record.items() if k not in ("ratelimit",)}
    out.update(
        timestamp_local=local_iso(ts),
        cost_usd=cost["cost_usd"],
        cost_quality=cost["cost_quality"],
        price_status=cost["price_status"],
        usage=cost["usage"],
    )
    return out


def recent(records: list[dict[str, Any]], limit: int = 100) -> list[dict[str, Any]]:
    newest = heapq.nlargest(max(0, limit), records, key=lambda r: (str(r.get("timestamp") or ""), str(r.get("ingested_at") or "")))
    return [enrich(record) for record in newest]


def aggregate(records: list[dict[str, Any]], window: str = "24h", now: datetime | None = None, cache: DerivedCache | None = None) -> dict[str, Any]:
    now = now or utc_now()
    selected = _select(_derive(records, cache), window, now)
    per_model: dict[str, dict[str, Any]] = {}
    per_key: dict[str, dict[str, Any]] = {}
    per_endpoint: dict[str, dict[str, Any]] = {}
    per_ip: dict[str, dict[str, Any]] = {}
    per_ua: dict[str, dict[str, Any]] = {}
    per_day: dict[str, dict[str, Any]] = {}
    series: dict[str, dict[str, Any]] = {}
    key_masks: dict[str, str | None] = {}
    total = _new_group("total")
    latencies: list[float] = []
    ttfts: list[float] = []
    stream = non_stream = 0
    cost_quality: dict[str, int] = {}
    unpriced_models: dict[str, int] = {}
    status_codes: dict[str, int] = {}
    first_ts: datetime | None = None
    hourly = window == "24h"

    for _record, d in selected:
        ts = d.ts
        if ts and (first_ts is None or ts < first_ts):
            first_ts = ts
        _add(total, d)
        model = d.model
        key_masks[d.key_name] = d.key_masked
        for groups, key in ((per_model, model), (per_key, d.key_name), (per_endpoint, d.endpoint), (per_ip, d.ip), (per_ua, d.ua)):
            group = groups.get(key)
            if group is None:
                group = groups[key] = _new_group(key)
            _add(group, d)
        if ts:
            for groups, key in ((per_day, d.day), (series, d.hour if hourly else d.day)):
                group = groups.get(key)
                if group is None:
                    group = groups[key] = _new_group(key)
                _add(group, d)
        if d.latency is not None:
            latencies.append(d.latency)
        if d.ttft:
            ttfts.append(d.ttft)
        if d.stream:
            stream += 1
        else:
            non_stream += 1
        quality = d.cost["cost_quality"]
        cost_quality[quality] = cost_quality.get(quality, 0) + 1
        if d.cost["cost_usd"] is None:
            unpriced_models[model] = unpriced_models.get(model, 0) + 1
        status_codes[d.status] = status_codes.get(d.status, 0) + 1

    requests = total["requests"]
    span = WINDOWS[window]
    if span is not None:
        minutes = span.total_seconds() / 60
    elif first_ts is not None:
        minutes = max(1.0, (now - first_ts).total_seconds() / 60)
    else:
        minutes = 0.0
    summary = {
        "requests": requests,
        "success": total["success"],
        "failed": total["failed"],
        "success_pct": round(100 * total["success"] / requests, 2) if requests else None,
        "failed_pct": round(100 * total["failed"] / requests, 2) if requests else None,
        "input_tokens": total["input_tokens"],
        "output_tokens": total["output_tokens"],
        "cache_read_tokens": total["cache_read_tokens"],
        "cache_write_tokens": total["cache_write_tokens"],
        "reasoning_tokens": total["reasoning_tokens"],
        "estimated_cost_usd": round(total["cost_usd"], 8) if total["priced_requests"] else (0.0 if requests == 0 else None),
        "priced_requests": total["priced_requests"],
        "unpriced_requests": total["unpriced_requests"],
        "unpriced_models": unpriced_models,
        "cost_quality": cost_quality,
        "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
        "p50_latency_ms": percentile(latencies, 50),
        "p95_latency_ms": percentile(latencies, 95),
        "avg_ttft_ms": round(sum(ttfts) / len(ttfts), 1) if ttfts else None,
        "ttft_samples": len(ttfts),
        "stream_requests": stream,
        "non_stream_requests": non_stream,
        "requests_per_min": round(requests / minutes, 4) if minutes else None,
        "window_minutes": round(minutes, 2),
        "status_codes": status_codes,
    }
    per_key_rows = _finish(per_key, requests, "key_name")
    for row in per_key_rows:
        row["key_masked"] = key_masks.get(row["key_name"])
    return {
        "window": window,
        "generated_at": iso_utc(now),
        "generated_at_local": local_iso(now),
        "window_start": iso_utc(now - span) if span else (iso_utc(first_ts) if first_ts else None),
        "timezone": "Europe/Amsterdam",
        "cost_basis": "list-price equivalent, not billed",
        "summary": summary,
        "per_model": _finish(per_model, requests, "model"),
        "per_key": per_key_rows,
        "per_endpoint": _finish(per_endpoint, requests, "endpoint"),
        "per_client_ip": _finish(per_ip, requests, "client_ip"),
        "per_user_agent": _finish(per_ua, requests, "user_agent"),
        "per_day": sorted(_finish(per_day, requests, "day"), key=lambda row: row["day"]),
        "series_granularity": "hour" if hourly else "day",
        "series": sorted(_finish(series, requests, "bucket"), key=lambda row: row["bucket"]),
    }
