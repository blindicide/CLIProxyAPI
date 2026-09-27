"""Aggregation of persisted usage records into dashboard analytics."""
from __future__ import annotations

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


def filter_window(records: Iterable[dict[str, Any]], window: str, now: datetime | None = None) -> list[dict[str, Any]]:
    if window not in WINDOWS:
        raise ValueError(f"unknown window {window!r}; expected one of {', '.join(WINDOWS)}")
    span = WINDOWS[window]
    if span is None:
        return list(records)
    cutoff = (now or utc_now()) - span
    out = []
    for record in records:
        ts = parse_ts(record.get("timestamp"))
        if ts and ts >= cutoff:
            out.append(record)
    return out


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
        "_latency": [],
    }


def _add(group: dict[str, Any], record: dict[str, Any], cost: dict[str, Any]) -> None:
    usage = cost["usage"]
    group["requests"] += 1
    if record.get("failed"):
        group["failed"] += 1
    else:
        group["success"] += 1
    group["input_tokens"] += usage["input_total"]
    group["output_tokens"] += usage["output"]
    group["cache_read_tokens"] += usage["cache_read"]
    group["cache_write_tokens"] += usage["cache_write"]
    group["reasoning_tokens"] += usage["reasoning"]
    if cost["cost_usd"] is None:
        group["unpriced_requests"] += 1
    else:
        group["priced_requests"] += 1
        group["cost_usd"] += cost["cost_usd"]
    latency = _num(record.get("latency_ms"))
    if latency is not None:
        group["_latency"].append(latency)


def _finish(groups: dict[str, dict[str, Any]], total_requests: int, key_name: str) -> list[dict[str, Any]]:
    rows = []
    for group in groups.values():
        latency = group.pop("_latency")
        group["avg_latency_ms"] = round(sum(latency) / len(latency), 1) if latency else None
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
    ordered = sorted(records, key=lambda r: (str(r.get("timestamp") or ""), str(r.get("ingested_at") or "")), reverse=True)
    return [enrich(record) for record in ordered[: max(0, limit)]]


def aggregate(records: list[dict[str, Any]], window: str = "24h", now: datetime | None = None) -> dict[str, Any]:
    now = now or utc_now()
    selected = filter_window(records, window, now)
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

    for record in selected:
        cost = record_cost(record)
        ts = parse_ts(record.get("timestamp"))
        if ts and (first_ts is None or ts < first_ts):
            first_ts = ts
        _add(total, record, cost)
        model = str(record.get("model") or "unknown")
        _add(per_model.setdefault(model, _new_group(model)), record, cost)
        key_name = str(record.get("api_key_name") or "no-key")
        key_masks[key_name] = record.get("api_key_masked")
        _add(per_key.setdefault(key_name, _new_group(key_name)), record, cost)
        endpoint = str(record.get("endpoint_path") or record.get("endpoint") or "unknown")
        _add(per_endpoint.setdefault(endpoint, _new_group(endpoint)), record, cost)
        ip = str(record.get("resolved_client_ip") or record.get("client_ip") or "unknown")
        _add(per_ip.setdefault(ip, _new_group(ip)), record, cost)
        ua = short_user_agent(record.get("user_agent"))
        _add(per_ua.setdefault(ua, _new_group(ua)), record, cost)
        if ts:
            local = ts.astimezone(LOCAL_TZ)
            day = local.strftime("%Y-%m-%d")
            _add(per_day.setdefault(day, _new_group(day)), record, cost)
            bucket = local.strftime("%Y-%m-%dT%H:00") if hourly else day
            _add(series.setdefault(bucket, _new_group(bucket)), record, cost)
        latency = _num(record.get("latency_ms"))
        if latency is not None:
            latencies.append(latency)
        ttft = _num(record.get("ttft_ms"))
        if ttft:
            ttfts.append(ttft)
        if record.get("stream"):
            stream += 1
        else:
            non_stream += 1
        cost_quality[cost["cost_quality"]] = cost_quality.get(cost["cost_quality"], 0) + 1
        if cost["cost_usd"] is None:
            unpriced_models[model] = unpriced_models.get(model, 0) + 1
        code = str(record.get("status_code") if record.get("status_code") is not None else "unknown")
        status_codes[code] = status_codes.get(code, 0) + 1

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
