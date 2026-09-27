"""CSV export of usage records (one row per request, costs at list-price equivalent)."""
from __future__ import annotations

import csv
import io
import itertools
from typing import Any, Iterable, Iterator

from analytics import enrich

COLUMNS = (
    "timestamp_utc", "timestamp_local", "model", "endpoint", "stream", "status_code", "failed",
    "input_tokens", "uncached_input_tokens", "cache_read_tokens", "cache_write_tokens",
    "output_tokens", "reasoning_tokens", "cost_usd", "cost_quality", "price_status",
    "latency_ms", "ttft_ms", "api_key_name", "api_key_masked", "client_ip", "user_agent",
    "id", "request_id", "execution_id", "session_id", "upstream_request_id",
)

# A cell starting with one of these is evaluated as a formula by spreadsheet apps.
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def safe_cell(value: Any) -> Any:
    """Neutralise spreadsheet formula injection in client-controlled text (e.g. user agents)."""
    if isinstance(value, str) and value.startswith(_FORMULA_PREFIXES):
        return "'" + value
    return value


def row(record: dict[str, Any]) -> list[Any]:
    item = enrich(record)
    usage = item["usage"]
    values = {
        "timestamp_utc": item.get("timestamp"),
        "timestamp_local": item.get("timestamp_local"),
        "model": item.get("model"),
        "endpoint": item.get("endpoint_path") or item.get("endpoint"),
        "stream": bool(item.get("stream")),
        "status_code": item.get("status_code"),
        "failed": bool(item.get("failed")),
        "input_tokens": usage["input_total"],
        "uncached_input_tokens": usage["uncached_input"] + usage["unclassified"],
        "cache_read_tokens": usage["cache_read"],
        "cache_write_tokens": usage["cache_write"],
        "output_tokens": usage["output"],
        "reasoning_tokens": usage["reasoning"],
        "cost_usd": "" if item["cost_usd"] is None else f"{item['cost_usd']:.8f}",
        "cost_quality": item["cost_quality"],
        "price_status": item["price_status"],
        "latency_ms": item.get("latency_ms"),
        "ttft_ms": item.get("ttft_ms"),
        "api_key_name": item.get("api_key_name"),
        "api_key_masked": item.get("api_key_masked"),
        "client_ip": item.get("resolved_client_ip") or item.get("client_ip"),
        "user_agent": item.get("user_agent"),
        "id": item.get("id"),
        "request_id": item.get("request_id"),
        "execution_id": item.get("execution_id"),
        "session_id": item.get("session_id"),
        "upstream_request_id": item.get("upstream_request_id"),
    }
    return [safe_cell(values[column]) for column in COLUMNS]


def csv_lines(records: Iterable[dict[str, Any]], archived: Iterable[dict[str, Any]] = ()) -> Iterator[str]:
    """Header + one line per record, oldest first, as text chunks for a streaming response.

    ``archived`` (streamed from data/archive; every archived record is older than the live
    ones) is written first in archive order - the order records were ingested, which is not
    strictly by timestamp - without being held in memory. Live records follow, oldest first."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")

    def flush() -> str:
        text = buffer.getvalue()
        buffer.seek(0)
        buffer.truncate()
        return text

    writer.writerow(COLUMNS)
    yield flush()
    ordered = sorted(records, key=lambda r: (str(r.get("timestamp") or ""), str(r.get("ingested_at") or "")))
    for index, record in enumerate(itertools.chain(archived, ordered), start=1):
        writer.writerow(row(record))
        if index % 500 == 0:
            yield flush()
    yield flush()
