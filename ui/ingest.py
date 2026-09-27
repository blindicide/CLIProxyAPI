"""Usage-queue ingestion: drain, normalise, mask and persist cproxy usage records.

The management ``usage-queue`` endpoint pops records destructively (FIFO) and the backend
prunes anything older than its retention window (60 s by default), so the drain loop keeps
popping until the queue is empty and fsyncs each batch before popping the next one.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger("cproxy-ui.ingest")

MAX_POPS_PER_CYCLE = 500
POP_BATCH = 50
FAIL_BODY_LIMIT = 600

# Only the rate-limit signals and upstream request id are kept from response headers.
_HEADER_PREFIX = "anthropic-ratelimit-"
_FRACTION = re.compile(r"(\.\d{6})\d+")


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_ts(value: Any) -> datetime | None:
    """Parse RFC3339 timestamps, including Go's nanosecond precision, into aware UTC datetimes."""
    if not isinstance(value, str) or not value:
        return None
    text = _FRACTION.sub(r"\1", value.strip()).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def key_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def mask_key(raw: str | None) -> str | None:
    """Irreversible display form of a client API key: ``sha256:<12 hex>…<last 4>``."""
    if not raw:
        return None
    tail = raw[-4:] if len(raw) >= 12 else ""
    return f"sha256:{key_hash(raw)[:12]}…{tail}"


def key_names_from_config(payload: Any) -> dict[str, str]:
    """Map sha256(key) -> display name from ``GET /v0/management/api-keys``.

    The endpoint returns bare key values, so the name is the key's position in config
    (``key-1``, ``key-2``…). Only hashes are kept; raw values are discarded immediately.
    """
    keys = payload.get("api-keys") if isinstance(payload, dict) else None
    names: dict[str, str] = {}
    if isinstance(keys, list):
        for index, value in enumerate(keys, start=1):
            if isinstance(value, str) and value:
                names[key_hash(value)] = f"key-{index}"
    return names


def _headers(record: dict[str, Any]) -> dict[str, str]:
    headers = record.get("response_headers")
    out: dict[str, str] = {}
    if not isinstance(headers, dict):
        return out
    for name, value in headers.items():
        lowered = str(name).lower()
        if lowered.startswith(_HEADER_PREFIX) or lowered == "request-id":
            if isinstance(value, list):
                value = value[0] if value else ""
            out[lowered] = str(value)
    return out


def ratelimit_from_signals(signals: dict[str, str]) -> dict[str, Any]:
    """Convert Anthropic unified rate-limit headers/signals into a compact structure."""
    lowered = {str(k).lower(): v for k, v in signals.items()}

    def get(name: str) -> Any:
        value = lowered.get(_HEADER_PREFIX + "unified-" + name)
        if isinstance(value, list):
            value = value[0] if value else None
        return value

    def as_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def as_epoch(value: Any) -> int | None:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    windows = {}
    for window in ("5h", "7d"):
        windows[window] = {
            "utilization": as_float(get(f"{window}-utilization")),
            "status": get(f"{window}-status"),
            "reset_epoch": as_epoch(get(f"{window}-reset")),
        }
    return {
        "windows": windows,
        "status": get("status"),
        "reset_epoch": as_epoch(get("reset")),
        "representative_claim": get("representative-claim"),
        "overage_status": get("overage-status"),
        "overage_disabled_reason": get("overage-disabled-reason"),
        "fallback_percentage": as_float(get("fallback-percentage")),
    }


def record_id(record: dict[str, Any]) -> str:
    execution_id = str(record.get("execution_id") or "")
    request_id = str(record.get("request_id") or "")
    if execution_id or request_id:
        return f"{execution_id}:{request_id}"
    stable = json.dumps({k: v for k, v in record.items() if k != "api_key"}, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(stable.encode("utf-8")).hexdigest()[:32]


def _endpoint_path(endpoint: str) -> str:
    parts = endpoint.split(" ", 1)
    return parts[1] if len(parts) == 2 else endpoint


def normalize_record(raw: dict[str, Any], key_names: dict[str, str], ingested_at: datetime | None = None) -> dict[str, Any]:
    """Build the persisted form of one queue record. The raw ``api_key`` never leaves this function."""
    raw_key = raw.get("api_key") if isinstance(raw.get("api_key"), str) else None
    ts = parse_ts(raw.get("timestamp"))
    fail = raw.get("fail") if isinstance(raw.get("fail"), dict) else {}
    headers = _headers(raw)
    endpoint = str(raw.get("endpoint") or "")
    status_code = fail.get("status_code")
    try:
        status_code = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        status_code = None
    fail_body = str(fail.get("body") or "")[:FAIL_BODY_LIMIT]
    record = {
        "id": record_id(raw),
        "timestamp": iso_utc(ts) if ts else None,
        "timestamp_source": raw.get("timestamp"),
        "ingested_at": iso_utc(ingested_at or utc_now()),
        "model": raw.get("model") or raw.get("alias") or "unknown",
        "alias": raw.get("alias"),
        "response_model": raw.get("response_model"),
        "provider": raw.get("provider"),
        "executor_type": raw.get("executor_type"),
        "endpoint": endpoint,
        "endpoint_path": _endpoint_path(endpoint),
        "stream": bool(raw.get("stream")),
        "generate": bool(raw.get("generate")),
        "failed": bool(raw.get("failed")),
        "status_code": status_code,
        "fail_body": fail_body,
        "latency_ms": raw.get("latency_ms"),
        "ttft_ms": raw.get("ttft_ms"),
        "source": raw.get("source"),
        "auth_index": raw.get("auth_index"),
        "auth_type": raw.get("auth_type"),
        "access_token_sha256": raw.get("access_token_sha256"),
        "client_ip": raw.get("client_ip"),
        "resolved_client_ip": raw.get("resolved_client_ip"),
        "x_forwarded_for": raw.get("x_forwarded_for"),
        "user_agent": raw.get("user_agent"),
        "api_key_masked": mask_key(raw_key),
        "api_key_name": (key_names.get(key_hash(raw_key)) if raw_key else None) or ("unknown-key" if raw_key else "no-key"),
        "tokens": raw.get("tokens") if isinstance(raw.get("tokens"), dict) else {},
        "token_breakdown": raw.get("token_breakdown") if isinstance(raw.get("token_breakdown"), dict) else {},
        "accounting_version": raw.get("accounting_version"),
        "request_id": raw.get("request_id"),
        "execution_id": raw.get("execution_id"),
        "trace_id": raw.get("trace_id"),
        "session_id": raw.get("session_id"),
        "reasoning_effort": raw.get("reasoning_effort"),
        "service_tier": raw.get("service_tier"),
        "upstream_request_id": headers.get("request-id"),
        "ratelimit": ratelimit_from_signals(headers) if headers else None,
    }
    if raw_key:
        # Defence in depth: scrub the raw key from every string field (e.g. echoed error bodies).
        text = json.dumps(record, ensure_ascii=False)
        if raw_key in text:
            record = json.loads(text.replace(raw_key, record["api_key_masked"] or "[masked]"))
    return record


class RecordStore:
    """Append-only JSONL store with id-based dedupe and a small ingest state file."""

    def __init__(self, requests_path: Path, state_path: Path) -> None:
        self.requests_path = requests_path
        self.state_path = state_path
        self.ids: set[str] = set()
        self.records: list[dict[str, Any]] = []
        self.state: dict[str, Any] = {
            "total_ingested": 0,
            "duplicates_skipped": 0,
            "malformed_skipped": 0,
            "last_drain_at": None,
            "last_ingest_at": None,
            "last_error": None,
            "last_error_at": None,
            "last_ok_at": None,
        }
        self._load()

    def _load(self) -> None:
        try:
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                self.state.update({k: v for k, v in saved.items() if k in self.state})
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        try:
            with self.requests_path.open(encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("skipping corrupt line in %s", self.requests_path.name)
                        continue
                    if isinstance(record, dict) and record.get("id") not in self.ids:
                        self.ids.add(record.get("id"))
                        self.records.append(record)
        except FileNotFoundError:
            pass

    def append(self, records: list[dict[str, Any]]) -> int:
        fresh = []
        for record in records:
            if record["id"] in self.ids:
                self.state["duplicates_skipped"] += 1
                continue
            self.ids.add(record["id"])
            fresh.append(record)
        if fresh:
            self.requests_path.parent.mkdir(parents=True, exist_ok=True)
            with self.requests_path.open("a", encoding="utf-8") as handle:
                for record in fresh:
                    handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self.records.extend(fresh)
            self.state["total_ingested"] += len(fresh)
            self.state["last_ingest_at"] = iso_utc(utc_now())
        return len(fresh)

    def save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        payload = {**self.state, "records_in_file": len(self.records)}
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, self.state_path)


Fetch = Callable[..., Awaitable[Any]]


async def drain_queue(fetch: Fetch, store: RecordStore, key_names: dict[str, str], *, max_pops: int = MAX_POPS_PER_CYCLE, batch: int = POP_BATCH) -> dict[str, Any]:
    """Pop the usage queue until it returns ``[]`` (bounded), persisting every batch before the next pop.

    ``fetch(path, **params)`` returns the decoded JSON body of a management GET.
    """
    popped = stored = pops = 0
    ids: list[str] = []
    while pops < max_pops:
        payload = await fetch("usage-queue", count=batch)
        pops += 1
        items = payload if isinstance(payload, list) else []
        if not items:
            break
        normalised = []
        for item in items:
            if isinstance(item, str):
                try:
                    item = json.loads(item)
                except json.JSONDecodeError:
                    item = None
            if not isinstance(item, dict):
                store.state["malformed_skipped"] += 1
                logger.warning("skipping non-object usage-queue item")
                continue
            if item.get("refresh") or item.get("support_refresh"):
                continue
            normalised.append(normalize_record(item, key_names))
        popped += len(items)
        stored += store.append(normalised)
        ids.extend(r["id"] for r in normalised)
    store.state["last_drain_at"] = iso_utc(utc_now())
    store.save_state()
    return {"pops": pops, "popped": popped, "stored": stored, "ids": ids, "exhausted": pops >= max_pops}
