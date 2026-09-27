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
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger("cproxy-ui.ingest")

MAX_POPS_PER_CYCLE = 500
# Records per usage-queue GET. cproxy's queue pops without ack, so a SIGKILL between a pop and
# its fsync loses exactly that batch (proven by tools/chaos_drill.py kill_after_pop): keep it
# small. 500 pops x 10 still drains 5,000 records per cycle.
POP_BATCH = 10
FAIL_BODY_LIMIT = 600
# cproxy's default for redis-usage-queue-retention-seconds; the live value is read from /config.
DEFAULT_QUEUE_RETENTION_SECONDS = 60.0
MAX_LOSS_WINDOWS = 50

# In-memory records are parsed from the exact JSON line written to disk, minus fields nothing
# reads back (analytics and /api/requests ignore ``ratelimit``; it stays on disk). Keys and
# values that repeat across records are interned: json.loads would otherwise allocate fresh
# copies for every line (measured ~8.2 KB -> ~2 KB per record).
IN_MEMORY_DROP = frozenset({"ratelimit"})
_REPEATED_VALUES = frozenset(
    {
        "model", "alias", "response_model", "provider", "executor_type", "endpoint", "endpoint_path",
        "source", "auth_index", "auth_type", "access_token_sha256", "client_ip", "resolved_client_ip",
        "x_forwarded_for", "user_agent", "api_key_masked", "api_key_name", "reasoning_effort",
        "service_tier", "quality", "fail_body",
    }
)


def _compact_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in IN_MEMORY_DROP:
            continue
        key = sys.intern(key)
        if key in _REPEATED_VALUES and isinstance(value, str):
            value = sys.intern(value)
        out[key] = value
    return out


def load_compact(line: str) -> Any:
    return json.loads(line, object_pairs_hook=_compact_pairs)


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


def loss_window(last_ok: datetime | None, now: datetime, retention_s: float) -> dict[str, Any] | None:
    """The span whose usage records were pruned unread, if the drainer was away too long.

    cproxy drops queue items older than ``retention_s``. With successful drains at ``last_ok``
    and ``now``, anything enqueued in (last_ok, now - retention_s) expired before this drain.
    """
    if last_ok is None:
        return None
    end = now - timedelta(seconds=retention_s)
    if end <= last_ok:
        return None
    return {
        "from": iso_utc(last_ok),
        "to": iso_utc(end),
        "unobserved_s": round((end - last_ok).total_seconds(), 1),
        "retention_s": retention_s,
        "detected_at": iso_utc(now),
    }


def retention_from_config(payload: Any) -> float | None:
    """Only this one field is read from ``GET /v0/management/config``; the rest is discarded."""
    value = payload.get("redis-usage-queue-retention-seconds") if isinstance(payload, dict) else None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    # cproxy treats <= 0 as its default and clamps to 3600.
    return min(seconds, 3600.0) if seconds > 0 else DEFAULT_QUEUE_RETENTION_SECONDS


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
        self.corrupt_lines = 0
        self.pending: list[dict[str, Any]] = []
        # Size to cut the file back to before the next write: set when a failed write could not
        # be rolled back immediately (e.g. read-only filesystem).
        self._truncate_to: int | None = None
        self.state: dict[str, Any] = {
            "total_ingested": 0,
            "duplicates_skipped": 0,
            "malformed_skipped": 0,
            "last_drain_at": None,
            "last_ingest_at": None,
            "last_error": None,
            "last_error_at": None,
            "last_ok_at": None,
            "last_write_error": None,
            "loss_windows": [],
            "loss_windows_total": 0,
            "archived_total": 0,
            "archive_last_attempt_at": None,
            "archive_last_success_at": None,
            "archive_last_error": None,
            "archive_last_result": None,
            "reconcile_missing": 0,
        }
        self._load()

    def _load(self) -> None:
        try:
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                self.state.update({k: v for k, v in saved.items() if k in self.state})
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        # Stream line by line: reading the whole file first tripled the load-time peak.
        torn_tail = False
        try:
            handle = self.requests_path.open("rb")
        except FileNotFoundError:
            return
        with handle:
            for raw in handle:
                torn_tail = not raw.endswith(b"\n")
                line = raw.decode("utf-8", errors="replace")
                if not line.strip():
                    continue
                try:
                    record = load_compact(line)
                except json.JSONDecodeError:
                    record = None
                if not isinstance(record, dict) or not record.get("id"):
                    self.corrupt_lines += 1
                    continue
                if record["id"] not in self.ids:
                    self.ids.add(record["id"])
                    self.records.append(record)
        if self.corrupt_lines:
            logger.warning("%s has %d corrupt line(s); they are kept on disk and ignored", self.requests_path.name, self.corrupt_lines)
        if torn_tail:
            # A crash mid-write left a torn last line. Terminate it so the next append starts on
            # a fresh line instead of being glued onto (and lost with) the fragment.
            with self.requests_path.open("ab") as out:
                out.write(b"\n")
                out.flush()
                os.fsync(out.fileno())
            logger.warning("terminated torn last line in %s", self.requests_path.name)

    def append(self, records: list[dict[str, Any]]) -> int:
        """Persist new records (fsync'd). Returns how many reached disk.

        Queue records are already popped when they get here, so a failed write (disk full,
        I/O error) must not drop them: they stay in ``pending`` and are retried first on the
        next append.
        """
        queued = {record["id"] for record in self.pending}
        fresh = []
        for record in records:
            if record["id"] in self.ids or record["id"] in queued:
                self.state["duplicates_skipped"] += 1
                continue
            queued.add(record["id"])
            fresh.append(record)
        batch = self.pending + fresh
        if not batch:
            return 0
        lines = [json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n" for record in batch]
        start: int | None = None
        try:
            self.requests_path.parent.mkdir(parents=True, exist_ok=True)
            if self._truncate_to is not None:
                self._rollback(self._truncate_to)
            start = self.requests_path.stat().st_size if self.requests_path.exists() else 0
            with self.requests_path.open("a", encoding="utf-8") as handle:
                for line in lines:
                    handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            # A write can fail part-way (ENOSPC after some lines): cut the file back to where this
            # attempt started, so the retry of the whole batch never duplicates lines on disk.
            if start is not None:
                self._truncate_to = start
                try:
                    self._rollback(start)
                except OSError:
                    pass  # e.g. read-only: done before the next write instead
            self.pending = batch
            self.state["last_write_error"] = f"{type(exc).__name__}: {exc.strerror or exc}"
            logger.error("could not persist %d usage record(s), keeping them in memory for retry: %s", len(batch), self.state["last_write_error"])
            return 0
        self.pending = []
        self.state["last_write_error"] = None
        self.ids.update(record["id"] for record in batch)
        # Same representation a restart would load: parsed back from the written lines.
        self.records.extend(load_compact(line) for line in lines)
        self.state["total_ingested"] += len(batch)
        self.state["last_ingest_at"] = iso_utc(utc_now())
        return len(batch)

    def _rollback(self, size: int) -> None:
        """Truncate requests.jsonl to ``size`` (only this process appends to it)."""
        if self.requests_path.stat().st_size > size:
            fd = os.open(self.requests_path, os.O_WRONLY)
            try:
                os.ftruncate(fd, size)
                os.fsync(fd)
            finally:
                os.close(fd)
        self._truncate_to = None

    def note_loss_window(self, window: dict[str, Any]) -> None:
        self.state["loss_windows"] = (list(self.state.get("loss_windows") or []) + [window])[-MAX_LOSS_WINDOWS:]
        self.state["loss_windows_total"] = int(self.state.get("loss_windows_total") or 0) + 1
        logger.warning(
            "usage records enqueued between %s and %s were pruned unread (%.0f s unobserved, queue retention %.0f s)",
            window["from"], window["to"], window["unobserved_s"], window["retention_s"],
        )

    def save_state(self) -> None:
        payload = {**self.state, "records_in_file": len(self.records)}
        tmp = self.state_path.with_suffix(".tmp")
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            os.replace(tmp, self.state_path)
        except OSError as exc:
            # State is advisory (records.jsonl is the source of truth); never abort a drain over it.
            logger.error("could not save ingest state: %s", exc)


Fetch = Callable[..., Awaitable[Any]]


async def drain_queue(
    fetch: Fetch,
    store: RecordStore,
    key_names: dict[str, str],
    *,
    max_pops: int = MAX_POPS_PER_CYCLE,
    batch: int = POP_BATCH,
    stop: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Pop the usage queue until it returns ``[]`` (bounded), persisting every batch before the next pop.

    ``fetch(path, **params)`` returns the decoded JSON body of a management GET. ``stop()`` is
    checked before each pop: once shutdown starts no new pop begins, but one already in flight
    is still persisted.
    """
    popped = stored = pops = 0
    ids: list[str] = []
    while pops < max_pops and not (stop and stop()):
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
