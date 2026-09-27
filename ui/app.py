"""cproxy-ui: analytics dashboard for the cproxy Claude Code subscription relay."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import re
import os
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.concurrency import iterate_in_threadpool, run_in_threadpool
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import archiver
import lifetime
from analytics import LOCAL_TZ, WINDOWS, DerivedCache, aggregate, filter_window, local_iso, recent
from export import csv_lines
from ingest import DEFAULT_QUEUE_RETENTION_SECONDS, RecordStore, drain_queue, iso_utc, key_names_from_config, loss_window, mask_key, parse_ts, ratelimit_from_signals, retention_from_config, utc_now
from tools import datastore
from pricing import AS_OF, BASIS, PRICING, SOURCE_URL, pricing_payload, resolve_model
from version import BUILD_DATE, SERVICE, VERSION

ROOT = Path(__file__).resolve().parent
MANAGEMENT_URL = os.getenv("CPROXY_MANAGEMENT_URL", "http://127.0.0.1:31524/v0/management")
POLL_INTERVAL_SECONDS = 2.0
KEY_NAMES_TTL_SECONDS = 60.0
# A management call is "fresh" if the poller succeeded within this many seconds.
STALE_AFTER_SECONDS = 30.0
# Identical /api/analytics queries within this many seconds reuse the last result unless
# new records arrived; the dashboard polls every 30 s per viewer.
ANALYTICS_TTL_SECONDS = 5.0
# On shutdown, let an in-flight queue pop finish and be persisted (cproxy has already removed
# those records). Longer than the 10 s HTTP timeout, well below systemd's 90 s stop timeout.
SHUTDOWN_GRACE_SECONDS = 15.0
# Storage warnings shown on the dashboard (history is never truncated automatically).
DISK_FREE_WARN_BYTES = 2 * 1024**3
BACKUP_STALE_SECONDS = 48 * 3600
BACKUP_PREFIX = "cproxy-ui-backup-"
# Warn when the service uses this share of its cgroup memory limit (MemoryHigh, else MemoryMax).
MEMORY_WARN_FRACTION = 0.7
# Automatic archiving (archiver.py): checked hourly, runs after ARCHIVE_HOUR local time and at
# most once per ARCHIVE_MIN_INTERVAL - after the 03:17 backup timer.
ARCHIVE_CHECK_SECONDS = 3600
ARCHIVE_HOUR = 4
ARCHIVE_MIN_INTERVAL = 20 * 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("cproxy-ui")
# httpx logs every request at INFO; the poller runs every 2 s.
logging.getLogger("httpx").setLevel(logging.WARNING)


SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
    "Cross-Origin-Opener-Policy": "same-origin",
}


def dashboard_csp(html: str) -> str:
    """Content-Security-Policy for the dashboard, pinning each inline <script> by its sha256.

    Inline ``style=`` attributes (bar widths, legend swatches) need 'unsafe-inline' for styles;
    scripts get no such allowance, so injected markup cannot execute code.
    """
    hashes = [
        "'sha256-" + base64.b64encode(hashlib.sha256(body.encode("utf-8")).digest()).decode("ascii") + "'"
        for body in re.findall(r"<script>(.*?)</script>", html, flags=re.DOTALL)
    ]
    return "; ".join(
        [
            "default-src 'none'",
            "script-src " + (" ".join(hashes) or "'none'"),
            "style-src 'unsafe-inline'",
            "img-src 'self' data:",
            "connect-src 'self'",
            "base-uri 'none'",
            "form-action 'none'",
            "frame-ancestors 'none'",
        ]
    )


class ManagementError(RuntimeError):
    """The cproxy management API is unreachable or rejected the request."""


class ManagementAPI:
    def __init__(self, base_url: str, key: str | None, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.key = key
        self.client = client

    async def get(self, path: str, **params: Any) -> Any:
        if not self.key:
            raise ManagementError("CPROXY_MANAGEMENT_KEY is not configured")
        try:
            response = await self.client.get(
                f"{self.base_url}/{path.lstrip('/')}",
                params=params or None,
                headers={"Authorization": f"Bearer {self.key}"},
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ManagementError(f"management API {path} returned HTTP {exc.response.status_code}") from exc
        except httpx.HTTPError as exc:
            raise ManagementError(f"management API unreachable ({type(exc).__name__})") from exc
        try:
            return response.json()
        except ValueError as exc:
            raise ManagementError(f"management API {path} returned invalid JSON") from exc


def _age_seconds(value: str | None, now: datetime) -> float | None:
    ts = parse_ts(value)
    return round((now - ts).total_seconds(), 1) if ts else None


def _epoch_times(epoch: int | None) -> dict[str, Any]:
    if epoch is None:
        return {"epoch": None, "utc": None, "local": None}
    dt = datetime.fromtimestamp(epoch, UTC)
    return {"epoch": epoch, "utc": iso_utc(dt), "local": local_iso(dt)}


def quota_from_auth_files(payload: Any, now: datetime) -> dict[str, Any]:
    """Credential table + unified 5h/7d quota from ``GET /v0/management/auth-files``."""
    files = payload.get("files") if isinstance(payload, dict) else None
    credentials = []
    for item in files if isinstance(files, list) else []:
        if not isinstance(item, dict):
            continue
        observed_at = None
        signals: dict[str, Any] = {}
        quota = item.get("quota") if isinstance(item.get("quota"), dict) else {}
        if isinstance(quota.get("signals"), dict) and quota["signals"]:
            signals, observed_at = quota["signals"], quota.get("observed_at")
        else:
            # Fall back to the most recently observed per-model signal set.
            model_quotas = item.get("model_quotas") if isinstance(item.get("model_quotas"), dict) else {}
            latest = max(
                (q for q in model_quotas.values() if isinstance(q, dict) and isinstance(q.get("signals"), dict)),
                key=lambda q: str(q.get("observed_at") or ""),
                default=None,
            )
            if latest:
                signals, observed_at = latest["signals"], latest.get("observed_at")
        limits = ratelimit_from_signals(signals) if signals else None
        if limits:
            for window in limits["windows"].values():
                epoch = window.pop("reset_epoch")
                window["reset"] = _epoch_times(epoch)
                util = window["utilization"]
                # used_pct is as observed on the last upstream response. Once the window's reset
                # time has passed, that figure no longer describes the current window.
                window["used_pct"] = round(util * 100, 1) if util is not None else None
                window["reset_passed"] = epoch is not None and epoch <= now.timestamp()
            limits["reset"] = _epoch_times(limits.pop("reset_epoch"))
        cooldowns = item.get("cooldowns") if isinstance(item.get("cooldowns"), list) else []
        credentials.append(
            {
                "id": item.get("id") or item.get("name"),
                "label": item.get("label") or item.get("email") or item.get("account"),
                "provider": item.get("provider"),
                "account_type": item.get("account_type"),
                "auth_index": item.get("auth_index"),
                "disabled": bool(item.get("disabled")),
                "failed": item.get("failed") if isinstance(item.get("failed"), int) else int(bool(item.get("failed"))),
                "cooldowns": cooldowns,
                "note": item.get("note"),
                "quota_observed_at": observed_at,
                "quota_observed_age_s": _age_seconds(observed_at, now),
                "quota_models": sorted((item.get("model_quotas") or {}).keys()) if isinstance(item.get("model_quotas"), dict) else [],
                "limits": limits,
                "recent_requests": item.get("recent_requests") if isinstance(item.get("recent_requests"), list) else [],
            }
        )
    return {"credentials": credentials, "has_data": any(c["limits"] for c in credentials)}


def memory_status(proc_root: Path = Path("/proc"), cgroup_root: Path = Path("/sys/fs/cgroup")) -> dict[str, Any]:
    """This process's RSS against its cgroup memory limit (systemd MemoryHigh/MemoryMax)."""
    rss = limit = None
    limit_kind = None
    try:
        for line in (proc_root / "self" / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
    except OSError:
        pass
    try:
        path = next(l.split("::", 1)[1] for l in (proc_root / "self" / "cgroup").read_text().splitlines() if l.startswith("0::"))
        group = cgroup_root / path.strip().lstrip("/")
        for kind in ("memory.high", "memory.max"):
            value = (group / kind).read_text().strip()
            if value != "max":
                limit, limit_kind = int(value), kind
                break
    except (OSError, StopIteration, ValueError):
        pass
    used = round(rss / limit, 3) if rss is not None and limit else None
    warning = None
    if used is not None and used >= MEMORY_WARN_FRACTION:
        warning = f"service memory at {used:.0%} of its {limit_kind} limit ({rss / 2**20:.0f} of {limit / 2**20:.0f} MiB); archive old history (tools/datastore.py archive)"
    return {"rss_bytes": rss, "limit_bytes": limit, "limit_kind": limit_kind, "used_fraction": used, "warning": warning}


def storage_status(data_dir: Path, now: datetime) -> dict[str, Any]:
    """Size of the history, free disk, and the newest backup written by tools/datastore.py."""
    requests_path = data_dir / "requests.jsonl"
    try:
        size = requests_path.stat().st_size
    except OSError:
        size = None
    try:
        disk = os.statvfs(data_dir)
        free = disk.f_bavail * disk.f_frsize
    except OSError:
        free = None
    backups = sorted((data_dir / "data" / "backups").glob(f"{BACKUP_PREFIX}*.tar.gz"))
    newest_at = None
    if backups:
        stamp = backups[-1].name[len(BACKUP_PREFIX) : -len(".tar.gz")]
        try:
            newest_at = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        except ValueError:
            newest_at = None
    backup_age = round((now - newest_at).total_seconds()) if newest_at else None
    memory = memory_status()
    warnings = [memory["warning"]] if memory["warning"] else []
    if free is not None and free < DISK_FREE_WARN_BYTES:
        warnings.append(f"only {free / 1024**3:.1f} GiB free on the data disk")
    if backup_age is None:
        warnings.append("no backup of requests.jsonl yet")
    elif backup_age > BACKUP_STALE_SECONDS:
        warnings.append(f"newest backup is {backup_age / 3600:.0f} h old")
    return {
        "requests_file_bytes": size,
        "disk_free_bytes": free,
        "backups": len(backups),
        "newest_backup_at": iso_utc(newest_at) if newest_at else None,
        "newest_backup_age_s": backup_age,
        "memory": memory,
        "warnings": warnings,
    }


def mask_key_usage(payload: Any) -> list[dict[str, Any]]:
    """Flatten ``/api-key-usage`` ({provider: {"base|key": {...}}}) with keys masked."""
    rows = []
    if not isinstance(payload, dict):
        return rows
    for provider, bucket in payload.items():
        if not isinstance(bucket, dict):
            continue
        for composite, entry in bucket.items():
            if not isinstance(entry, dict):
                continue
            base_url, _, key = str(composite).rpartition("|")
            rows.append({"provider": provider, "base_url": base_url or None, "key_masked": mask_key(key), "success": entry.get("success"), "failed": entry.get("failed")})
    return rows


def create_app(
    *,
    management_url: str = MANAGEMENT_URL,
    management_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    data_dir: Path = ROOT,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    start_poller: bool = True,
    clock: Any = time.monotonic,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(poller(app)) if start_poller else None
        archive_task = asyncio.create_task(archive_loop(app)) if start_poller else None
        try:
            yield
        finally:
            app.state.stopping.set()
            if archive_task:
                archive_task.cancel()
                try:
                    await archive_task
                except asyncio.CancelledError:
                    pass
            if task:
                try:
                    await asyncio.wait_for(asyncio.shield(task), SHUTDOWN_GRACE_SECONDS)
                except TimeoutError:
                    logger.error("usage drain still running after %.0f s grace; cancelling", SHUTDOWN_GRACE_SECONDS)
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            if app.state.own_client:
                await app.state.client.aclose()

    app = FastAPI(title="cproxy analytics", version=VERSION, lifespan=lifespan)
    # Dashboard polls (/api/requests is ~2 KB per row) compress ~8-10x; nginx does not gzip here.
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.state.client = client or httpx.AsyncClient(timeout=10)
    app.state.own_client = client is None
    app.state.management = ManagementAPI(management_url, management_key or os.getenv("CPROXY_MANAGEMENT_KEY"), app.state.client)
    try:
        recovered = archiver.recover_interrupted_rewrite(data_dir)
        if recovered:
            logger.warning("archive rewrite recovery: %s", recovered)
    except OSError:
        logger.exception("could not recover an interrupted archive rewrite; requests.jsonl left as is")
    app.state.store = RecordStore(data_dir / "requests.jsonl", data_dir / "data" / "ingest_state.json")
    app.state.key_names = {}
    app.state.key_names_at = 0.0
    app.state.key_names_error = None
    app.state.queue_retention_s = DEFAULT_QUEUE_RETENTION_SECONDS
    app.state.queue_retention_source = "default"
    app.state.started_at = time.monotonic()
    app.state.last_drain = None
    app.state.drain_lock = asyncio.Lock()
    app.state.stopping = asyncio.Event()
    app.state.derived = DerivedCache()
    app.state.analytics_cache = {}
    app.state.lifetime = lifetime.LifetimeCache(data_dir)

    async def refresh_key_names(app: FastAPI, force: bool = False) -> None:
        if force or time.monotonic() - app.state.key_names_at > KEY_NAMES_TTL_SECONDS:
            app.state.key_names_at = time.monotonic()
            try:
                app.state.key_names = key_names_from_config(await app.state.management.get("api-keys"))
                app.state.key_names_error = None
            except ManagementError as exc:
                # Never let key naming block the drain: unread records are pruned after the retention.
                # Previous names are kept; unknown keys are stored as "unknown-key" (still masked).
                app.state.key_names_error = str(exc)
                logger.warning("could not refresh client key names, draining anyway: %s", exc)
            try:
                retention = retention_from_config(await app.state.management.get("config"))
            except ManagementError as exc:
                retention = None
                logger.warning("could not read queue retention from cproxy config, keeping %.0f s: %s", app.state.queue_retention_s, exc)
            if retention is not None:
                app.state.queue_retention_s = retention
                app.state.queue_retention_source = "cproxy config"

    async def drain_once(app: FastAPI) -> dict[str, Any]:
        store: RecordStore = app.state.store
        async with app.state.drain_lock:
            try:
                await refresh_key_names(app)
                started = utc_now()
                result = await drain_queue(app.state.management.get, store, app.state.key_names, stop=app.state.stopping.is_set)
            except ManagementError as exc:
                store.state["last_error"] = str(exc)
                store.state["last_error_at"] = iso_utc(utc_now())
                store.save_state()
                raise
            window = loss_window(parse_ts(store.state.get("last_ok_at")), started, app.state.queue_retention_s)
            if window:
                store.note_loss_window(window)
            store.state["last_ok_at"] = store.state["last_drain_at"]
            store.state["last_error"] = None
            if window:
                store.save_state()
            app.state.last_drain = result
            if result["stored"]:
                logger.info("ingested %d usage record(s) in %d pop(s)", result["stored"], result["pops"])
            return result

    app.state.drain_once = drain_once

    async def poller(app: FastAPI) -> None:
        stopping: asyncio.Event = app.state.stopping
        while not stopping.is_set():
            try:
                result = await drain_once(app)
                if result["exhausted"]:
                    continue  # the queue still has records: drain again immediately
            except ManagementError as exc:
                logger.warning("usage drain failed: %s", exc)
            except Exception:
                logger.exception("usage drain crashed")
            try:
                await asyncio.wait_for(stopping.wait(), poll_interval)
            except TimeoutError:
                pass

    async def archive_once(app: FastAPI, now: datetime | None = None) -> dict[str, Any]:
        """Move records older than ARCHIVE_AFTER_DAYS to a verified archive (never deletes)."""
        store: RecordStore = app.state.store
        now = now or utc_now()
        cutoff = archiver.cutoff_for(now)
        store.state["archive_last_attempt_at"] = iso_utc(now)
        if store.pending:
            store.state["archive_last_error"] = "skipped: records are waiting to be written (disk problem)"
            store.save_state()
            return {"archived": 0, "skipped": "pending writes"}
        if not archiver.has_old_records(store.records, cutoff):
            store.state["archive_last_error"] = None
            store.save_state()
            return {"archived": 0}
        try:
            plan = await run_in_threadpool(archiver.prepare, data_dir, cutoff)
            if plan is None:
                store.state["archive_last_error"] = None
                store.save_state()
                return {"archived": 0}
            lock = datastore.history_lock(data_dir, exclusive=True)
            await run_in_threadpool(lock.__enter__)  # waits for a running backup to finish
            try:
                async with app.state.drain_lock:
                    await run_in_threadpool(archiver.commit, data_dir, plan)
                    old = plan["old_ids"]
                    store.records = [r for r in store.records if r.get("id") not in old]
                    store.ids -= old
                    app.state.derived = DerivedCache()
                    app.state.analytics_cache.clear()
            finally:
                await run_in_threadpool(lock.__exit__, None, None, None)
        except (archiver.ArchiveError, OSError, ValueError) as exc:
            # Everything stays where it was; only this job's temp files were removed.
            store.state["archive_last_error"] = f"{type(exc).__name__}: {exc}"
            store.save_state()
            logger.error("automatic archive failed, history left untouched: %s", exc)
            return {"archived": 0, "error": store.state["archive_last_error"]}
        result = archiver.summary(plan, now)
        store.state["archived_total"] = int(store.state.get("archived_total") or 0) + result["archived"]
        store.state["archive_last_success_at"] = iso_utc(now)
        store.state["archive_last_error"] = None
        store.state["archive_last_result"] = result
        store.save_state()
        logger.info("archived %d record(s) older than %s into %s", result["archived"], iso_utc(cutoff), result["archive"])
        return result

    app.state.archive_once = archive_once

    def archive_due(now: datetime) -> bool:
        if now.astimezone(LOCAL_TZ).hour < ARCHIVE_HOUR:
            return False
        last = parse_ts(app.state.store.state.get("archive_last_attempt_at"))
        return last is None or (now - last).total_seconds() >= ARCHIVE_MIN_INTERVAL

    app.state.archive_due = archive_due

    async def archive_loop(app: FastAPI) -> None:
        stopping: asyncio.Event = app.state.stopping
        while not stopping.is_set():
            try:
                if archive_due(utc_now()):
                    await archive_once(app)
            except Exception:
                logger.exception("archive job crashed; history left untouched")
            try:
                await asyncio.wait_for(stopping.wait(), ARCHIVE_CHECK_SECONDS)
            except TimeoutError:
                pass

    def management_status(now: datetime) -> dict[str, Any]:
        state = app.state.store.state
        ok_age = _age_seconds(state.get("last_ok_at"), now)
        reachable = state.get("last_error") is None and ok_age is not None and ok_age <= STALE_AFTER_SECONDS
        return {
            "reachable": reachable,
            "last_ok_at": state.get("last_ok_at"),
            "last_ok_age_s": ok_age,
            "last_error": state.get("last_error"),
            "last_error_at": state.get("last_error_at"),
            "key_names_error": app.state.key_names_error,
        }

    def archive_status() -> dict[str, Any]:
        state = app.state.store.state
        return {
            "after_days": archiver.ARCHIVE_AFTER_DAYS,
            "archived_total": state.get("archived_total") or 0,
            "archives": len(datastore.archive_files(data_dir)),
            "last_attempt_at": state.get("archive_last_attempt_at"),
            "last_success_at": state.get("archive_last_success_at"),
            "last_error": state.get("archive_last_error"),
            "last_result": state.get("archive_last_result"),
        }

    def ingest_status(now: datetime) -> dict[str, Any]:
        state = app.state.store.state
        return {
            "records": len(app.state.store.records),
            "total_ingested": state.get("total_ingested"),
            "duplicates_skipped": state.get("duplicates_skipped"),
            "malformed_skipped": state.get("malformed_skipped"),
            "corrupt_lines": app.state.store.corrupt_lines,
            "pending_writes": len(app.state.store.pending),
            "last_write_error": state.get("last_write_error"),
            "last_drain_at": state.get("last_drain_at"),
            "last_ingest_at": state.get("last_ingest_at"),
            "last_ingest_age_s": _age_seconds(state.get("last_ingest_at"), now),
            "poll_interval_s": poll_interval,
            "storage": storage_status(data_dir, now),
            "archive": archive_status(),
            "queue_retention_s": app.state.queue_retention_s,
            "queue_retention_source": app.state.queue_retention_source,
            "loss_windows_total": state.get("loss_windows_total") or 0,
            "loss_windows": (state.get("loss_windows") or [])[-5:],
        }

    dashboard_html = (ROOT / "dashboard.html").read_text(encoding="utf-8").replace("__CPROXY_UI_VERSION__", VERSION).replace("__CPROXY_UI_BUILD__", BUILD_DATE)
    dashboard_headers = {"Cache-Control": "no-cache", "Content-Security-Policy": dashboard_csp(dashboard_html)}

    @app.middleware("http")
    async def security_headers(request: Request, call_next: Any) -> Any:
        response = await call_next(request)
        for name, value in SECURITY_HEADERS.items():
            response.headers.setdefault(name, value)
        if request.url.path.startswith("/api/"):
            # Live figures: never let a browser or proxy serve them from cache.
            response.headers.setdefault("Cache-Control", "no-store")
        return response

    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    async def dashboard() -> HTMLResponse:
        return HTMLResponse(dashboard_html, headers=dashboard_headers)

    @app.api_route("/api/health", methods=["GET", "HEAD"])
    async def health() -> JSONResponse:
        now = utc_now()
        mgmt = management_status(now)
        payload = {
            "status": "ok" if mgmt["reachable"] and not app.state.store.pending else "degraded",
            "service": SERVICE,
            "version": VERSION,
            "build_date": BUILD_DATE,
            "now": iso_utc(now),
            "now_local": local_iso(now),
            "uptime_s": round(time.monotonic() - app.state.started_at),
            "management": mgmt,
            "ingest": ingest_status(now),
            "pricing": {"source_url": SOURCE_URL, "as_of": AS_OF},
        }
        return JSONResponse(payload)

    @app.get("/api/analytics")
    async def analytics(window: str = Query("24h")) -> JSONResponse:
        if window not in WINDOWS:
            return JSONResponse({"error": f"window must be one of {', '.join(WINDOWS)}"}, status_code=400)
        now = utc_now()
        records = app.state.store.records
        cached = app.state.analytics_cache.get(window)
        if cached and cached[1] == len(records) and clock() - cached[0] < ANALYTICS_TTL_SECONDS:
            result = dict(cached[2])
        else:
            # CPU-bound: run off the event loop so the usage poller keeps draining meanwhile.
            computed = await run_in_threadpool(aggregate, list(records), window, now, app.state.derived)
            if window == "all":
                archived = await run_in_threadpool(app.state.lifetime.archived, set(app.state.store.ids))
                computed["lifetime"] = lifetime.combine(computed["summary"], archived)
            app.state.analytics_cache[window] = (clock(), len(records), computed)
            result = dict(computed)
        result["version"] = VERSION
        result["management"] = management_status(now)
        result["ingest"] = ingest_status(now)
        result["pricing"] = {"source_url": SOURCE_URL, "as_of": AS_OF, "basis": BASIS}
        return JSONResponse(result)

    @app.get("/api/export.csv")
    async def export_csv(window: str = Query("all")) -> Any:
        if window not in WINDOWS:
            return JSONResponse({"error": f"window must be one of {', '.join(WINDOWS)}"}, status_code=400)
        now = utc_now()
        selected = await run_in_threadpool(filter_window, list(app.state.store.records), window, now, app.state.derived)
        stamp = now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        return StreamingResponse(
            iterate_in_threadpool(csv_lines(selected)),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="cproxy-usage-{window}-{stamp}.csv"'},
        )

    @app.get("/api/pricing")
    async def pricing() -> JSONResponse:
        return JSONResponse(pricing_payload())

    @app.get("/api/requests")
    async def requests(limit: int = Query(100, ge=1, le=5000)) -> JSONResponse:
        records = list(app.state.store.records)
        rows = await run_in_threadpool(recent, records, limit)
        return JSONResponse({"total": len(records), "limit": limit, "requests": rows})

    @app.get("/api/quota")
    async def quota() -> JSONResponse:
        now = utc_now()
        try:
            auth_files = await app.state.management.get("auth-files")
        except ManagementError as exc:
            return JSONResponse({"available": False, "error": str(exc), "checked_at": iso_utc(now), "credentials": [], "has_data": False}, status_code=503)
        result: dict[str, Any] = {"available": True, "checked_at": iso_utc(now), "checked_at_local": local_iso(now), **quota_from_auth_files(auth_files, now)}
        extras: dict[str, Any] = {}
        for name, path in (("api_key_usage", "api-key-usage"), ("quota_providers", "quota/providers"), ("api_keys", "api-keys")):
            try:
                extras[name] = await app.state.management.get(path)
            except ManagementError as exc:
                extras[name] = {"error": str(exc)}
        keys = extras["api_keys"].get("api-keys") if isinstance(extras["api_keys"], dict) else None
        result["client_keys"] = [{"name": f"key-{i}", "key_masked": mask_key(k)} for i, k in enumerate(keys or [], start=1) if isinstance(k, str)]
        result["upstream_key_usage"] = mask_key_usage(extras["api_key_usage"])
        providers = extras["quota_providers"].get("providers") if isinstance(extras["quota_providers"], dict) else None
        result["quota_providers"] = providers if isinstance(providers, list) else []
        return JSONResponse(result)

    @app.get("/api/models")
    async def models() -> JSONResponse:
        served: list[dict[str, Any]] = []
        error = None
        try:
            payload = await app.state.management.get("model-definitions/claude")
            items = payload.get("models") if isinstance(payload, dict) else None
            served = [m for m in items or [] if isinstance(m, dict)]
        except ManagementError as exc:
            error = str(exc)
        rows = []
        for model in served:
            model_id = str(model.get("id") or "")
            price = PRICING.get(resolve_model(model_id) or "")
            rows.append(
                {
                    "id": model_id,
                    "display_name": model.get("display_name"),
                    "context_length": model.get("context_length"),
                    "max_completion_tokens": model.get("max_completion_tokens"),
                    "price_model": price["id"] if price else None,
                    "price_status": price["price_status"] if price else "unknown_model",
                    "price": {k: price[k] for k in ("input", "output", "cache_write_5m", "cache_write_1h", "cache_read")} if price else None,
                }
            )
        return JSONResponse({"available": error is None, "error": error, "count": len(rows), "models": rows, "pricing": {"source_url": SOURCE_URL, "as_of": AS_OF}})

    return app


app = create_app()
