# cproxy-ui

Analytics dashboard for **cproxy**, the Claude Code subscription relay
(this repo's CLIProxyAPI fork). FastAPI + uvicorn, one self-contained HTML page,
no database. Operations (deploy, nginx, systemd, retention) live in
[`../deploy/cproxy/README-DEPLOY.md`](../deploy/cproxy/README-DEPLOY.md); this file
is the developer view.

Version: `version.py` (`VERSION`, `BUILD_DATE`). It is stamped into the HTML
server-side and returned by `/api/health` and `/api/analytics`.

## Layout

| file | role |
|---|---|
| `app.py` | FastAPI app factory `create_app()`, management API client, poller, endpoints, security headers |
| `ingest.py` | usage-queue drain loop, record normalisation + key masking, `RecordStore` (JSONL + state), loss-window detection |
| `pricing.py` | Anthropic list-price table, model id resolution, per-record cost |
| `analytics.py` | window filtering and aggregation (`aggregate`, `recent`), `DerivedCache` |
| `export.py` | CSV export rows (`/api/export.csv`) with formula-injection guard |
| `dashboard.html` | the whole UI: inline CSS + vanilla JS + inline SVG charts, no external assets |
| `tests/` | pytest suite; `fixtures/usage_queue_sample.redacted.json` holds real captured queue records |

## Data flow

```
cproxy :31524  --GET /v0/management/usage-queue?count=50 (pops!)-->  drain_queue()
                                                                     normalize_record()  (mask api_key)
                                                                     RecordStore.append() (fsync)
                                                                          |
                                                                   requests.jsonl  +  data/ingest_state.json
                                                                          |
                          /api/analytics, /api/requests  <--  aggregate()/recent() (DerivedCache, 5 s result cache)
                          /api/quota, /api/models        <--  live management GETs (auth-files, api-keys, ...)
```

- The queue GET is **destructive** and cproxy prunes unread items after
  `redis-usage-queue-retention-seconds` (60 s live). The poller therefore drains
  until `[]` every 2 s, persists each batch before popping the next, and never
  reads the queue anywhere else. Do not `curl` the usage-queue by hand: every
  record you pop is gone from the dashboard.
- Dedupe key is `id = "<execution_id>:<request_id>"`; replays and restarts cannot
  double-count.
- A write failure keeps records in `RecordStore.pending` and retries them; a torn
  last line (crash mid-write) is terminated on load and ignored as corrupt, never
  deleted.
- Two successful drains further apart than the retention mean records were
  pruned unread: the span is recorded as a *loss window* and shown on the
  dashboard.
- Requests cproxy rejects before provider dispatch (unknown model → 400) emit no
  usage record and can never be counted.

## Stored record (`requests.jsonl`, one JSON object per line)

Normalised from the queue record; `response_headers` is reduced to the rate-limit
signals (`ratelimit`) and `upstream_request_id`.

`id`, `timestamp` (UTC `Z`), `timestamp_source` (original, +02:00), `ingested_at`,
`model`, `alias`, `response_model`, `provider`, `executor_type`, `endpoint`
(`"POST /v1/messages"`), `endpoint_path`, `stream`, `generate`, `failed`,
`status_code`, `fail_body` (first 600 chars), `latency_ms`, `ttft_ms`, `source`,
`auth_index`, `auth_type`, `access_token_sha256`, `client_ip`,
`resolved_client_ip`, `x_forwarded_for`, `user_agent`, **`api_key_masked`**
(`sha256:<12 hex>…<last 4>`), **`api_key_name`** (`key-N` = position in cproxy's
`api-keys`), `tokens`, `token_breakdown`, `accounting_version`, `request_id`,
`execution_id`, `trace_id`, `session_id`, `reasoning_effort`, `service_tier`,
`upstream_request_id`, `ratelimit`.

The raw `api_key` is never stored: it is masked in `normalize_record()` and any
copy of it in other string fields (e.g. an echoed error body) is scrubbed.
Costs are **not** stored; they are computed at read time so a pricing update
applies to history.

## API

All JSON. `window` is `24h | 7d | 30d | all` (anything else → 400).

| endpoint | top-level keys |
|---|---|
| `GET /` (+HEAD) | dashboard HTML (CSP-pinned inline script) |
| `GET /api/health` (+HEAD) | `status` (`ok`/`degraded`), `service`, `version`, `build_date`, `now`, `now_local`, `uptime_s`, `management`, `ingest`, `pricing` |
| `GET /api/analytics?window=` | `window`, `window_start`, `generated_at(_local)`, `timezone`, `cost_basis`, `summary`, `per_model`, `per_key`, `per_endpoint`, `per_client_ip`, `per_user_agent`, `per_day`, `series_granularity` (`hour` for 24h else `day`), `series` (chronological; hour buckets are local time with UTC offset, e.g. `2026-10-25T02:00+01:00`, so the repeated DST hour stays separate), `version`, `management`, `ingest`, `pricing` |
| `GET /api/pricing` | `source_url`, `as_of`, `currency`, `unit`, `basis`, `formula`, `cache_write_ttl_assumption`, notes, `models` |
| `GET /api/quota` | `available`, `checked_at(_local)`, `credentials` (5h/7d utilisation, resets local+UTC, overage, `failed`, `cooldowns`), `has_data`, `client_keys` (masked), `upstream_key_usage`, `quota_providers`; 503 when the management API is down |
| `GET /api/requests?limit=N` | `total`, `limit`, `requests` (newest first, with `cost_usd`, `cost_quality`, `usage`, `timestamp_local`); `1 ≤ N ≤ 5000` |
| `GET /api/export.csv?window=` | CSV download (default `all`), one row per request, oldest first: timestamps (UTC + local), model, endpoint, status, token buckets, `cost_usd` (empty when unpriced), `cost_quality`, latency, masked key, client IP, user agent, ids. Cells starting with `= + - @` are prefixed with `'` (spreadsheet formula-injection guard) |
| `GET /api/models` | `available`, `error`, `count`, `models` (served ids joined with price rows), `pricing` |

`ingest` carries `records`, `total_ingested`, `duplicates_skipped`,
`malformed_skipped`, `corrupt_lines`, `pending_writes`, `last_write_error`,
`last_drain_at`, `last_ingest_at`, `last_ingest_age_s`, `poll_interval_s`,
`queue_retention_s`, `queue_retention_source`, `loss_windows_total`,
`loss_windows` (last 5). `management` carries `reachable` (a successful poll in
the last 30 s), `last_ok_at`, `last_ok_age_s`, `last_error`, `last_error_at`.

Unknown numbers are `null`, never `0`: an empty window has `avg_latency_ms: null`,
and a group with only unpriced traffic has `cost_usd: null`.

## Pricing rules

`pricing.py` encodes the official table (source + `as_of` exposed by
`/api/pricing`). Per record:

```
cost = (uncached_input + unclassified) * input + cache_read * cache_read
     + cache_write * cache_write_5m + output * output          (USD per 1M tokens)
```

- `token_breakdown` is used when `quality == "complete"`; otherwise the flat
  `tokens{}` (Claude semantics: `input_tokens` excludes cache, `output_tokens`
  includes thinking) and `cost_quality` becomes `fallback_tokens:<quality>`.
- Cache writes use the 5m rate (records have no TTL split).
- Reasoning is billed as output.
- `claude-3-7-sonnet-20250219` is `unpriced_legacy`; unknown models are
  `unknown_model`. Both give `cost_usd: null` and are excluded from totals.
- Date-suffixed ids resolve to their family row (`claude-opus-4-5` →
  `claude-opus-4-5-20251101`).

To change prices, update `_ROWS` and `AS_OF` in `pricing.py` **and**
`EXPECTED` in `tests/test_pricing.py`. The regression test asserts values, so a
one-sided edit fails.

## Development

```bash
python3 -m venv venv && venv/bin/pip install -r requirements.txt
venv/bin/pytest -q
CPROXY_MANAGEMENT_KEY=... venv/bin/uvicorn app:app --host 127.0.0.1 --port 24689   # dev port; 24688 is production
```

Running a second instance against the live cproxy **steals usage records** from
production (the queue pops). For UI work, point `CPROXY_MANAGEMENT_URL` at a mock,
or use `create_app(start_poller=False)`.

Tests use `httpx.MockTransport` for the management API and `httpx.ASGITransport`
for the app; no network, no sleeps (time-dependent behaviour takes an injectable
`clock` or explicit timestamps).

### Fixture and secret policy

- `tests/fixtures/usage_queue_sample.json` is the raw capture and contains the
  live client key: it is **gitignored** and used only when present locally.
- `tests/fixtures/usage_queue_sample.redacted.json` is committed: identical
  except for a same-length synthetic key. A clean checkout runs the suite on it.
- Never put real key values in tests (including short ones configured in
  cproxy's `api-keys`). Before pushing, grep the outgoing diff for the values in
  `deploy/cproxy.env`.

### Gates before every change ships

1. `venv/bin/pytest -q` green.
2. `sudo systemctl restart cproxy-ui` (never the `cproxy` unit), then
   `/api/health` reports `status: ok` with `pending_writes: 0`.
3. The dashboard renders with no console errors (CSP violations show up there).
4. No key values in `requests.jsonl`, the state file, any `/api/*` response, the
   journal or the diff.
