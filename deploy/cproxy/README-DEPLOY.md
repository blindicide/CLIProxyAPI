# cproxy deployment (EPYC / theta-gryphonis)

Claude Code subscription -> OpenAI + Anthropic compatible API.

- Public endpoint: https://cproxy.net.a.blindicide.ru
- Backend: `cli-proxy-api` on `127.0.0.1:31524` (systemd unit `cproxy.service`)
- Config: `config.yaml` (0600, contains the client API key + management key)
- Credentials: `auths/` (0600 JSON, one file per Claude OAuth identity)
- TLS: Let's Encrypt, vhost `/etc/nginx/conf.d/cproxy.net.a.blindicide.ru.conf`
- Port: 31524, reserved in `~/port-pool.txt`

## Build

Requires Go 1.26+ (EPYC: `/usr/local/go/bin/go`, Go 1.27.1).

```bash
go build -buildvcs=false \
  -ldflags="-s -w -X 'main.Version=<version>' -X 'main.Commit=<sha>' -X 'main.BuildDate=<iso8601>'" \
  -o cli-proxy-api ./cmd/server
```

## Claude credential

Normal OAuth login (auto-refreshing; needs a browser reaching this host's
`localhost:54545` callback, e.g. over an SSH tunnel):

```bash
./cli-proxy-api -claude-login -no-browser
```

Writes `auths/claude-<hash>-<email>.json` with `access_token` + `refresh_token`.
A credential file without `refresh_token` is usable until its `expired`
timestamp and then returns 401 -- use it only for smoke tests, so the proxy can
never rotate a refresh-token chain that another client (e.g. the Claude Code
CLI) also owns.

Long-lived alternative (`claude setup-token`), inference-only:

```json
{
  "type": "claude",
  "access_token": "sk-ant-oat01-...",
  "is_setup_token": true,
  "skip_account_profile": true,
  "email": "you@example.com",
  "expired": "2027-09-27T00:00:00+02:00"
}
```

## Endpoints

- `POST /v1/chat/completions` (OpenAI, streaming + tools)
- `POST /v1/messages` (Anthropic native)
- `GET /v1/models`
- `/v0/management`, `/v8/management` (management key, localhost only)

## Ops

```bash
sudo systemctl status|restart|stop cproxy
journalctl -u cproxy -f
```

## Credential in use (2026-09-27, verified)

Dedicated `claude setup-token` value registered as
`auths/claude-<sha256(org_uuid)[:8]>-<email>.json` with
`"is_setup_token": true` + `"skip_account_profile": true` (the token has no
`user:profile` scope, so an `/api/oauth/profile` lookup returns 403 — that 403 is
the cheap way to *identify* a setup token vs. a CLI access token). No
`refresh_token` is stored, so this credential can never rotate the Claude Code
CLI's own refresh chain.

The bootstrap credential used for the first smoke test (a copy of
`~/.claude/.credentials.json`'s access token, no refresh token, guaranteed to
die within the hour) is kept out of the auth dir under `retired/`.

## Live quota signals (no extra calls needed)

`GET /v0/management/auth-files` (management key, localhost) surfaces the
Anthropic rate-limit headers observed on the last real request:

```
5h  util=0.62  status=allowed  reset=<epoch>
7d  util=0.72  status=allowed  reset=<epoch>
overage=org_level_disabled  fallback=available
```

Use this instead of probing upstream to answer "am I near the limit".
A transient `429 rate_limit_error` passes through to the client
(`{"type":"error",...}`) while the credential stays `failed: 0, cooldowns: []`.
Note `401 "OAuth access token has been revoked"` vs `429 rate_limit_error`:
401 means a dead credential, 429 means an authenticated but throttled request.

## Analytics dashboard (cproxy-ui)

FastAPI + uvicorn app in `ui/` (version in `ui/version.py`, currently 0.1.0),
modelled on the agy-proxy `ui/` skin.

- Public: https://cproxy.net.a.blindicide.ru/ (dashboard at `/`, API at
  `^/(v1|v0|health)` — note the backend health route is `/healthz`, which the
  prefix regex also matches) and https://cproxy-ui.net.a.blindicide.ru/
  (same split, own LE cert).
- Backend: `127.0.0.1:24688`, systemd unit `cproxy-ui.service`
  (template `deploy/cproxy-ui/cproxy-ui.service`).
- Secrets: `deploy/cproxy-ui.env` (0600, gitignored) holds
  `CPROXY_MANAGEMENT_KEY` (template `deploy/cproxy-ui/cproxy-ui.env.example`).
- nginx: `deploy/cproxy-ui/cproxy-ui.net.a.blindicide.ru.conf`; the main
  `cproxy.net.a.blindicide.ru.conf` got `location /` → 24688 and its API block
  narrowed to `^/(v1|v0|health)` (backup `*.bak-20260927-150346` alongside it).

### Endpoints

`GET /` dashboard · `/api/health` · `/api/analytics?window=24h|7d|30d|all` ·
`/api/pricing` · `/api/quota` · `/api/requests?limit=N` · `/api/models`.

### Data flow

- The poller pops `GET /v0/management/usage-queue?count=50` until it returns
  `[]` (max 500 pops per cycle), fsyncs each batch into `ui/requests.jsonl`,
  then sleeps 2 s. State: `ui/data/ingest_state.json`. Dedupe key:
  `execution_id:request_id`, so restarts/replays never double-count.
- **The queue is not durable**: cproxy prunes items older than
  `redis-usage-queue-retention-seconds` (default and current value 60 s) and
  keeps them in memory only. If cproxy-ui is down for more than ~60 s, or
  cproxy restarts, usage from that gap is lost.
- Requests that cproxy rejects before provider dispatch (e.g. unknown model →
  400 `model_not_found`) emit no usage record and never appear in the dashboard.
- The raw client `api_key` is masked at ingest (`sha256:<12 hex>…<last 4>`,
  plus `key-N` = position in `api-keys`). It is never written to disk, logs
  or API responses (covered by tests).

### Pricing

`ui/pricing.py` — official Anthropic list prices
(https://platform.claude.com/docs/en/about-claude/pricing, as of 2026-09-27),
USD per 1M tokens. All costs are the **list-price equivalent, not billed**.
Cache writes are priced at the 5m rate (records carry no TTL split), reasoning
tokens are billed as output, and `claude-3-7-sonnet-20250219` is
`unpriced_legacy` (shown as `—`, excluded from totals). When pricing changes,
update `_ROWS`, `AS_OF` and `tests/test_pricing.py::EXPECTED` together.

### Ops

```bash
cd ui && python3 -m venv venv && venv/bin/pip install -r requirements.txt
venv/bin/pytest -q                       # tests (uses tests/fixtures/*.redacted.json in a clean checkout)
sudo systemctl status|restart cproxy-ui
journalctl -u cproxy-ui -f
curl -s http://127.0.0.1:24688/api/health | python3 -m json.tool
```

`/api/health` returns `status: degraded` with `management.last_error` when the
management API is unreachable; the dashboard then shows a red stale banner
instead of presenting old numbers as fresh.
