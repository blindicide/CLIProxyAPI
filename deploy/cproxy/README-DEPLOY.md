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

FastAPI + uvicorn app in `ui/` (version in `ui/version.py`; history in `ui/CHANGELOG.md`),
modelled on the agy-proxy `ui/` skin.
Developer docs (architecture, record schema, API shapes, pricing rules, test
and secret policy): [`ui/README.md`](../../ui/README.md).

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

### Sandbox (cproxy-ui.service)

The unit is sandboxed (`systemd-analyze security cproxy-ui`: 1.1 "OK", was 9.2):
`/home` is an empty tmpfs inside the service, `ui/` is bind-mounted back
read-only, and only `ui/requests.jsonl` and `ui/data/` are writable. `auths/`,
`config.yaml`, `deploy/*.env`, `~/.claude` and other projects are invisible to
the process (the EnvironmentFile is read by systemd before the sandbox applies).
No capabilities, seccomp `@system-service`, IP traffic limited to localhost,
`UMask=0077`.

Memory caps (the host is shared; a runaway process must never make the kernel
OOM-kill someone else's): `cproxy-ui` has `MemoryHigh=600M` / `MemoryMax=900M`
and the backup unit `MemoryMax=256M`, no swap. All records live in memory
(~3.3 KB each at load, ~4.3 KB under query load, so ~190k records fit under the
cap). `/api/health` → `ingest.storage.memory` shows RSS against the limit and
the dashboard warns at 70%; that is the moment to run
`tools/datastore.py archive`. Past the cap, only cproxy-ui is killed and
restarted.

Consequences for operators:
- after editing the template, reinstall it:
  `sudo install -m 644 deploy/cproxy-ui/cproxy-ui.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart cproxy-ui`;
- `requests.jsonl` is a bind-mounted file: the app only appends to it; never
  replace it with a rename (`mv`/`os.replace`) while the service is running;
- code changes in `ui/` need a restart (the service cannot write
  `__pycache__`; `PYTHONDONTWRITEBYTECODE=1` is set).

Rollback: the pre-sandbox unit is the same file minus the `# --- sandbox`
section, `ExecStartPre` and `UMask`.

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

### Operator note: usage-queue retention (record-loss window)

cproxy keeps usage-queue records **in memory only** and prunes anything older
than `redis-usage-queue-retention-seconds` (live value: 60 s, the default;
cproxy clamps it to a maximum of 3600). cproxy-ui drains the queue every 2 s,
so in normal operation nothing is lost. Records are lost when:

- cproxy-ui is down (restart, deploy, crash) for longer than the retention
  window: everything older than the window at drain time is gone for good;
- cproxy itself restarts: the in-memory queue is dropped regardless of the
  retention setting.

**Recommendation:** raise `redis-usage-queue-retention-seconds` in
`config.yaml` (for example to `3600`) at the next planned cproxy maintenance,
so a cproxy-ui outage of up to an hour loses nothing. Memory cost is small
(each raw queue record is about 2.8 KB). This is an operator decision: `config.yaml` and
the `cproxy` unit are live and shared with the Claude Code CLI, so cproxy-ui
tooling never edits or restarts them. After the change, confirm the value (it
is read-only here) with:

```bash
K=$(grep '^CPROXY_MANAGEMENT_KEY=' deploy/cproxy.env | cut -d= -f2-)
curl -s -H "Authorization: Bearer $K" http://127.0.0.1:31524/v0/management/config \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["redis-usage-queue-retention-seconds"])'
```

### Operator decision: the dashboard is public

**Current state (decided 2026-09-27): public, no authentication**, same as the
agy-proxy dashboard this mirrors. The mandated acceptance checks
(`curl https://cproxy.net.a.blindicide.ru/api/health`, `/api/analytics?window=24h`)
are unauthenticated as well.

What anyone on the internet can read at `/` and `/api/*`:

- request metadata: time, model, endpoint, status, tokens, list-price cost,
  latency, **client IPs**, **user agents**, request/session/execution ids;
- the first 600 characters of **upstream error bodies**;
- the credential **account label/email**, 5h/7d quota utilisation and resets;
- the full request history via `/api/requests` and `/api/export.csv`.

Never exposed: API keys (client and upstream keys are masked as
`sha256:<12 hex>…<last 4>` at ingest and in every response; covered by tests),
the management key, credentials, `config.yaml`.

If that tradeoff changes, apply **one** of the options below to the main vhost
(and the same block to `cproxy-ui.net.a.blindicide.ru.conf` if that host should
match). Both protect only `location /` (dashboard + `/api/*`); the relay API
(`^/(v1|v0|health)`) is a separate location and keeps working unchanged. After
either change the unauthenticated acceptance curls above return 401/403 by
design; use `curl -u <user>` (option A) or run them from an allowed IP (B).

**1. Back up** (mandatory, never edit without it):

```bash
C=/etc/nginx/conf.d/cproxy.net.a.blindicide.ru.conf
B=$C.bak-$(date +%Y%m%d-%H%M%S); sudo cp "$C" "$B"; echo "backup: $B"
```

**2a. Option A — HTTP basic auth.** Create the password file (outside the repo,
readable by nginx only):

```bash
sudo htpasswd -c /etc/nginx/cproxy-ui.htpasswd <user>     # prompts for the password
sudo chown root:www-data /etc/nginx/cproxy-ui.htpasswd && sudo chmod 640 /etc/nginx/cproxy-ui.htpasswd
```

Then add two lines at the top of the `location / {` block that proxies to
`127.0.0.1:24688` (`sudoedit "$C"`):

```nginx
    location / {
        auth_basic           "cproxy analytics";
        auth_basic_user_file /etc/nginx/cproxy-ui.htpasswd;
        proxy_pass http://127.0.0.1:24688;
        ...
```

**2b. Option B — IP allowlist.** Instead of 2a, add at the top of the same
`location / {` block (one `allow` per address or CIDR):

```nginx
    location / {
        allow 203.0.113.10;        # replace with your address(es)
        deny  all;
        proxy_pass http://127.0.0.1:24688;
        ...
```

**3. Test and reload:**

```bash
sudo nginx -t && sudo systemctl reload nginx
```

**4. Re-verify all of it** — the dashboard is protected and the API is not
broken:

```bash
H=https://cproxy.net.a.blindicide.ru
K=$(grep '^CPROXY_API_KEY=' deploy/cproxy.env | cut -d= -f2-)
curl -s -o /dev/null -w "/ unauthenticated: %{http_code} (expect 401 or 403)\n" $H/
curl -s -o /dev/null -w "/api/health unauthenticated: %{http_code} (expect 401 or 403)\n" $H/api/health
curl -s -u <user> $H/api/health | head -c 120; echo "   <- option A: expect status ok"
curl -s -H "Authorization: Bearer $K" $H/v1/models | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["data"]), "models (expect 17)")'
curl -s -H "Authorization: Bearer $K" -H 'Content-Type: application/json' $H/v1/chat/completions \
  -d '{"model":"claude-haiku-4-5-20251001","max_tokens":10,"messages":[{"role":"user","content":"Say ok"}]}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["choices"][0]["message"]["content"])'
curl -s -o /dev/null -w "/healthz: %{http_code} (expect 200)\n" $H/healthz
```

**5. If any API check fails, roll back immediately:**

```bash
sudo cp "$B" "$C" && sudo nginx -t && sudo systemctl reload nginx
```

and repeat the `/v1/models` and completion checks. Never leave the API broken.

### History retention, backup and restore

**Retention policy: records are archived, never deleted.** Once a day (after
04:00 local, following the 03:17 backup timer) cproxy-ui moves records older
than **180 days** (`CPROXY_UI_ARCHIVE_AFTER_DAYS`) from `ui/requests.jsonl`
into `ui/data/archive/requests-before-<date>-<UTC>.jsonl.gz`. It does so only
after all of the following hold:

1. a fresh backup of the whole history is written and verifies;
2. the archive is written, fsync'd and read back with exactly the expected ids;
3. every archived id is also in that fresh backup;
4. the replacement live content (kept + anything appended meanwhile) is
   journaled, fsync'd and verified.

The live file is then rewritten in place, under the service's write lock and an
exclusive `data/.history.lock` (backups take it shared, so they never capture a
half-rewrite). A journal marker makes an interrupted rewrite finish on the next
start. If any check fails, nothing is removed, only the job's temp files are
cleaned up, and `/api/health` → `ingest.archive.last_error` plus a dashboard
warning report it. The service runs the job itself because it owns the file
and the in-memory records; the timer only makes the nightly backup.
Conservation check at any time:
`venv/bin/python tools/datastore.py audit` (live ∪ archives == total ingested).

Archived records leave the dashboard's windows and charts (which cover the live
file) but not the totals: `/api/analytics?window=all` → `lifetime` adds them
back (each record once, costs at current prices), shown as a "Lifetime" card;
`tools/verify_totals.py --lifetime` checks it independently. It grows by about
2 KB per request on disk, and cproxy-ui keeps about 3.2 KB per request in memory
(~0.6 GiB RAM per 200k requests). `/api/health` → `ingest.storage` reports the
file size, free disk and newest backup, and the dashboard warns when free disk
drops below 2 GiB or the newest backup is older than 48 h. Service logs go to
journald and rotate with it.

All operations use `ui/tools/datastore.py` (stdlib only, run as `clawuser`
from `ui/`):

| task | command |
|---|---|
| backup (also daily via `cproxy-ui-backup.timer`, 03:17 ± 15 min, keeps 30) | `venv/bin/python tools/datastore.py backup` |
| check a backup | `venv/bin/python tools/datastore.py verify data/backups/<file>.tar.gz` |
| sizes, record count, newest backup | `venv/bin/python tools/datastore.py status` |
| conservation audit (live ∪ archives == ingested) | `venv/bin/python tools/datastore.py audit` |
| restore (merge) | see below |
| archive old history | see below |

A backup is `data/backups/cproxy-ui-backup-<UTC>.tar.gz` (0600) with
`requests.jsonl` (complete lines only, so it is consistent even mid-append),
`ingest_state.json` and a `MANIFEST.json` (record count + SHA-256), verified
right after writing. It lives on the same disk: copy `data/backups/` off-host
for disaster recovery.

**Restore is a merge**: records in the backup that are missing from the live
file are added, nothing is removed, so restoring an old backup can never drop
newer records. **Archive** moves records older than a date into
`data/archive/requests-before-<date>-<UTC>.jsonl.gz` after checking that the
split partitions the records exactly; archived records then no longer appear in
the dashboard's analytics (they stay in the archive). Both keep the previous
file as `requests.jsonl.pre-restore-*` / `.pre-archive-*` (delete it yourself
once satisfied) and **refuse to run while cproxy-ui is active**, because
`requests.jsonl` is bind-mounted into the running service. The stop must be
short: cproxy drops unread usage records after its 60 s queue retention (a
longer gap is reported as a loss window).

```bash
cd /home/clawuser/projects/cproxy/ui
venv/bin/python tools/datastore.py backup                       # safety net first
sudo systemctl stop cproxy-ui
venv/bin/python tools/datastore.py restore data/backups/<file>.tar.gz
#   or: venv/bin/python tools/datastore.py archive --before 2026-01-01
sudo systemctl start cproxy-ui
curl -s http://127.0.0.1:24688/api/health | python3 -m json.tool | head -20
venv/bin/python tools/verify_totals.py                          # totals still consistent
```

Timer install (already done on this host):

```bash
sudo install -m 644 deploy/cproxy-ui/cproxy-ui-backup.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now cproxy-ui-backup.timer
sudo systemctl start cproxy-ui-backup.service     # first backup now
```

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
cd ui && python3 -m venv venv && venv/bin/pip install -r requirements.lock   # exact tested versions
venv/bin/pytest -q                       # tests (uses tests/fixtures/*.redacted.json in a clean checkout)
sudo systemctl status|restart cproxy-ui
journalctl -u cproxy-ui -f
curl -s http://127.0.0.1:24688/api/health | python3 -m json.tool
cd ui && venv/bin/python tools/verify_totals.py   # dashboard totals == independent recomputation (exit 0)
```

`/api/health` returns `status: degraded` with `management.last_error` when the
management API is unreachable; the dashboard then shows a red stale banner
instead of presenting old numbers as fresh.
