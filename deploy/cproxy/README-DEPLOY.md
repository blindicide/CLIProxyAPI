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
