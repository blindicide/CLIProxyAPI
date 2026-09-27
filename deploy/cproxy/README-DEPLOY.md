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
