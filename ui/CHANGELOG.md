# cproxy-ui changelog

Versions are shown in the dashboard header/footer, `/api/health` and
`/api/analytics` (`ui/version.py`).

## Unreleased

- Hourly buckets carry the UTC offset and are ordered by real time, so the
  repeated hour on a DST fall-back day is not merged (`ab02be3`).
- Quota windows whose reset time has passed since the last observed upstream
  response are flagged (`reset_passed`) and shown as "unknown" instead of
  presenting the old utilisation as current.
- Runtime and test dependencies split; tests move to pytest >= 9.0.3
  (PYSEC-2026-1845, predictable `/tmp/pytest-of-<user>`); `requirements.lock`
  pins the exact tested set, which pip-audit reports clean.
- Responses over 1 KB are gzip-compressed by the app (nginx does not compress
  here): a 30 s dashboard poll drops from ~64 KB to ~7.7 KB.
- Upstream API-key usage rows skip malformed entries; tests for the live
  quota shape, key masking, backlog draining and schema drift (coverage 98%).

## 0.2.0 — 2026-09-27

Data safety
- A crash mid-write can no longer glue the next record onto a torn line and
  lose it; failed writes (disk full) keep records in memory and retry
  (`1263ca7`).
- A failing `/api-keys` refresh no longer aborts the drain, so records are not
  left to expire in cproxy's queue (`8549748`).
- Shutdown persists an in-flight queue pop instead of cancelling it; every
  deploy restart used to risk dropping those records (`24a2923`).
- Spans lost to cproxy's usage-queue retention (drainer away longer than
  `redis-usage-queue-retention-seconds`) are detected, persisted and shown; the
  retention is read live from cproxy's config (`d9cf5e2`).

Security
- Hash-pinned Content-Security-Policy for the dashboard, baseline security
  headers everywhere, `no-store` on `/api/*`, HEAD support for monitors
  (`9052090`).
- systemd sandbox: secrets and other projects invisible to the service, code
  read-only, only `requests.jsonl` + `data/` writable, localhost-only network;
  `systemd-analyze security` 9.2 → 1.1 (`464a79e`).

Performance
- Per-record derivation cache, 5 s analytics result cache, aggregation in the
  threadpool so the poller is never blocked: 50k records, window=all
  919 → 365 ms (`df461d2`).
- About 3× less memory per stored record (9.3 → 3.2 KB) through interned,
  compact in-memory records (`a991bcd`).

Features
- `GET /api/export.csv?window=` with a spreadsheet formula-injection guard,
  and a CSV button in the dashboard (`ff31bd2`).

Accessibility
- WCAG AA text contrast in both themes, scoped and captioned tables, charts
  that announce their data, visible keyboard focus (`80cd5ef`).

Docs
- Developer README (`9d88151`); operator note on queue retention (`5873278`).

## 0.1.0 — 2026-09-27 (tag `cproxy-ui-v0.1`)

Initial release: usage-queue drain with dedupe and key masking, Anthropic
list-price cost estimation, analytics API, dashboard with quota panel, charts
and tables; systemd unit and nginx vhosts for cproxy-ui.net.a.blindicide.ru and
the `/` split on cproxy.net.a.blindicide.ru.
