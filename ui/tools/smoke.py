#!/usr/bin/env python3
"""Operator acceptance check for cproxy + cproxy-ui (the mandate's section 11, repeatable).

    venv/bin/python tools/smoke.py                        # public host, no quota spent
    venv/bin/python tools/smoke.py --completion           # + one tiny real completion
    venv/bin/python tools/smoke.py --base https://cproxy-ui.net.a.blindicide.ru --json

Checks (PASS/FAIL each; exit 0 only when all pass):
  services     systemd units cproxy and cproxy-ui are active (skipped with --no-systemd)
  health       /api/health?strict=1 answers 200 with status ok
  pricing      /api/pricing carries the official source_url and as_of
  analytics    /api/analytics?window=24h has the expected shape and a coverage block
  dashboard    / serves the dashboard marker and the same version as /api/health
  models       /v1/models through the public host lists 17 models (client key from --env-file)
  totals       tools/verify_totals.py: every window MATCHes an independent recomputation
  audit        tools/datastore.py audit: live + archives == total ingested
  completion   (--completion only) one real chat completion returns content

The client key is read from the env file and only ever sent to the relay; it is never
printed. Standard library only (the UI checks read local files, so run it on the host).
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

UI = Path(__file__).resolve().parent.parent
REPO = UI.parent
SOURCE_URL = "https://platform.claude.com/docs/en/about-claude/pricing"
EXPECTED_MODELS = 17


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, UI / "tools" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def http(method: str, url: str, headers: dict | None = None, body: bytes | None = None) -> tuple[int, str]:
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # operator tool, not a relay path
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def read_env_key(env_file: Path, name: str = "CPROXY_API_KEY") -> str | None:
    try:
        for line in env_file.read_text().splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip() or None
    except OSError:
        return None
    return None


Fetch = Callable[[str, str, dict | None, bytes | None], tuple[int, str]]


def check_services(active: Callable[[str], bool]) -> tuple[bool, str]:
    states = {unit: active(unit) for unit in ("cproxy", "cproxy-ui")}
    return all(states.values()), ", ".join(f"{u}={'active' if ok else 'NOT active'}" for u, ok in states.items())


def check_health(fetch: Fetch, base: str) -> tuple[bool, str, dict]:
    status, text = fetch("GET", f"{base}/api/health?strict=1", None, None)
    try:
        body = json.loads(text)
    except ValueError:
        return False, f"HTTP {status}, not JSON", {}
    ok = status == 200 and body.get("status") == "ok"
    return ok, f"HTTP {status}, status={body.get('status')}, version={body.get('version')}, records={body.get('ingest', {}).get('records')}", body


def check_pricing(fetch: Fetch, base: str) -> tuple[bool, str]:
    status, text = fetch("GET", f"{base}/api/pricing", None, None)
    body = json.loads(text) if status == 200 else {}
    ok = body.get("source_url") == SOURCE_URL and bool(body.get("as_of"))
    return ok, f"source_url={body.get('source_url')}, as_of={body.get('as_of')}"


def check_analytics(fetch: Fetch, base: str) -> tuple[bool, str]:
    status, text = fetch("GET", f"{base}/api/analytics?window=24h", None, None)
    body = json.loads(text) if status == 200 else {}
    needed = {"summary", "per_model", "per_day", "series", "coverage", "management", "ingest"}
    missing = sorted(needed - body.keys())
    summary = body.get("summary", {})
    return not missing, (f"missing {missing}" if missing else f"24h requests={summary.get('requests')}, cost_usd={summary.get('estimated_cost_usd')}")


def check_dashboard(fetch: Fetch, base: str, version: str | None) -> tuple[bool, str]:
    status, text = fetch("GET", f"{base}/", None, None)
    marker = '<meta name="cproxy-ui" content="dashboard">' in text
    match = re.search(r'<meta name="cproxy-ui-version" content="([^"]+)">', text)
    page_version = match.group(1) if match else None
    ok = status == 200 and marker and page_version is not None and page_version == version
    return ok, f"HTTP {status}, marker={'yes' if marker else 'no'}, page version={page_version}, health version={version}"


def check_models(fetch: Fetch, base: str, key: str | None) -> tuple[bool, str]:
    if not key:
        return False, "no CPROXY_API_KEY in the env file"
    status, text = fetch("GET", f"{base}/v1/models", {"Authorization": f"Bearer {key}"}, None)
    try:
        count = len(json.loads(text)["data"])
    except (ValueError, KeyError, TypeError):
        return False, f"HTTP {status}, unexpected body"
    return count == EXPECTED_MODELS, f"HTTP {status}, {count} models (expected {EXPECTED_MODELS})"


def check_completion(fetch: Fetch, base: str, key: str | None) -> tuple[bool, str]:
    if not key:
        return False, "no CPROXY_API_KEY in the env file"
    body = json.dumps({"model": "claude-haiku-4-5-20251001", "max_tokens": 10,
                       "messages": [{"role": "user", "content": "Reply with exactly: smoke ok"}]}).encode()
    status, text = fetch("POST", f"{base}/v1/chat/completions", {"Authorization": f"Bearer {key}", "Content-Type": "application/json", "User-Agent": "cproxy-ui-smoke/1"}, body)
    try:
        content = json.loads(text)["choices"][0]["message"]["content"]
    except (ValueError, KeyError, IndexError, TypeError):
        return False, f"HTTP {status}, no content"
    return status == 200 and bool(content), f"HTTP {status}, content={content!r}"


def check_totals(local_ui: str, requests_file: Path) -> tuple[bool, str]:
    verify = _load("verify_totals")
    failures = []
    records = verify.load_lifetime(requests_file)
    for window in ("24h", "7d", "30d", "all"):
        analytics = verify.fetch(f"{local_ui}/api/analytics?window={window}")
        _, problems = verify.compare(analytics, records)
        if problems:
            failures.append(f"{window}: {problems[:2]}")
    return not failures, "all windows MATCH" if not failures else "; ".join(failures)


def check_audit(ui: Path) -> tuple[bool, str]:
    result = _load("datastore").audit(ui)
    return result["ok"], f"unique={result['unique_records']} (live {result['live_records']} + archived {result['archived_records']}), ingested={result['total_ingested']}"


def systemd_active(unit: str) -> bool:
    try:
        return subprocess.run(["systemctl", "is-active", "--quiet", unit], check=False).returncode == 0
    except FileNotFoundError:
        return False


def run(args, fetch: Fetch = http, active: Callable[[str], bool] = systemd_active) -> list[dict[str, Any]]:
    base = args.base.rstrip("/")
    key = read_env_key(Path(args.env_file))
    results: list[dict[str, Any]] = []

    def record(name: str, fn: Callable[[], tuple]) -> Any:
        try:
            out = fn()
        except Exception as exc:  # a crashed check is a failed check, never a crashed report
            out = (False, f"error: {type(exc).__name__}: {exc}")
        results.append({"check": name, "ok": bool(out[0]), "detail": out[1]})
        return out

    if not args.no_systemd:
        record("services", lambda: check_services(active))
    health = record("health", lambda: check_health(fetch, base))
    version = health[2].get("version") if len(health) > 2 else None
    record("pricing", lambda: check_pricing(fetch, base))
    record("analytics", lambda: check_analytics(fetch, base))
    record("dashboard", lambda: check_dashboard(fetch, base, version))
    record("models", lambda: check_models(fetch, base, key))
    record("totals", lambda: check_totals(args.local_ui.rstrip("/"), Path(args.ui) / "requests.jsonl"))
    record("audit", lambda: check_audit(Path(args.ui)))
    if args.completion:
        record("completion", lambda: check_completion(fetch, base, key))
    return results


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--base", default="https://cproxy.net.a.blindicide.ru", help="public host (dashboard at /, API at /v1)")
    p.add_argument("--local-ui", default="http://127.0.0.1:24688", help="cproxy-ui backend for the totals check")
    p.add_argument("--ui", default=str(UI), help="the ui/ directory (requests.jsonl, data/)")
    p.add_argument("--env-file", default=str(REPO / "deploy" / "cproxy.env"), help="file with CPROXY_API_KEY")
    p.add_argument("--completion", action="store_true", help="also send one tiny real completion (uses quota)")
    p.add_argument("--no-systemd", action="store_true", help="skip the systemd unit check")
    p.add_argument("--json", action="store_true")
    return p


def main(argv=None, fetch: Fetch = http, active: Callable[[str], bool] = systemd_active) -> int:
    args = parser().parse_args(argv)
    results = run(args, fetch=fetch, active=active)
    ok = all(r["ok"] for r in results)
    if args.json:
        print(json.dumps({"ok": ok, "checks": results}, indent=2))
    else:
        for r in results:
            print(f"{'PASS' if r['ok'] else 'FAIL'}  {r['check']:<10} {r['detail']}")
        print("ALL PASS" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
