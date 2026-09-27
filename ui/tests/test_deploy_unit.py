"""Guards on the committed systemd unit: the sandbox must keep secrets and code out of reach."""
from __future__ import annotations

from pathlib import Path

UNIT = Path(__file__).resolve().parents[2] / "deploy" / "cproxy-ui" / "cproxy-ui.service"


def directives() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for line in UNIT.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "[")) and "=" in line:
            key, value = line.split("=", 1)
            out.setdefault(key, []).append(value)
    return out


def test_home_is_hidden_and_only_data_is_writable():
    d = directives()
    assert d["ProtectHome"] == ["tmpfs"]
    assert d["ProtectSystem"] == ["strict"]
    assert d["BindReadOnlyPaths"] == ["/home/clawuser/projects/cproxy/ui"]
    writable = " ".join(d["BindPaths"]).split()
    assert writable == ["/home/clawuser/projects/cproxy/ui/requests.jsonl", "/home/clawuser/projects/cproxy/ui/data"]
    assert "ReadWritePaths" not in d
    for secret in ("auths", "config.yaml", ".claude", "cproxy.env"):
        assert not any(secret in path for path in writable + d["BindReadOnlyPaths"])


def test_privileges_and_network_are_locked_down():
    d = directives()
    assert d["User"] == ["clawuser"]
    assert d["NoNewPrivileges"] == ["yes"]
    assert d["CapabilityBoundingSet"] == [""]
    assert d["IPAddressDeny"] == ["any"] and d["IPAddressAllow"] == ["localhost"]
    assert d["UMask"] == ["0077"]
    assert "--host 127.0.0.1 --port 24688" in d["ExecStart"][0]


def test_unit_never_manages_cproxy_itself():
    exec_lines = [v for k, vs in directives().items() if k.startswith("Exec") for v in vs]
    assert exec_lines
    for line in exec_lines:
        for forbidden in ("systemctl", "config.yaml", "auths", ".claude"):
            assert forbidden not in line, line
