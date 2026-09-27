"""Static accessibility guards on dashboard.html (the live audit runs in a headless browser)."""
from __future__ import annotations

import re
from pathlib import Path

HTML = (Path(__file__).resolve().parents[1] / "dashboard.html").read_text(encoding="utf-8")


def _luminance(hex_color: str) -> float:
    channels = [int(hex_color[i : i + 2], 16) / 255 for i in (1, 3, 5)]
    linear = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def contrast(a: str, b: str) -> float:
    hi, lo = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def _theme(selector: str) -> dict[str, str]:
    block = HTML.split(selector, 1)[1].split("}", 1)[0]
    tokens = dict(re.findall(r"--([\w-]+):(#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3})\b", block))
    return {k: v if len(v) == 7 else "#" + "".join(c * 2 for c in v[1:]) for k, v in tokens.items()}


def test_every_table_has_a_caption():
    tables = re.findall(r"<table [^>]*>", HTML)
    assert tables and all("data-caption=" in t for t in tables)
    assert "<caption class=\"sr-only\">" in HTML


def test_header_cells_are_scoped():
    assert "<th scope=\"col\"" in HTML
    assert not re.search(r"<th(?! scope=)[ >]", HTML)


def test_charts_get_a_data_summary():
    for label in ("Requests", "Tokens", "Estimated cost"):
        assert f'lab,"{label}")' in HTML
    assert "svg.setAttribute(\"aria-label\",summary)" in HTML


def test_focus_is_visible():
    assert ":focus-visible{outline:2px solid var(--accent)" in HTML


def test_text_colours_meet_wcag_aa():
    dark = _theme(":root{")
    light = _theme(':root:not([data-theme="dark"]){')
    for theme, name in ((dark, "dark"), (light, "light")):
        for token in ("text", "muted", "ok", "warn", "bad", "accent"):
            for surface in ("bg", "panel", "panel2"):
                ratio = contrast(theme[token], theme[surface])
                assert ratio >= 4.5, f"{name} --{token} on --{surface}: {ratio:.2f}"
