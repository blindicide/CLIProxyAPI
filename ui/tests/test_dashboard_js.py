"""Runs the dashboard JavaScript unit tests (tests/js) under Node when it is available."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

NODE = shutil.which("node")
SUITE = Path(__file__).parent / "js" / "dashboard.test.mjs"


@pytest.mark.skipif(NODE is None, reason="node is not installed")
def test_dashboard_javascript():
    result = subprocess.run([NODE, str(SUITE)], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "passed" in result.stdout
