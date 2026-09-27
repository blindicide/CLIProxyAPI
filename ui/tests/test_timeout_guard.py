"""The hang guard fails a stuck test with a traceback and lets the run finish."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_stuck_test_fails_and_the_run_continues(tmp_path):
    (tmp_path / "test_stuck.py").write_text(
        "import threading\nimport time\n\n\n"
        "def test_before():\n    assert True\n\n\n"
        "def test_stuck():\n    threading.Event().wait()  # waits forever\n\n\n"
        "def test_after():\n    assert True\n"
    )
    env = {**os.environ, "CPROXY_UI_TEST_TIMEOUT": "2", "PYTHONPATH": str(Path(__file__).parent)}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "timeout_guard", "-p", "no:cacheprovider", str(tmp_path)],
        capture_output=True, text=True, timeout=60, env=env, cwd=tmp_path,
    )
    assert result.returncode == 1
    assert "1 failed, 2 passed" in result.stdout
    assert "TimeoutError: test exceeded 2 s" in result.stdout and "test_stuck" in result.stdout
