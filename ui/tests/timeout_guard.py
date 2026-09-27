"""Fail loudly instead of hanging (a hung asyncio test once blocked the gates).

Each test gets CPROXY_UI_TEST_TIMEOUT seconds (default 60; every test here takes well under
2 s). Past that, SIGALRM raises TimeoutError inside the test, so it fails with a normal
traceback showing where it was stuck and the rest of the run continues. If the process never
returns to Python (e.g. blocked in C), faulthandler hard-exits 30 s later with every
thread's stack, so a run can never hang forever.
"""
from __future__ import annotations

import faulthandler
import os
import signal
import threading

import pytest

TIMEOUT = float(os.getenv("CPROXY_UI_TEST_TIMEOUT", "60"))
HARD_EXIT_GRACE = 30.0


def _expired(signum, frame):
    raise TimeoutError(f"test exceeded {TIMEOUT:.0f} s (CPROXY_UI_TEST_TIMEOUT); stuck at the frame above")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    usable = hasattr(signal, "SIGALRM") and threading.current_thread() is threading.main_thread()
    previous = signal.signal(signal.SIGALRM, _expired) if usable else None
    if usable:
        signal.setitimer(signal.ITIMER_REAL, TIMEOUT)
    faulthandler.dump_traceback_later(TIMEOUT + HARD_EXIT_GRACE, exit=True)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()
        if usable:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
