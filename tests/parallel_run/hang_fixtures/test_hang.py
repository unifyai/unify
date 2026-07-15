"""Hang-only fixture used to verify --session-timeout kills hung sessions."""

import time


def test_hangs_forever():
    """Sleep longer than any reasonable session-timeout used in parallel_run tests."""
    time.sleep(3600)
