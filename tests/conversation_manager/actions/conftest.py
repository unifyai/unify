"""
tests/conversation_manager/actions/conftest.py
==============================================

Shared fixtures for the CM action-routing tests.

These tests all assert that the CM brain dispatches an `act` (or a
fast-path equivalent like `web_act` / `desktop_act` / `query_past_
transcripts` / `ask_about_contacts`) in response to user requests.
The brain prompt correctly defers with "my desktop/browser is still
booting, I'll start as soon as it's ready" when
``cm.vm_ready=False`` or ``cm.file_sync_complete=False`` — but
that's the wrong production answer for tests that need an action
to fire.

A handful of files in this directory already set
``cm.cm.vm_ready = True`` / ``cm.cm.file_sync_complete = True``
inline at the top of each test (test_act_failure_context.py,
test_desktop_fast_path_routing.py, test_take_action.py,
test_persist_interactive_tutorial.py, test_web_act_fast_path_
routing.py). The remaining files (test_multi_action.py,
test_steer_action.py, test_query_past_transcripts.py, …) did NOT
— they were silently broken by the discover_test_paths.py matrix
bug.

Centralise the flag-flip here as an autouse fixture so every
action test starts from "env is ready"; the few tests that
deliberately exercise the not-ready path can still override
inline.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _mark_environment_ready(request):
    """Set vm_ready + file_sync_complete on every test in this directory.

    Without this, the brain defers user requests with "my
    desktop/browser is still booting, I'll start as soon as it's
    ready" — correct production behaviour but it suppresses act()
    dispatch for any test that asserts an action was started.

    Only flips flags on a CM the test actually uses. Tests under
    ``actions/integration/`` use ``initialized_cm_codeact`` (a
    different fixture) and have their own autouse fixture in
    ``actions/integration/test_files.py``; we don't want to spin up
    an unrelated ``initialized_cm`` just to set flags on it. The
    fixturenames check makes this autouse a no-op for those.
    """
    if "initialized_cm" in request.fixturenames:
        cm = request.getfixturevalue("initialized_cm")
        cm.cm.vm_ready = True
        cm.cm.file_sync_complete = True
