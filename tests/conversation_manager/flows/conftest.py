"""
tests/conversation_manager/flows/conftest.py
=============================================

Shared fixtures for end-to-end CM flow tests.

Like ``actions/conftest.py``, these tests rely on the CM brain
dispatching real work (acts, fast paths, outbound comms) in
response to user input. The brain correctly defers with "my
desktop/browser is still booting" when ``cm.vm_ready=False`` or
``cm.file_sync_complete=False``, but every flow test in this
directory implicitly assumes the env is ready (otherwise the
assertion about EmailSent / SMSSent / ActorHandleStarted /
attachment paths can never hold).

Centralise the flag-flip as an autouse fixture so every flow
test starts from "env ready".
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _mark_environment_ready(request):
    """Set vm_ready + file_sync_complete on every test in this directory."""
    if "initialized_cm" in request.fixturenames:
        cm = request.getfixturevalue("initialized_cm")
        cm.cm.vm_ready = True
        cm.cm.file_sync_complete = True
