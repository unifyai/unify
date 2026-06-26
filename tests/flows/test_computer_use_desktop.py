"""Opt-in computer-use smoke test against a real agent-service desktop.

Computer use (``primitives.computer.desktop.*``) drives an external
``agent-service`` backend over HTTP; it cannot run in-process. This test is
therefore opt-in: it auto-skips unless an agent-service is reachable (the
selfhost ``desktop`` container or a VM-backed desktop). When one is present it
points the desktop primitive at it and drives the live brain to capture the
screen, asserting the real computer-use path round-trips.

To run it locally, bring up an agent-service and export its base URL:

    FLOW_DESKTOP_URL=http://localhost:3000 \\
        UNIFY_KEY=local-test-api-key .venv/bin/pytest \\
        tests/flows/test_computer_use_desktop.py
"""

from __future__ import annotations

import asyncio
import os
import urllib.error
import urllib.request

import pytest

from unity.function_manager.primitives.runtime import DEFAULT_AGENT_SERVER_URL
from unity.session_details import SESSION_DETAILS

from tests.flows.harness import FlowHarness
from tests.helpers import capture_events

_DESKTOP_URL = os.environ.get("FLOW_DESKTOP_URL", DEFAULT_AGENT_SERVER_URL)


def _agent_service_reachable(url: str) -> bool:
    """True when an agent-service answers at ``url`` within a short timeout."""

    try:
        with urllib.request.urlopen(url, timeout=2.0):
            return True
    except urllib.error.HTTPError:
        # Any HTTP response means the service is up (even 404/405).
        return True
    except Exception:
        return False


_REQUIRES_AGENT_SERVICE = pytest.mark.skipif(
    not _agent_service_reachable(_DESKTOP_URL),
    reason=(
        f"No agent-service reachable at {_DESKTOP_URL}; computer-use flow is "
        "opt-in. Start the selfhost desktop container or set FLOW_DESKTOP_URL."
    ),
)


@_REQUIRES_AGENT_SERVICE
@pytest.mark.asyncio
async def test_desktop_screenshot_via_computer_use(
    flow_session: FlowHarness,
) -> None:
    """Brain captures the desktop through primitives.computer.desktop."""

    from unity.function_manager.primitives.runtime import _vm_ready

    previous_desktop_url = SESSION_DETAILS.assistant.desktop_url
    previous_vm_ready = _vm_ready.is_set()
    # Wire the desktop backend the way an assigned runtime would, so
    # primitives.computer.desktop resolves to the live agent-service.
    SESSION_DETAILS.assistant.desktop_url = _DESKTOP_URL
    _vm_ready.set()
    try:
        async with capture_events("DesktopPrimitiveInvoked") as desktop_events:
            await flow_session.inject_unify_message(
                "Take a screenshot of your computer desktop, then tell me in one "
                "short sentence what is visible on screen.",
            )
            reply = await flow_session.wait_for_unify_reply(timeout=300.0)
            # The desktop primitive publishes its invocation fire-and-forget;
            # yield once so that publish task lands before the capture scope
            # closes and joins outstanding callbacks.
            await asyncio.sleep(0)
        assert str(
            reply.content or "",
        ).strip(), "Assistant produced no reply after capture"
        invoked_methods = {
            event.payload.get("method")
            for event in desktop_events
            if isinstance(event.payload, dict)
        }
        assert "get_screenshot" in invoked_methods, (
            "Expected the brain to capture the screen through "
            "primitives.computer.desktop.get_screenshot, but saw desktop "
            f"primitive calls: {sorted(m for m in invoked_methods if m)!r}"
        )
    finally:
        SESSION_DETAILS.assistant.desktop_url = previous_desktop_url
        if not previous_vm_ready:
            _vm_ready.clear()
