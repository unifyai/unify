"""An assistant without the desktop add-on is told so, not made to wait.

Both ``primitives.computer.desktop`` and ``primitives.computer.web`` route
through ``_make_session_method``, which blocks on ``_vm_ready`` for five
minutes and then raises "Managed VM did not become ready within 5 minutes".
On an assistant that has the add-on and is still booting that is correct. On
one that never had it, it is a five-minute stall ending in a message about a
VM the user never provisioned -- and it is the browser namespace too, so the
whole browser lane fails that way rather than saying it is unavailable.

Entitlement is knowable before the wait, so the wait should not happen.
"""

from __future__ import annotations

import pytest

from unify.function_manager.primitives.runtime import _require_desktop_entitlement
from unify.session_details import SESSION_DETAILS, AssistantDetails


@pytest.fixture()
def assistant():
    original = SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = AssistantDetails()
    yield SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = original


class TestTheGate:
    def test_an_assistant_without_the_addon_is_refused_at_once(self, assistant):
        assistant.desktop_mode = "none"
        assistant.managed_desktop_status = None

        with pytest.raises(RuntimeError) as excinfo:
            _require_desktop_entitlement("web.act")

        assert "No managed desktop is provisioned" in str(excinfo.value)

    def test_the_refusal_names_a_route_that_still_works(self, assistant):
        # A dead end is worse than a redirection: connected workspace and
        # integration reads need no desktop at all.
        assistant.desktop_mode = "none"
        assistant.managed_desktop_status = None

        with pytest.raises(RuntimeError) as excinfo:
            _require_desktop_entitlement("web.act")

        message = str(excinfo.value)
        assert "without a desktop" in message
        assert "web.act" in message

    def test_an_entitled_assistant_passes_through_to_the_wait(self, assistant):
        # Entitled but no URL yet is exactly the booting case the wait exists
        # for, so the gate must not intercept it.
        assistant.desktop_mode = "ubuntu"
        assistant.managed_desktop_status = "active"
        assistant.desktop_url = ""

        _require_desktop_entitlement("desktop.click")

    def test_a_windows_desktop_is_entitled_too(self, assistant):
        assistant.desktop_mode = "windows"
        assistant.managed_desktop_status = "active"

        _require_desktop_entitlement("desktop.click")

    def test_an_addon_switched_off_is_refused(self, assistant):
        # The mode survives when the subscription lapses, so mode alone is not
        # entitlement.
        assistant.desktop_mode = "ubuntu"
        assistant.managed_desktop_status = "inactive"

        with pytest.raises(RuntimeError):
            _require_desktop_entitlement("desktop.click")
