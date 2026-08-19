"""An actor is told which routes exist before it is told how to use them.

``primitives.computer.desktop`` and ``primitives.computer.web`` are present on
the namespace whether or not a machine backs them, and the prompt described
both in full regardless. On an assistant without the add-on that is an
instruction manual for something that cannot run: the actor reaches for a
browser, is refused, and reads the refusal as a fault worth retrying rather
than as a capability this assistant does not have.

Naming the routes that *do* work is the part that stops the retry loop --
connected workspaces, integrations, and a plain public-URL fetch cover most of
what a browser was being reached for.
"""

from __future__ import annotations

import pytest

from unify.actor.environments.computer import ComputerEnvironment
from unify.session_details import SESSION_DETAILS, AssistantDetails


@pytest.fixture()
def assistant():
    original = SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = AssistantDetails()
    yield SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = original


def _context(env_self) -> str:
    """Call the prompt builder without constructing a live environment."""
    return ComputerEnvironment.get_prompt_context(env_self)


class TestWithoutADesktop:
    def test_the_section_says_it_is_unavailable(self, assistant):
        assistant.desktop_mode = "none"
        assistant.managed_desktop_status = None

        context = _context(object())

        assert "NOT AVAILABLE" in context

    def test_it_names_the_routes_that_still_work(self, assistant):
        assistant.desktop_mode = "none"
        assistant.managed_desktop_status = None

        context = _context(object())

        assert "primitives.web.fetch" in context
        assert "connected workspace" in context.lower()
        assert "integration" in context.lower()

    def test_it_says_the_failure_is_not_transient(self, assistant):
        # Otherwise the actor retries a capability that will never appear.
        assistant.desktop_mode = "none"
        assistant.managed_desktop_status = None

        context = _context(object())

        assert "not" in context.lower() and "transient" in context.lower()

    def test_the_anonymous_link_case_is_pointed_at_the_fetch(self, assistant):
        # The one thing a browser was genuinely needed for on this path.
        assistant.desktop_mode = "none"
        assistant.managed_desktop_status = None

        assert "anyone with the link" in _context(object()).lower()

    def test_an_addon_switched_off_reads_as_unavailable(self, assistant):
        assistant.desktop_mode = "ubuntu"
        assistant.managed_desktop_status = "inactive"

        assert "NOT AVAILABLE" in _context(object())
