"""Multi-turn unify chat retains prior assistant reply in follow-up."""

from __future__ import annotations

import pytest

from tests.flows.harness import FlowHarness


@pytest.mark.asyncio
async def test_multi_turn_remembers_prior_reply(flow_session: FlowHarness) -> None:
    """First turn reply is visible when the user asks a follow-up question.

    The marker is an ordinary project name rather than a secret-shaped token.
    Asking for a "codeword" like ``SILVER-FOX-9182`` back reads as a request to
    disclose a credential, and the assistant is told elsewhere that credentials
    are never shared through chat — it answered "I can't share that." and failed
    a test that is only trying to prove the first turn is still in context.
    """

    marker = "MARMALADE LIGHTHOUSE"
    await flow_session.inject_unify_message(
        f"My project is called {marker}. Reply ACK only.",
    )
    first = await flow_session.wait_for_unify_reply(timeout=240.0)
    assert str(first.content or "").strip()

    await flow_session.inject_unify_message(
        "What is my project called? Reply with the project name only.",
    )
    second = await flow_session.wait_for_unify_reply(timeout=240.0)
    assert marker in str(second.content or "")
