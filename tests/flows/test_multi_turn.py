"""Multi-turn unify chat retains prior assistant reply in follow-up."""

from __future__ import annotations

import pytest

from tests.flows.harness import FlowHarness


@pytest.mark.asyncio
async def test_multi_turn_remembers_prior_reply(flow_session: FlowHarness) -> None:
    """First turn reply is visible when the user asks a follow-up question."""

    marker = "SILVER-FOX-9182"
    await flow_session.inject_unify_message(
        f"Remember this codeword exactly: {marker}. Reply ACK only.",
    )
    first = await flow_session.wait_for_unify_reply(timeout=240.0)
    assert str(first.content or "").strip()

    await flow_session.inject_unify_message(
        "What codeword did I just ask you to remember? Reply with the codeword only.",
    )
    second = await flow_session.wait_for_unify_reply(timeout=240.0)
    assert marker in str(second.content or "")
