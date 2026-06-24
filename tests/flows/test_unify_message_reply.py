"""Inbound unify message produces an assistant reply on the unify channel."""

from __future__ import annotations

import pytest

from tests.flows.harness import FlowHarness


@pytest.mark.asyncio
async def test_unify_message_produces_reply(flow_session: FlowHarness) -> None:
    """User unify chat message -> CM turn -> outbound unify reply."""

    await flow_session.inject_unify_message(
        "Reply to me with exactly the word PONG and nothing else.",
    )
    reply = await flow_session.wait_for_unify_reply(timeout=180.0)
    content = str(reply.content or "").strip().upper()
    assert "PONG" in content

    outbox = flow_session.read_outbox()
    assert outbox, "Expected unify_message_outbound envelope in outbox"
    assert outbox[-1]["event"]["role"] == "assistant"
