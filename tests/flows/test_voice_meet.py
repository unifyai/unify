"""Web voice meeting (unify_meet) lifecycle health invariants.

Exercises the real meet lifecycle in-process without a LiveKit server: the
agent-joined signal, an inbound spoken utterance routed through the brain, and
clean teardown. Asserts the call-manager state invariants a healthy voice call
must hold rather than audio output (which requires the LiveKit transport).
"""

from __future__ import annotations

import pytest

from tests.flows.harness import FlowHarness


@pytest.mark.asyncio
async def test_unify_meet_lifecycle_is_healthy(flow_session: FlowHarness) -> None:
    """Meet starts active, ingests a spoken utterance, then tears down clean."""

    await flow_session.start_meet()
    await flow_session.wait_until(
        flow_session.meet_active,
        timeout=30.0,
        description="unify meet to become active",
    )

    utterance = "Hello, can you hear me on this call?"
    baseline = len(flow_session.meet_messages())
    await flow_session.speak_in_meet(utterance)

    await flow_session.wait_until(
        lambda: any(utterance in m for m in flow_session.meet_messages()),
        timeout=120.0,
        description="spoken utterance to be ingested into the meet transcript",
    )
    # The brain must keep the meet healthy while handling the utterance.
    assert flow_session.meet_active(), "Meet ended while handling an utterance"
    assert len(flow_session.meet_messages()) > baseline

    await flow_session.end_meet()
    await flow_session.wait_until(
        lambda: not flow_session.meet_active(),
        timeout=30.0,
        description="unify meet to tear down",
    )
    call_manager = flow_session.cm.call_manager
    assert call_manager.unify_meet_start_timestamp is None
    assert call_manager.room_name is None
