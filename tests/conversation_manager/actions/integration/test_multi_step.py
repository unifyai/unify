"""
Multi-step ConversationManager → CodeActActor integration tests.

These cover realistic “do X then Y” sequences where ConversationManager must:
- start an actor action
- observe completion (sometimes via injected ActorResult in step-driven tests)
- continue the sequence and emit the correct outbound events / persist side effects
"""

import pytest

from tests.helpers import _handle_project
from tests.conversation_manager.conftest import BOSS
from tests.conversation_manager.actions.integration.helpers import (
    assert_no_errors,
    get_actor_started_event,
    inject_actor_result,
    run_cm_until_wait,
    wait_for_actor_completion,
)
from unify.conversation_manager.events import UnifyMessageReceived, UnifyMessageSent

pytestmark = [pytest.mark.integration, pytest.mark.eval]


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_find_contact_then_send_message(initialized_cm_codeact):
    """
    Find a contact, then send them a chat message.

    Contract: CM can take an actor result (contact lookup) and continue the sequence
    by emitting the outbound chat event addressed to the found contact.
    """
    cm = initialized_cm_codeact

    result = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "I can't remember Alice's contact details. Please find her and then "
                "send her a message saying: Meeting at 3pm."
            ),
        ),
    )

    actor_event = get_actor_started_event(result)
    handle_id = actor_event.handle_id

    final = await wait_for_actor_completion(cm, handle_id, timeout=300)
    # Ensure the CM brain can observe completion deterministically.
    await inject_actor_result(cm, handle_id=handle_id, result=final, success=True)

    # Deterministically run the CM brain until it decides to wait again.
    followup_events = await run_cm_until_wait(cm, max_steps=5)

    sent_events = [e for e in followup_events if isinstance(e, UnifyMessageSent)]
    assert (
        sent_events
    ), "Expected a UnifyMessageSent event after actor completed and CM continued."
    assert "meeting at 3pm" in (sent_events[0].content or "").lower()
    assert sent_events[0].contact.get("first_name") == "Alice"
    assert_no_errors(result)
