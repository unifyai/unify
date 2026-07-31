"""Meeting-chat handling for browser meets (Google Meet / Teams), both ways.

A typed message is a turn. The inbound handler shipped once without scheduling
an LLM run, which is invisible in any single-event test: the message still
reached the brain's context, so it looked delivered, and it was only answered
when somebody happened to speak afterwards. The wake assertions below are the
regression guard for that.

Both directions are covered together because the failure mode is asymmetry --
recording what participants type but not what the assistant types leaves a
transcript of questions with no answers.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains import event_handlers
from unify.conversation_manager.domains.event_handlers import EventHandler
from unify.conversation_manager.events import (
    GoogleMeetChatMessage,
    GoogleMeetChatSent,
    TeamsMeetChatMessage,
    TeamsMeetChatSent,
)


def _make_cm(*, meet_session_id: str = "recall-bot-1") -> MagicMock:
    cm = MagicMock()
    cm.contact_index.push_message = MagicMock(return_value=7)
    cm.call_manager.meet_session_id = meet_session_id
    cm.request_llm_run = AsyncMock()
    return cm


async def _settle(post: MagicMock, *, expect_call: bool) -> None:
    """Let the detached store write reach the mock before asserting on it.

    The handler dispatches it through ``create_task(to_thread(...))``, so a bare
    assertion runs first and reads an uncalled mock -- which passes a
    ``not_called`` check for the wrong reason.
    """
    for _ in range(100):
        if post.called:
            return
        await asyncio.sleep(0.01)
    assert not expect_call, "the call-store write never ran"


def _event(cls, content: str = "can you share the doc?"):
    return cls(
        contact={"contact_id": 4, "first_name": "Ada", "surname": "Owner"},
        sender_name="Ada Owner",
        content=content,
        sender_email="ada@example.com",
    )


@pytest.mark.parametrize("cls", [GoogleMeetChatMessage, TeamsMeetChatMessage])
@pytest.mark.asyncio
async def test_chat_message_wakes_the_brain(cls) -> None:
    """Someone typing must schedule a turn, not wait for someone to speak.

    ``delay=0`` rather than a debounce: a chat question is already a complete
    thought, unlike a burst of partial utterances.
    """
    cm = _make_cm()
    with patch(
        "unify.conversation_manager.domains.event_handlers."
        "post_call_utterances_to_orchestra",
    ):
        await EventHandler.handle_event(_event(cls), cm)

    cm.request_llm_run.assert_awaited_once()
    assert cm.request_llm_run.await_args.kwargs["delay"] == 0
    assert cm.request_llm_run.await_args.kwargs["triggering_contact_id"] == 4


@pytest.mark.parametrize("cls", [GoogleMeetChatMessage, TeamsMeetChatMessage])
@pytest.mark.asyncio
async def test_chat_message_lands_in_the_call_store_tagged_as_chat(cls) -> None:
    """Under the meet's own session key, so it joins the call's transcript.

    ``kind`` is what lets a reader tell it from a spoken line -- Console hides
    the seek control on chat rows, and a typed line has no audio to seek to.
    """
    cm = _make_cm(meet_session_id="recall-bot-42")
    with patch(
        "unify.conversation_manager.domains.event_handlers."
        "post_call_utterances_to_orchestra",
    ) as post:
        await EventHandler.handle_event(_event(cls), cm)
        await _settle(post, expect_call=True)

    call_key, utterances = post.call_args.args
    assert call_key == "recall-bot-42"
    (utterance,) = utterances
    assert utterance["metadata"]["kind"] == "chat"
    assert utterance["speaker_name"] == "Ada Owner"
    # Never the assistant: its own chat goes out through send_meet_chat.
    assert utterance["speaker_assistant_id"] is None
    # The bare message. The `<meeting chat>` marker is a prompt affordance on
    # the contact thread, not part of the durable record.
    assert utterance["content"] == "can you share the doc?"


@pytest.mark.asyncio
async def test_chat_message_still_wakes_the_brain_without_a_session_id() -> None:
    """A missing session key must not cost the reply as well as the record.

    The store write is what needs the key; the brain does not.
    """
    cm = _make_cm(meet_session_id="")
    with patch(
        "unify.conversation_manager.domains.event_handlers."
        "post_call_utterances_to_orchestra",
    ) as post:
        await EventHandler.handle_event(_event(GoogleMeetChatMessage), cm)
        await _settle(post, expect_call=False)

    post.assert_not_called()
    cm.request_llm_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_chat_message_reaches_the_contact_thread_marked_as_typed() -> None:
    """The brain reads the thread as flat text, so provenance rides in the body.

    Nothing there carries the structured ``kind`` the stored rows do.
    """
    cm = _make_cm()
    with patch(
        "unify.conversation_manager.domains.event_handlers."
        "post_call_utterances_to_orchestra",
    ):
        await EventHandler.handle_event(_event(GoogleMeetChatMessage), cm)

    content = cm.contact_index.push_message.call_args.kwargs["message_content"]
    assert content == "<meeting chat> can you share the doc?"


@pytest.mark.parametrize("cls", [GoogleMeetChatSent, TeamsMeetChatSent])
@pytest.mark.asyncio
async def test_assistant_chat_is_recorded_as_its_own(cls, monkeypatch) -> None:
    """Otherwise the transcript shows a chat question with no answer beside it.

    The assistant's reply went to chat, so nothing on the speech path records
    it.
    """
    assistant = event_handlers.SESSION_DETAILS.assistant
    monkeypatch.setattr(assistant, "agent_id", 55)
    # `name` is derived from these two and has no setter.
    monkeypatch.setattr(assistant, "first_name", "Ada")
    monkeypatch.setattr(assistant, "surname", "Assistant")
    cm = _make_cm()
    event = cls(contact={"contact_id": 4}, content="https://example.com/doc")

    with patch(
        "unify.conversation_manager.domains.event_handlers."
        "post_call_utterances_to_orchestra",
    ) as post:
        await EventHandler.handle_event(event, cm)
        await _settle(post, expect_call=True)

    (utterance,) = post.call_args.args[1]
    assert utterance["metadata"]["kind"] == "chat"
    assert utterance["speaker_assistant_id"] == 55
    assert utterance["speaker_name"] == "Ada Assistant"
    assert utterance["content"] == "https://example.com/doc"


@pytest.mark.asyncio
async def test_assistant_chat_does_not_wake_the_brain() -> None:
    """It just acted. A run here would have it react to its own message."""
    cm = _make_cm()
    event = GoogleMeetChatSent(contact={"contact_id": 4}, content="here you go")

    with patch(
        "unify.conversation_manager.domains.event_handlers."
        "post_call_utterances_to_orchestra",
    ):
        await EventHandler.handle_event(event, cm)

    cm.request_llm_run.assert_not_awaited()
    cm.notifications_bar.push_notif.assert_not_called()
