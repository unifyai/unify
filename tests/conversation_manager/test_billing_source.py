"""LLM work is billed by who started the turn, not by the shape of the reply."""

from unify.conversation_manager.events import (
    HUMAN_INITIATED_EVENTS,
    TASK_INITIATED_EVENTS,
    billing_source,
)


def test_human_turns_bill_as_chat_or_call():
    assert billing_source("UnifyMessageReceived", is_voice=False) == (
        "chat",
        "Chat reply",
    )
    assert billing_source("WhatsAppReceived", is_voice=False) == ("chat", "Chat reply")
    assert billing_source("InboundPhoneUtterance", is_voice=True) == (
        "call",
        "Voice reply",
    )
    assert billing_source("ApiMessageReceived", is_voice=False) == (
        "chat",
        "Chat reply",
    )


def test_scheduler_turns_bill_as_task():
    for name in TASK_INITIATED_EVENTS:
        assert billing_source(name, is_voice=False) == ("task", "Task run")


def test_everything_else_bills_as_system_with_the_event_as_label():
    # The coordinator woken to write a welcome email is not a chat, however
    # chat-like the turn it produces.
    assert billing_source("CoordinatorOnboardingEvent", is_voice=False) == (
        "system",
        "CoordinatorOnboardingEvent",
    )
    assert billing_source("NotificationInjectedEvent", is_voice=True) == (
        "system",
        "NotificationInjectedEvent",
    )
    assert billing_source("", is_voice=False) == ("system", "System turn")


def test_the_two_sets_do_not_overlap_and_only_name_real_events():
    from unify.conversation_manager import events

    assert not (HUMAN_INITIATED_EVENTS & TASK_INITIATED_EVENTS)
    for name in HUMAN_INITIATED_EVENTS | TASK_INITIATED_EVENTS:
        assert isinstance(getattr(events, name), type), name
