"""Runtime checks for the durable coordinator chat intro delivery gate.

The handler tests mock ``schedule_coordinator_chat_intro_delivery``; these
exercise the real gating helpers so import/path regressions fail in CI.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from unify.conversation_manager.domains.contact_index import UnifyMessage
from unify.conversation_manager.domains.coordinator_onboarding import (
    _boss_thread_has_assistant_unify_message,
    _should_deliver_coordinator_chat_intro,
)
from unify.conversation_manager.cm_types.medium import Medium


def _armed_cm(**overrides: object) -> SimpleNamespace:
    cm = SimpleNamespace(
        coordinator_onboarding_active=True,
        coordinator_intro_watched=True,
        coordinator_pending_chat_intro=True,
        coordinator_chat_intro_armed_at="2026-07-05T11:48:00+00:00",
        boss_contact_id=1,
        contact_index=None,
    )
    for key, value in overrides.items():
        setattr(cm, key, value)
    return cm


def test_should_deliver_when_chat_intro_armed_and_transcript_empty() -> None:
    assert _should_deliver_coordinator_chat_intro(_armed_cm()) is True


def test_should_not_deliver_when_pending_chat_intro_cleared() -> None:
    cm = _armed_cm(coordinator_pending_chat_intro=False)
    assert _should_deliver_coordinator_chat_intro(cm) is False


def test_should_not_deliver_when_intro_not_watched() -> None:
    cm = _armed_cm(coordinator_intro_watched=False)
    assert _should_deliver_coordinator_chat_intro(cm) is False


def test_should_not_deliver_when_onboarding_inactive() -> None:
    cm = _armed_cm(coordinator_onboarding_active=False)
    assert _should_deliver_coordinator_chat_intro(cm) is False


def test_should_not_deliver_when_boss_thread_has_assistant_message() -> None:
    contact_index = MagicMock()
    contact_index.get_messages_for_contact.return_value = [
        UnifyMessage(
            name="T-W1N",
            content="Hey, great to meet you.",
            timestamp=datetime.now(timezone.utc),
            role="assistant",
        ),
    ]
    cm = _armed_cm(contact_index=contact_index)
    assert _boss_thread_has_assistant_unify_message(cm) is True
    assert _should_deliver_coordinator_chat_intro(cm) is False
    contact_index.get_messages_for_contact.assert_called_once_with(
        1,
        Medium.UNIFY_MESSAGE,
    )


def test_boss_thread_ignores_user_messages_only() -> None:
    contact_index = MagicMock()
    contact_index.get_messages_for_contact.return_value = [
        UnifyMessage(
            name="Boss",
            content="Hello?",
            timestamp=datetime.now(timezone.utc),
            role="user",
        ),
    ]
    cm = _armed_cm(contact_index=contact_index)
    assert _boss_thread_has_assistant_unify_message(cm) is False
    assert _should_deliver_coordinator_chat_intro(cm) is True
