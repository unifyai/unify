"""Dispatch behavior of ``_handle_coordinator_onboarding_event``.

The pure narration string is covered in ``test_coordinator_onboarding_narration``;
this file pins the two routing decisions the demo-completion redesign depends on:

* ``workspace_demo_requested`` must NOT arm a pending outbound (demos no longer
  auto-complete from a tagged summary) yet must still trigger a run so the brain
  goes and does the task.
* ``onboarding_step_completed`` must refresh the standing render but must NOT
  trigger a run — the completion came from the brain's own tool call, so a run
  here would make it acknowledge itself in a redundant second turn.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from unify.conversation_manager.domains.coordinator_onboarding import (
    _handle_coordinator_onboarding_event,
)
from unify.conversation_manager.events import CoordinatorOnboardingEvent
from unify.settings import SETTINGS

_RENDER = {"steps": [{"id": "workspace-mailbox", "status": "done"}]}


def _fake_cm() -> SimpleNamespace:
    """Minimal stand-in exposing only what the handler touches."""
    return SimpleNamespace(
        coordinator_onboarding_active=True,
        set_coordinator_onboarding_render=MagicMock(),
        set_pending_onboarding_outbound=MagicMock(),
        record_onboarding_trigger_clicked=MagicMock(),
        clear_onboarding_clicked_trigger_steps=MagicMock(),
        clear_active_learning_beat=MagicMock(),
        notifications_bar=SimpleNamespace(push_notif=MagicMock()),
        _session_logger=SimpleNamespace(info=MagicMock()),
        _current_event_trace={},
    )


@pytest.mark.anyio
async def test_workspace_demo_requested_refreshes_render_without_arming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", True)
    cm = _fake_cm()
    event = CoordinatorOnboardingEvent(
        subtype="workspace_demo_requested",
        message="The user just clicked 'Summarise my mailbox'.",
        details={"step_id": "workspace-mailbox", "onboarding": _RENDER},
    )

    should_run = await _handle_coordinator_onboarding_event(event, cm)

    # The brain must run to perform the task, but no outbound is armed: the demo
    # completes explicitly, never from a tagged send.
    assert should_run is True
    cm.set_pending_onboarding_outbound.assert_not_called()
    cm.set_coordinator_onboarding_render.assert_called_once_with(_RENDER)


@pytest.mark.anyio
async def test_render_updated_refreshes_render_without_notif_or_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", True)
    cm = _fake_cm()
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_render_updated",
        message="Onboarding progress updated.",
        details={"reason": "contact_identity_updated", "onboarding": _RENDER},
    )

    should_run = await _handle_coordinator_onboarding_event(event, cm)

    assert should_run is False
    cm.set_coordinator_onboarding_render.assert_called_once_with(_RENDER)
    cm.notifications_bar.push_notif.assert_not_called()
    cm.set_pending_onboarding_outbound.assert_not_called()


@pytest.mark.anyio
async def test_session_started_chat_delivers_scripted_intro_without_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", True)
    sleep = AsyncMock()
    monkeypatch.setattr(
        "unify.conversation_manager.domains.coordinator_onboarding.asyncio.sleep",
        sleep,
    )
    cm = _fake_cm()
    send_intro = AsyncMock()
    monkeypatch.setattr(
        "unify.conversation_manager.domains.brain_action_tools.ConversationManagerBrainActionTools",
        lambda _cm: SimpleNamespace(send_unify_message_to_boss=send_intro),
    )
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_session_started",
        message="User just opened the onboarding chat with you.",
        details={"medium": "chat"},
    )

    should_run = await _handle_coordinator_onboarding_event(event, cm)

    assert should_run is False
    sleep.assert_awaited_once()
    send_intro.assert_awaited_once()
    assert send_intro.await_args.kwargs["content"]
    assert "T-W1N" in send_intro.await_args.kwargs["content"]
    assert "Any questions before we start with the onboarding?" not in (
        send_intro.await_args.kwargs["content"]
    )
    cm.notifications_bar.push_notif.assert_called_once()


@pytest.mark.anyio
async def test_session_started_call_suppresses_run_without_chat_intro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", True)
    cm = _fake_cm()
    send_intro = AsyncMock()
    monkeypatch.setattr(
        "unify.conversation_manager.domains.brain_action_tools.ConversationManagerBrainActionTools",
        lambda _cm: SimpleNamespace(send_unify_message_to_boss=send_intro),
    )
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_session_started",
        message="User just started an onboarding voice call with you.",
        details={"medium": "call"},
    )

    should_run = await _handle_coordinator_onboarding_event(event, cm)

    assert should_run is False
    send_intro.assert_not_called()
    cm.notifications_bar.push_notif.assert_called_once()


@pytest.mark.anyio
async def test_step_completed_refreshes_render_but_suppresses_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SETTINGS, "UNITY_CONSOLE_UI", True)
    cm = _fake_cm()
    event = CoordinatorOnboardingEvent(
        subtype="onboarding_step_completed",
        message="The 'workspace-mailbox' onboarding step is now complete.",
        details={"step_id": "workspace-mailbox", "onboarding": _RENDER},
    )

    should_run = await _handle_coordinator_onboarding_event(event, cm)

    assert should_run is False
    cm.set_coordinator_onboarding_render.assert_called_once_with(_RENDER)
    cm.notifications_bar.push_notif.assert_called_once()
    cm.set_pending_onboarding_outbound.assert_not_called()
