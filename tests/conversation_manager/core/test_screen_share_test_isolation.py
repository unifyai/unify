"""Verify screen-share state does not leak across CM tests."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tests.conversation_manager.conftest import (
    _reset_call_state,
    _reset_screen_share_state,
)
from unify.conversation_manager.cm_types.screenshot import ScreenshotEntry


def _screenshot_entry() -> ScreenshotEntry:
    return ScreenshotEntry(
        b64="AAAA",
        utterance="test utterance",
        timestamp=datetime.now(timezone.utc),
        source="user",
    )


def _every_surface() -> tuple[str, ...]:
    from unify.conversation_manager.conversation_manager import (
        CALL_SCOPED_MEET_SURFACES,
        DESKTOP_SCOPED_MEET_SURFACES,
    )

    return (*CALL_SCOPED_MEET_SURFACES, *DESKTOP_SCOPED_MEET_SURFACES)


def test_reset_screen_share_state_helper_clears_buffer():
    """Every surface, not just the two this test used to name.

    A surface added to the CM and forgotten here is exactly the leak the helper
    exists to prevent, so the expectation is taken from the registries.
    """
    surfaces = _every_surface()
    cm = SimpleNamespace(
        _frontend_reported_meet_surfaces={surfaces[0]},
        _screenshot_buffer=[_screenshot_entry()],
        **{name: True for name in surfaces},
    )
    driver = SimpleNamespace(cm=cm)

    _reset_screen_share_state(driver)  # type: ignore[arg-type]

    for name in surfaces:
        assert getattr(cm, name) is False, name
    assert cm._frontend_reported_meet_surfaces == set()
    assert cm._screenshot_buffer == []


@pytest.mark.requires_orchestra
def test_previous_test_pollutes_screen_share_state(conversation_manager):
    conversation_manager.cm.user_screen_share_active = True
    conversation_manager.cm.assistant_screen_share_active = True
    conversation_manager.cm._screenshot_buffer.append(_screenshot_entry())

    assert conversation_manager.cm.user_screen_share_active is True
    assert conversation_manager.cm.assistant_screen_share_active is True
    assert len(conversation_manager.cm._screenshot_buffer) == 1


@pytest.mark.requires_orchestra
def test_initialized_cm_resets_screen_share_state(initialized_cm):
    assert initialized_cm.cm.user_screen_share_active is False
    assert initialized_cm.cm.assistant_screen_share_active is False
    assert initialized_cm.cm._screenshot_buffer == []


def test_reset_call_state_hands_back_a_between_calls_cm():
    """A shared CM left mid-call is the next test's clean-state precondition.

    Without this the next test does not merely fail — the call-init handler
    drops its event while ``mode.is_voice`` holds, so ``start_call`` never runs
    and the failure surfaces as "called 0 times", far from the test that
    actually left the call open.
    """
    from unify.contact_manager.types.contact import UNASSIGNED
    from unify.conversation_manager.cm_types import Mode

    call_manager = SimpleNamespace(
        call_contact={"contact_id": 3},
        conference_name="conf-1",
        room_name="unity_7_meet",
        call_session_id="session-1",
        unify_meet_call_session_id="session-1",
        provider_call_sid="CA123",
        call_start_timestamp=1.0,
        unify_meet_start_timestamp=1.0,
        google_meet_start_timestamp=1.0,
        teams_meet_start_timestamp=1.0,
        call_exchange_id=7,
        unify_meet_exchange_id=7,
        google_meet_exchange_id=7,
        teams_meet_exchange_id=7,
    )
    driver = SimpleNamespace(
        cm=SimpleNamespace(mode=Mode.CALL, call_manager=call_manager),
    )

    _reset_call_state(driver)  # type: ignore[arg-type]

    assert driver.cm.mode is Mode.TEXT
    assert not driver.cm.mode.is_voice
    assert call_manager.call_contact is None
    assert call_manager.room_name is None
    assert call_manager.call_session_id == ""
    assert call_manager.unify_meet_call_session_id == ""
    assert call_manager.call_exchange_id == UNASSIGNED
    assert call_manager.unify_meet_exchange_id == UNASSIGNED
