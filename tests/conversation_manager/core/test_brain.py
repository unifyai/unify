"""
tests/conversation_manager/core/test_brain.py
==============================================

Unit tests for the BrainSpec data structure and build_brain_spec helper
in ``domains/brain.py``.

Covers:
- The plain-text state message shape
- Boss details resolved from the session's boss contact
- Assistant identity (bio, contact details) flowing into the system prompt
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from unify.common.prompt_helpers import PromptParts
from unify.conversation_manager.domains import brain as brain_module
from unify.conversation_manager.domains.brain import BrainSpec, build_brain_spec
from unify.session_details import SESSION_DETAILS

# =============================================================================
# Helpers
# =============================================================================


def _make_brain_spec(state_prompt: str = "<state>test</state>") -> BrainSpec:
    """Create a minimal BrainSpec for testing."""
    parts = PromptParts()
    parts.add("You are a helpful assistant.")
    return BrainSpec(system_prompt=parts, state_prompt=state_prompt)


BOSS_CONTACT = {
    "first_name": "Dana",
    "surname": "Owner",
    "phone_number": "+15551234567",
    "email_address": "dana@acme.com",
}


def _make_cm(contacts: dict[int, dict] | None = None):
    """The smallest ConversationManager-like object build_brain_spec needs."""
    contacts = contacts if contacts is not None else {1: BOSS_CONTACT}
    return SimpleNamespace(
        contact_index=SimpleNamespace(
            get_contact=lambda contact_id: contacts.get(contact_id),
        ),
    )


def _make_snapshot():
    return SimpleNamespace(full_render="<state>ready</state>")


@pytest.fixture
def assistant_identity(monkeypatch):
    """Pin the assistant identity fields build_brain_spec reads."""
    monkeypatch.setattr(SESSION_DETAILS, "boss_contact_id", 1)
    monkeypatch.setattr(SESSION_DETAILS.assistant, "job_title", "")
    monkeypatch.setattr(SESSION_DETAILS.assistant, "about", "Operations assistant.")
    monkeypatch.setattr(SESSION_DETAILS.assistant, "number", "+15557654321")
    monkeypatch.setattr(SESSION_DETAILS.assistant, "email", "assistant@acme.com")
    return SESSION_DETAILS.assistant


# =============================================================================
# Tests
# =============================================================================


class TestBrainSpecStateMessage:
    """Tests for BrainSpec.state_message()."""

    def test_state_message_is_plain_text(self):
        """The state message is a plain text user turn tagged as a snapshot."""
        spec = _make_brain_spec(state_prompt="<state>hello</state>")
        msg = spec.state_message()

        assert msg["role"] == "user"
        assert isinstance(msg["content"], str)
        assert msg["content"] == "<state>hello</state>"
        assert msg["_cm_state_snapshot"] is True

    def test_snapshot_clock_reaches_state_message(self):
        """The snapshot's trailing ``Current time`` pane reaches the model.

        The clock rides at the tail of the rendered snapshot rather than in
        the system prompt, so the state message must carry it through
        verbatim.
        """
        from unify.common.prompt_helpers import now
        from unify.conversation_manager.domains.contact_index import ContactIndex
        from unify.conversation_manager.domains.notifications import NotificationBar
        from unify.conversation_manager.domains.renderer import Renderer

        snapshot = Renderer().render_state(
            ContactIndex(),
            NotificationBar(),
            in_flight_actions={},
            last_snapshot=datetime(2026, 2, 13, 11, 0, 0, tzinfo=timezone.utc),
        )
        expected_tail = f"Current time: {now()}."
        assert snapshot.full_render.endswith(expected_tail)

        plain = _make_brain_spec(state_prompt=snapshot.full_render).state_message()
        assert plain["content"].endswith(expected_tail)


class TestBuildBrainSpec:
    """Tests for build_brain_spec prompt construction."""

    def test_uses_resolved_boss_contact_id(self, monkeypatch, assistant_identity):
        """The main brain prompt reads boss details from the session contact id."""
        captured_prompt_kwargs = {}

        def fake_build_system_prompt(**kwargs):
            captured_prompt_kwargs.update(kwargs)
            parts = PromptParts()
            parts.add("system")
            return parts

        monkeypatch.setattr(SESSION_DETAILS, "boss_contact_id", 43)
        monkeypatch.setattr(
            brain_module,
            "build_system_prompt",
            fake_build_system_prompt,
        )

        contacts = {
            43: {
                "first_name": "Resolved",
                "surname": "Boss",
                "phone_number": "+123",
                "email_address": "boss@example.com",
            },
        }

        build_brain_spec(_make_cm(contacts), _make_snapshot())

        assert captured_prompt_kwargs["contact_id"] == 43
        assert captured_prompt_kwargs["first_name"] == "Resolved"
        assert captured_prompt_kwargs["surname"] == "Boss"
        assert captured_prompt_kwargs["phone_number"] == "+123"
        assert captured_prompt_kwargs["email_address"] == "boss@example.com"

    def test_state_prompt_is_the_rendered_snapshot(self, assistant_identity):
        """The snapshot's full render becomes the state prompt verbatim."""
        spec = build_brain_spec(_make_cm(), _make_snapshot())

        assert spec.state_prompt == "<state>ready</state>"

    def test_bio_carries_job_title_and_about(self, monkeypatch, assistant_identity):
        """The Bio section names the role and carries the about text."""
        monkeypatch.setattr(assistant_identity, "job_title", "Ops lead")

        spec = build_brain_spec(_make_cm(), _make_snapshot())
        prompt = spec.system_prompt.flatten()

        assert "Role / specialization: Ops lead." in prompt
        assert "Operations assistant." in prompt

    def test_boss_details_rendered_from_contact(self, assistant_identity):
        """The boss contact's details appear under Boss details."""
        spec = build_brain_spec(_make_cm(), _make_snapshot())
        prompt = spec.system_prompt.flatten()

        assert "- Contact ID: 1" in prompt
        assert "- First Name: Dana" in prompt
        assert "- Surname: Owner" in prompt
        assert "- Phone Number: +15551234567" in prompt
        assert "- Email Address: dana@acme.com" in prompt

    def test_missing_assistant_contact_details_are_flagged(
        self,
        monkeypatch,
        assistant_identity,
    ):
        """Without a number or email the prompt says so, rather than staying silent."""
        monkeypatch.setattr(assistant_identity, "number", "")
        monkeypatch.setattr(assistant_identity, "email", "")

        spec = build_brain_spec(_make_cm(), _make_snapshot())
        prompt = spec.system_prompt.flatten()

        assert "I have no phone number configured" in prompt
        assert "I have no email address configured" in prompt

    def test_configured_assistant_contact_details_are_not_flagged(
        self,
        assistant_identity,
    ):
        """With both details on file the missing-detail notices are absent."""
        spec = build_brain_spec(_make_cm(), _make_snapshot())
        prompt = spec.system_prompt.flatten()

        assert "I have no phone number configured" not in prompt
        assert "I have no email address configured" not in prompt

    def test_unknown_boss_contact_falls_back_to_empty_names(self, assistant_identity):
        """A boss contact missing from the index still yields a valid prompt."""
        spec = build_brain_spec(_make_cm({}), _make_snapshot())
        prompt = spec.system_prompt.flatten()

        assert "- Contact ID: 1" in prompt
        assert "- First Name: " in prompt
        assert "- Phone Number:" not in prompt
