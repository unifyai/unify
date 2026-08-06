"""The channel inventory a headless run is told about.

A scheduled task cannot see the activation payload or any rendered inbound
message, so anything it is not told about it will either attempt blindly or
silently substitute. The guidance is derived from session state alone.
"""

import pytest

from unify.comms.capabilities import offline_comms_guidance


@pytest.fixture(autouse=True)
def _blank_channels(monkeypatch):
    for field, value in (
        ("number", ""),
        ("email", ""),
        ("email_provider", "google_workspace"),
        ("whatsapp_number", ""),
        ("discord_bot_id", ""),
        ("slack_bot_user_id", ""),
        ("has_ms_teams_bot", False),
    ):
        monkeypatch.setattr(
            f"unify.comms.capabilities.SESSION_DETAILS.assistant.{field}",
            value,
        )


def test_lists_only_configured_channels(monkeypatch):
    monkeypatch.setattr(
        "unify.comms.capabilities.SESSION_DETAILS.assistant.email",
        "twin@unify.ai",
    )
    monkeypatch.setattr(
        "unify.comms.capabilities.SESSION_DETAILS.assistant.slack_bot_user_id",
        "U_BOT",
    )

    guidance = offline_comms_guidance()

    assert "send_email" in guidance
    assert "send_slack_message" in guidance
    assert "send_whatsapp" not in guidance
    assert "send_ms_teams_bot_message" not in guidance


def test_teams_channels_are_distinguished(monkeypatch):
    """The two Teams paths behave differently — one can open a conversation
    and one cannot — so the run must not read them as interchangeable."""
    monkeypatch.setattr(
        "unify.comms.capabilities.SESSION_DETAILS.assistant.email_provider",
        "microsoft_365",
    )
    monkeypatch.setattr(
        "unify.comms.capabilities.SESSION_DETAILS.assistant.has_ms_teams_bot",
        True,
    )

    guidance = offline_comms_guidance()

    assert "send_teams_message" in guidance
    assert "can start a new chat" in guidance
    assert "send_ms_teams_bot_message" in guidance
    assert "Reply-only" in guidance


def test_google_workspace_does_not_advertise_teams(monkeypatch):
    monkeypatch.setattr(
        "unify.comms.capabilities.SESSION_DETAILS.assistant.email",
        "twin@unify.ai",
    )

    guidance = offline_comms_guidance()

    assert "send_teams_message" not in guidance
