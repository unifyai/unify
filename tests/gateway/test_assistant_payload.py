"""Regression tests for the gateway's Orchestra assistant payload mapping.

Orchestra returns ``null`` for unset nullable columns (e.g. a coordinator with
no configured voice, phone, or surname). ``_assistant_payload`` must coerce those
to ``""`` so the runtime's ``SESSION_DETAILS`` (str-typed fields) and its
``export_to_env`` (which rejects ``None``) never receive ``None`` over the
``assistant_update`` path.
"""

from __future__ import annotations

from unify.gateway.adapters.common import _assistant_payload


def _orchestra_assistant(**overrides):
    """A minimal Orchestra assistant record with nullable fields set to None."""
    base = {
        "agent_id": 1,
        "user_id": "user-1",
        "api_key": "key",  # pragma: allowlist secret
        "user_first_name": None,
        "user_last_name": None,
        "first_name": None,
        "surname": None,
        "age": None,
        "nationality": None,
        "about": None,
        "phone": None,
        "email": None,
        "user_phone": None,
        "user_email": None,
        "voice_provider": None,
        "voice_id": None,
        "is_coordinator": True,
    }
    base.update(overrides)
    return base


def test_assistant_payload_coerces_null_string_fields_to_empty():
    payload = _assistant_payload(_orchestra_assistant())
    for field in (
        "assistant_first_name",
        "assistant_surname",
        "assistant_nationality",
        "assistant_about",
        "assistant_age",
        "assistant_number",
        "assistant_email",
        "user_first_name",
        "user_surname",
        "user_number",
        "user_email",
        "voice_provider",
        "voice_id",
    ):
        assert payload[field] == "", f"{field} should be '' not {payload[field]!r}"


def test_assistant_payload_preserves_present_values():
    payload = _assistant_payload(
        _orchestra_assistant(
            surname="Lenton",
            voice_provider="elevenlabs",
            voice_id="abc123",
            user_phone="+15555550000",
        ),
    )
    assert payload["assistant_surname"] == "Lenton"
    assert payload["voice_provider"] == "elevenlabs"
    assert payload["voice_id"] == "abc123"
    assert payload["user_number"] == "+15555550000"
