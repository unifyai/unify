"""Assistant comms identity env overrides for CM integration tests."""

from __future__ import annotations

import os

TEST_ASSISTANT_EMAIL = "assistant@test.example.com"
TEST_ASSISTANT_NUMBER = "+15550001000"
TEST_ASSISTANT_WHATSAPP_NUMBER = "+15550001000"


def ensure_test_assistant_identity_env() -> None:
    """Ensure medium-faithful send tools can be exposed in CM eval tests.

    ``setdefault`` is not enough: a blank ``ASSISTANT_NUMBER=`` entry loaded
    from ``.env`` is already set and leaves ``SESSION_DETAILS.assistant.number``
    empty, hiding ``send_sms`` / ``send_email`` from the live tool schema.
    """
    if not (os.environ.get("ASSISTANT_EMAIL") or "").strip():
        os.environ["ASSISTANT_EMAIL"] = TEST_ASSISTANT_EMAIL
    if not (os.environ.get("ASSISTANT_NUMBER") or "").strip():
        os.environ["ASSISTANT_NUMBER"] = TEST_ASSISTANT_NUMBER
    if not (os.environ.get("ASSISTANT_WHATSAPP_NUMBER") or "").strip():
        os.environ["ASSISTANT_WHATSAPP_NUMBER"] = TEST_ASSISTANT_WHATSAPP_NUMBER
