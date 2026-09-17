"""Assistant identity env overrides for CM integration tests."""

from __future__ import annotations

import os

TEST_ASSISTANT_EMAIL = "assistant@test.example.com"
TEST_ASSISTANT_NUMBER = "+15550001000"


def ensure_test_assistant_identity_env() -> None:
    """Pin the assistant identity CM tests boot with, ignoring machine state.

    The assistant's email and number are forced to fake values so the system
    prompt renders the same contact-detail notices on every machine.
    ``setdefault`` is not enough: a blank ``ASSISTANT_NUMBER=`` entry loaded
    from ``.env`` is already set and leaves ``SESSION_DETAILS.assistant.number``
    empty, flipping the prompt onto its "no phone number configured" branch.

    ``ASSISTANT_ID`` is forced blank unconditionally: a developer's ``.env`` may
    pin an assistant id, and a CM boot inheriting it would bind its context
    root to that assistant instead of the default local identity the tests
    seed. Blank rather than deleted: ``load_dotenv()`` (run when
    ``unify.conversation_manager`` modules import) does not override existing
    variables, even blank ones, while a deleted variable would be re-loaded
    straight back from ``.env``.
    """
    if not (os.environ.get("ASSISTANT_EMAIL") or "").strip():
        os.environ["ASSISTANT_EMAIL"] = TEST_ASSISTANT_EMAIL
    if not (os.environ.get("ASSISTANT_NUMBER") or "").strip():
        os.environ["ASSISTANT_NUMBER"] = TEST_ASSISTANT_NUMBER
    os.environ["ASSISTANT_ID"] = ""
