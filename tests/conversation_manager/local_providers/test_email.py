"""Unit tests for the self-host local email provider."""

from __future__ import annotations

import pytest

from unify.conversation_manager.local_providers.email import _as_message_id


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CAJ123@mail.gmail.com", "<CAJ123@mail.gmail.com>"),
        ("<CAJ123@mail.gmail.com>", "<CAJ123@mail.gmail.com>"),
        ("  CAJ123@mail.gmail.com  ", "<CAJ123@mail.gmail.com>"),
        ("<CAJ123@mail.gmail.com", "<CAJ123@mail.gmail.com>"),
        ("CAJ123@mail.gmail.com>", "<CAJ123@mail.gmail.com>"),
        ("", ""),
    ],
)
def test_as_message_id_normalizes_brackets(raw, expected):
    """The SMTP provider threads solely via In-Reply-To/References, so the
    Message-ID must be angle-bracketed regardless of how the slow brain passed
    it — otherwise the reply lands as a new thread."""
    assert _as_message_id(raw) == expected
