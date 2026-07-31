"""Placeholder-content guard: bare schema tokens must never reach a user."""

import pytest

from unify.common.plain_text import is_placeholder_outbound_content


@pytest.mark.parametrize(
    "content",
    [
        None,
        "",
        "   ",
        "value",
        "string",
        " Value ",
        "STRING",
        '"value"',
        "'string'",
        "`text`",
        "content",
        "message",
        "body",
        "...",
    ],
)
def test_placeholder_content_detected(content):
    assert is_placeholder_outbound_content(content)


@pytest.mark.parametrize(
    "content",
    [
        "ok",
        "Hey!",
        "The value of this approach is clear.",
        "String theory is fascinating.",
        "value proposition draft attached",
        "no",
    ],
)
def test_real_content_passes(content):
    assert not is_placeholder_outbound_content(content)
