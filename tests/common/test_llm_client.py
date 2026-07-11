"""Tests for shared LLM client factories."""

from __future__ import annotations

import pytest
from unittest.mock import patch

from unify.common.llm_client import (
    new_llm_client,
    new_slow_brain_llm_client,
    resolve_default_model,
    resolve_slow_brain_model,
)
from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS


@pytest.fixture(autouse=True)
def _reset_session_models():
    yield
    SESSION_DETAILS.assistant.default_model = ""
    SESSION_DETAILS.assistant.default_reasoning_effort = ""
    SESSION_DETAILS.assistant.slow_brain_model = ""
    SESSION_DETAILS.assistant.slow_brain_reasoning_effort = ""


def test_resolve_default_model_falls_back_to_settings() -> None:
    """Without a per-assistant default, the platform UNIFY_MODEL applies."""
    assert resolve_default_model() == (SETTINGS.UNIFY_MODEL, None)


def test_resolve_default_model_prefers_session_default() -> None:
    SESSION_DETAILS.assistant.default_model = "claude-fable-5@anthropic"
    SESSION_DETAILS.assistant.default_reasoning_effort = "medium"
    assert resolve_default_model() == ("claude-fable-5@anthropic", "medium")


def test_resolve_slow_brain_model_uses_configured_slow_brain() -> None:
    """Without an assistant override, the shared slow-brain setting applies."""
    assert resolve_slow_brain_model() == (
        SETTINGS.conversation.SLOW_BRAIN_MODEL,
        SETTINGS.conversation.SLOW_BRAIN_REASONING_EFFORT,
    )


def test_resolve_slow_brain_model_prefers_session_slow_brain() -> None:
    SESSION_DETAILS.assistant.slow_brain_model = "claude-fable-5@anthropic"
    SESSION_DETAILS.assistant.slow_brain_reasoning_effort = "medium"
    assert resolve_slow_brain_model() == ("claude-fable-5@anthropic", "medium")


def test_resolve_slow_brain_model_ignores_actor_default() -> None:
    """Actor default_model must not override the slow brain."""
    SESSION_DETAILS.assistant.default_model = "claude-fable-5@anthropic"
    SESSION_DETAILS.assistant.default_reasoning_effort = "medium"
    assert resolve_slow_brain_model() == (
        SETTINGS.conversation.SLOW_BRAIN_MODEL,
        SETTINGS.conversation.SLOW_BRAIN_REASONING_EFFORT,
    )


def test_resolve_slow_brain_model_falls_back_to_global_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty slow-brain setting falls through to the global shared model."""
    monkeypatch.setattr(SETTINGS.conversation, "SLOW_BRAIN_MODEL", "")
    monkeypatch.setattr(SETTINGS.conversation, "SLOW_BRAIN_REASONING_EFFORT", "high")
    assert resolve_slow_brain_model() == (SETTINGS.UNIFY_MODEL, None)


def test_new_slow_brain_llm_client_uses_terra_high_by_default() -> None:
    with patch("unify.common.llm_client.unillm.AsyncUnify") as mock_async:
        new_slow_brain_llm_client(origin="ConversationManager")
        mock_async.assert_called_once_with(
            "gpt-5.6-terra@openai",
            reasoning_effort="high",
            service_tier="priority",
            stateful=False,
            origin="ConversationManager",
        )


def test_new_slow_brain_llm_client_uses_assistant_slow_brain() -> None:
    SESSION_DETAILS.assistant.slow_brain_model = "gpt-5.6-luna@openai"
    SESSION_DETAILS.assistant.slow_brain_reasoning_effort = "low"
    with patch("unify.common.llm_client.unillm.AsyncUnify") as mock_async:
        new_slow_brain_llm_client(origin="ConversationManager")
        mock_async.assert_called_once_with(
            "gpt-5.6-luna@openai",
            reasoning_effort="low",
            service_tier="priority",
            stateful=False,
            origin="ConversationManager",
        )


def test_new_llm_client_uses_assistant_default_model_and_effort() -> None:
    """The assistant default pins model and overrides call-site effort."""
    SESSION_DETAILS.assistant.default_model = "gpt-5.5@openai"
    SESSION_DETAILS.assistant.default_reasoning_effort = "low"
    with patch("unify.common.llm_client.unillm.AsyncUnify") as mock_async:
        new_llm_client(reasoning_effort="high")
        mock_async.assert_called_once_with(
            "gpt-5.5@openai",
            reasoning_effort="low",
            service_tier="priority",
            stateful=False,
            origin=None,
        )


def test_new_llm_client_without_effort_keeps_call_site_effort() -> None:
    """A default model with no effort leaves per-call-site efforts intact."""
    SESSION_DETAILS.assistant.default_model = SETTINGS.UNIFY_MODEL
    with patch("unify.common.llm_client.unillm.AsyncUnify") as mock_async:
        new_llm_client(reasoning_effort="high")
        mock_async.assert_called_once_with(
            SETTINGS.UNIFY_MODEL,
            reasoning_effort="high",
            service_tier="priority",
            stateful=False,
            origin=None,
        )


def test_new_llm_client_explicit_model_bypasses_assistant_default() -> None:
    """Call sites pinning a model (fast brain, profiles) are untouched."""
    SESSION_DETAILS.assistant.default_model = "gpt-5.5@openai"
    SESSION_DETAILS.assistant.default_reasoning_effort = "low"
    with patch("unify.common.llm_client.unillm.AsyncUnify") as mock_async:
        new_llm_client("claude-4.8-opus@anthropic", reasoning_effort="high")
        mock_async.assert_called_once_with(
            "claude-4.8-opus@anthropic",
            reasoning_effort="high",
            service_tier="priority",
            stateful=False,
            origin=None,
        )
