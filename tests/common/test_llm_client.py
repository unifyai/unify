"""Tests for shared LLM client factories."""

from __future__ import annotations

from unittest.mock import patch

from unify.common.llm_client import new_vision_llm_client
from unify.settings import SETTINGS


def test_new_vision_llm_client_uses_vision_settings() -> None:
    """Vision Q&A uses UNIFY_VISION_MODEL, not the text-only default."""
    with patch("unify.common.llm_client.unillm.AsyncUnify") as mock_async:
        client = new_vision_llm_client(origin="test.vision")
        mock_async.assert_called_once_with(
            SETTINGS.UNIFY_VISION_MODEL,
            reasoning_effort=SETTINGS.UNIFY_VISION_REASONING_EFFORT,
            service_tier="priority",
            stateful=False,
            origin="test.vision",
        )
        assert client is mock_async.return_value
