"""
tests/async_tool_loop/conftest.py
======================================

Shared pytest fixtures for async tool loop tests.

LLM-based tests run against the production default model (``UNIFY_MODEL``).
Override ``UNIFY_MODEL`` in the environment for one-off cross-provider runs.
"""

from __future__ import annotations

import pytest

from unify.settings import SETTINGS

_DEFAULT_MODEL = SETTINGS.UNIFY_MODEL

# Single production-default config — ``new_llm_client`` supplies
# reasoning_effort unless a test overrides it explicitly.
DEFAULT_LLM_CONFIG: dict[str, str] = {"model": _DEFAULT_MODEL}

LLM_CONFIGS = [
    pytest.param(
        DEFAULT_LLM_CONFIG,
        id=_DEFAULT_MODEL.replace("@", "-"),
    ),
]


@pytest.fixture(params=LLM_CONFIGS)
def llm_config(request) -> dict[str, str]:
    """Fixture providing the production-default LLM client configuration."""

    return request.param
