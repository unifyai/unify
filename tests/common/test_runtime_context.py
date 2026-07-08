"""Tests for canonical runtime context root resolution."""

from __future__ import annotations

import unisdk

from unify.common.runtime_context import resolve_runtime_context_root
from unify.session_details import SESSION_DETAILS


def test_resolve_runtime_context_root_uses_session_details_in_production() -> None:
    unisdk.unset_context()
    root = resolve_runtime_context_root(test=False)
    expected = f"{SESSION_DETAILS.user_context}/{SESSION_DETAILS.assistant_context}"
    assert root == expected


def test_resolve_runtime_context_root_uses_active_context_in_tests() -> None:
    pytest_root = "tests/example/test_foo/default/0"
    unisdk.set_context(pytest_root, relative=False, skip_create=True)
    try:
        assert resolve_runtime_context_root(test=True) == pytest_root
    finally:
        unisdk.unset_context()


def test_resolve_runtime_context_root_falls_back_when_test_context_missing() -> None:
    unisdk.unset_context()
    expected = f"{SESSION_DETAILS.user_context}/{SESSION_DETAILS.assistant_context}"
    assert resolve_runtime_context_root(test=True) == expected


def test_resolve_runtime_context_root_for_team_owned_assistants() -> None:
    unisdk.unset_context()
    original_owner = SESSION_DETAILS.owner_team_id
    original_agent = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.owner_team_id = 5
    SESSION_DETAILS.assistant.agent_id = 42
    try:
        assert resolve_runtime_context_root(test=False) == "Teams/5/Assistants/42"
    finally:
        SESSION_DETAILS.owner_team_id = original_owner
        SESSION_DETAILS.assistant.agent_id = original_agent
