"""Tests for canonical runtime context root resolution."""

from __future__ import annotations

import unisdk

import unify.common.runtime_context as runtime_context
from unify.common.context_registry import ContextRegistry
from unify.common.runtime_context import (
    bind_runtime_context_root,
    resolve_runtime_context_root,
)
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


def test_bind_replaces_a_prebound_root_instead_of_joining(monkeypatch) -> None:
    """A resolved root is bound absolutely, never appended to the active one.

    Regression: with a pre-bound harness root and an unassigned session,
    bind used to JOIN "default/0" onto the active context (growing a new
    path segment per call) while caching the bare relative string as the
    registry base — splitting reads and writes across two roots.
    """
    monkeypatch.setattr(
        runtime_context,
        "resolve_runtime_context_root",
        lambda **_: "default/0",
    )
    original_base = ContextRegistry._base_context
    unisdk.set_context(
        "colleague/track/run/default/0",
        relative=False,
        skip_create=True,
    )
    try:
        bind_runtime_context_root(skip_create=True, strict=True)
        active = unisdk.get_active_context()
        assert active["read"] == "default/0"
        assert active["write"] == "default/0"
        # Repeated binds are idempotent — no path growth.
        bind_runtime_context_root(skip_create=True, strict=True)
        assert unisdk.get_active_context()["read"] == "default/0"
    finally:
        unisdk.unset_context()
        ContextRegistry.set_base_context(original_base or "")
        if original_base is None:
            ContextRegistry._base_context = None


def test_bind_keeps_registry_base_and_active_context_coherent(monkeypatch) -> None:
    """After bind, the registry base IS the bound context, verbatim."""
    monkeypatch.setattr(
        runtime_context,
        "resolve_runtime_context_root",
        lambda **_: "default/0",
    )
    original_base = ContextRegistry._base_context
    unisdk.set_context(
        "colleague/track/run/default/0",
        relative=False,
        skip_create=True,
    )
    try:
        bind_runtime_context_root(skip_create=True, strict=True)
        assert ContextRegistry._base_context == unisdk.get_active_context()["read"]
    finally:
        unisdk.unset_context()
        ContextRegistry.set_base_context(original_base or "")
        if original_base is None:
            ContextRegistry._base_context = None


def test_bind_honors_prebound_root_when_resolve_does(monkeypatch) -> None:
    """The TEST-mode contract: a pre-bound root is the session identity."""
    prebound = "colleague/track/run/default/0"
    monkeypatch.setattr(
        runtime_context,
        "resolve_runtime_context_root",
        lambda **_: prebound,
    )
    original_base = ContextRegistry._base_context
    unisdk.set_context(prebound, relative=False, skip_create=True)
    try:
        bind_runtime_context_root(skip_create=True, strict=True)
        assert unisdk.get_active_context()["read"] == prebound
        assert ContextRegistry._base_context == prebound
    finally:
        unisdk.unset_context()
        ContextRegistry.set_base_context(original_base or "")
        if original_base is None:
            ContextRegistry._base_context = None


def test_contacts_context_resolves_through_the_session_root(monkeypatch) -> None:
    """prompt_helpers must not rebuild '{user}/{agent}' as a literal path."""
    from unify.common.prompt_helpers import _contacts_context

    monkeypatch.setattr(
        runtime_context,
        "resolve_runtime_context_root",
        lambda **_: "colleague/track/run/default/0",
    )
    assert _contacts_context() == "colleague/track/run/default/0/Contacts"
