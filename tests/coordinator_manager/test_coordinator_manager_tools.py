from __future__ import annotations

import inspect

import pytest

from droid.coordinator_manager.coordinator_manager import CoordinatorManager
from droid.manager_registry import ManagerRegistry
from droid.session_details import SESSION_DETAILS


@pytest.fixture(autouse=True)
def _reset_state():
    SESSION_DETAILS.reset()
    ManagerRegistry.clear()
    yield
    SESSION_DETAILS.reset()
    ManagerRegistry.clear()


def test_coordinator_manager_blocks_non_coordinator_sessions():
    SESSION_DETAILS.is_coordinator = False

    manager = CoordinatorManager()
    result = manager.list_assistants()

    assert result["error_kind"] == "permission_denied"
    assert result["details"]["is_coordinator"] is False


def test_coordinator_manager_delegates_reads_for_coordinator(monkeypatch):
    SESSION_DETAILS.is_coordinator = True
    SESSION_DETAILS.unify_key = "owner-key"
    SESSION_DETAILS.org_id = 7

    monkeypatch.setattr(
        "droid.coordinator_manager.coordinator_manager.unify.list_assistants",
        lambda **_: [{"agent_id": 42, "first_name": "Ops", "organization_id": 7}],
    )

    manager = CoordinatorManager()
    result = manager.list_assistants(agent_id=42)

    assert result == [{"agent_id": 42, "first_name": "Ops", "organization_id": 7}]


def test_coordinator_manager_blocks_mutations_when_role_is_not_coordinator(monkeypatch):
    SESSION_DETAILS.is_coordinator = False
    called = {"create_team": False}

    def _fake_create_team(*_args, **_kwargs):
        called["create_team"] = True
        return {"team_id": 99}

    monkeypatch.setattr(
        "droid.coordinator_manager.coordinator_manager.unify.create_team",
        _fake_create_team,
    )

    manager = CoordinatorManager()
    result = manager.create_team(name="Ops", description="Operations")

    assert result["error_kind"] == "permission_denied"
    assert called["create_team"] is False


def test_coordinator_manager_exposes_rich_primitive_docstrings():
    """Actor-facing coordinator primitives keep actionable usage guidance."""

    methods = (
        "create_assistant",
        "delegate_to_colleague",
        "create_team",
        "add_team_member",
        "commission_colleague_into_team",
    )
    for method_name in methods:
        doc = inspect.getdoc(getattr(CoordinatorManager, method_name))
        assert doc is not None
        paragraphs = [paragraph for paragraph in doc.split("\n\n") if paragraph.strip()]
        assert len(paragraphs) >= 2
