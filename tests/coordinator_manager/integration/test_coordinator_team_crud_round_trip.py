"""Local-stack integration coverage for coordinator team CRUD primitives."""

from __future__ import annotations

import pytest
import unisdk

from tests.coordinator_manager.integration.local_stack_harness import (
    create_organization,
    delete_assistant,
    delete_team,
    unique_org_name,
)
from unity.coordinator_manager.coordinator_manager import CoordinatorManager
from unity.manager_registry import ManagerRegistry
from unity.session_details import SESSION_DETAILS

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_stack,
    pytest.mark.no_unify_context,
]


def _is_tool_error(result) -> bool:
    return isinstance(result, dict) and "error_kind" in result


def _assert_tool_success(result, *, step: str):
    assert not _is_tool_error(result), f"{step} failed: {result}"


def _team_id(record: dict) -> int:
    raw_team_id = record.get("team_id", record.get("id"))
    assert raw_team_id is not None, f"team record missing team_id: {record}"
    return int(raw_team_id)


def _assistant_id(record: dict) -> int:
    raw_assistant_id = record.get("agent_id", record.get("assistant_id"))
    assert raw_assistant_id is not None, f"assistant record missing id: {record}"
    return int(raw_assistant_id)


def _configure_coordinator_session(
    *,
    org_api_key: str,
    org_id: int,
    orchestra_url: str,
):
    SESSION_DETAILS.reset()
    ManagerRegistry.clear()
    SESSION_DETAILS.is_coordinator = True
    SESSION_DETAILS.unify_key = org_api_key
    SESSION_DETAILS.org_id = org_id
    unisdk.BASE_URL = orchestra_url


def test_coordinator_team_crud_round_trip_local_stack(require_local_stack):
    """Coordinator team tools round-trip through the real SDK and Orchestra API."""

    urls = require_local_stack
    org = create_organization(urls, name=unique_org_name("Coordinator Team CRUD"))
    _configure_coordinator_session(
        org_api_key=org["api_key"],
        org_id=org["id"],
        orchestra_url=urls.orchestra_url,
    )
    manager = CoordinatorManager()

    created_team_id: int | None = None
    colleague_id: int | None = None

    try:
        created = manager.create_team(
            name="Ops HQ",
            description="Operations shared team for coordinator CRUD coverage",
        )
        _assert_tool_success(created, step="create_team")
        assert "team_id" in created
        created_team_id = _team_id(created)

        listed = manager.list_teams()
        _assert_tool_success(listed, step="list_teams")
        listed_team_ids = {_team_id(team) for team in listed}
        assert created_team_id in listed_team_ids
        for team in listed:
            assert "team_id" in team
            assert _team_id(team) is not None

        colleague = manager.create_assistant(
            first_name="Patch",
            surname="Supervisor",
            about="Doer colleague for team membership coverage.",
            config={"is_local": True, "create_infra": False},
        )
        _assert_tool_success(colleague, step="create_assistant")
        colleague_id = _assistant_id(colleague)

        added = manager.add_team_member(
            team_id=created_team_id,
            assistant_id=colleague_id,
        )
        _assert_tool_success(added, step="add_team_member")

        members = manager.list_team_members(team_id=created_team_id)
        _assert_tool_success(members, step="list_team_members")
        member_assistant_ids = {
            int(member["assistant_id"])
            for member in members
            if member.get("assistant_id") is not None
        }
        assert colleague_id in member_assistant_ids

        removed = manager.remove_team_member(
            team_id=created_team_id,
            assistant_id=colleague_id,
        )
        _assert_tool_success(removed, step="remove_team_member")

        members_after_remove = manager.list_team_members(team_id=created_team_id)
        _assert_tool_success(
            members_after_remove,
            step="list_team_members after remove",
        )
        remaining_ids = {
            int(member["assistant_id"])
            for member in members_after_remove
            if member.get("assistant_id") is not None
        }
        assert colleague_id not in remaining_ids

        deleted_team_id = created_team_id
        deleted = manager.delete_team(team_id=created_team_id)
        _assert_tool_success(deleted, step="delete_team")
        created_team_id = None

        listed_after_delete = manager.list_teams()
        _assert_tool_success(listed_after_delete, step="list_teams after delete")
        remaining_team_ids = {_team_id(team) for team in listed_after_delete}
        assert deleted_team_id not in remaining_team_ids
    finally:
        if created_team_id is not None:
            delete_team(urls, org, created_team_id)
        if colleague_id is not None:
            delete_assistant(urls, org, colleague_id)
        SESSION_DETAILS.reset()
        ManagerRegistry.clear()
