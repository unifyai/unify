"""Local-stack LLM coverage for shared team memory across coordinators."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

import pytest
import unisdk

from tests.conversation_manager.core.test_coordinator_product_literacy_eval import (
    _PRIMARY_LLM_CONFIG,
)
from tests.coordinator_manager.integration.local_stack_harness import (
    add_org_member,
    create_organization,
    create_user,
    credit_organization,
    delete_team,
    fetch_admin_assistant_record,
    unique_org_name,
)
from tests.destination_routing_helpers import (
    assert_tool_destination,
    run_direct_routing_loop,
)
from unify.actor.code_act_actor import CodeActActor
from unify.actor.environments import StateManagerEnvironment
from unify.common.context_registry import ContextRegistry
from unify.common.llm_helpers import methods_to_tool_dict
from unify.coordinator_manager.coordinator_manager import CoordinatorManager
from unify.function_manager.function_manager import FunctionManager
from unify.function_manager.primitives import PrimitiveScope, Primitives
from unify.guidance_manager.guidance_manager import GuidanceManager
from unify.manager_registry import ManagerRegistry
from unify.session_details import SESSION_DETAILS, AssistantDetails, TeamSummary

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_stack,
    pytest.mark.llm_call,
    pytest.mark.no_unify_context,
]

_ASSISTANTS_PROJECT_NAME = "Assistants"


def _is_tool_error(result: Any) -> bool:
    return isinstance(result, dict) and "error_kind" in result


def _assert_tool_success(result: Any, *, step: str) -> None:
    assert not _is_tool_error(result), f"{step} failed: {result}"


def _team_id(record: dict) -> int:
    raw_team_id = record.get("team_id", record.get("id"))
    assert raw_team_id is not None, f"team record missing team_id: {record}"
    return int(raw_team_id)


def _assistant_id(record: dict) -> int:
    raw_assistant_id = record.get("agent_id", record.get("assistant_id"))
    assert raw_assistant_id is not None, f"assistant record missing id: {record}"
    return int(raw_assistant_id)


def _team_summaries_from_admin_record(record: dict) -> list[TeamSummary]:
    summaries: list[TeamSummary] = []
    for item in record.get("team_summaries") or []:
        summaries.append(
            TeamSummary(
                team_id=int(item["team_id"]),
                name=str(item["name"]),
                description=str(item.get("description") or ""),
            ),
        )
    return summaries


def _reset_runtime() -> None:
    SESSION_DETAILS.reset()
    ManagerRegistry.clear()
    ContextRegistry.clear()
    try:
        unisdk.unset_context()
    except Exception:
        pass


@contextmanager
def _organization_api_key(api_key: str) -> Iterator[None]:
    previous = os.environ.get("UNIFY_KEY")
    os.environ["UNIFY_KEY"] = api_key
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("UNIFY_KEY", None)
        else:
            os.environ["UNIFY_KEY"] = previous


def _shared_guidance_rows(*, org_api_key: str, team_id: int, sentinel: str) -> list:
    logs = unisdk.get_logs(
        project=_ASSISTANTS_PROJECT_NAME,
        context=f"Teams/{team_id}/Guidance",
        api_key=org_api_key,
        limit=100,
    )
    return [row for row in logs if sentinel in json.dumps(row.entries)]


def _configure_coordinator_session(
    *,
    org_api_key: str,
    org_id: int,
    orchestra_url: str,
    coordinator: dict,
    team_summaries: list[TeamSummary],
) -> None:
    _reset_runtime()
    SESSION_DETAILS.is_coordinator = True
    SESSION_DETAILS.unify_key = org_api_key
    SESSION_DETAILS.org_id = org_id
    SESSION_DETAILS.assistant = AssistantDetails(
        agent_id=_assistant_id(coordinator),
        first_name=coordinator.get("first_name") or "Coordinator",
        surname=coordinator.get("surname") or "",
        is_coordinator=True,
    )
    SESSION_DETAILS.user.id = str(coordinator["user_id"])
    SESSION_DETAILS.user.first_name = coordinator.get("first_name") or "Member"
    SESSION_DETAILS.user.surname = coordinator.get("surname") or ""
    SESSION_DETAILS.team_ids = [summary.team_id for summary in team_summaries]
    SESSION_DETAILS.team_summaries = team_summaries
    unisdk.BASE_URL = orchestra_url


def _activate_coordinator_context(*, org_api_key: str, coordinator: dict) -> None:
    unisdk.activate(
        _ASSISTANTS_PROJECT_NAME,
        overwrite=False,
        api_key=org_api_key,
    )
    unisdk.set_context(
        f"{coordinator['user_id']}/{_assistant_id(coordinator)}",
        relative=False,
    )


async def _run_coordinator_read_query(query: str) -> Any:
    scope = PrimitiveScope.single("coordinator")
    primitives = Primitives(primitive_scope=scope)
    environment = StateManagerEnvironment(primitives)
    function_manager = FunctionManager(primitive_scope=scope, include_primitives=True)
    actor = CodeActActor(environments=[environment], function_manager=function_manager)
    try:
        handle = await actor.act(
            query,
            clarification_enabled=False,
            can_store=False,
        )
        return await asyncio.wait_for(handle.result(), timeout=240)
    finally:
        await actor.close()


def _result_text(result: Any) -> str:
    if result is None:
        return ""
    for attr in ("response", "content", "text", "message"):
        value = getattr(result, attr, None)
        if isinstance(value, str) and value.strip():
            return value
    if isinstance(result, dict):
        for key in ("response", "content", "text", "message"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value
    return str(result)


@pytest.mark.asyncio
async def test_shared_team_guidance_reaches_member_coordinators_local_stack(
    require_local_stack,
):
    """Owner coordinator LLM-writes team guidance that member coordinators can read."""

    urls = require_local_stack
    unisdk.BASE_URL = urls.orchestra_url

    suffix = uuid.uuid4().hex[:8]
    org = create_organization(
        urls,
        name=unique_org_name("Shared Team Multi Coordinator"),
    )
    credit_organization(urls, organization_id=int(org["id"]))

    member_users: list[dict] = []
    created_team_id: int | None = None

    try:
        for index in (1, 2):
            member_users.append(
                create_user(
                    urls,
                    email=f"team-member-{index}-{suffix}@example.com",
                    name=f"Member {index}",
                ),
            )
            add_org_member(urls, org, user_id=member_users[-1]["id"])

        _configure_coordinator_session(
            org_api_key=org["api_key"],
            org_id=int(org["id"]),
            orchestra_url=urls.orchestra_url,
            coordinator=org["owner_coordinator"],
            team_summaries=[],
        )
        manager = CoordinatorManager()

        team_description = (
            "Shared launch coordination memory for revenue operations, "
            "support handoffs, launch SOPs, and escalation rules."
        )
        created = manager.create_team(
            name=f"Launch War Room {suffix}",
            description=team_description,
        )
        _assert_tool_success(created, step="create_team")
        created_team_id = _team_id(created)
        team_summary = TeamSummary(
            team_id=created_team_id,
            name=str(created["name"]),
            description=team_description,
        )

        member_coordinators: list[dict] = []
        for member in member_users:
            added = manager.add_team_member(
                team_id=created_team_id,
                member_user_id=member["id"],
            )
            _assert_tool_success(added, step="add_team_member")
            coordinator_id = _assistant_id(added)
            admin_record = fetch_admin_assistant_record(urls, coordinator_id)
            assert created_team_id in [
                int(team_id) for team_id in admin_record.get("team_ids") or []
            ]
            member_coordinators.append(
                {
                    "user_id": member["id"],
                    "coordinator": admin_record,
                },
            )

        sentinel = f"LAUNCH-HANDOFF-{uuid.uuid4().hex[:10]}"
        owner_coordinator = org["owner_coordinator"]
        _configure_coordinator_session(
            org_api_key=org["api_key"],
            org_id=int(org["id"]),
            orchestra_url=urls.orchestra_url,
            coordinator=owner_coordinator,
            team_summaries=[team_summary],
        )
        _activate_coordinator_context(
            org_api_key=org["api_key"],
            coordinator=owner_coordinator,
        )

        write_message = (
            f"Save this to the {team_summary.name} shared team guidance: "
            f"{sentinel} means escalate blocked enterprise accounts to the "
            "on-call director within 15 minutes."
        )
        with _organization_api_key(org["api_key"]):
            messages = await run_direct_routing_loop(
                llm_config=dict(_PRIMARY_LLM_CONFIG),
                tools=methods_to_tool_dict(
                    GuidanceManager().add_guidance,
                    include_class_name=True,
                ),
                accessible_teams=[team_summary],
                message=write_message,
                loop_id="shared-team-multi-coordinator-write",
            )
            assert_tool_destination(messages, "add_guidance", f"team:{created_team_id}")

            shared_rows = _shared_guidance_rows(
                org_api_key=org["api_key"],
                team_id=created_team_id,
                sentinel=sentinel,
            )
        assert shared_rows, "owner write did not persist shared team guidance"

        read_query = (
            f"What does {sentinel} mean in the {team_summary.name} shared team "
            "guidance? Quote the stored guidance verbatim."
        )
        for member in member_coordinators:
            member_summaries = _team_summaries_from_admin_record(member["coordinator"])
            assert member_summaries, "member coordinator missing team summaries"
            _configure_coordinator_session(
                org_api_key=org["api_key"],
                org_id=int(org["id"]),
                orchestra_url=urls.orchestra_url,
                coordinator=member["coordinator"],
                team_summaries=member_summaries,
            )
            _activate_coordinator_context(
                org_api_key=org["api_key"],
                coordinator=member["coordinator"],
            )
            with _organization_api_key(org["api_key"]):
                read_result = await _run_coordinator_read_query(read_query)
            response_text = _result_text(read_result).lower()
            assert sentinel.lower() in response_text, (
                f"member coordinator {member['user_id']} did not surface sentinel; "
                f"response={response_text!r}"
            )
            assert "15" in response_text or "fifteen" in response_text
    finally:
        if created_team_id is not None:
            delete_team(urls, org, created_team_id)
        _reset_runtime()
