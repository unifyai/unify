from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest
import requests
import unify
from unify.utils import http
from unify.utils.helpers import _create_request_header
from unify.utils.http import RequestError

from tests.conversation_manager.core.test_coordinator_product_literacy_eval import (
    _BOSS_CONTACT,
    _PRIMARY_LLM_CONFIG,
    _RecordingTools,
    _act_queries,
    _coordinator_primitive_mentions,
    CoordinatorScenario,
    DialogueTurn,
    _format_failure,
    _run_target_decision,
    _tool_payloads,
)
from tests.destination_routing_helpers import (
    assert_tool_destination,
    run_direct_routing_loop,
)
from unity.actor.code_act_actor import CodeActActor
from unity.actor.environments import StateManagerEnvironment
from unity.common.context_registry import ContextRegistry
from unity.common.llm_helpers import methods_to_tool_dict
from unity.function_manager.function_manager import FunctionManager
from unity.function_manager.primitives import PrimitiveScope, Primitives
from unity.guidance_manager.guidance_manager import GuidanceManager
from unity.session_details import SESSION_DETAILS, AssistantDetails, TeamSummary

pytestmark = [pytest.mark.eval, pytest.mark.integration, pytest.mark.llm_call]

_ASSISTANTS_PROJECT_NAME = "Assistants"
_MEMORY_CONTEXTS = {"Guidance", "Knowledge"}


@dataclass(frozen=True)
class LiveOrganization:
    """Disposable organization in the configured Orchestra environment."""

    organization_id: int
    api_key: str
    coordinator: dict[str, Any]


@pytest.fixture(autouse=True)
def _reset_runtime_context() -> Iterator[None]:
    SESSION_DETAILS.reset()
    ContextRegistry.clear()
    try:
        unify.unset_context()
    except Exception:
        pass
    try:
        yield
    finally:
        SESSION_DETAILS.reset()
        ContextRegistry.clear()
        try:
            unify.unset_context()
        except Exception:
            pass


class _AssistantAwareRecordingTools(_RecordingTools):
    """Side-effect-free Coordinator tools backed by configured assistant ids."""

    def __init__(self, assistants: list[dict[str, Any]]) -> None:
        self._assistants = assistants

    def list_assistants(
        self,
        *,
        phone: str | None = None,
        email: str | None = None,
        agent_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """List assistants visible to the Coordinator for lookup/disambiguation."""

        del phone, email
        if agent_id is None:
            return list(self._assistants)
        return [
            assistant
            for assistant in self._assistants
            if int(assistant["agent_id"]) == int(agent_id)
        ]


class _WorkspaceAwareRecordingTools(_AssistantAwareRecordingTools):
    """Side-effect-free Coordinator tools backed by the live test ids."""

    def __init__(
        self,
        *,
        assistants: list[dict[str, Any]],
        teams: list[dict[str, Any]],
        memberships: dict[int, list[dict[str, Any]]],
    ) -> None:
        super().__init__(assistants)
        self._teams = teams
        self._memberships = memberships

    def list_teams(
        self,
        *,
        owner_user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List shared workspaces visible to the current Coordinator."""

        del owner_user_id
        return list(self._teams)

    def list_team_members(self, *, team_id: int) -> list[dict[str, Any]]:
        """List assistant members for a reachable shared workspace."""

        return list(self._memberships.get(int(team_id), []))


def _headers(api_key: str) -> dict[str, str]:
    return _create_request_header(api_key)


def _require_live_orchestra_url() -> str:
    orchestra_url = os.environ.get("ORCHESTRA_URL")
    if not orchestra_url:
        pytest.skip("Coordinator persistence evals require ORCHESTRA_URL")
    return orchestra_url.rstrip("/")


def _require_user_key() -> str:
    user_key = os.environ.get("UNIFY_KEY")
    if not user_key:
        pytest.skip("UNIFY_KEY is required")
    return user_key


def _require_admin_key() -> str:
    admin_key = os.environ.get("COORDINATOR_TEST_ADMIN_KEY") or os.environ.get(
        "ORCHESTRA_ADMIN_KEY",
    )
    if not admin_key:
        pytest.skip("COORDINATOR_TEST_ADMIN_KEY or ORCHESTRA_ADMIN_KEY is required")
    return admin_key


def _delete_test_organization(
    base_url: str,
    organization_id: int,
    api_key: str,
) -> None:
    response = None
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = http.delete(
                f"{base_url}/organizations/{organization_id}",
                headers=_headers(api_key),
                raise_for_status=False,
                timeout=30,
            )
        except (RequestError, requests.exceptions.RequestException) as exc:
            last_error = exc
            response = None
        else:
            if response.status_code in {204, 404}:
                return
        if attempt < 2:
            time.sleep(1)
    assert response is not None, last_error
    assert response.status_code in {204, 404}, response.text


def _credit_test_organization(
    *,
    base_url: str,
    organization_id: int,
    admin_key: str,
) -> None:
    response = http.post(
        f"{base_url}/admin/create_recharge",
        headers=_headers(admin_key),
        json={
            "organization_id": organization_id,
            "quantity": 10,
            "type": "promo",
        },
        timeout=30,
    )
    assert response.status_code < 300, response.text


@contextmanager
def _managed_test_organization() -> Iterator[LiveOrganization]:
    base_url = _require_live_orchestra_url()
    admin_key = _require_admin_key()
    user_key = _require_user_key()
    try:
        owner = unify.get_user_basic_info(api_key=user_key)
    except (RequestError, requests.exceptions.RequestException) as exc:
        pytest.skip(f"Coordinator persistence eval needs a valid user key: {exc}")
    org_name = f"Coordinator Eval {uuid.uuid4().hex[:12]}"

    response = http.post(
        f"{base_url}/organizations",
        headers=_headers(user_key),
        json={
            "name": org_name,
            "timezone": owner.get("timezone") or "UTC",
        },
        timeout=30,
    )
    assert response.status_code == 201, response.text
    organization = response.json()
    organization_id = int(organization["id"])
    _credit_test_organization(
        base_url=base_url,
        organization_id=organization_id,
        admin_key=admin_key,
    )

    try:
        coordinator = {
            "agent_id": int(organization["coordinator_id"]),
            "first_name": "Avery",
            "surname": "Coordinator",
            "is_coordinator": True,
            "user_id": owner["user_id"],
        }
        yield LiveOrganization(
            organization_id=organization_id,
            api_key=organization["api_key"],
            coordinator=coordinator,
        )
    finally:
        _delete_test_organization(base_url, organization_id, user_key)


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


def _create_test_assistant(
    *,
    first_name: str,
    organization: LiveOrganization,
) -> dict[str, Any]:
    return unify.create_assistant(
        first_name=first_name,
        surname="Ops",
        config={
            "create_infra": False,
            "is_local": True,
            "timezone": "UTC",
        },
        api_key=organization.api_key,
    )


def _configure_session(
    *,
    organization: LiveOrganization,
    coordinator: dict[str, Any],
    teams: list[TeamSummary] | None = None,
) -> None:
    SESSION_DETAILS.unify_key = organization.api_key
    SESSION_DETAILS.org_id = organization.organization_id
    SESSION_DETAILS.assistant = AssistantDetails(
        agent_id=int(coordinator["agent_id"]),
        first_name=coordinator.get("first_name") or "Avery",
        surname=coordinator.get("surname") or "Coordinator",
        is_coordinator=True,
    )
    SESSION_DETAILS.user.id = coordinator["user_id"]
    SESSION_DETAILS.user.first_name = _BOSS_CONTACT["first_name"]
    SESSION_DETAILS.user.surname = _BOSS_CONTACT["surname"]
    SESSION_DETAILS.team_ids = [team.team_id for team in teams or []]
    SESSION_DETAILS.team_summaries = teams or []


def _activate_assistant_context(
    *,
    organization: LiveOrganization,
    assistant: dict[str, Any],
) -> None:
    unify.activate(
        _ASSISTANTS_PROJECT_NAME,
        overwrite=False,
        api_key=organization.api_key,
    )
    unify.set_context(f"{assistant['user_id']}/{assistant['agent_id']}", relative=False)


def _row_entries_text(rows: list[Any]) -> str:
    return json.dumps([row.entries for row in rows], sort_keys=True).lower()


def _single_tool_call(result, tool_name: str):
    calls = [tool for tool in result.tools if tool.name == tool_name]
    assert len(calls) == 1, json.dumps(_tool_payloads(result), indent=2)
    return calls[0]


async def _run_coordinator_code_act_query(query: str) -> Any:
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


def _logs(context: str, organization: LiveOrganization) -> list[Any]:
    return unify.get_logs(
        project=_ASSISTANTS_PROJECT_NAME,
        context=context,
        api_key=organization.api_key,
        limit=100,
    )


@pytest.mark.asyncio
async def test_coordinator_persists_confirmed_colleague_setup_rows():
    """Natural Coordinator text can select and persist colleague-owned rows."""

    with _managed_test_organization() as organization:
        assistant = _create_test_assistant(
            first_name=f"Revenue{uuid.uuid4().hex[:10]}",
            organization=organization,
        )
        assistant_id = int(assistant["agent_id"])

        scenario = CoordinatorScenario(
            scenario_id="live-colleague-setup-persistence",
            title="Confirmed colleague setup persists",
            business_context=(
                "A B2B SaaS founder has confirmed that a Revenue Ops colleague "
                "should own renewal-risk monitoring and blocked-account guidance."
            ),
            turns=(
                DialogueTurn(
                    "user",
                    f"Revenue Ops is assistant {assistant_id}. We already agreed "
                    "it should own the weekday renewal-risk summary and remember "
                    "how to check blocked enterprise accounts. Please set that "
                    "up on Revenue Ops now.",
                    new=True,
                ),
            ),
            masked_components=(
                "The target colleague id is explicitly supplied.",
                "No shared team has been requested.",
            ),
            rubric=(
                "The response should use `act` and route implementation through "
                "`primitives.coordinator.delegate_to_colleague` for the supplied "
                "Revenue Ops assistant, with a plain-English assignment covering "
                "the weekday renewal-risk summary and blocked enterprise account "
                "guidance."
            ),
            required_tools=frozenset({"act", "delegate_to_colleague"}),
            forbidden_tools=frozenset({"create_team", "add_team_member"}),
        )

        selection = await _run_target_decision(
            scenario=scenario,
            llm_config=dict(_PRIMARY_LLM_CONFIG),
            tool_source=_AssistantAwareRecordingTools(
                assistants=[
                    {
                        "agent_id": assistant_id,
                        "first_name": assistant["first_name"],
                        "surname": assistant.get("surname"),
                    },
                ],
            ),
        )
        called_tools = {tool.name for tool in selection.tools}
        assert "act" in called_tools, _format_failure(scenario, selection)
        assert not (called_tools & scenario.forbidden_tools), _format_failure(
            scenario,
            selection,
        )
        act_call = _single_tool_call(selection, "act")
        act_query = str(act_call.args.get("query") or "")
        primitive_mentions = _coordinator_primitive_mentions(_act_queries(selection))
        assert "delegate_to_colleague" in primitive_mentions, _format_failure(
            scenario,
            selection,
        )
        assert not (primitive_mentions & scenario.forbidden_tools), _format_failure(
            scenario,
            selection,
        )
        assert str(assistant_id) in act_query, _format_failure(scenario, selection)
        assert "renewal" in act_query.lower(), _format_failure(scenario, selection)
        assert "blocked" in act_query.lower(), _format_failure(scenario, selection)
        assert "enterprise" in act_query.lower(), _format_failure(scenario, selection)


@pytest.mark.asyncio
async def test_coordinator_persists_confirmed_shared_team_guidance():
    """Natural Coordinator text can select shared setup and persist team rows."""

    with _managed_test_organization() as organization:
        suffix = uuid.uuid4().hex[:8]
        revenue = _create_test_assistant(
            first_name=f"Revenue{suffix}",
            organization=organization,
        )
        support = _create_test_assistant(
            first_name=f"Support{suffix}",
            organization=organization,
        )
        coordinator = organization.coordinator
        team_description = (
            "Shared launch coordination memory for revenue operations, "
            "support handoffs, launch SOPs, and escalation rules."
        )
        team = unify.create_team(
            name=f"Launch War Room {suffix}",
            description=team_description,
            api_key=organization.api_key,
        )
        team_id = int(team["team_id"])
        for assistant in (revenue, support):
            unify.add_team_member(
                team_id,
                int(assistant["agent_id"]),
                api_key=organization.api_key,
            )

        sentinel = f"LAUNCH-HANDOFF-{uuid.uuid4().hex[:10]}"
        team_summary = TeamSummary(
            team_id=team_id,
            name=team["name"],
            description=team_description,
        )
        scenario = CoordinatorScenario(
            scenario_id="live-shared-team-setup-persistence",
            title="Confirmed shared-team setup persists",
            business_context=(
                "A product launch team wants two colleagues to share one launch "
                "handoff SOP from an existing shared team."
            ),
            turns=(
                DialogueTurn(
                    "user",
                    f"{team['name']} team {team_id} already has Revenue Ops "
                    f"assistant {revenue['agent_id']} and Support Ops assistant "
                    f"{support['agent_id']} as members. Put the {sentinel} launch "
                    "handoff SOP in that shared team so both colleagues use the "
                    "same source.",
                    new=True,
                ),
            ),
            masked_components=(
                "A reachable shared team id and assistant ids are supplied.",
                "The user says membership is already settled.",
                "The user explicitly wants one shared source across colleagues.",
            ),
            rubric=(
                "The response should treat this as shared-team setup, not "
                "colleague-owned setup. It should use `act` with instructions to "
                "write the handoff SOP into the supplied shared team, and must "
                "not call `delegate_to_colleague`."
            ),
            required_tools=frozenset({"act"}),
            forbidden_tools=frozenset(
                {
                    "delegate_to_colleague",
                    "create_team",
                    "add_team_member",
                },
            ),
            team_summaries=(team_summary,),
        )
        selection_tools = _WorkspaceAwareRecordingTools(
            assistants=[
                {
                    "agent_id": int(revenue["agent_id"]),
                    "first_name": revenue["first_name"],
                    "surname": revenue.get("surname"),
                },
                {
                    "agent_id": int(support["agent_id"]),
                    "first_name": support["first_name"],
                    "surname": support.get("surname"),
                },
            ],
            teams=[{"team_id": team_id, "name": team["name"]}],
            memberships={
                team_id: [
                    {
                        "team_id": team_id,
                        "assistant_id": int(revenue["agent_id"]),
                        "name": "Revenue Ops",
                    },
                    {
                        "team_id": team_id,
                        "assistant_id": int(support["agent_id"]),
                        "name": "Support Ops",
                    },
                ],
            },
        )

        selection = await _run_target_decision(
            scenario=scenario,
            llm_config=dict(_PRIMARY_LLM_CONFIG),
            tool_source=selection_tools,
        )
        called_tools = {tool.name for tool in selection.tools}
        assert "act" in called_tools, _format_failure(scenario, selection)
        assert not (called_tools & scenario.forbidden_tools), _format_failure(
            scenario,
            selection,
        )

        act_call = _single_tool_call(selection, "act")
        act_query = act_call.args["query"]
        primitive_mentions = _coordinator_primitive_mentions(_act_queries(selection))
        assert not (primitive_mentions & scenario.forbidden_tools), _format_failure(
            scenario,
            selection,
        )
        assert sentinel in act_query, _format_failure(scenario, selection)
        assert str(team_id) in act_query or f"team:{team_id}" in act_query

        _configure_session(
            organization=organization,
            coordinator=coordinator,
            teams=[team_summary],
        )
        with _organization_api_key(organization.api_key):
            _activate_assistant_context(
                organization=organization,
                assistant=coordinator,
            )
            manager = GuidanceManager()
            messages = await run_direct_routing_loop(
                llm_config=dict(_PRIMARY_LLM_CONFIG),
                tools=methods_to_tool_dict(
                    manager.add_guidance,
                    include_class_name=True,
                ),
                accessible_teams=[team_summary],
                message=act_query,
                loop_id="coordinator-shared-team-persistence",
            )

        assert_tool_destination(messages, "add_guidance", f"team:{team_id}")
        shared_rows = _logs(f"Teams/{team_id}/Guidance", organization)
        matching_shared = [
            row for row in shared_rows if sentinel in json.dumps(row.entries)
        ]
        assert matching_shared
        assert all(
            row.entries["authoring_assistant_id"] == int(coordinator["agent_id"])
            for row in matching_shared
        )

        personal_rows = _logs(
            f"{coordinator['user_id']}/{coordinator['agent_id']}/Guidance",
            organization,
        )
        assert sentinel not in _row_entries_text(personal_rows)
