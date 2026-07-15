"""Actor control-plane E2E for provider triggers against local Orchestra + stub."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest
import requests

from tests.helpers import _handle_project
from tests.provider_trigger_delivery import (
    create_github_composio_connection,
    deliver_signed_composio_webhook,
    fetch_active_generation_for_binding,
    fetch_latest_receipt_run_key,
    load_composio_github_issue_fixture,
    orchestra_api_base,
    orchestra_api_key,
    run_orchestra_trigger_worker_cycle,
)
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler import typed_tasks_client
from unify.task_scheduler.task_scheduler import TaskScheduler

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)
_WEBHOOK_SECRET = os.getenv("COMPOSIO_WEBHOOK_SECRET", "test-composio-webhook-secret")


def _provider_event_trigger(
    *,
    connection_id: str,
    state: str = "enabled",
) -> dict[str, Any]:
    payload = json.loads(
        (_FIXTURE_DIR / "task_trigger.provider_event.v1.json").read_text(
            encoding="utf-8",
        ),
    )
    payload["connection_id"] = connection_id
    payload["state"] = state
    payload["filters"] = [
        {
            "field": "repository",
            "operator": "is",
            "value": "octocat/Hello-World",
        },
    ]
    return payload


def _mirror_orchestra_task(
    scheduler: TaskScheduler,
    *,
    orchestra_task: dict[str, Any],
) -> int:
    scheduler._sync_provider_event_task_row(typed_response=orchestra_task)
    return int(orchestra_task["task_id"])


def _fetch_binding_health(*, assistant_id: int, task_id: int) -> dict[str, Any]:
    response = requests.get(
        f"{orchestra_api_base()}/v0/assistants/{assistant_id}/tasks/{task_id}/trigger-health",
        headers={"Authorization": f"Bearer {orchestra_api_key()}"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["info"]


def _wait_for_binding_ready(
    *,
    assistant_id: int,
    task_id: int,
    max_cycles: int = 6,
) -> dict[str, Any]:
    """Run worker cycles until the binding leaves terminal health states."""

    health = _fetch_binding_health(assistant_id=assistant_id, task_id=task_id)
    ready_states = {"healthy", "provisioning", "recovering"}
    for _attempt in range(max_cycles):
        if health["runtime_health"] in ready_states:
            return health
        run_orchestra_trigger_worker_cycle()
        health = _fetch_binding_health(assistant_id=assistant_id, task_id=task_id)
    pytest.fail(
        f"provider trigger binding did not become ready: {health['runtime_health']} "
        f"({health.get('last_stable_error_code')})",
    )


@pytest.fixture
def orchestra_assistant_and_scheduler(monkeypatch: pytest.MonkeyPatch):
    """Create one assistant, pin session agent_id, and return a TaskScheduler."""

    monkeypatch.delenv("COMPOSIO_API_KEY", raising=False)
    monkeypatch.setenv("COMPOSIO_WEBHOOK_SECRET", _WEBHOOK_SECRET)

    import unisdk

    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"ProviderE2E{suffix}",
        surname="Actor",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    original_agent_id = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.assistant.agent_id = agent_id
    scheduler = TaskScheduler()
    connection = create_github_composio_connection(assistant_id=agent_id)
    try:
        yield scheduler, agent_id, connection
    finally:
        SESSION_DETAILS.assistant.agent_id = original_agent_id


@pytest.mark.requires_orchestra
@_handle_project
def test_actor_discovery_tools_see_stub_backed_catalog_and_connection(
    orchestra_assistant_and_scheduler,
) -> None:
    scheduler, _agent_id, connection = orchestra_assistant_and_scheduler

    catalog = scheduler._list_provider_trigger_catalog()
    assert catalog["outcome"] == "provider trigger catalog listed"
    events = catalog["details"].get("events") or []
    assert any(event.get("event_slug") == "github.issue_created" for event in events)

    connections = scheduler._list_provider_trigger_connections(
        event_slug="github.issue_created",
        backend_id="composio",
    )
    listed = connections["details"].get("connections") or []
    assert any(item["connection_id"] == connection["connection_id"] for item in listed)


@pytest.mark.requires_orchestra
@_handle_project
def test_actor_enable_and_stub_delivery_create_one_provider_run(
    orchestra_assistant_and_scheduler,
) -> None:
    scheduler, agent_id, connection = orchestra_assistant_and_scheduler

    created = typed_tasks_client.create_task(
        payload={
            "name": "GitHub issue triage E2E",
            "description": "Actor-enabled provider trigger.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    )
    task_id = _mirror_orchestra_task(scheduler, orchestra_task=created)
    binding_id = created.get("provider_event_binding_id")
    assert binding_id

    scheduler._retry_provider_trigger(task_id=task_id)
    if os.getenv("COMPOSIO_API_KEY"):
        pytest.skip("local Composio stub is inactive while COMPOSIO_API_KEY is set")
    run_orchestra_trigger_worker_cycle()
    _wait_for_binding_ready(assistant_id=agent_id, task_id=task_id)

    for _attempt in range(3):
        run_orchestra_trigger_worker_cycle()
        try:
            generation = fetch_active_generation_for_binding(binding_id=binding_id)
            break
        except RuntimeError:
            generation = None
    else:
        pytest.fail("active provider-trigger generation was not provisioned")

    payload = load_composio_github_issue_fixture(
        external_trigger_id=generation["external_trigger_id"],
        connected_account_id=connection.get("provider_connection_id", "ca_local_stub"),
        provider_user_id=connection.get(
            "provider_user_id",
            "assistant:provider-trigger-probe",
        ),
    )
    first = deliver_signed_composio_webhook(
        ingress_key=generation["ingress_key"],
        payload=payload,
        signing_secret=_WEBHOOK_SECRET,
        webhook_id="unity_actor_match_1",
    )
    second = deliver_signed_composio_webhook(
        ingress_key=generation["ingress_key"],
        payload=payload,
        signing_secret=_WEBHOOK_SECRET,
        webhook_id="unity_actor_match_2",
    )
    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["status"] == "accepted"
    assert first.json()["receipt_id"] == second.json()["receipt_id"]

    run_key = fetch_latest_receipt_run_key(binding_id=binding_id)
    run_response = requests.post(
        f"{orchestra_api_base()}/v0/task-run/get",
        headers={"Authorization": f"Bearer {orchestra_api_key()}"},
        json={
            "project_name": "Assistants",
            "assistant_id": str(agent_id),
            "run_key": run_key,
            "source_task_log_id": task_id,
        },
        timeout=30,
    )
    run_response.raise_for_status()
    run = run_response.json().get("run")
    assert run is not None
    assert run["source_type"] == "provider_event"


@pytest.mark.requires_orchestra
@_handle_project
def test_actor_non_matching_stub_delivery_creates_no_run(
    orchestra_assistant_and_scheduler,
) -> None:
    scheduler, agent_id, connection = orchestra_assistant_and_scheduler

    created = typed_tasks_client.create_task(
        payload={
            "name": "GitHub issue filter miss",
            "description": "Provider trigger with strict filters.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    )
    task_id = _mirror_orchestra_task(scheduler, orchestra_task=created)
    binding_id = created.get("provider_event_binding_id")
    assert binding_id

    scheduler._retry_provider_trigger(task_id=task_id)
    if os.getenv("COMPOSIO_API_KEY"):
        pytest.skip("local Composio stub is inactive while COMPOSIO_API_KEY is set")
    run_orchestra_trigger_worker_cycle()
    _wait_for_binding_ready(assistant_id=agent_id, task_id=task_id)
    generation = fetch_active_generation_for_binding(binding_id=binding_id)

    payload = load_composio_github_issue_fixture(
        external_trigger_id=generation["external_trigger_id"],
        connected_account_id=connection.get("provider_connection_id", "ca_local_stub"),
        provider_user_id=connection.get(
            "provider_user_id",
            "assistant:provider-trigger-probe",
        ),
        repository="unifyai/demo",
    )
    response = deliver_signed_composio_webhook(
        ingress_key=generation["ingress_key"],
        payload=payload,
        signing_secret=_WEBHOOK_SECRET,
        webhook_id="unity_actor_nonmatch_1",
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ignored"
    assert response.json().get("run_key") in {None, ""}
