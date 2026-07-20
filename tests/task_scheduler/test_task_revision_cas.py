"""Provider-event task revision tests for Unity typed Tasks API plumbing."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
import unisdk
from unisdk import BASE_URL
from unisdk.utils import http
from unisdk.utils.http import RequestError

from tests.provider_trigger_delivery import create_github_composio_connection
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.trigger import ProviderEventTrigger, parse_task_trigger
from unify.task_scheduler.types.priority import Priority
from unify.task_scheduler.types.status import Status
from unify.task_scheduler.types.task import Task
from unify.task_scheduler.types.task_row_field import (
    RuntimeTaskStatus,
    split_provider_event_task_update,
)
from unify.task_scheduler.typed_tasks_client import format_task_etag
from unify.task_scheduler import typed_tasks_client
from unify.session_details import SESSION_DETAILS

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)


def _provider_event_trigger(*, connection_id: str | None = None) -> dict:
    payload = json.loads(
        (_FIXTURE_DIR / "task_trigger.provider_event.v1.json").read_text(
            encoding="utf-8",
        ),
    )
    if connection_id is not None:
        payload["connection_id"] = connection_id
    return payload


def _provider_event_task(*, task_revision: int | None = 1) -> Task:
    trigger = parse_task_trigger(_provider_event_trigger())
    assert isinstance(trigger, ProviderEventTrigger)
    return Task(
        task_id=42,
        instance_id=0,
        name="GitHub issue triage",
        description="Triage new GitHub issues.",
        status=Status.triggerable,
        trigger=trigger,
        priority=Priority.normal,
        task_revision=task_revision,
    )


def test_split_provider_event_task_update_partitions_authored_and_runtime() -> None:
    authored, runtime = split_provider_event_task_update(
        {
            "description": "Updated scope.",
            "status": Status.active.value,
        },
    )
    assert authored == {"description": "Updated scope."}
    assert runtime == {"status": Status.active.value}


def test_split_provider_event_task_update_routes_lifecycle_status_to_authored() -> None:
    authored, runtime = split_provider_event_task_update(
        {"status": Status.triggerable.value},
    )
    assert authored == {"status": Status.triggerable.value}
    assert runtime == {}


def test_split_provider_event_task_update_keeps_runtime_statuses_on_log_path() -> None:
    for status in RuntimeTaskStatus:
        authored, runtime = split_provider_event_task_update({"status": status.value})
        assert authored == {}
        assert runtime == {"status": status.value}


@pytest.mark.requires_orchestra
def test_provider_event_authored_patch_round_trip_via_typed_tasks_api() -> None:
    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"Revision{suffix}",
        surname="CAS",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    connection = create_github_composio_connection(assistant_id=agent_id)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('UNIFY_KEY', 'local-test-api-key')}",
        "Content-Type": "application/json",
    }

    created = http.post(
        f"{BASE_URL}/assistants/{agent_id}/tasks",
        headers=headers,
        json={
            "name": "GitHub issue triage",
            "description": "Triage new GitHub issues.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    ).json()["info"]
    assert created["task_revision"] == 1

    patched = http.patch(
        f"{BASE_URL}/assistants/{agent_id}/tasks/{created['task_id']}",
        headers={**headers, "If-Match": format_task_etag(1)},
        json={"description": "Updated triage scope."},
    ).json()["info"]
    assert patched["task_revision"] == 2
    assert patched["description"] == "Updated triage scope."


@pytest.mark.requires_orchestra
def test_stale_if_match_raises_task_revision_conflict() -> None:
    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"Stale{suffix}",
        surname="Revision",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    connection = create_github_composio_connection(assistant_id=agent_id)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('UNIFY_KEY', 'local-test-api-key')}",
        "Content-Type": "application/json",
    }

    created = http.post(
        f"{BASE_URL}/assistants/{agent_id}/tasks",
        headers=headers,
        json={
            "name": "GitHub issue handler",
            "description": "Handle GitHub issues.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    ).json()["info"]

    http.patch(
        f"{BASE_URL}/assistants/{agent_id}/tasks/{created['task_id']}",
        headers={**headers, "If-Match": format_task_etag(1)},
        json={"description": "First writer."},
    )

    with pytest.raises(RequestError) as exc_info:
        http.patch(
            f"{BASE_URL}/assistants/{agent_id}/tasks/{created['task_id']}",
            headers={**headers, "If-Match": format_task_etag(1)},
            json={"description": "Second writer loses."},
        )
    assert exc_info.value.response.status_code == 409
    assert "task_revision_conflict" in exc_info.value.response.text


@pytest.mark.requires_orchestra
def test_log_seam_rejects_authored_provider_event_update() -> None:
    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"Logs{suffix}",
        surname="Reject",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    connection = create_github_composio_connection(assistant_id=agent_id)
    user_id = os.environ.get(
        "AUTH_ACCOUNT_USER_ID",
        "67abcd12-1fac-4a8f-afe9-c54698c96971",
    )
    context_name = f"{user_id}/{agent_id}/Tasks"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.environ.get('UNIFY_KEY', 'local-test-api-key')}",
        "Content-Type": "application/json",
    }

    created = http.post(
        f"{BASE_URL}/assistants/{agent_id}/tasks",
        headers=headers,
        json={
            "name": "GitHub issue triage",
            "description": "Triage new GitHub issues.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    ).json()["info"]

    with pytest.raises(RequestError) as exc_info:
        http.put(
            f"{BASE_URL}/logs",
            headers=headers,
            json={
                "logs": [created["log_event_id"]],
                "context": context_name,
                "entries": {"description": "Should fail."},
                "overwrite": True,
            },
        )
    assert exc_info.value.response.status_code == 400
    assert (
        exc_info.value.response.json()["detail"]
        == "provider_event_authored_update_use_typed_tasks_api"
    )


def test_task_scheduler_has_provider_event_helper() -> None:
    scheduler = TaskScheduler()
    task = _provider_event_task(task_revision=3)
    assert scheduler._task_has_provider_event_trigger(task) is True


@pytest.fixture
def _orchestra_assistant_scheduler():
    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"RevisionCAS{suffix}",
        surname="Actor",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    connection = create_github_composio_connection(assistant_id=agent_id)
    original_agent_id = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.assistant.agent_id = agent_id
    scheduler = TaskScheduler()
    try:
        yield scheduler, agent_id, connection
    finally:
        SESSION_DETAILS.assistant.agent_id = original_agent_id


@pytest.mark.requires_orchestra
def test_pause_provider_trigger_returns_revision_conflict_outcome(
    _orchestra_assistant_scheduler,
) -> None:
    scheduler, _agent_id, connection = _orchestra_assistant_scheduler
    created = typed_tasks_client.create_task(
        payload={
            "name": "GitHub issue triage",
            "description": "Triage new GitHub issues.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    )
    task_id = int(created["task_id"])
    conflict = scheduler._pause_provider_trigger(
        task_id=task_id,
        task_revision=0,
    )
    assert conflict["outcome"] == "task_revision_conflict"
