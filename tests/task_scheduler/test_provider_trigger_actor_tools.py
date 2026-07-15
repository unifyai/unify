"""Integration tests for CodeActActor provider-trigger TaskScheduler tools."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
import unisdk

from tests.helpers import _handle_project
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler import typed_tasks_client
from unify.task_scheduler.provider_trigger_actor import CONNECTION_SUMMARY_KEYS
from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.status import Status

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)

_SECRET_KEYS = frozenset(
    {
        "access_token",
        "refresh_token",
        "client_secret",
        "oauth_token",
        "credentials",
        "api_key",
        "secret_refs",
    },
)


def _provider_event_trigger() -> dict[str, Any]:
    return json.loads(
        (_FIXTURE_DIR / "task_trigger.provider_event.v1.json").read_text(
            encoding="utf-8",
        ),
    )


def _mirror_orchestra_task(
    scheduler: TaskScheduler,
    *,
    orchestra_task: dict[str, Any],
) -> int:
    """Mirror one Orchestra typed task row into the local Tasks store."""

    scheduler._sync_provider_event_task_row(typed_response=orchestra_task)
    return int(orchestra_task["task_id"])


def _seed_non_provider_task(scheduler: TaskScheduler) -> int:
    next_task_id = int(scheduler._store.get_metric_max(key="task_id") or 0) + 1
    with scheduler._use_task_destination(None):
        scheduler._store.log(
            entries={
                "task_id": next_task_id,
                "instance_id": 0,
                "name": "Scheduled follow-up",
                "description": "A scheduled task without provider triggers.",
                "status": Status.scheduled.value,
                "enabled": True,
                "priority": "normal",
            },
            new=True,
        )
    return next_task_id


@pytest.fixture
def orchestra_assistant_and_scheduler():
    """Create one assistant, pin session agent_id, and return a TaskScheduler."""

    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"ProviderTrigger{suffix}",
        surname="Actor",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    original_agent_id = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.assistant.agent_id = agent_id
    scheduler = TaskScheduler()
    try:
        yield scheduler, agent_id
    finally:
        SESSION_DETAILS.assistant.agent_id = original_agent_id


@pytest.mark.requires_orchestra
@_handle_project
def test_provider_trigger_discovery_tools_use_typed_api_and_redact_connections(
    orchestra_assistant_and_scheduler,
) -> None:
    scheduler, _agent_id = orchestra_assistant_and_scheduler

    catalog = scheduler._list_provider_trigger_catalog()
    assert catalog["outcome"] == "provider trigger catalog listed"
    events = catalog["details"].get("events") or []
    assert any(event.get("event_slug") == "github.issue_created" for event in events)

    contract = scheduler._describe_provider_trigger_resources(
        event_slug="github.issue_created",
    )
    assert contract["outcome"] == "provider trigger resource contract described"
    assert contract["details"]["resource_kind"] == "github_repository"
    assert contract["details"]["filters"]

    connections = scheduler._list_provider_trigger_connections(
        backend_id="composio",
    )
    assert connections["outcome"] == "provider trigger connections listed"
    for connection in connections["details"].get("connections") or []:
        assert set(connection).issubset(CONNECTION_SUMMARY_KEYS)
        assert not _SECRET_KEYS.intersection(connection)
        serialized = json.dumps(connection)
        for secret_key in _SECRET_KEYS:
            assert secret_key not in serialized


@pytest.mark.requires_orchestra
@_handle_project
def test_provider_trigger_lifecycle_mutations_use_typed_api_and_surface_revision_conflict(
    orchestra_assistant_and_scheduler,
) -> None:
    scheduler, agent_id = orchestra_assistant_and_scheduler

    created = typed_tasks_client.create_task(
        payload={
            "name": "GitHub issue triage",
            "description": "Triage new GitHub issues.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    )
    task_id = _mirror_orchestra_task(scheduler, orchestra_task=created)

    scheduler._update_task(
        task_id=task_id,
        description="Updated triage scope.",
    )
    refreshed = typed_tasks_client.get_task(task_id=task_id)
    assert refreshed["task_revision"] == 2
    task = scheduler._get_task_or_raise(task_id)
    assert int(task.task_revision) == 2

    paused = scheduler._pause_provider_trigger(
        task_id=task_id,
        task_revision=int(task.task_revision),
    )
    assert paused["outcome"] == "provider trigger paused"
    paused_revision = int(paused["details"]["task_revision"])

    resumed = scheduler._resume_provider_trigger(
        task_id=task_id,
        task_revision=paused_revision,
    )
    assert resumed["outcome"] == "provider trigger resumed"
    resumed_revision = int(resumed["details"]["task_revision"])

    retried = scheduler._retry_provider_trigger(task_id=task_id)
    assert retried["outcome"] == "provider trigger reconciliation requested"

    conflict = scheduler._pause_provider_trigger(
        task_id=task_id,
        task_revision=1,
    )
    assert conflict["outcome"] == "task_revision_conflict"
    assert conflict["details"]["latest_task_revision"] > 1
    assert "reconcile" in conflict["details"]["message"]

    deleted = scheduler._delete_task(task_id=task_id)
    assert deleted["outcome"] == "task deleted"
    with pytest.raises(ValueError, match="Task not found"):
        typed_tasks_client.get_task(task_id=task_id)
    with pytest.raises(ValueError, match="No task found with id="):
        scheduler._get_task_or_raise(task_id)


@pytest.mark.requires_orchestra
@_handle_project
def test_provider_trigger_health_and_event_context_tools_are_actor_safe(
    orchestra_assistant_and_scheduler,
) -> None:
    scheduler, _agent_id = orchestra_assistant_and_scheduler

    created = typed_tasks_client.create_task(
        payload={
            "name": "GitHub issue handler",
            "description": "Handle GitHub issues.",
            "status": "triggerable",
            "trigger": _provider_event_trigger(),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    )
    task_id = _mirror_orchestra_task(scheduler, orchestra_task=created)

    health = scheduler._get_provider_trigger_health(task_id=task_id)
    assert health["outcome"] == "provider trigger health inspected"
    details = health["details"]
    assert "composed_state" in details
    assert details["task_revision"] == created["task_revision"]
    assert "manual_run_available" in details

    with pytest.raises(ValueError, match="does not have a provider-event trigger"):
        scheduler._get_provider_trigger_health(
            task_id=_seed_non_provider_task(scheduler),
        )

    stale_delete = scheduler._delete_provider_event_context(
        task_id=task_id,
        run_id=1,
        task_revision=0,
    )
    assert stale_delete["outcome"] == "task_revision_conflict"
