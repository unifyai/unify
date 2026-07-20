"""Actor control-plane E2E for Pipedream provider triggers against local Orchestra."""

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
    create_github_pipedream_connection,
    deliver_signed_pipedream_webhook,
    ensure_pipedream_integration_backend_enabled,
    fetch_active_generation_signing_secret,
    fetch_latest_receipt_run_key,
    load_pipedream_github_issue_fixture,
    orchestra_api_base,
    orchestra_api_key,
    probe_github_repository_full_name,
    run_orchestra_trigger_worker_cycle,
)
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler import typed_tasks_client
from unify.task_scheduler.task_scheduler import TaskScheduler

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)


def _pipedream_provider_event_trigger(
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
    payload["backend_id"] = "pipedream"
    payload["state"] = state
    payload["provider_trigger_slug"] = "github-new-or-updated-issue"
    payload["trigger_config"] = {
        "repoFullname": probe_github_repository_full_name(),
    }
    return payload


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
    health = _fetch_binding_health(assistant_id=assistant_id, task_id=task_id)
    ready_states = {"healthy", "provisioning", "recovering"}
    for _attempt in range(max_cycles):
        if health["runtime_health"] in ready_states:
            return health
        run_orchestra_trigger_worker_cycle()
        health = _fetch_binding_health(assistant_id=assistant_id, task_id=task_id)
    pytest.fail(
        f"pipedream provider trigger binding did not become ready: "
        f"{health['runtime_health']} ({health.get('last_stable_error_code')})",
    )


@pytest.fixture
def orchestra_pipedream_assistant_and_scheduler(monkeypatch: pytest.MonkeyPatch):
    """Create one assistant with a Pipedream GitHub connection and scheduler."""

    monkeypatch.delenv("PIPEDREAM_CLIENT_ID", raising=False)
    monkeypatch.delenv("PIPEDREAM_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("PIPEDREAM_PROJECT_ID", raising=False)

    ensure_pipedream_integration_backend_enabled()

    import unisdk

    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"PipedreamE2E{suffix}",
        surname="Actor",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    original_agent_id = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.assistant.agent_id = agent_id
    scheduler = TaskScheduler()
    connection = create_github_pipedream_connection(assistant_id=agent_id)
    try:
        yield scheduler, agent_id, connection
    finally:
        SESSION_DETAILS.assistant.agent_id = original_agent_id


@pytest.mark.requires_orchestra
@_handle_project
def test_actor_lists_pipedream_backend_in_catalog(
    orchestra_pipedream_assistant_and_scheduler,
) -> None:
    scheduler, _agent_id, connection = orchestra_pipedream_assistant_and_scheduler

    catalog = scheduler._list_provider_trigger_catalog()
    triggers = catalog["details"].get("triggers") or []
    github_trigger = next(
        trigger
        for trigger in triggers
        if trigger.get("backend_id") == "pipedream"
        and trigger.get("canonical_app_slug") == "github"
    )
    assert github_trigger["provider_trigger_slug"]

    connections = scheduler._list_provider_trigger_connections(
        canonical_app_slug="github",
        backend_id="pipedream",
    )
    listed = connections["details"].get("connections") or []
    assert any(item["connection_id"] == connection["connection_id"] for item in listed)


@pytest.mark.requires_orchestra
@_handle_project
def test_actor_enable_and_pipedream_stub_delivery_create_one_provider_run(
    orchestra_pipedream_assistant_and_scheduler,
) -> None:
    scheduler, agent_id, connection = orchestra_pipedream_assistant_and_scheduler

    created = typed_tasks_client.create_task(
        payload={
            "name": "Pipedream GitHub issue triage E2E",
            "description": "Actor-enabled Pipedream provider trigger.",
            "status": "triggerable",
            "trigger": _pipedream_provider_event_trigger(
                connection_id=connection["connection_id"],
            ),
            "enabled": True,
            "offline": False,
            "priority": "normal",
        },
    )
    task_id = int(created["task_id"])
    binding_id = created.get("provider_event_binding_id")
    assert binding_id

    scheduler._retry_provider_trigger(task_id=task_id)
    if any(
        os.getenv(name)
        for name in (
            "PIPEDREAM_CLIENT_ID",
            "PIPEDREAM_CLIENT_SECRET",
            "PIPEDREAM_PROJECT_ID",
        )
    ):
        pytest.skip("local Pipedream stub is inactive while live credentials are set")
    run_orchestra_trigger_worker_cycle()
    _wait_for_binding_ready(assistant_id=agent_id, task_id=task_id)

    for _attempt in range(3):
        run_orchestra_trigger_worker_cycle()
        try:
            generation = fetch_active_generation_signing_secret(binding_id=binding_id)
            break
        except RuntimeError:
            generation = None
    else:
        pytest.fail("active Pipedream provider-trigger generation was not provisioned")

    payload = load_pipedream_github_issue_fixture(
        repository=probe_github_repository_full_name(),
    )
    first = deliver_signed_pipedream_webhook(
        ingress_key=generation["ingress_key"],
        payload=payload,
        signing_secret=generation["signing_secret"],
    )
    second = deliver_signed_pipedream_webhook(
        ingress_key=generation["ingress_key"],
        payload=payload,
        signing_secret=generation["signing_secret"],
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
