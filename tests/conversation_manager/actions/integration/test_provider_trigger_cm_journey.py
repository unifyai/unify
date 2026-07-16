"""CM → act() NL journey for provider-event task authoring."""

from __future__ import annotations

import os
import uuid

import pytest
import unisdk

from tests.helpers import _handle_project
from tests.conversation_manager.conftest import BOSS
from tests.conversation_manager.actions.integration.helpers import (
    assert_no_errors,
    get_actor_started_event,
    wait_for_actor_completion,
)
from tests.provider_trigger_delivery import (
    create_github_composio_connection,
    fetch_orchestra_task_by_name_fragment,
    probe_github_repository_full_name,
    probe_github_trigger_config,
)
from unify.conversation_manager.events import SMSReceived
from unify.session_details import SESSION_DETAILS
from unify.task_scheduler import typed_tasks_client


def _provider_trigger_sms(*, token: str) -> str:
    repo = probe_github_repository_full_name()
    return (
        "Create a provider-event triggerable task using the provider trigger catalog tools: "
        f"when a new issue is created in GitHub repo {repo}, summarize it for me. "
        "Use my connected GitHub account from the provider trigger connections list. "
        "Discover the matching trigger slug from the catalog and pin the exact connection_id. "
        f"Include token {token} in the task name so I can find it later."
    )


@pytest.fixture
def orchestra_provider_trigger_assistant(monkeypatch: pytest.MonkeyPatch):
    """Create one Orchestra assistant with a Composio GitHub connection."""

    monkeypatch.delenv("COMPOSIO_API_KEY", raising=False)
    monkeypatch.setenv(
        "COMPOSIO_WEBHOOK_SECRET",
        os.getenv("COMPOSIO_WEBHOOK_SECRET", "test-composio-webhook-secret"),
    )

    suffix = uuid.uuid4().hex[:8]
    assistant = unisdk.create_assistant(
        first_name=f"CMProvider{suffix}",
        surname="Trigger",
        config={"create_infra": False},
    )
    agent_id = int(assistant["agent_id"])
    original_agent_id = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.assistant.agent_id = agent_id
    connection = create_github_composio_connection(assistant_id=agent_id)
    connection["provider_connection_id"] = "ca_local_stub"
    connection["provider_user_id"] = "assistant:provider-trigger-probe"
    try:
        yield agent_id, connection, suffix
    finally:
        SESSION_DETAILS.assistant.agent_id = original_agent_id


def _catalog_triggers_by_slug() -> dict[str, dict]:
    catalog = typed_tasks_client.get_trigger_catalog()
    triggers = catalog.get("triggers") or []
    return {
        str(item["provider_trigger_slug"]): item
        for item in triggers
        if isinstance(item, dict) and item.get("provider_trigger_slug")
    }


@pytest.mark.asyncio
@pytest.mark.requires_orchestra
@pytest.mark.llm_call
@pytest.mark.slow
@pytest.mark.timeout(600)
@_handle_project
async def test_cm_creates_provider_event_task_via_nl(
    initialized_cm_codeact,
    orchestra_provider_trigger_assistant,
) -> None:
    """Boss SMS → CM → act() → nested TaskScheduler creates a catalog-backed trigger task."""

    agent_id, connection, suffix = orchestra_provider_trigger_assistant
    assert SESSION_DETAILS.assistant.agent_id == agent_id
    token = f"PT-{suffix}"
    expected_config = probe_github_trigger_config()

    result = await initialized_cm_codeact.step_until_wait(
        SMSReceived(contact=BOSS, content=_provider_trigger_sms(token=token)),
        max_steps=30,
    )
    actor_event = get_actor_started_event(result)
    await wait_for_actor_completion(
        initialized_cm_codeact.cm,
        actor_event.handle_id,
        timeout=600,
    )
    assert_no_errors(result)

    orchestra_task = fetch_orchestra_task_by_name_fragment(
        assistant_id=agent_id,
        name_fragment=token,
    )
    trigger = orchestra_task.get("trigger") or {}
    assert trigger.get("kind") == "provider_event"
    assert trigger.get("connection_id") == connection["connection_id"]
    assert trigger.get("canonical_app_slug") == "github"

    slug = trigger.get("provider_trigger_slug")
    assert slug
    catalog_entry = _catalog_triggers_by_slug().get(str(slug))
    assert catalog_entry is not None, f"{slug!r} is not in the staged catalog"
    assert catalog_entry.get("backend_id") == trigger.get("backend_id")

    actual_config = trigger.get("trigger_config") or {}
    assert actual_config.get("owner") == expected_config["owner"]
    assert actual_config.get("repo") == expected_config["repo"]
    assert orchestra_task.get("provider_event_binding_id")
