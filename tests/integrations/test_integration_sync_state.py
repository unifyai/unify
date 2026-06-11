from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from unity.function_manager.function_manager import FunctionManager
from unity.conversation_manager.domains.integration_sync import (
    _handle_integration_tools_sync_failed,
    _handle_integration_tools_sync_requested,
    _handle_integration_tools_sync_completed,
    _integration_tools_sync_requested_from_payload,
    _schedule_startup_integration_sync,
)
from unity.conversation_manager.domains.notifications import NotificationBar
from unity.conversation_manager.events import (
    IntegrationToolsSyncCompleted,
    IntegrationToolsSyncFailed,
    IntegrationToolsSyncRequested,
)
from unity.integrations.sync_state import IntegrationSyncCoordinator
from unity.integrations.primitives import integration_owner_scope_from_session


class FakeIntegrationOps:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def list_connections(self, **scope):
        self.calls.append(("list_connections", (), scope))
        return [
            {
                "connection_id": "conn-salesforce",
                "canonical_app_slug": "salesforce",
                "status": "connected",
            },
            {
                "connection_id": "conn-slack",
                "canonical_app_slug": "slack",
                "status": "pending",
            },
        ]


class FakeFunctionManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.result = {
            "status": "synced",
            "apps": [{"key": "composio:salesforce", "rows": 3}],
        }

    def sync_provider_integration_tools(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class FakeEventBroker:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, topic, payload):
        self.published.append((topic, payload))


@pytest.mark.anyio
async def test_sync_coordinator_materializes_connected_apps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeIntegrationOps()
    function_manager = FakeFunctionManager()
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr(
        "unity.manager_registry.ManagerRegistry.get_function_manager",
        lambda **kwargs: function_manager,
    )
    coordinator = IntegrationSyncCoordinator(
        owner_scope={"owner_scope": "assistant", "assistant_id": 42},
    )

    states = await coordinator.schedule_connected_apps()
    assert [state.app_slug for state in states] == ["salesforce"]

    await coordinator._tasks["salesforce"]

    assert coordinator.snapshot()["salesforce"].status == "ready"
    assert coordinator.snapshot()["salesforce"].tool_count == 3
    assert client.calls == [
        ("list_connections", (), {"owner_scope": "assistant", "assistant_id": 42}),
    ]
    assert function_manager.calls == [
        {
            "app_slug": "salesforce",
            "connection_id": "conn-salesforce",
            "operation": "materialize",
        },
    ]


def test_assistant_owner_scope_omits_unrelated_owner_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "unity.session_details.SESSION_DETAILS",
        SimpleNamespace(
            assistant=SimpleNamespace(agent_id=2103),
            user=SimpleNamespace(id="user-123"),
            org=SimpleNamespace(id=9),
            team=SimpleNamespace(id=7),
        ),
    )

    assert integration_owner_scope_from_session() == {
        "owner_scope": "assistant",
        "assistant_id": 2103,
    }


def test_sync_requested_payload_parses_camel_case_metadata() -> None:
    event = _integration_tools_sync_requested_from_payload(
        {
            "appSlug": "gmail",
            "appDisplayName": "Gmail",
            "connectionId": "conn-gmail",
            "message": "sync gmail tools",
        },
    )

    assert event is not None
    assert event.app_slug == "gmail"
    assert event.app_display_name == "Gmail"
    assert event.connection_id == "conn-gmail"
    assert event.operation == "materialize"


def test_sync_requested_payload_parses_cleanup_operation() -> None:
    event = _integration_tools_sync_requested_from_payload(
        {
            "extraEventFields": {
                "appSlug": "gmail",
                "connectionId": "conn-gmail",
                "operation": "cleanup",
            },
        },
    )

    assert event is not None
    assert event.app_slug == "gmail"
    assert event.connection_id == "conn-gmail"
    assert event.operation == "cleanup"


@pytest.mark.anyio
async def test_sync_requested_handler_schedules_domain_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function_manager = FakeFunctionManager()
    event_broker = FakeEventBroker()
    monkeypatch.setattr(
        "unity.manager_registry.ManagerRegistry.get_function_manager",
        lambda **kwargs: function_manager,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        FakeIntegrationOps().list_connections,
    )
    cm = SimpleNamespace(
        integration_sync_coordinator=IntegrationSyncCoordinator(owner_scope={}),
        notifications_bar=NotificationBar(),
        event_broker=event_broker,
    )

    should_wake = await _handle_integration_tools_sync_requested(
        IntegrationToolsSyncRequested(
            app_slug="Salesforce",
            app_display_name="Salesforce",
            connection_id="conn-salesforce",
        ),
        cm,
    )
    await cm.integration_sync_coordinator._tasks["salesforce"]
    await asyncio.sleep(0)

    assert should_wake is True
    assert cm.integration_sync_coordinator.snapshot()["salesforce"].status == "ready"
    assert cm.notifications_bar.notifications[-1].type == "Integrations"
    assert function_manager.calls == [
        {
            "app_slug": "salesforce",
            "connection_id": "conn-salesforce",
            "operation": "materialize",
        },
    ]
    assert event_broker.published[-1][0].endswith("integration_tools_sync_completed")
    published = json.loads(event_broker.published[-1][1])
    assert published["payload"]["connection_id"] == "conn-salesforce"
    assert published["payload"]["operation"] == "materialize"


@pytest.mark.anyio
async def test_cleanup_sync_maps_to_removed_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function_manager = FakeFunctionManager()
    function_manager.result = {
        "status": "removed",
        "apps": [],
        "removed_apps": ["composio:gmail"],
        "rows_deleted": 2,
    }
    event_broker = FakeEventBroker()
    monkeypatch.setattr(
        "unity.manager_registry.ManagerRegistry.get_function_manager",
        lambda **kwargs: function_manager,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        FakeIntegrationOps().list_connections,
    )
    cm = SimpleNamespace(
        integration_sync_coordinator=IntegrationSyncCoordinator(owner_scope={}),
        notifications_bar=NotificationBar(),
        event_broker=event_broker,
    )

    should_wake = await _handle_integration_tools_sync_requested(
        IntegrationToolsSyncRequested(
            app_slug="Gmail",
            app_display_name="Gmail",
            connection_id="conn-gmail",
            operation="cleanup",
        ),
        cm,
    )
    await cm.integration_sync_coordinator._tasks["gmail"]
    await asyncio.sleep(0)

    assert should_wake is True
    state = cm.integration_sync_coordinator.snapshot()["gmail"]
    assert state.status == "removed"
    assert cm.integration_sync_coordinator.prompt_summary() == ""
    assert function_manager.calls == [
        {
            "app_slug": "gmail",
            "connection_id": "conn-gmail",
            "operation": "cleanup",
        },
    ]
    published = json.loads(event_broker.published[-1][1])
    assert published["payload"]["operation"] == "cleanup"


@pytest.mark.anyio
async def test_sync_failed_handler_surfaces_failed_state() -> None:
    cm = SimpleNamespace(
        integration_sync_coordinator=IntegrationSyncCoordinator(owner_scope={}),
        notifications_bar=NotificationBar(),
    )

    should_wake = await _handle_integration_tools_sync_failed(
        IntegrationToolsSyncFailed(
            app_slug="salesforce",
            app_display_name="Salesforce",
            error="provider unavailable",
        ),
        cm,
    )

    assert should_wake is True
    state = cm.integration_sync_coordinator.snapshot()["salesforce"]
    assert state.status == "failed"
    assert state.error == "provider unavailable"
    assert cm.notifications_bar.notifications[-1].type == "Integrations"


@pytest.mark.anyio
async def test_startup_sync_schedules_connected_apps_without_brain_action_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function_manager = FakeFunctionManager()
    event_broker = FakeEventBroker()
    monkeypatch.setattr(
        "unity.manager_registry.ManagerRegistry.get_function_manager",
        lambda **kwargs: function_manager,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        FakeIntegrationOps().list_connections,
    )
    cm = SimpleNamespace(
        integration_sync_coordinator=IntegrationSyncCoordinator(
            owner_scope={"owner_scope": "assistant", "assistant_id": 42},
        ),
        event_broker=event_broker,
    )

    _schedule_startup_integration_sync(cm)
    for _ in range(5):
        await asyncio.sleep(0)
        if "salesforce" in cm.integration_sync_coordinator._tasks:
            break
    await cm.integration_sync_coordinator._tasks["salesforce"]
    await asyncio.sleep(0)

    assert function_manager.calls == [
        {
            "app_slug": "salesforce",
            "connection_id": "conn-salesforce",
            "operation": "materialize",
        },
    ]
    assert event_broker.published[-1][0].endswith("integration_tools_sync_completed")


def test_provider_tool_row_is_materialized_as_integration_namespace_primitive() -> None:
    fm = object.__new__(FunctionManager)
    row = fm._integration_tool_to_function_row(
        {
            "tool_id": "composio:salesforce:query_records",
            "backend_id": "composio",
            "provider_app_id": "salesforce",
            "provider_tool_id": "QUERY_RECORDS",
            "canonical_name": "primitives.integrations.salesforce.query_records",
            "app_slug": "salesforce",
            "app_display_name": "Salesforce",
            "tool_display_name": "Query Records",
            "description": "Query Salesforce records.",
            "activation_state": "connected_ready",
            "connection_id": "conn-salesforce",
            "required_scopes": ["records.read"],
            "granted_scopes": ["records.read"],
            "action_class": "read",
            "confirmation_required": False,
            "input_schema": {
                "type": "object",
                "required": ["object_name"],
                "properties": {
                    "object_name": {"type": "string"},
                    "limit": {"type": "integer"},
                },
            },
        },
    )

    assert row["name"] == "primitives.integrations.salesforce.query_records"
    assert row["argspec"] == "(object_name: str, limit: int = None) -> dict"
    assert (
        row["primitive_class"] == "unity.integrations.primitives.IntegrationPrimitives"
    )
    assert row["integration_source"] == "provider_backed"
    assert row["integration_tool_id"] == "composio:salesforce:query_records"
    assert row["integration_metadata"]["namespace"] == "primitives.integrations"
    assert "_integration_distance" not in row


def test_provider_tool_row_hash_is_order_stable() -> None:
    rows = [
        {"name": "b", "docstring": "B", "integration_tool_id": "tool-b"},
        {"name": "a", "docstring": "A", "integration_tool_id": "tool-a"},
    ]
    assert FunctionManager._hash_integration_rows(
        rows,
    ) == FunctionManager._hash_integration_rows(
        list(reversed(rows)),
    )


@pytest.mark.anyio
async def test_sync_completion_surfaces_state_without_deferred_action_resume() -> None:
    cm = SimpleNamespace(
        integration_sync_coordinator=IntegrationSyncCoordinator(owner_scope={}),
        notifications_bar=NotificationBar(),
    )

    should_wake = await _handle_integration_tools_sync_completed(
        IntegrationToolsSyncCompleted(
            app_slug="salesforce",
            app_display_name="Salesforce",
            tool_count=12,
        ),
        cm,
    )

    assert should_wake is True
    assert cm.integration_sync_coordinator.snapshot()["salesforce"].status == "ready"
    assert cm.notifications_bar.notifications[-1].type == "Integrations"
