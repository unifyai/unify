from __future__ import annotations

import logging
from types import SimpleNamespace

from unity.function_manager.function_manager import FunctionManager
from unity.function_manager.primitives.scope import PrimitiveScope
from unity.settings import SETTINGS

MOCK_TOOL = {
    "tool_id": "composio:hubspot:search_contacts",
    "backend_id": "composio",
    "provider_app_id": "hubspot",
    "provider_tool_id": "hubspot.search_contacts",
    "canonical_name": "primitives.integrations.hubspot.search_contacts",
    "function_manager_name": "primitives_integrations__hubspot__search_contacts",
    "app_slug": "hubspot",
    "app_display_name": "HubSpot",
    "app_icon_url": "https://provider.example/icons/hubspot.png",
    "tool_display_name": "Search contacts",
    "description": "Find HubSpot CRM contacts by email, company, or lifecycle stage.",
    "activation_state": "connected_ready",
    "connection_id": "conn-1",
    "required_scopes": ["crm.objects.contacts.read"],
    "action_class": "read",
    "confirmation_required": False,
    "schema_available": True,
    "provider_error_status": None,
}


class FakeIntegrationOps:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.connections = [
            {
                "connection_id": "conn-1",
                "canonical_app_slug": "hubspot",
                "status": "connected",
            },
        ]
        self.results = [MOCK_TOOL]

    def list_connections(self, **scope):
        self.calls.append(("list_connections", (), scope))
        return list(self.connections)

    def search_tools(self, query: str, **payload):
        self.calls.append(("search_tools", (query,), payload))
        results = [
            item
            for item in self.results
            if item.get("activation_state") == "connected_ready"
        ]
        offset = payload.get("offset", 0)
        limit = payload.get("limit", len(results))
        return results[offset : offset + limit]

    def get_tools(self, **payload):
        self.calls.append(("get_tools", (), payload))
        results = [
            item
            for item in self.results
            if item.get("activation_state")
            == payload.get("activation_state", "connected_ready")
        ]
        offset = payload.get("offset", 0)
        limit = payload.get("limit", len(results))
        return {"items": results[offset : offset + limit], "total": len(results)}


def _fake_function_manager() -> FunctionManager:
    fm = FunctionManager.__new__(FunctionManager)
    fm._include_primitives = True
    fm._primitive_scope = PrimitiveScope.single("integrations")
    fm._integration_owner_scope = lambda: {
        "owner_scope": "assistant",
        "assistant_id": 42,
    }
    fm._get_stored_integration_tool_hash_by_app = lambda: {}
    fm._store_integration_tool_hash_by_app = lambda hashes: setattr(
        fm,
        "_stored_hashes",
        hashes,
    )
    fm._delete_provider_integration_rows_for_apps = (
        lambda app_keys: setattr(fm, "_deleted_apps", app_keys) or 0
    )
    fm._inserted_rows = []
    fm._insert_primitives = lambda rows: fm._inserted_rows.extend(rows)
    return fm


def _capture_function_manager_logs(caplog):
    sync_logger = logging.getLogger("unity.function_manager.function_manager")
    sync_logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger="unity.function_manager.function_manager")
    return sync_logger


def test_materializes_connected_provider_tools_with_active_only_search(
    monkeypatch,
) -> None:
    client = FakeIntegrationOps()
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="hubspot")

    assert result["status"] == "synced"
    assert result["apps"] == [{"key": "composio:hubspot", "rows": 1, "rows_deleted": 0}]
    assert client.calls == [
        ("list_connections", (), {"owner_scope": "assistant", "assistant_id": 42}),
        (
            "get_tools",
            (),
            {
                "limit": 500,
                "offset": 0,
                "activation_state": "connected_ready",
                "include_unconnected": False,
                "canonical_app_slug": "hubspot",
                "owner_scope": "assistant",
                "assistant_id": 42,
            },
        ),
    ]
    row = fm._inserted_rows[0]
    assert row["name"] == "primitives.integrations.hubspot.search_contacts"
    assert row["integration_tool_id"] == "composio:hubspot:search_contacts"
    assert row["backend_id"] == "composio"
    metadata = row["integration_metadata"]
    assert metadata["activation_state"] == "connected_ready"
    assert metadata["required_scopes"] == ["crm.objects.contacts.read"]
    assert metadata["connection_id"] == "conn-1"
    assert metadata["action_class"] == "read"
    assert metadata["confirmation_required"] is False
    assert metadata["schema_available"] is True
    assert metadata["provider_app_id"] == "hubspot"
    assert metadata["provider_tool_id"] == "hubspot.search_contacts"
    assert metadata["app_icon_url"] == "https://provider.example/icons/hubspot.png"
    assert metadata.get("provider_error_status") is None
    assert row["integration_source"] == "provider_backed"
    assert row["depends_on"] == []


def test_provider_integration_function_id_is_stable_signed_int32() -> None:
    tool_id = "composio:hubspot:search_contacts"

    first = FunctionManager._provider_integration_function_id(tool_id)
    second = FunctionManager._provider_integration_function_id(tool_id)
    different_tool = FunctionManager._provider_integration_function_id(
        "composio:hubspot:create_contact",
    )

    assert first == second
    assert first != different_tool
    assert 0 <= first <= 0x7FFFFFFF
    assert 0 <= different_tool <= 0x7FFFFFFF


def test_materialization_excludes_unconnected_tools(monkeypatch) -> None:
    client = FakeIntegrationOps()
    client.results = [
        {
            **MOCK_TOOL,
            "activation_state": "not_connected",
            "connection_id": None,
        },
    ]
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="hubspot")

    assert result["status"] == "unchanged"
    assert fm._inserted_rows == []


def test_staging_sync_logs_zero_row_state(monkeypatch, caplog) -> None:
    monkeypatch.setattr(SETTINGS, "DEPLOY_ENV", "staging")
    client = FakeIntegrationOps()
    client.results = []
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()
    sync_logger = _capture_function_manager_logs(caplog)

    try:
        result = fm.sync_provider_integration_tools(app_slug="hubspot")
    finally:
        sync_logger.removeHandler(caplog.handler)

    assert result["status"] == "unchanged"
    assert "Provider integration sync filtered tools" in caplog.text
    assert "raw_tools=0" in caplog.text
    assert "status=unchanged" in caplog.text


def test_staging_sync_logs_skipped_state(monkeypatch, caplog) -> None:
    monkeypatch.setattr(SETTINGS, "DEPLOY_ENV", "staging")
    fm = _fake_function_manager()
    fm._include_primitives = False
    sync_logger = _capture_function_manager_logs(caplog)

    try:
        result = fm.sync_provider_integration_tools(app_slug="hubspot")
    finally:
        sync_logger.removeHandler(caplog.handler)

    assert result["status"] == "skipped"
    assert "Provider integration sync skipped" in caplog.text
    assert "reason=integrations_not_in_scope" in caplog.text


def test_staging_sync_logs_insert_mismatch(monkeypatch, caplog) -> None:
    monkeypatch.setattr(SETTINGS, "DEPLOY_ENV", "staging")
    client = FakeIntegrationOps()
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_private_fields",
        lambda _context: [],
    )
    monkeypatch.setattr("unify.get_logs", lambda **_kwargs: [])
    fm = _fake_function_manager()
    fm._primitives_ctx = "Functions/Primitives"
    fm._insert_primitives = lambda _rows: None
    sync_logger = _capture_function_manager_logs(caplog)

    try:
        result = fm.sync_provider_integration_tools(app_slug="hubspot")
    finally:
        sync_logger.removeHandler(caplog.handler)

    assert result["status"] == "synced"
    assert "Provider integration sync insert attempt key=composio:hubspot rows=1" in (
        caplog.text
    )
    assert "Provider integration write verification mismatch" in caplog.text
    assert "expected_rows=1 observed_rows=0" in caplog.text


def test_materialization_pages_app_scoped_provider_tools(monkeypatch) -> None:
    client = FakeIntegrationOps()
    client.results = [
        MOCK_TOOL,
        {
            **MOCK_TOOL,
            "tool_id": "composio:hubspot:create_contact",
            "canonical_name": "primitives.integrations.hubspot.create_contact",
            "function_manager_name": "primitives_integrations__hubspot__create_contact",
            "provider_tool_id": "hubspot.create_contact",
            "tool_display_name": "Create contact",
            "action_class": "write",
        },
    ]
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="HubSpot", limit=1)

    assert result["status"] == "synced"
    assert result["apps"] == [{"key": "composio:hubspot", "rows": 2, "rows_deleted": 0}]
    assert [call[2]["offset"] for call in client.calls if call[0] == "get_tools"] == [
        0,
        1,
    ]
    assert all(call[0] != "search_tools" for call in client.calls)
    assert all(
        call[2]["canonical_app_slug"] == "hubspot"
        for call in client.calls
        if call[0] == "get_tools"
    )
    assert [row["name"] for row in fm._inserted_rows] == [
        "primitives.integrations.hubspot.search_contacts",
        "primitives.integrations.hubspot.create_contact",
    ]


def test_materialized_rows_include_pipedream_metadata_and_provider_error_status(
    monkeypatch,
) -> None:
    client = FakeIntegrationOps()
    client.connections = [
        {
            "connection_id": "conn-pd",
            "canonical_app_slug": "slack",
            "status": "connected",
        },
    ]
    client.results = [
        {
            **MOCK_TOOL,
            "tool_id": "pipedream:slack:send_message",
            "backend_id": "pipedream",
            "provider_app_id": "slack",
            "provider_tool_id": "slack-send-message",
            "canonical_name": "primitives.integrations.slack.send_message",
            "function_manager_name": "primitives_integrations__slack__send_message",
            "app_slug": "slack",
            "app_display_name": "Slack",
            "app_icon_url": "https://provider.example/icons/slack.png",
            "tool_display_name": "Send message",
            "activation_state": "connected_ready",
            "connection_id": "conn-pd",
            "required_scopes": ["chat:write"],
            "action_class": "write",
            "confirmation_required": True,
            "provider_error_status": "provider_request_failed",
        },
    ]
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="slack")

    assert result["status"] == "synced"
    row = fm._inserted_rows[0]
    assert row["name"] == "primitives.integrations.slack.send_message"
    assert row["backend_id"] == "pipedream"
    metadata = row["integration_metadata"]
    assert metadata["provider_app_id"] == "slack"
    assert metadata["provider_tool_id"] == "slack-send-message"
    assert metadata["app_icon_url"] == "https://provider.example/icons/slack.png"
    assert metadata["activation_state"] == "connected_ready"
    assert metadata["provider_error_status"] == "provider_request_failed"
    assert row["verify"] is True


def test_insert_primitives_preserves_validated_integration_metadata(
    monkeypatch,
) -> None:
    captured: list[dict] = []

    def fake_create_logs(**kwargs):
        captured.extend(kwargs["entries"])

    monkeypatch.setattr(
        "unity.function_manager.function_manager.unity_create_logs",
        fake_create_logs,
    )
    monkeypatch.setattr(
        FunctionManager,
        "_delete_primitives_by_function_ids",
        lambda self, function_ids: None,
    )
    fm = _fake_function_manager()
    fm._primitives_ctx = "Functions/Primitives"
    row = fm._integration_tool_to_function_row(
        {
            **MOCK_TOOL,
            "action_class": "write",
            "confirmation_required": True,
            "guidance_ids": [101],
        },
    )

    FunctionManager._insert_primitives(fm, [row])

    assert captured[0]["verify"] is True
    assert captured[0]["guidance_ids"] == [101]
    assert captured[0]["integration_source"] == "provider_backed"
    assert captured[0]["integration_tool_id"] == "composio:hubspot:search_contacts"
    assert "implementation" in captured[0]
    assert captured[0]["implementation"] is None
    assert "precondition" in captured[0]
    assert captured[0]["precondition"] is None
    assert "integration_metadata" in captured[0]
    assert captured[0]["integration_metadata"]["provider_error_status"] is None
    assert captured[0]["integration_metadata"]["match_reason"] is None


def test_insert_primitives_preserves_static_primitive_null_shape(monkeypatch) -> None:
    captured: list[dict] = []

    def fake_create_logs(**kwargs):
        captured.extend(kwargs["entries"])

    monkeypatch.setattr(
        "unity.function_manager.function_manager.unity_create_logs",
        fake_create_logs,
    )
    monkeypatch.setattr(
        FunctionManager,
        "_delete_primitives_by_function_ids",
        lambda self, function_ids: None,
    )
    fm = _fake_function_manager()
    fm._primitives_ctx = "Functions/Primitives"

    FunctionManager._insert_primitives(
        fm,
        [
            {
                "name": "primitives.integrations.search_integrations",
                "function_id": 123,
                "argspec": "(query: str) -> dict",
                "docstring": "Search integrations.",
                "embedding_text": "Function Name: primitives.integrations.search_integrations",
                "implementation": None,
                "depends_on": [],
                "precondition": None,
                "verify": False,
                "is_primitive": True,
                "guidance_ids": [],
                "primitive_class": "unity.integrations.primitives.IntegrationPrimitives",
                "primitive_method": "search_integrations",
            },
        ],
    )

    assert captured[0]["implementation"] is None
    assert captured[0]["precondition"] is None
    assert captured[0]["depends_on"] == []


def test_insert_primitives_replaces_exact_function_ids(monkeypatch) -> None:
    deleted_ids: list[int] = []
    captured: list[dict] = []

    monkeypatch.setattr(
        FunctionManager,
        "_delete_primitives_by_function_ids",
        lambda self, function_ids: deleted_ids.extend(function_ids),
    )
    monkeypatch.setattr(
        "unity.function_manager.function_manager.unity_create_logs",
        lambda **kwargs: captured.extend(kwargs["entries"]),
    )
    fm = _fake_function_manager()
    fm._primitives_ctx = "Functions/Primitives"

    FunctionManager._insert_primitives(
        fm,
        [
            {
                "name": "primitives.integrations.search_integrations",
                "function_id": 123,
                "argspec": "(query: str) -> dict",
                "docstring": "Search integrations.",
                "embedding_text": "Function Name: primitives.integrations.search_integrations",
                "implementation": None,
                "depends_on": [],
                "precondition": None,
                "verify": False,
                "is_primitive": True,
                "guidance_ids": [],
                "primitive_class": "unity.integrations.primitives.IntegrationPrimitives",
                "primitive_method": "search_integrations",
            },
        ],
    )

    assert deleted_ids == [123]
    assert captured[0]["function_id"] == 123


def test_static_primitive_sync_delete_spares_provider_backed_rows(monkeypatch) -> None:
    deleted: list[int] = []

    class FakeRegistry:
        def manager_specs(self, _scope):
            return [
                SimpleNamespace(
                    manager_alias="integrations",
                    primitive_class_path="unity.integrations.primitives.IntegrationPrimitives",
                ),
            ]

    def fake_get_logs(**kwargs):
        assert 'integration_source != "provider_backed"' in kwargs["filter"]
        return [
            SimpleNamespace(
                id=7,
                entries={"name": "primitives.integrations.search_tools"},
            ),
        ]

    def fake_delete_logs(**kwargs):
        deleted.extend(kwargs["logs"])

    monkeypatch.setattr("unify.get_logs", fake_get_logs)
    monkeypatch.setattr("unify.delete_logs", fake_delete_logs)
    fm = _fake_function_manager()
    fm._registry = FakeRegistry()
    fm._primitives_ctx = "Functions/Primitives"

    FunctionManager._delete_primitives_for_managers(fm, ["integrations"])

    assert deleted == [7]


def test_function_manager_queries_do_not_call_integration_ops(monkeypatch) -> None:
    primitive_row = {
        **FunctionManager.__new__(FunctionManager)._integration_tool_to_function_row(
            MOCK_TOOL,
        ),
        "implementation": None,
    }

    def fail_ops(*args, **kwargs):
        raise AssertionError(
            "FunctionManager query paths must not call integration ops",
        )

    monkeypatch.setattr("unity.integrations.ops.list_connections", fail_ops)
    monkeypatch.setattr("unity.integrations.ops.get_tools", fail_ops)
    monkeypatch.setattr(
        "unify.get_logs",
        lambda **kwargs: (
            [SimpleNamespace(entries=primitive_row)]
            if kwargs.get("context") == "Functions/Primitives"
            else []
        ),
    )
    monkeypatch.setattr(
        "unity.function_manager.function_manager.table_search_top_k",
        lambda **kwargs: (
            [primitive_row] if kwargs.get("context") == "Functions/Primitives" else []
        ),
    )
    fm = _fake_function_manager()
    fm._read_compositional_contexts = lambda: []
    fm._read_function_contexts = lambda _table_name: ["Functions/Primitives"]
    fm._scoped_filter = lambda expr: expr
    fm._scoped_primitive_filter = lambda: "is_primitive == True"
    fm._get_logs_with_retry = lambda context, **kwargs: [primitive_row]
    fm.sync_primitives = lambda: False

    assert (
        "primitives.integrations.hubspot.search_contacts"
        in FunctionManager.list_functions(fm)
    )
    assert FunctionManager.filter_functions(fm, filter="is_primitive == True")[0][
        "name"
    ] == ("primitives.integrations.hubspot.search_contacts")
    assert FunctionManager.search_functions(fm, query="hubspot contacts")[0][
        "name"
    ] == ("primitives.integrations.hubspot.search_contacts")


def test_materialization_hash_match_skips_delete_and_insert(monkeypatch) -> None:
    client = FakeIntegrationOps()
    expected_row = FunctionManager.__new__(
        FunctionManager,
    )._integration_tool_to_function_row(MOCK_TOOL)
    current_hash = FunctionManager._hash_integration_rows([expected_row])
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()
    fm._get_stored_integration_tool_hash_by_app = lambda: {
        "composio:hubspot": current_hash,
    }

    result = fm.sync_provider_integration_tools(app_slug="hubspot")

    assert result["status"] == "unchanged"
    assert result["apps"] == []
    assert result["unchanged_apps"] == [{"key": "composio:hubspot", "rows": 1}]
    assert fm._inserted_rows == []


def test_disconnect_cleanup_removes_materialized_rows(monkeypatch) -> None:
    client = FakeIntegrationOps()
    client.connections = []
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()
    fm._get_stored_integration_tool_hash_by_app = lambda: {
        "composio:hubspot": "old-hash",
    }

    result = fm.sync_provider_integration_tools(app_slug="hubspot")

    assert result["status"] == "removed"
    assert result["removed_apps"] == ["composio:hubspot"]
    assert fm._deleted_apps == [("composio", "hubspot")]


def test_connection_cleanup_deletes_only_matching_materialized_rows(
    monkeypatch,
) -> None:
    client = FakeIntegrationOps()
    client.connections = []
    deleted_logs: list[int] = []
    logs = [
        SimpleNamespace(
            id=11,
            entries={
                "backend_id": "composio",
                "app_slug": "hubspot",
                "integration_metadata": {"connection_id": "conn-1"},
            },
        ),
        SimpleNamespace(
            id=22,
            entries={
                "backend_id": "composio",
                "app_slug": "hubspot",
                "integration_metadata": {"connection_id": "conn-2"},
            },
        ),
    ]
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    monkeypatch.setattr("unify.get_logs", lambda **_kwargs: logs)
    monkeypatch.setattr(
        "unify.delete_logs",
        lambda **kwargs: deleted_logs.extend(kwargs["logs"]),
    )
    fm = _fake_function_manager()
    fm._primitives_ctx = "Functions/Primitives"
    fm._get_stored_integration_tool_hash_by_app = lambda: {
        "composio:hubspot": "old-hash",
    }

    result = fm.sync_provider_integration_tools(
        app_slug="hubspot",
        connection_id="conn-1",
        operation="cleanup",
    )

    assert result["status"] == "removed"
    assert result["removed_apps"] == ["composio:hubspot"]
    assert result["rows_deleted"] == 1
    assert deleted_logs == [11]
    assert fm._stored_hashes == {}


def test_missing_active_connection_for_specific_sync_returns_error(monkeypatch) -> None:
    client = FakeIntegrationOps()
    client.connections = []
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr("unity.integrations.ops.get_tools", client.get_tools)
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(
        app_slug="hubspot",
        connection_id="conn-missing",
    )

    assert result["status"] == "error"
    assert result["error"]["code"] == "provider_connection_not_active"
    assert result["apps"] == []
    assert result["rows_deleted"] == 0
    assert getattr(fm, "_deleted_apps", []) == []
