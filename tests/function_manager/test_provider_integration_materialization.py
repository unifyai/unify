from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

import unity.function_manager.function_manager as fm_module
from unity.function_manager.function_manager import FunctionManager
from unity.function_manager.primitives.scope import PrimitiveScope
from unity.function_manager.types.function import Function
from unity.integrations.primitives import IntegrationPrimitives
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
    "behavior_hints": ["read_only", "external"],
    "confirmation_required": False,
    "approval_level": "auto",
    "input_schema": {
        "type": "object",
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "description": "Search text for matching contacts.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of contacts to return.",
                "default": 10,
            },
        },
    },
    "output_schema": {"type": "object"},
    "examples": [{"arguments": {"query": "alice@example.com", "limit": 5}}],
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
    catalog_calls: list[dict[str, object]] = []

    def fake_list_catalog_tools(**kwargs):
        catalog_calls.append(kwargs)
        return list(client.results)

    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        fake_list_catalog_tools,
    )
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="hubspot")

    assert result["status"] == "synced"
    assert result["apps"] == [{"key": "composio:hubspot", "rows": 1, "rows_deleted": 0}]
    assert client.calls == [
        ("list_connections", (), {"owner_scope": "assistant", "assistant_id": 42}),
    ]
    row = fm._inserted_rows[0]
    assert row["name"] == "primitives.integrations.hubspot.search_contacts"
    assert row["argspec"] == "(query: str, limit: int = 10) -> dict"
    assert set(row) >= {"metadata"}
    assert "integration_tool_id" not in row
    assert "backend_id" not in row
    assert "input_schema" not in row
    assert "output_schema" not in row
    assert "examples" not in row
    assert "Parameters\n----------" in row["docstring"]
    assert "query : str" in row["docstring"]
    assert "await primitives.integrations.hubspot.search_contacts" in row["docstring"]
    embedding_text = row["embedding_text"]
    # Layer 1 normalization (integrations-only) strips scaffolding labels and the
    # raw argspec, and splits identifiers into words so the pooled vector is
    # dominated by signal rather than dotted/cased identifier noise.
    assert "Function Name:" not in embedding_text
    assert "Signature:" not in embedding_text
    assert "search contacts" in embedding_text
    assert "hub spot" in embedding_text
    assert "query" in embedding_text and "limit" in embedding_text
    assert "crm.objects.contacts.read" not in embedding_text
    metadata = row["metadata"]
    assert metadata["source"] == "provider_backed"
    integration = metadata["integration"]
    assert integration["tool_id"] == "composio:hubspot:search_contacts"
    assert integration["backend_id"] == "composio"
    assert integration["app_slug"] == "hubspot"
    assert integration["input_schema"]["properties"]["query"]["type"] == "string"
    assert integration["output_schema"] == {"type": "object"}
    assert integration["examples"] == [
        {"arguments": {"query": "alice@example.com", "limit": 5}},
    ]
    assert integration["required_scopes"] == ["crm.objects.contacts.read"]
    assert integration["action_class"] == "read"
    assert integration["behavior_hints"] == ["read_only", "external"]
    assert integration["confirmation_required"] is False
    assert integration["schema_available"] is True
    assert integration["provider_app_id"] == "hubspot"
    assert integration["provider_tool_id"] == "hubspot.search_contacts"
    assert integration["labels"] == {
        "app_display_name": "HubSpot",
        "app_icon_url": "https://provider.example/icons/hubspot.png",
        "tool_display_name": "Search contacts",
    }
    assert integration["app_icon_url"] == "https://provider.example/icons/hubspot.png"
    assert "connection_id" not in integration
    assert "activation_state" not in integration
    assert "granted_scopes" not in integration
    assert "provider_error_status" not in integration
    assert "match_reason" not in integration
    assert "approval_level" not in integration
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


def test_function_model_keeps_provider_details_inside_metadata() -> None:
    forbidden_fields = {
        "integration_tool_id",
        "integration_source",
        "integration_metadata",
        "input_schema",
        "output_schema",
        "behavior_hints",
        "backend_id",
        "app_slug",
    }

    assert "metadata" in Function.model_fields
    assert not forbidden_fields.intersection(Function.model_fields)


def test_search_functions_uses_compact_provider_projection(monkeypatch) -> None:
    fm = FunctionManager.__new__(FunctionManager)
    fm._include_primitives = True
    fm._read_compositional_contexts = lambda: []
    fm._read_function_contexts = lambda _table_name: ["Functions/Primitives"]
    fm._scoped_primitive_filter = lambda: None
    fm._primitives_ctx = "Functions/Primitives"
    fm.sync_primitives = lambda: None
    captured: dict[str, object] = {}

    full_row = {
        **fm._integration_tool_to_function_row(MOCK_TOOL),
        "implementation": "print('large implementation')",
    }

    def fake_ranked_search(contexts, query, *, limit, unique_id_field, backfill):
        captured["allowed_fields"] = contexts[0].allowed_fields
        captured["query"] = query
        captured["limit"] = limit
        captured["unique_id_field"] = unique_id_field
        captured["backfill"] = backfill
        return [full_row]

    monkeypatch.setattr(fm_module, "federated_ranked_search", fake_ranked_search)

    results = fm.search_functions(
        query="hubspot contact search",
        n=1,
        include_implementations=False,
    )

    assert len(results) == 1
    assert "input_schema" not in results[0]
    assert "output_schema" not in results[0]
    assert "examples" not in results[0]
    assert "implementation" not in results[0]
    assert results[0]["metadata"]["source"] == "provider_backed"
    assert results[0]["metadata"]["integration"] == {
        "tool_id": "composio:hubspot:search_contacts",
        "backend_id": "composio",
        "app_slug": "hubspot",
        "source_type": "third_party",
        "namespace": "primitives.integrations",
        "provider_app_id": "hubspot",
        "provider_tool_id": "hubspot.search_contacts",
        "labels": {
            "app_display_name": "HubSpot",
            "app_icon_url": "https://provider.example/icons/hubspot.png",
            "tool_display_name": "Search contacts",
        },
        "app_display_name": "HubSpot",
        "app_icon_url": "https://provider.example/icons/hubspot.png",
        "tool_display_name": "Search contacts",
        "required_scopes": ["crm.objects.contacts.read"],
        "action_class": "read",
        "behavior_hints": ["read_only", "external"],
        "confirmation_required": False,
        "schema_available": True,
    }
    assert "input_schema" not in results[0]["metadata"]["integration"]
    assert "output_schema" not in results[0]["metadata"]["integration"]
    assert "examples" not in results[0]["metadata"]["integration"]
    allowed_fields = captured["allowed_fields"]
    assert isinstance(allowed_fields, list)
    assert "input_schema" not in allowed_fields
    assert "output_schema" not in allowed_fields
    assert "examples" not in allowed_fields
    assert "metadata" in allowed_fields
    assert "behavior_hints" not in allowed_fields


@pytest.mark.anyio
async def test_execute_function_dispatches_provider_backed_primitive_by_row(
    monkeypatch,
) -> None:
    calls: list[tuple[str, dict]] = []

    def fake_run_tool(tool_id, arguments, **_payload):
        calls.append((tool_id, arguments))
        return {"status": "ok", "tool_id": tool_id, "arguments": arguments}

    monkeypatch.setattr("unity.integrations.ops.run_tool", fake_run_tool)
    fm = FunctionManager.__new__(FunctionManager)
    fm._include_primitives = True
    fm._get_function_data_by_name = lambda name: None
    fm._get_primitive_data_by_name = lambda name: None
    fm._get_stored_primitive_data_by_name = lambda name: {
        "name": "primitives.integrations.gmail.fetch_emails",
        "is_primitive": True,
        "primitive_class": "unity.integrations.primitives.IntegrationPrimitives",
        "primitive_method": "primitives_integrations__gmail__fetch_emails",
        "metadata": {
            "source": "provider_backed",
            "integration": {"tool_id": "composio:gmail:fetch_emails"},
        },
    }
    primitives = SimpleNamespace(
        integrations=IntegrationPrimitives(owner_scope={"assistant_id": 42}),
    )

    result = await fm.execute_function(
        function_name="primitives.integrations.gmail.fetch_emails",
        call_kwargs={"query": "is:unread", "max_results": 5},
        extra_namespaces={"primitives": primitives},
    )

    assert result == {
        "status": "ok",
        "tool_id": "composio:gmail:fetch_emails",
        "arguments": {"query": "is:unread", "max_results": 5},
    }
    assert calls == [
        (
            "composio:gmail:fetch_emails",
            {"query": "is:unread", "max_results": 5},
        ),
    ]


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
    catalog_calls: list[dict[str, object]] = []

    def fake_list_catalog_tools(**kwargs):
        catalog_calls.append(kwargs)
        return list(client.results)

    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        fake_list_catalog_tools,
    )
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
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
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
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
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
    catalog_calls: list[dict[str, object]] = []

    def fake_list_catalog_tools(**kwargs):
        catalog_calls.append(kwargs)
        return list(client.results)

    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        fake_list_catalog_tools,
    )
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="HubSpot", limit=1)

    assert result["status"] == "synced"
    assert result["apps"] == [{"key": "composio:hubspot", "rows": 2, "rows_deleted": 0}]
    assert all(call[0] != "search_tools" for call in client.calls)
    assert catalog_calls == [{"canonical_app_slug": "hubspot", "limit": 1}]
    assert [row["name"] for row in fm._inserted_rows] == [
        "primitives.integrations.hubspot.search_contacts",
        "primitives.integrations.hubspot.create_contact",
    ]


def test_materialized_rows_include_pipedream_metadata_without_user_state(
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
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
    fm = _fake_function_manager()

    result = fm.sync_provider_integration_tools(app_slug="slack")

    assert result["status"] == "synced"
    row = fm._inserted_rows[0]
    assert row["name"] == "primitives.integrations.slack.send_message"
    metadata = row["metadata"]["integration"]
    assert metadata["backend_id"] == "pipedream"
    assert metadata["provider_app_id"] == "slack"
    assert metadata["provider_tool_id"] == "slack-send-message"
    assert metadata["app_icon_url"] == "https://provider.example/icons/slack.png"
    # Connection state and provider errors are runtime concerns, not row data.
    assert "activation_state" not in metadata
    assert "provider_error_status" not in metadata
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
    assert captured[0]["metadata"]["source"] == "provider_backed"
    assert (
        captured[0]["metadata"]["integration"]["tool_id"]
        == "composio:hubspot:search_contacts"
    )
    assert "implementation" in captured[0]
    assert captured[0]["implementation"] is None
    assert "precondition" in captured[0]
    assert captured[0]["precondition"] is None
    assert "integration_metadata" not in captured[0]
    assert "provider_error_status" not in captured[0]["metadata"]["integration"]
    assert "match_reason" not in captured[0]["metadata"]["integration"]


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
    monkeypatch.setattr(
        "unify.get_logs",
        lambda **kwargs: (
            [SimpleNamespace(entries=primitive_row)]
            if kwargs.get("context") == "Functions/Primitives"
            else []
        ),
    )
    monkeypatch.setattr(
        fm_module,
        "federated_ranked_search",
        lambda *_args, **_kwargs: [primitive_row],
    )
    fm = _fake_function_manager()
    fm._read_compositional_contexts = lambda: []
    fm._primitives_ctx = "Functions/Primitives"
    fm._scoped_filter = lambda expr: expr
    fm._scoped_primitive_filter = lambda: "is_primitive == True"
    fm._get_logs_with_retry = lambda context, **kwargs: [primitive_row]

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
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
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
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
    fm = _fake_function_manager()
    fm._get_stored_integration_tool_hash_by_app = lambda: {
        "composio:hubspot": "old-hash",
    }

    result = fm.sync_provider_integration_tools(app_slug="hubspot")

    assert result["status"] == "removed"
    assert result["removed_apps"] == ["composio:hubspot"]
    assert fm._deleted_apps == [("composio", "hubspot")]


def test_disconnect_cleanup_deletes_only_metadata_provider_rows(
    monkeypatch,
) -> None:
    deleted: list[int] = []
    captured: dict[str, str] = {}
    logs = [
        SimpleNamespace(
            id=1,
            entries={
                "integration_source": "provider_backed",
                "backend_id": "composio",
                "app_slug": "gmail",
            },
        ),
        SimpleNamespace(
            id=2,
            entries={
                "metadata": {
                    "source": "provider_backed",
                    "integration": {
                        "backend_id": "composio",
                        "app_slug": "gmail",
                    },
                },
            },
        ),
        SimpleNamespace(
            id=3,
            entries={
                "integration_source": "provider_backed",
                "backend_id": "composio",
                "app_slug": "slack",
            },
        ),
    ]

    def fake_get_logs(**kwargs):
        captured["filter"] = kwargs["filter"]
        return logs

    def fake_delete_logs(**kwargs):
        deleted.extend(kwargs["logs"])

    monkeypatch.setattr("unify.get_logs", fake_get_logs)
    monkeypatch.setattr("unify.delete_logs", fake_delete_logs)
    monkeypatch.setattr(fm_module, "list_private_fields", lambda *_args, **_kwargs: [])
    fm = FunctionManager.__new__(FunctionManager)
    fm._primitives_ctx = "Functions/Primitives"

    removed = FunctionManager._delete_provider_integration_rows_for_apps(
        fm,
        [("composio", "gmail")],
    )

    assert removed == 1
    assert deleted == [2]
    assert 'metadata["source"] == "provider_backed"' in captured["filter"]
    assert 'metadata["integration"]["app_slug"] == "gmail"' in captured["filter"]
    assert 'integration_source == "provider_backed"' not in captured["filter"]


def test_connection_cleanup_removes_app_rows_when_last_connection_drops(
    monkeypatch,
) -> None:
    client = FakeIntegrationOps()
    client.connections = []
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
    fm = _fake_function_manager()
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
    assert fm._deleted_apps == [("composio", "hubspot")]
    assert fm._stored_hashes == {}


def test_connection_cleanup_keeps_rows_while_other_connection_remains(
    monkeypatch,
) -> None:
    client = FakeIntegrationOps()
    client.connections = [
        {
            "connection_id": "conn-2",
            "canonical_app_slug": "hubspot",
            "status": "connected",
        },
    ]
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
    fm = _fake_function_manager()
    fm._get_stored_integration_tool_hash_by_app = lambda: {
        "composio:hubspot": "old-hash",
    }

    result = fm.sync_provider_integration_tools(
        app_slug="hubspot",
        connection_id="conn-1",
        operation="cleanup",
    )

    # Rows are connection-agnostic: another live connection still serves the
    # app, so nothing is deleted and the hash map is untouched.
    assert result["status"] == "removed"
    assert result["removed_apps"] == []
    assert result["rows_deleted"] == 0
    assert getattr(fm, "_deleted_apps", []) == []
    assert not hasattr(fm, "_stored_hashes")


def test_missing_active_connection_for_specific_sync_returns_error(monkeypatch) -> None:
    client = FakeIntegrationOps()
    client.connections = []
    monkeypatch.setattr(
        "unity.integrations.ops.list_connections",
        client.list_connections,
    )
    monkeypatch.setattr(
        "unity.function_manager.function_manager.list_catalog_tools",
        lambda **_kwargs: list(client.results),
    )
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
