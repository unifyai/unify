from __future__ import annotations

import inspect
import json
from types import SimpleNamespace

import pytest

from unity.integrations import ops as ops_module
from unity.integrations.primitives import IntegrationPrimitives


class FakeIntegrationClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.search_results = [
            {
                "tool_id": "composio:hubspot:search_contacts",
                "app_slug": "hubspot",
                "canonical_name": "primitives.integrations.hubspot.search_contacts",
            },
        ]
        self.app_results = [
            {
                "canonical_app_slug": "hubspot",
                "display_name": "HubSpot",
                "supported": True,
                "connection_status": "connected",
                "connection_id": "conn-1",
                "external_account_label": "Sales Hub",
                "auth_modes": ["oauth"],
                "tool_count": 2,
                "score": 100.0,
                "match_reason": "exact app match",
            },
            {
                "canonical_app_slug": "notion",
                "display_name": "Notion",
                "supported": True,
                "connection_status": None,
                "connection_id": None,
                "auth_modes": ["oauth"],
                "tool_count": 5,
                "score": 10.0,
                "match_reason": "matched terms: docs",
            },
        ]

    def list_connections(self, **scope):
        self.calls.append(("list_connections", (), scope))
        return [{"connection_id": "conn-1"}]

    def search_tools(self, query, **payload):
        self.calls.append(("search_tools", (query,), payload))
        return list(self.search_results)

    def search_apps(self, query, **payload):
        self.calls.append(("search_apps", (query,), payload))
        return list(self.app_results)

    def get_tool_schema(self, tool_id, **scope):
        self.calls.append(("get_tool_schema", (tool_id,), scope))
        return {"tool_id": tool_id, "input_schema": {"type": "object"}}

    def run_tool(self, tool_id, arguments, **payload):
        self.calls.append(("run_tool", (tool_id, arguments), payload))
        return {"status": "ok", "tool_id": tool_id, "arguments": arguments}

    def test_connection(self, connection_id):
        self.calls.append(("test_connection", (connection_id,), {}))
        return {"status": "ok", "connection_id": connection_id}


def patch_ops_from_client(monkeypatch, client: FakeIntegrationClient) -> None:
    monkeypatch.setattr(ops_module, "list_connections", client.list_connections)
    monkeypatch.setattr(ops_module, "search_apps", client.search_apps)
    monkeypatch.setattr(ops_module, "search_tools", client.search_tools)
    monkeypatch.setattr(ops_module, "get_tool_schema", client.get_tool_schema)
    monkeypatch.setattr(ops_module, "run_tool", client.run_tool)
    monkeypatch.setattr(ops_module, "test_connection", client.test_connection)


def stub_materialized_tool(monkeypatch, *, name: str, tool_id: str) -> None:
    monkeypatch.setattr("unify.get_active_context", lambda: {"read": "user-1/42"})

    def fake_get_logs(**kwargs):
        if (
            kwargs.get("filter")
            == f'name == "{name}" and integration_source == "provider_backed"'
        ):
            return [
                SimpleNamespace(
                    entries={
                        "name": name,
                        "integration_source": "provider_backed",
                        "integration_tool_id": tool_id,
                        "primitive_method": name.replace(".", "__"),
                        "docstring": f"Execute {name}.",
                    },
                ),
            ]
        return []

    monkeypatch.setattr("unify.get_logs", fake_get_logs)


def stub_materialized_app(monkeypatch, *, app_slug: str, names: list[str]) -> None:
    monkeypatch.setattr("unify.get_active_context", lambda: {"read": "user-1/42"})

    def fake_get_logs(**kwargs):
        if (
            kwargs.get("filter")
            == f'app_slug == "{app_slug}" and integration_source == "provider_backed"'
        ):
            return [
                SimpleNamespace(
                    entries={
                        "name": name,
                        "docstring": f"Execute {name}.",
                        "action_class": "read",
                        "integration_source": "provider_backed",
                        "app_slug": app_slug,
                    },
                )
                for name in names
            ]
        return []

    monkeypatch.setattr("unify.get_logs", fake_get_logs)


def stub_native_app(monkeypatch, *, app_slug: str, function_names: list[str]) -> None:
    monkeypatch.setattr("unify.get_active_context", lambda: {"read": "user-1/42"})

    def fake_get_logs(**kwargs):
        filter_text = kwargs.get("filter")
        if (
            kwargs.get("context") == "user-1/42/Integrations/Manifests"
            and filter_text == f'slug == "{app_slug}"'
        ):
            return [
                SimpleNamespace(
                    entries={
                        "slug": app_slug,
                        "required_secrets_json": "[]",
                        "function_names_json": json.dumps(function_names),
                    },
                ),
            ]
        for name in function_names:
            if (
                kwargs.get("context") == "user-1/42/Functions/Primitives"
                and filter_text == f'name == "{name}"'
            ):
                return [
                    SimpleNamespace(
                        entries={
                            "name": name,
                            "docstring": f"Execute {name}.",
                            "action_class": "read",
                            "integration_source": "native_function",
                            "app_slug": app_slug,
                        },
                    ),
                ]
        return []

    monkeypatch.setattr("unify.get_logs", fake_get_logs)


def test_ops_functions_delegate_to_unify_integration_helpers(monkeypatch) -> None:
    calls: list[tuple[str, tuple, dict]] = []

    def helper(name):
        def _fake(*args, **kwargs):
            calls.append((name, args, kwargs))
            return {"helper": name}

        return _fake

    monkeypatch.setattr(
        "unity.integrations.ops.unify.list_integration_connections",
        helper("list_connections"),
        raising=False,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.unify.search_integration_apps",
        helper("search_apps"),
        raising=False,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.unify.get_integration_tools",
        helper("get_tools"),
        raising=False,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.unify.search_integration_tools",
        helper("search_tools"),
        raising=False,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.unify.get_integration_tool_schema",
        helper("get_tool_schema"),
        raising=False,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.unify.run_integration_tool",
        helper("run_tool"),
        raising=False,
    )
    monkeypatch.setattr(
        "unity.integrations.ops.unify.test_integration_connection",
        helper("test_connection"),
        raising=False,
    )

    ops_module.list_connections(
        owner_scope="assistant",
        assistant_id=42,
    )
    ops_module.search_apps(
        "Slack",
        owner_scope="assistant",
        assistant_id=42,
        limit=3,
    )
    ops_module.get_tools(
        canonical_app_slug="slack",
        owner_scope="assistant",
        assistant_id=42,
    )
    ops_module.search_tools(
        "Discord guilds",
        owner_scope="assistant",
        assistant_id=42,
    )
    ops_module.get_tool_schema(
        "tool-1",
        owner_scope="assistant",
    )
    ops_module.run_tool(
        "tool-1",
        {"query": "alice"},
        confirmation_token="confirm",
        owner_scope="assistant",
    )
    ops_module.test_connection("conn-1")

    assert calls == [
        (
            "list_connections",
            (),
            {
                "owner_scope": "assistant",
                "assistant_id": 42,
            },
        ),
        (
            "search_apps",
            ("Slack",),
            {
                "limit": 3,
                "offset": 0,
                "owner_scope": "assistant",
                "assistant_id": 42,
            },
        ),
        (
            "get_tools",
            (),
            {
                "limit": 100,
                "offset": 0,
                "canonical_app_slug": "slack",
                "activation_state": None,
                "include_unconnected": False,
                "owner_scope": "assistant",
                "assistant_id": 42,
            },
        ),
        (
            "search_tools",
            ("Discord guilds",),
            {
                "limit": 20,
                "offset": 0,
                "include_unconnected": False,
                "canonical_app_slug": None,
                "owner_scope": "assistant",
                "assistant_id": 42,
            },
        ),
        (
            "get_tool_schema",
            ("tool-1",),
            {
                "owner_scope": "assistant",
            },
        ),
        (
            "run_tool",
            ("tool-1", {"query": "alice"}),
            {
                "confirmation_token": "confirm",
                "owner_scope": "assistant",
            },
        ),
        (
            "test_connection",
            ("conn-1",),
            {},
        ),
    ]


def test_ops_functions_re_raise_unify_keyerror_like_unify_logging(monkeypatch) -> None:
    def raise_missing_key(*_args, **_kwargs):
        raise KeyError("UNIFY_KEY is missing. Please make sure it is set correctly!")

    monkeypatch.setattr(
        "unity.integrations.ops.unify.search_integration_apps",
        raise_missing_key,
        raising=False,
    )

    with pytest.raises(KeyError, match="UNIFY_KEY is missing"):
        ops_module.search_apps("Slack")


def test_ops_module_no_longer_owns_raw_integration_routes_or_client_class() -> None:
    source = inspect.getsource(ops_module)

    assert "class IntegrationRuntimeClient" not in source
    assert '"/integrations/' not in source
    assert "http.request" not in source
    assert "_call_unify" not in source
    assert "getattr(" not in source
    assert "api_key=" not in source
    assert "base_url=" not in source


@pytest.mark.anyio
async def test_helper_methods_delegate_to_client_with_scope_payloads(
    monkeypatch,
) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    primitives = IntegrationPrimitives(owner_scope={})

    assert await primitives.list_connected(
        owner_scope="assistant",
        assistant_id=42,
        user_id="user-1",
    ) == [{"connection_id": "conn-1"}]
    assert (
        await primitives.search_tools(
            "HubSpot leads",
            assistant_id=42,
            user_id="user-1",
            include_unconnected=False,
            limit=7,
        )
        == client.search_results
    )
    assert await primitives.get_tool_schema(
        "tool-1",
        assistant_id=42,
    ) == {"tool_id": "tool-1", "input_schema": {"type": "object"}}
    assert await primitives.execute_tool(
        "tool-1",
        {"query": "alice"},
        assistant_id=42,
        confirmation_token="confirm",
    ) == {"status": "ok", "tool_id": "tool-1", "arguments": {"query": "alice"}}
    assert await primitives.manage_connection("conn-1", action="test") == {
        "status": "ok",
        "connection_id": "conn-1",
    }

    assert client.calls == [
        (
            "list_connections",
            (),
            {
                "owner_scope": "assistant",
                "user_id": "user-1",
                "assistant_id": 42,
            },
        ),
        (
            "search_tools",
            ("HubSpot leads",),
            {
                "owner_scope": "assistant",
                "user_id": "user-1",
                "assistant_id": 42,
                "include_unconnected": False,
                "limit": 7,
            },
        ),
        (
            "get_tool_schema",
            ("tool-1",),
            {
                "owner_scope": "assistant",
                "assistant_id": 42,
            },
        ),
        (
            "run_tool",
            ("tool-1", {"query": "alice"}),
            {
                "owner_scope": "assistant",
                "assistant_id": 42,
                "confirmation_token": "confirm",
            },
        ),
        ("test_connection", ("conn-1",), {}),
    ]


@pytest.mark.anyio
async def test_search_integrations_reports_connection_and_materialization_status(
    monkeypatch,
) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    stub_materialized_app(
        monkeypatch,
        app_slug="hubspot",
        names=[
            "primitives.integrations.hubspot.search_contacts",
            "primitives.integrations.hubspot.create_contact",
        ],
    )
    primitives = IntegrationPrimitives(
        owner_scope={
            "owner_scope": "assistant",
            "assistant_id": 42,
            "user_id": "user-1",
        },
    )

    result = await primitives.search_integrations(
        "HubSpot",
        include_tools=True,
        limit=7,
    )

    assert result["status"] == "ok"
    assert result["results"][0] == {
        "canonical_app_slug": "hubspot",
        "display_name": "HubSpot",
        "source_type": "third_party",
        "source_label": "Third-party",
        "supported": True,
        "deployment_status": "global_catalog",
        "connection_status": "connected",
        "connection_id": "conn-1",
        "external_account_label": "Sales Hub",
        "auth_modes": ["oauth"],
        "tool_count": 2,
        "materialized_function_count": 2,
        "materialized_tool_count": 2,
        "sync_status": "materialized",
        "next_action": "Search FunctionManager for executable materialized integration tools.",
        "score": 100.0,
        "match_reason": "exact app match",
        "materialized_tools": [
            {
                "name": "primitives.integrations.hubspot.search_contacts",
                "description": "Execute primitives.integrations.hubspot.search_contacts.",
                "action_class": "read",
            },
            {
                "name": "primitives.integrations.hubspot.create_contact",
                "description": "Execute primitives.integrations.hubspot.create_contact.",
                "action_class": "read",
            },
        ],
    }
    assert result["results"][1]["connection_status"] == "not_connected"
    assert result["results"][1]["next_action"] == (
        "Ask the user to connect this integration in Console Integrations."
    )
    assert client.calls == [
        (
            "search_apps",
            ("HubSpot",),
            {
                "owner_scope": "assistant",
                "assistant_id": 42,
                "user_id": "user-1",
                "limit": 7,
            },
        ),
    ]


@pytest.mark.anyio
async def test_search_integrations_enriches_native_app_activation(monkeypatch) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    client.app_results = [
        {
            "canonical_app_slug": "matterport",
            "display_name": "Matterport",
            "source_type": "native",
            "source_label": "Native",
            "supported": True,
            "score": 91.0,
            "match_reason": "embedding similarity over integration app catalog text",
        },
    ]
    stub_native_app(
        monkeypatch,
        app_slug="matterport",
        function_names=["matterport_graphql_query"],
    )
    primitives = IntegrationPrimitives(owner_scope={})

    result = await primitives.search_integrations("3d tours", include_tools=True)

    assert result["results"][0]["source_label"] == "Native"
    assert result["results"][0]["deployment_status"] == "enabled"
    assert result["results"][0]["connection_status"] == "ready"
    assert result["results"][0]["sync_status"] == "materialized"
    assert result["results"][0]["materialized_function_count"] == 1
    assert result["results"][0]["next_action"] == (
        "Search FunctionManager for executable native integration functions."
    )


@pytest.mark.anyio
async def test_search_integrations_returns_supported_empty_result_without_tool_search(
    monkeypatch,
) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    client.app_results = []
    primitives = IntegrationPrimitives(owner_scope={})

    result = await primitives.search_integrations("UnsupportedApp")

    assert result == {
        "status": "ok",
        "query": "UnsupportedApp",
        "results": [],
        "message": "No supported integration matched this query.",
    }
    assert client.calls == [
        (
            "search_apps",
            ("UnsupportedApp",),
            {"owner_scope": "assistant", "limit": 10},
        ),
    ]


def test_search_integrations_is_only_fixed_integration_discovery_primitive() -> None:
    assert IntegrationPrimitives._PRIMITIVE_METHODS == ("search_integrations",)


@pytest.mark.anyio
async def test_dynamic_app_tool_namespace_resolves_from_materialized_row(
    monkeypatch,
) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    stub_materialized_tool(
        monkeypatch,
        name="primitives.integrations.hubspot.search_contacts",
        tool_id="composio:hubspot:search_contacts",
    )
    primitives = IntegrationPrimitives(owner_scope={})

    result = await primitives.hubspot.search_contacts(query="alice@example.com")

    assert result["status"] == "ok"
    assert result["tool_id"] == "composio:hubspot:search_contacts"
    assert client.calls == [
        (
            "run_tool",
            ("composio:hubspot:search_contacts", {"query": "alice@example.com"}),
            {
                "owner_scope": "assistant",
                "confirmation_token": None,
            },
        ),
    ]


@pytest.mark.anyio
async def test_first_wave_dynamic_namespace_executes_discord_tool(monkeypatch) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    stub_materialized_tool(
        monkeypatch,
        name="primitives.integrations.discord.list_my_guilds",
        tool_id="composio:discord:list_my_guilds",
    )
    primitives = IntegrationPrimitives(
        owner_scope={
            "owner_scope": "assistant",
            "assistant_id": 42,
            "user_id": "user-1",
        },
    )

    result = await primitives.discord.list_my_guilds(limit=10)

    assert result["status"] == "ok"
    assert result["tool_id"] == "composio:discord:list_my_guilds"
    assert client.calls == [
        (
            "run_tool",
            ("composio:discord:list_my_guilds", {"limit": 10}),
            {
                "owner_scope": "assistant",
                "assistant_id": 42,
                "user_id": "user-1",
                "confirmation_token": None,
            },
        ),
    ]


@pytest.mark.anyio
async def test_callable_for_tool_dispatches_execution(monkeypatch) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    primitives = IntegrationPrimitives(owner_scope={})

    callable_tool = primitives.callable_for_tool(
        {
            "integration_tool_id": "composio:hubspot:search_contacts",
            "primitive_method": "primitives_integrations__hubspot__search_contacts",
            "docstring": "Search HubSpot contacts.",
        },
    )

    assert callable_tool is not None
    assert callable_tool.__name__ == "primitives_integrations__hubspot__search_contacts"
    assert callable_tool.__doc__ == "Search HubSpot contacts."
    assert await callable_tool(query="alice") == {
        "status": "ok",
        "tool_id": "composio:hubspot:search_contacts",
        "arguments": {"query": "alice"},
    }


@pytest.mark.anyio
async def test_default_owner_scope_is_shared_across_helper_and_namespace_execution(
    monkeypatch,
) -> None:
    client = FakeIntegrationClient()
    patch_ops_from_client(monkeypatch, client)
    stub_materialized_tool(
        monkeypatch,
        name="primitives.integrations.hubspot.search_contacts",
        tool_id="composio:hubspot:search_contacts",
    )
    primitives = IntegrationPrimitives(
        owner_scope={
            "owner_scope": "assistant",
            "assistant_id": 42,
            "user_id": "user-1",
        },
    )

    await primitives.search_tools("HubSpot contacts")
    await primitives.hubspot.search_contacts(query="alice")

    assert client.calls[0] == (
        "search_tools",
        ("HubSpot contacts",),
        {
            "include_unconnected": False,
            "limit": 20,
            "owner_scope": "assistant",
            "assistant_id": 42,
            "user_id": "user-1",
        },
    )
    assert client.calls[1] == (
        "run_tool",
        ("composio:hubspot:search_contacts", {"query": "alice"}),
        {
            "confirmation_token": None,
            "owner_scope": "assistant",
            "assistant_id": 42,
            "user_id": "user-1",
        },
    )


@pytest.mark.anyio
async def test_provider_runtime_envelopes_are_returned_without_hiding_failures(
    monkeypatch,
) -> None:
    class EnvelopeClient(FakeIntegrationClient):
        def __init__(self, envelope):
            super().__init__()
            self.envelope = envelope

        def run_tool(self, tool_id, arguments, **payload):
            self.calls.append(("run_tool", (tool_id, arguments), payload))
            return dict(self.envelope)

    for envelope in [
        {
            "status": "provider_error",
            "error": {"code": "provider_request_failed", "message": "timeout"},
        },
        {
            "status": "reconnect_required",
            "activation_state": "expired",
            "error": {"code": "expired"},
        },
        {
            "status": "confirmation_required",
            "error": {"code": "confirmation_required"},
        },
        {
            "status": "ok",
            "result": {"rows": [{"id": "contact-1"}]},
        },
    ]:
        client = EnvelopeClient(envelope)
        patch_ops_from_client(monkeypatch, client)
        primitives = IntegrationPrimitives(owner_scope={})
        assert await primitives.execute_tool("tool-1", {"query": "alice"}) == envelope
