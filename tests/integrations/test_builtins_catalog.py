import importlib.util
from pathlib import Path

from unity.integrations import builtins_catalog


def _seed_script_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "seed_builtins_catalog.py"
    spec = importlib.util.spec_from_file_location("seed_builtins_catalog_script", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _app(slug: str) -> dict:
    return {
        "backend_id": "composio",
        "provider_app_id": slug,
        "canonical_app_slug": slug,
        "display_name": slug.title(),
        "description": f"{slug.title()} app.",
        "auth_modes": ["oauth"],
    }


def _tool(slug: str, name: str) -> dict:
    return {
        "tool_id": f"composio:{slug}:{name}",
        "backend_id": "composio",
        "provider_app_id": slug,
        "provider_tool_id": f"{slug}.{name}",
        "canonical_name": f"primitives.integrations.{slug}.{name}",
        "function_manager_name": f"primitives_integrations__{slug}__{name}",
        "app_slug": slug,
        "app_display_name": slug.title(),
        "tool_display_name": name.replace("_", " ").title(),
        "description": f"{name.replace('_', ' ').title()} for {slug}.",
        "required_scopes": ["read"],
        "action_class": "read",
        "input_schema": {"type": "object"},
        "output_schema": {"type": "object"},
    }


def test_seed_builtin_integrations_hash_guards_app_and_tool_units(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []
    stored_hashes: dict[str, str] = {}
    deleted: list[tuple[str, list[int]]] = []
    inserted: list[tuple[str, list[dict]]] = []
    seen_filters: set[str] = set()

    app = {
        "provider_app_id": "gmail",
        "canonical_app_slug": "gmail",
        "display_name": "Gmail",
        "description": "Gmail mailboxes",
        "auth_modes": ["oauth"],
    }
    tool = {
        "provider_app_id": "gmail",
        "provider_tool_id": "gmail.fetch_emails",
        "app_display_name": "Gmail",
        "display_name": "Fetch emails",
        "description": "Fetch matching emails.",
        "required_scopes": ["gmail.readonly"],
        "action_class": "read",
        "input_schema": {"type": "object"},
        "output_schema": {"type": "object"},
    }

    monkeypatch.setattr(
        builtins_catalog.unify,
        "create_project",
        lambda *args, **kwargs: calls.append(("create_project", kwargs)),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "ensure_builtins_project",
        lambda project: calls.append(("ensure_builtins_project", {"project": project})),
    )
    monkeypatch.setattr(
        builtins_catalog.unify,
        "create_context",
        lambda *args, **kwargs: calls.append(("create_context", kwargs)),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "ensure_vector_column",
        lambda *args, **kwargs: calls.append(("ensure_vector_column", kwargs)),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "list_private_fields",
        lambda *_args, **_kwargs: [],
    )

    def fake_get_logs(**kwargs):
        if kwargs.get("context") == builtins_catalog.BUILTINS_INTEGRATION_META_CONTEXT:
            return []
        assert kwargs["return_ids_only"] is True
        filter_expr = kwargs["filter"]
        if filter_expr in seen_filters:
            return []
        seen_filters.add(filter_expr)
        return [17]

    monkeypatch.setattr(builtins_catalog.unify, "get_logs", fake_get_logs)
    monkeypatch.setattr(
        builtins_catalog.unify,
        "delete_logs",
        lambda **kwargs: deleted.append((kwargs["context"], list(kwargs["logs"]))),
    )
    monkeypatch.setattr(
        builtins_catalog.unify,
        "create_logs",
        lambda **kwargs: inserted.append((kwargs["context"], list(kwargs["entries"]))),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "read_seed_hashes",
        lambda *_args, **_kwargs: dict(stored_hashes),
    )

    def fake_write_seed_hashes(_project, hashes, **_kwargs):
        stored_hashes.clear()
        stored_hashes.update(hashes)

    monkeypatch.setattr(builtins_catalog, "write_seed_hashes", fake_write_seed_hashes)

    assert (
        builtins_catalog.seed_builtin_integrations(
            apps=[app],
            tools=[tool],
            backend_id="composio",
            project="Builtins",
        )
        is True
    )
    assert inserted[0][0] == builtins_catalog.BUILTINS_INTEGRATION_APPS_CONTEXT
    assert inserted[0][1][0]["backend_id"] == "composio"
    assert inserted[1][0] == builtins_catalog.BUILTINS_INTEGRATION_TOOLS_CONTEXT
    assert inserted[1][1][0]["metadata"]["integration"]["required_scopes"] == [
        "gmail.readonly",
    ]
    assert inserted[1][1][0]["metadata"]["integration"]["backend_id"] == "composio"
    assert "connection_id" not in inserted[1][1][0]["metadata"]["integration"]
    assert stored_hashes

    inserted.clear()
    deleted.clear()

    assert (
        builtins_catalog.seed_builtin_integrations(
            apps=[app],
            tools=[tool],
            backend_id="composio",
            project="Builtins",
        )
        is False
    )
    assert inserted == []
    assert deleted == []


def test_seed_builtin_integrations_preserves_unlisted_app_scope(monkeypatch) -> None:
    stored_hashes = {
        "app:composio:gmail": "old",
        "tools:composio:gmail": "old",
        "app:composio:slack": "old",
        "tools:composio:slack": "old",
    }
    filters: list[str] = []
    seen_filters: set[str] = set()

    monkeypatch.setattr(builtins_catalog, "ensure_builtins_project", lambda *_: None)
    monkeypatch.setattr(builtins_catalog.unify, "create_context", lambda *_, **__: None)
    monkeypatch.setattr(builtins_catalog, "ensure_vector_column", lambda *_, **__: None)
    monkeypatch.setattr(
        builtins_catalog,
        "list_private_fields",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        builtins_catalog,
        "read_seed_hashes",
        lambda *_args, **_kwargs: dict(stored_hashes),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "write_seed_hashes",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(builtins_catalog.unify, "create_logs", lambda **_kwargs: None)

    def fake_get_logs(**kwargs):
        if kwargs.get("context") == builtins_catalog.BUILTINS_INTEGRATION_META_CONTEXT:
            return []
        assert kwargs["return_ids_only"] is True
        filter_expr = kwargs["filter"]
        filters.append(filter_expr)
        if filter_expr in seen_filters:
            return []
        seen_filters.add(filter_expr)
        return [17]

    monkeypatch.setattr(builtins_catalog.unify, "get_logs", fake_get_logs)
    monkeypatch.setattr(builtins_catalog.unify, "delete_logs", lambda **_kwargs: None)

    assert builtins_catalog.seed_builtin_integrations(
        apps=[_app("gmail")],
        tools=[_tool("gmail", "list_threads")],
        backend_id="composio",
        app_slugs=["gmail"],
        prune_unlisted_apps=False,
        project="Builtins",
    )

    assert any("gmail" in item for item in filters)
    assert any("canonical_app_slug in" in item for item in filters)
    assert any('metadata["integration"]["app_slug"] in' in item for item in filters)
    assert not any(" or " in item for item in filters)
    assert not any("slack" in item for item in filters)


def test_seed_builtin_integrations_prunes_unlisted_app_scope(monkeypatch) -> None:
    stored_hashes = {
        "app:composio:gmail": "old",
        "tools:composio:gmail": "old",
        "app:composio:slack": "old",
        "tools:composio:slack": "old",
    }
    filters: list[str] = []
    seen_filters: set[str] = set()

    monkeypatch.setattr(builtins_catalog, "ensure_builtins_project", lambda *_: None)
    monkeypatch.setattr(builtins_catalog.unify, "create_context", lambda *_, **__: None)
    monkeypatch.setattr(builtins_catalog, "ensure_vector_column", lambda *_, **__: None)
    monkeypatch.setattr(
        builtins_catalog,
        "list_private_fields",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        builtins_catalog,
        "read_seed_hashes",
        lambda *_args, **_kwargs: dict(stored_hashes),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "write_seed_hashes",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(builtins_catalog.unify, "create_logs", lambda **_kwargs: None)

    def fake_get_logs(**kwargs):
        if kwargs.get("context") == builtins_catalog.BUILTINS_INTEGRATION_META_CONTEXT:
            return []
        assert kwargs["return_ids_only"] is True
        filter_expr = kwargs["filter"]
        filters.append(filter_expr)
        if filter_expr in seen_filters:
            return []
        seen_filters.add(filter_expr)
        return [17]

    monkeypatch.setattr(builtins_catalog.unify, "get_logs", fake_get_logs)
    monkeypatch.setattr(builtins_catalog.unify, "delete_logs", lambda **_kwargs: None)

    assert builtins_catalog.seed_builtin_integrations(
        apps=[_app("gmail")],
        tools=[_tool("gmail", "list_threads")],
        backend_id="composio",
        app_slugs=["gmail"],
        prune_unlisted_apps=True,
        project="Builtins",
    )

    assert any("gmail" in item for item in filters)
    assert any("slack" in item for item in filters)
    assert not any(" or " in item for item in filters)


def test_seed_builtin_integrations_app_only_prune_preserves_tool_units(
    monkeypatch,
) -> None:
    stored_hashes = {
        "app:composio:gmail": "old",
        "tools:composio:gmail": "old",
        "app:composio:slack": "old",
        "tools:composio:slack": "old",
    }
    deleted: list[tuple[str, list[int]]] = []
    inserted: list[tuple[str, list[dict]]] = []
    seen_filters: set[str] = set()

    builtins_catalog._ENSURED_STORAGE_PROJECTS.clear()
    builtins_catalog._ENSURED_EMBEDDING_PROJECTS.clear()
    monkeypatch.setattr(builtins_catalog, "ensure_builtins_project", lambda *_: None)
    monkeypatch.setattr(builtins_catalog.unify, "create_context", lambda *_, **__: None)
    monkeypatch.setattr(builtins_catalog, "ensure_vector_column", lambda *_, **__: None)
    monkeypatch.setattr(
        builtins_catalog,
        "list_private_fields",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        builtins_catalog,
        "read_seed_hashes",
        lambda *_args, **_kwargs: dict(stored_hashes),
    )

    def fake_write_seed_hashes(_project, hashes, **_kwargs):
        stored_hashes.clear()
        stored_hashes.update(hashes)

    monkeypatch.setattr(
        builtins_catalog,
        "write_seed_hashes",
        fake_write_seed_hashes,
    )

    def fake_get_logs(**kwargs):
        if kwargs.get("context") == builtins_catalog.BUILTINS_INTEGRATION_META_CONTEXT:
            return []
        assert kwargs["return_ids_only"] is True
        filter_expr = kwargs["filter"]
        if filter_expr in seen_filters:
            return []
        seen_filters.add(filter_expr)
        return [17]

    monkeypatch.setattr(builtins_catalog.unify, "get_logs", fake_get_logs)
    monkeypatch.setattr(
        builtins_catalog.unify,
        "delete_logs",
        lambda **kwargs: deleted.append((kwargs["context"], list(kwargs["logs"]))),
    )
    monkeypatch.setattr(
        builtins_catalog.unify,
        "create_logs",
        lambda **kwargs: inserted.append((kwargs["context"], list(kwargs["entries"]))),
    )

    assert builtins_catalog.seed_builtin_integrations(
        apps=[_app("gmail")],
        tools=None,
        backend_id="composio",
        app_slugs=["gmail", "slack"],
        prune_unlisted_apps=True,
        project="Builtins",
    )

    assert (
        builtins_catalog.BUILTINS_INTEGRATION_TOOLS_CONTEXT,
        [17],
    ) not in deleted
    assert "tools:composio:gmail" in stored_hashes
    assert "tools:composio:slack" in stored_hashes
    assert "app:composio:slack" not in stored_hashes

    deleted.clear()
    inserted.clear()
    seen_filters.clear()

    assert (
        builtins_catalog.seed_builtin_integrations(
            apps=[_app("gmail")],
            tools=None,
            backend_id="composio",
            app_slugs=["gmail", "slack"],
            prune_unlisted_apps=True,
            project="Builtins",
        )
        is False
    )

    assert deleted == []
    assert inserted == []
    assert "tools:composio:gmail" in stored_hashes
    assert "tools:composio:slack" in stored_hashes


def test_seed_builtins_script_manifest_payload_supports_prune_flag() -> None:
    payload = _seed_script_module()._sync_payload(
        backend_id="composio",
        config={
            "status": "enabled",
            "sync": {
                "mode": "partial",
                "app_slugs": ["gmail"],
                "prune_unlisted_apps": True,
            },
        },
    )

    assert payload is not None
    assert payload["prune_unlisted_apps"] is True
    assert payload["app_slugs"] == ["gmail"]


def test_seed_builtins_script_combined_bootstrap_seeds_sync_result(
    tmp_path,
    monkeypatch,
) -> None:
    module = _seed_script_module()
    manifest = tmp_path / "manifest.toml"
    manifest.write_text(
        """
schema_version = 1
environment = "selfhost"

[providers.composio]
status = "enabled"

[providers.composio.sync]
mode = "partial"
app_slugs = ["gmail"]
prune_unlisted_apps = true
""",
        encoding="utf-8",
    )
    requests: list[tuple[str, str, dict | None]] = []
    seeded: list[tuple[dict, dict]] = []

    def fake_admin_request(*, method, path, payload=None, **_kwargs):
        requests.append((method, path, payload))
        if method == "GET":
            raise RuntimeError("GET bootstrap-state failed with HTTP 404")
        if path == "/admin/integrations/sync":
            return {
                "status": "success",
                "apps_upserted": 1,
                "tools_upserted": 1,
                "apps": [_app("gmail")],
                "tools": [_tool("gmail", "list_threads")],
                "matched_app_slugs": ["gmail"],
                "cache_version": payload["cache_version"],
            }
        return {}

    def fake_seed_sync_result(*, result, sync_payload):
        seeded.append((result, sync_payload))
        return True

    monkeypatch.setattr(module, "_admin_request", fake_admin_request)
    monkeypatch.setattr(module, "_seed_sync_result", fake_seed_sync_result)

    assert module._sync_integration_bootstrap_manifest(
        manifest_path=str(manifest),
        base_url="http://orchestra/v0",
        admin_key="admin",
    )

    assert requests[0][0] == "GET"
    assert requests[1][1] == "/admin/integrations/backends"
    assert requests[2][1] == "/admin/integrations/sync"
    assert requests[2][2]["prune_unlisted_apps"] is True
    assert requests[3][1] == "/admin/integrations/bootstrap-state"
    assert requests[3][2]["last_sync_diagnostics"]["seed_owner"] == "public-builtins"
    assert requests[3][2]["last_sync_diagnostics"]["builtins_seeded"] is True
    assert seeded[0][1]["app_slugs"] == ["gmail"]


def test_seed_builtins_script_skips_previously_seeded_manifest(
    tmp_path,
    monkeypatch,
) -> None:
    module = _seed_script_module()
    manifest = tmp_path / "manifest.toml"
    manifest.write_text(
        """
schema_version = 1
environment = "staging"

[providers.composio]
status = "enabled"

[providers.composio.sync]
mode = "partial"
app_slugs = ["gmail"]
""",
        encoding="utf-8",
    )
    loaded = module._load_manifest(str(manifest))
    plan = module._provider_plan(
        manifest=loaded,
        backend_id="composio",
        config=loaded["providers"]["composio"],
    )
    requests: list[tuple[str, str, dict | None]] = []

    def fake_admin_request(*, method, path, payload=None, **_kwargs):
        requests.append((method, path, payload))
        assert method == "GET"
        return {
            "desired_hash": plan.desired_hash,
            "last_status": "success",
            "last_sync_diagnostics": {
                "seed_owner": "public-builtins",
                "builtins_seeded": True,
            },
        }

    monkeypatch.setattr(module, "_admin_request", fake_admin_request)

    assert (
        module._sync_integration_bootstrap_manifest(
            manifest_path=str(manifest),
            base_url="http://orchestra/v0",
            admin_key="admin",
        )
        is False
    )

    assert requests == [
        (
            "GET",
            "/admin/integrations/bootstrap-state?environment=staging&backend_id=composio",
            None,
        ),
    ]


def test_seed_builtins_script_full_composio_batches_seed_apps_once(monkeypatch) -> None:
    module = _seed_script_module()
    admin_payloads: list[dict] = []
    seeded_payloads: list[dict] = []

    def fake_admin_request(*, path, payload=None, **_kwargs):
        assert path == "/admin/integrations/sync"
        admin_payloads.append(payload)
        if payload["sync_tools"] is False:
            return {
                "status": "success",
                "apps_upserted": 2,
                "tools_upserted": 0,
                "apps": [_app("gmail"), _app("slack")],
                "tools": [],
                "matched_app_slugs": ["gmail", "slack"],
            }
        return {
            "status": "success",
            "apps_upserted": len(payload["app_slugs"]),
            "tools_upserted": len(payload["app_slugs"]),
            "apps": [_app(slug.lower()) for slug in payload["app_slugs"]],
            "tools": [_tool(payload["app_slugs"][0].lower(), "list_threads")],
            "matched_app_slugs": [slug.lower() for slug in payload["app_slugs"]],
        }

    def fake_seed_sync_result(*, result, sync_payload):
        seeded_payloads.append(sync_payload)
        return True

    monkeypatch.setenv("UNITY_INTEGRATION_BOOTSTRAP_BATCH_SIZE", "1")
    monkeypatch.setattr(module, "_admin_request", fake_admin_request)
    monkeypatch.setattr(module, "_seed_sync_result", fake_seed_sync_result)

    changed, result = module._sync_and_seed_provider(
        base_url="http://orchestra/v0",
        admin_key="admin",
        sync_payload={
            "backend_id": "composio",
            "app_slugs": [],
            "sync_mode": "full",
            "include_all_managed_apps": True,
            "sync_tools": True,
            "prune_unlisted_apps": True,
        },
    )

    assert changed is True
    assert result["apps_upserted"] == 2
    assert len(admin_payloads) == 3
    assert seeded_payloads[0]["_seed_phase"] == "app-only-sync"
    assert seeded_payloads[0].get("_seed_apps", True) is True
    assert seeded_payloads[0]["_seed_tools"] is False
    assert [payload["_seed_apps"] for payload in seeded_payloads[1:]] == [False, False]


def test_seed_builtin_integrations_dedupes_and_deletes_tools_by_function_id(
    monkeypatch,
) -> None:
    stored_hashes: dict[str, str] = {}
    filters: list[str] = []
    deleted: list[tuple[str, list[int]]] = []
    inserted: list[tuple[str, list[dict]]] = []
    seen_filters: set[str] = set()

    monkeypatch.setattr(builtins_catalog, "ensure_builtins_project", lambda *_: None)
    monkeypatch.setattr(builtins_catalog.unify, "create_context", lambda *_, **__: None)
    monkeypatch.setattr(builtins_catalog, "ensure_vector_column", lambda *_, **__: None)
    monkeypatch.setattr(
        builtins_catalog,
        "list_private_fields",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        builtins_catalog,
        "read_seed_hashes",
        lambda *_args, **_kwargs: dict(stored_hashes),
    )
    monkeypatch.setattr(
        builtins_catalog,
        "write_seed_hashes",
        lambda _project, hashes, **_kwargs: stored_hashes.update(hashes),
    )

    def fake_get_logs(**kwargs):
        if kwargs.get("context") == builtins_catalog.BUILTINS_INTEGRATION_META_CONTEXT:
            return []
        assert kwargs["return_ids_only"] is True
        filter_expr = kwargs["filter"]
        filters.append(filter_expr)
        if filter_expr in seen_filters:
            return []
        seen_filters.add(filter_expr)
        return [17]

    monkeypatch.setattr(builtins_catalog.unify, "get_logs", fake_get_logs)
    monkeypatch.setattr(
        builtins_catalog.unify,
        "delete_logs",
        lambda **kwargs: deleted.append((kwargs["context"], list(kwargs["logs"]))),
    )
    monkeypatch.setattr(
        builtins_catalog.unify,
        "create_logs",
        lambda **kwargs: inserted.append((kwargs["context"], list(kwargs["entries"]))),
    )

    tool = _tool("gmail", "list_threads")

    assert builtins_catalog.seed_builtin_integrations(
        apps=[],
        tools=[tool, dict(tool)],
        backend_id="composio",
        app_slugs=["gmail"],
        project="Builtins",
    )

    tool_inserts = [
        rows
        for context, rows in inserted
        if context == builtins_catalog.BUILTINS_INTEGRATION_TOOLS_CONTEXT
    ]
    assert len(tool_inserts) == 1
    assert len(tool_inserts[0]) == 1
    function_id = tool_inserts[0][0]["function_id"]
    assert any(f"function_id in [{function_id}]" in item for item in filters)
    assert (
        builtins_catalog.BUILTINS_INTEGRATION_TOOLS_CONTEXT,
        [17],
    ) in deleted
