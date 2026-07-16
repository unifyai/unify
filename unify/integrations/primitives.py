"""Actor-facing integration app discovery primitives."""

from __future__ import annotations

import inspect
import json
import keyword
from typing import Any, Optional

from unify.integrations import ops as integration_ops
from unify.integrations.builtins_catalog import list_catalog_apps, list_catalog_tools
from unify.integrations.function_metadata import (
    integration_backend_id,
    integration_connection_id,
    integration_input_schema,
    integration_metadata,
    integration_tool_id,
    is_provider_backed_function,
)


def _normalize_app_slug(value: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _clean_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


def _execution_descriptor_from_row(row: dict[str, Any]) -> dict[str, Any]:
    metadata = integration_metadata(row)
    labels = metadata.get("labels") if isinstance(metadata.get("labels"), dict) else {}
    return _clean_payload(
        {
            "backend_id": metadata.get("backend_id"),
            "provider_app_id": metadata.get("provider_app_id"),
            "canonical_app_slug": metadata.get("app_slug"),
            "app_display_name": metadata.get("app_display_name")
            or labels.get("app_display_name"),
            "app_icon_url": metadata.get("app_icon_url") or labels.get("app_icon_url"),
            "provider_tool_id": metadata.get("provider_tool_id"),
            "canonical_name": row.get("name"),
            "function_manager_name": row.get("primitive_method"),
            "tool_display_name": metadata.get("tool_display_name")
            or labels.get("tool_display_name"),
            "action_class": metadata.get("action_class"),
            "behavior_hints": metadata.get("behavior_hints"),
            "required_scopes": metadata.get("required_scopes"),
            "input_schema": metadata.get("input_schema"),
            "output_schema": metadata.get("output_schema"),
            "examples": metadata.get("examples"),
            "confirmation_required": metadata.get("confirmation_required"),
        },
    )


def _annotation_for_schema(schema: Any) -> Any:
    if not isinstance(schema, dict):
        return Any
    raw_type = schema.get("type")
    if isinstance(raw_type, list):
        raw_type = next((item for item in raw_type if item != "null"), None)
    if raw_type == "string":
        return str
    if raw_type == "integer":
        return int
    if raw_type == "number":
        return float
    if raw_type == "boolean":
        return bool
    if raw_type == "array":
        return list
    if raw_type == "object":
        return dict
    return Any


def _signature_for_input_schema(input_schema: Any) -> inspect.Signature | None:
    if not isinstance(input_schema, dict):
        return None
    properties = input_schema.get("properties")
    if not isinstance(properties, dict) or not properties:
        return None
    required = set(input_schema.get("required") or [])
    parameters: list[inspect.Parameter] = []
    for name, schema in properties.items():
        if (
            not isinstance(name, str)
            or not name.isidentifier()
            or keyword.iskeyword(name)
        ):
            continue
        default = inspect._empty
        if name not in required:
            default = schema.get("default") if isinstance(schema, dict) else None
            if default is inspect._empty:
                default = None
        parameters.append(
            inspect.Parameter(
                name,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=_annotation_for_schema(schema),
            ),
        )
    if not parameters:
        return None
    return inspect.Signature(parameters, return_annotation=dict)


def integration_owner_scope_from_session() -> dict[str, Any]:
    """Best-effort owner scope for Orchestra-backed provider integrations."""

    scope: dict[str, Any] = {"owner_scope": "assistant"}
    try:
        from unify.session_details import SESSION_DETAILS

        assistant_id = getattr(
            getattr(SESSION_DETAILS, "assistant", None),
            "agent_id",
            None,
        )
        user_id = getattr(getattr(SESSION_DETAILS, "user", None), "id", None)
        org_id = getattr(getattr(SESSION_DETAILS, "org", None), "id", None)
        team_id = getattr(getattr(SESSION_DETAILS, "team", None), "id", None)
        if assistant_id is not None:
            scope["assistant_id"] = assistant_id
            return scope
        if user_id:
            scope["user_id"] = user_id
        if org_id is not None:
            scope["org_id"] = org_id
        if team_id is not None:
            scope["team_id"] = team_id
    except Exception:
        pass
    return scope


def _canonical_owner_scope(scope: dict[str, Any]) -> dict[str, Any]:
    owner_scope = scope.get("owner_scope") or "assistant"
    scoped_ids = {
        "assistant": ("assistant_id",),
        "user": ("user_id",),
        "org": ("org_id",),
        "team": ("team_id",),
    }.get(str(owner_scope), ())
    if not scoped_ids:
        scope.setdefault("owner_scope", "assistant")
        return scope
    return {
        key: value
        for key, value in scope.items()
        if key == "owner_scope"
        or key in scoped_ids
        or key
        not in {
            "assistant_id",
            "user_id",
            "org_id",
            "team_id",
        }
    }


class _IntegrationAppNamespace:
    """Dynamic namespace for ergonomic `primitives.integrations.<app>.<tool>` calls."""

    def __init__(self, owner: "IntegrationPrimitives", app_slug: str) -> None:
        self._owner = owner
        self._app_slug = app_slug

    def __getattr__(self, tool_name: str):
        try:
            callable_tool = self._owner.callable_for_app_tool(self._app_slug, tool_name)
            if callable_tool is not None:
                return callable_tool
        except AttributeError:
            pass

        async def _call(**arguments: Any) -> Any:
            callable_tool = self._owner.callable_for_app_tool(self._app_slug, tool_name)
            if callable_tool is None:
                raise AttributeError(
                    f"Unknown integration tool: primitives.integrations.{self._app_slug}.{tool_name}",
                )
            return await callable_tool(**arguments)

        _call.__name__ = tool_name
        _call.__doc__ = (
            f"Execute provider-backed integration tool "
            f"`primitives.integrations.{self._app_slug}.{tool_name}`. "
            "Search or inspect schema first when the required arguments are unclear."
        )
        return _call


class IntegrationPrimitives:
    """Integration app discovery and provider runtime helpers.

    Actor-facing discovery is intentionally app-only: native Unity-deploy
    packages and third-party provider apps are searched through Orchestra's
    global app catalog, then enriched with current-assistant state in Unity.
    Executable functions/tools are still discovered through normal
    FunctionManager search after an app is active and materialized.

    Provider and Orchestra transport retries are built into ``execute_tool`` /
    Orchestra ``run_tool``. Plans must call tools once and handle the final
    envelope — never author ad-hoc retry/sleep loops around Composio or
    provider GraphQL failures.
    """

    # Concrete app/tool rows are materialized by FunctionManager sync. This
    # single helper is catalog/status discovery only, not execution discovery.
    _PRIMITIVE_METHODS: tuple[str, ...] = (
        "search_integrations",
        "get_app_usage_mode",
        "set_app_usage_mode",
        "review_tool_permissions",
        "update_tool_permissions",
        "resolve_tool_execution",
    )

    def __init__(
        self,
        *,
        owner_scope: Optional[dict[str, Any]] = None,
    ) -> None:
        self._tool_row_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._base_owner_scope = owner_scope

    def _effective_owner_scope(self, **overrides: Any) -> dict[str, Any]:
        scope = dict(
            (
                self._base_owner_scope
                if self._base_owner_scope is not None
                else integration_owner_scope_from_session()
            ),
        )
        for key, value in overrides.items():
            if value is not None:
                scope[key] = value
        scope.setdefault("owner_scope", "assistant")
        return _canonical_owner_scope(scope)

    async def search_integrations(
        self,
        query: Optional[str] = None,
        *,
        include_tools: bool = False,
        limit: int = 10,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> dict[str, Any]:
        """Search supported integration apps and report activation readiness.

        Use this when the user asks whether an app is supported, active for the
        current assistant, connected if it is provider-backed, or ready to use.
        It searches the public Builtins app catalogue for both ``Native``
        Unity-deploy packages and ``Third-party`` provider apps, then enriches
        each result with current assistant connection/materialization state.

        Do not use this to discover executable functions or provider tools.
        Once a result says the app is ready/materialized, search FunctionManager
        for concrete native functions or materialized provider rows.

        Parameters
        ----------
        query : str, optional
            Natural-language app search query, such as ``"Slack"`` or
            ``"Salesforce"``. If omitted, returns the default catalog
            page from Orchestra.
        include_tools : bool, default False
            Include a short preview of already-materialized FunctionManager rows
            for each matched app. This is a preview only; executable discovery
            still happens through FunctionManager search.
        limit : int, default 10
            Maximum number of supported app matches to return from Orchestra.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.
        """

        effective_scope = self._effective_owner_scope(
            owner_scope=owner_scope,
            org_id=org_id,
            team_id=team_id,
            user_id=user_id,
            assistant_id=assistant_id,
        )
        raw_results = list_catalog_apps(query=query, limit=limit)
        connections = integration_ops.list_connections(**effective_scope)
        if isinstance(connections, dict) and connections.get("error"):
            return {
                "status": "error",
                "query": query,
                "error": connections.get("error"),
                "results": [],
            }
        connections_by_app: dict[str, list[dict[str, Any]]] = {}
        for connection in connections or []:
            if not isinstance(connection, dict):
                continue
            app_slug = connection.get("canonical_app_slug")
            if not isinstance(app_slug, str):
                continue
            normalized = _normalize_app_slug(app_slug)
            connections_by_app.setdefault(normalized, []).append(connection)
        if not raw_results:
            return {
                "status": "ok",
                "query": query,
                "results": [],
                "message": "No supported integration matched this query.",
            }
        results = []
        for app in raw_results:
            if not isinstance(app, dict):
                continue
            app_slug = app.get("canonical_app_slug") or app.get("app_slug")
            source_type = app.get("source_type") or "third_party"
            source_label = app.get("source_label") or (
                "Native" if source_type == "native" else "Third-party"
            )
            manifest_row = (
                self._native_manifest_row_for_app(app_slug)
                if source_type == "native" and isinstance(app_slug, str)
                else None
            )
            connection_rows = (
                connections_by_app.get(_normalize_app_slug(app_slug), [])
                if isinstance(app_slug, str)
                else []
            )
            connection_row = next(
                (row for row in connection_rows if row.get("status") == "connected"),
                connection_rows[0] if connection_rows else None,
            )
            materialized_rows = (
                self._materialized_function_rows_for_native_app(manifest_row)
                if source_type == "native"
                else (
                    self._materialized_tool_rows_for_app(
                        app_slug,
                        backend_id=(
                            connection_row.get("backend_id")
                            if isinstance(connection_row, dict)
                            else None
                        ),
                    )
                    if isinstance(app_slug, str)
                    else []
                )
            )
            materialized_function_count = len(materialized_rows)
            deployment_status = (
                ("enabled" if manifest_row else "not_enabled")
                if source_type == "native"
                else "global_catalog"
            )
            connection_status = (
                self._native_connection_status(manifest_row)
                if source_type == "native"
                else (
                    connection_row.get("status")
                    if isinstance(connection_row, dict)
                    else "not_connected"
                )
            )
            sync_status = self._sync_status_for_result(
                source_type,
                connection_status,
                materialized_function_count,
            )
            item = {
                "canonical_app_slug": app_slug,
                "display_name": app.get("display_name"),
                "source_type": source_type,
                "source_label": source_label,
                "supported": app.get("supported", True),
                "deployment_status": deployment_status,
                "connection_status": connection_status or "not_connected",
                "connection_id": (
                    connection_row.get("connection_id")
                    if isinstance(connection_row, dict)
                    else None
                ),
                "external_account_label": (
                    connection_row.get("external_account_label")
                    if isinstance(connection_row, dict)
                    else None
                ),
                "connections": [
                    {
                        "connection_id": row.get("connection_id"),
                        "status": row.get("status"),
                        "external_account_label": row.get("external_account_label"),
                        "backend_id": row.get("backend_id"),
                        "provider_connection_id": row.get("provider_connection_id"),
                    }
                    for row in connection_rows
                    if isinstance(row, dict)
                ],
                "account_count": len(connection_rows),
                "auth_modes": app.get("auth_modes") or [],
                "tool_count": app.get("tool_count", 0),
                "materialized_function_count": materialized_function_count,
                "materialized_tool_count": materialized_function_count,
                "sync_status": sync_status,
                "next_action": self._next_action(
                    source_type,
                    deployment_status,
                    connection_status,
                    sync_status,
                ),
                "score": app.get("score"),
                "match_reason": app.get("match_reason"),
            }
            if include_tools:
                item["materialized_tools"] = [
                    {
                        "name": row.get("name"),
                        "description": row.get("docstring"),
                        "action_class": integration_metadata(row).get("action_class"),
                    }
                    for row in materialized_rows[:25]
                ]
            results.append(item)
        return {"status": "ok", "query": query, "results": results}

    @staticmethod
    def _sync_status_for_result(
        source_type: str,
        connection_status: Any,
        materialized_function_count: int,
    ) -> str:
        if source_type == "native" and connection_status == "not_enabled":
            return "not_enabled"
        if source_type == "native" and connection_status == "missing_required_secrets":
            return "missing_required_secrets"
        if source_type != "native" and connection_status != "connected":
            return "not_connected"
        if materialized_function_count > 0:
            return "materialized"
        return "not_yet_synced"

    @staticmethod
    def _next_action(
        source_type: str,
        deployment_status: str,
        connection_status: Any,
        sync_status: str,
    ) -> str:
        if source_type == "native":
            if deployment_status != "enabled":
                return "Ask the user/operator to enable this native integration for the assistant deployment."
            if connection_status == "missing_required_secrets":
                return "Ask the user/operator to add the required native integration secrets in Console."
            if sync_status == "materialized":
                return "Search FunctionManager for executable native integration functions."
            return "Tell the user the native integration is enabled and functions are still syncing."
        if connection_status != "connected":
            return "Ask the user to connect this integration in Console Integrations."
        if sync_status == "materialized":
            return (
                "Search FunctionManager for executable materialized integration tools."
            )
        return "Tell the user the integration is connected and tools are still syncing."

    @staticmethod
    def _native_connection_status(manifest_row: dict[str, Any] | None) -> str:
        if not manifest_row:
            return "not_enabled"
        required = _json_list(manifest_row.get("required_secrets_json"))
        return "missing_required_secrets" if required else "ready"

    def _native_manifest_row_for_app(self, app_slug: str) -> dict[str, Any] | None:
        normalized = _normalize_app_slug(app_slug)
        try:
            import unisdk

            active = unisdk.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            context = (
                f"{root}/Integrations/Manifests" if root else "Integrations/Manifests"
            )
            rows = unisdk.get_logs(
                context=context,
                filter=f'slug == "{normalized}"',
                limit=1,
            )
            if rows:
                return dict(rows[0].entries)
        except Exception:
            pass
        return None

    def _materialized_function_rows_for_native_app(
        self,
        manifest_row: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if not manifest_row:
            return []
        function_names = [
            str(name) for name in _json_list(manifest_row.get("function_names_json"))
        ]
        if not function_names:
            return []
        try:
            import unisdk

            active = unisdk.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            context = f"{root}/Functions/Primitives" if root else "Functions/Primitives"
            rows: list[dict[str, Any]] = []
            for name in function_names[:100]:
                matched = unisdk.get_logs(
                    context=context,
                    filter=f'name == "{name}"',
                    limit=1,
                )
                rows.extend(dict(row.entries) for row in matched)
            return rows
        except Exception:
            return []

    def _connected_backend_for_app(self, app_slug: str) -> str | None:
        normalized = _normalize_app_slug(app_slug)
        connections = integration_ops.list_connections(**self._effective_owner_scope())
        if isinstance(connections, dict) and connections.get("error"):
            return None
        for connection in connections or []:
            if not isinstance(connection, dict):
                continue
            if connection.get("status") != "connected":
                continue
            raw_app = connection.get("canonical_app_slug")
            if not isinstance(raw_app, str):
                continue
            if _normalize_app_slug(raw_app) != normalized:
                continue
            backend_id = connection.get("backend_id")
            return str(backend_id) if backend_id else None
        return None

    def _materialized_tool_rows_for_app(
        self,
        app_slug: str,
        *,
        backend_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized = _normalize_app_slug(app_slug)
        try:
            import unisdk

            active = unisdk.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            contexts = (
                [f"{root}/Functions/Primitives"] if root else ["Functions/Primitives"]
            )
            row_filter = (
                'metadata["source"] == "provider_backed" '
                f'and metadata["integration"]["app_slug"] == {json.dumps(normalized)}'
            )
            if backend_id:
                row_filter = (
                    f"({row_filter}) and "
                    f'metadata["integration"]["backend_id"] == {json.dumps(backend_id)}'
                )
            for context in contexts:
                rows = unisdk.get_logs(
                    context=context,
                    filter=row_filter,
                    limit=100,
                )
                if rows:
                    return [dict(row.entries) for row in rows]
        except Exception:
            pass
        return []

    async def list_connected(
        self,
        *,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """List connected or pending provider-backed app integrations.

        Use this when the task depends on knowing which external SaaS apps are
        connected for the current owner scope before choosing a concrete tool.
        This is a runtime status helper; primary tool discovery still happens
        through FunctionManager rows named
        ``primitives.integrations.<app>.<tool>``.

        Parameters
        ----------
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope, usually ``"assistant"``. Use ``"org"``, ``"team"``,
            or ``"user"`` when explicitly operating at that scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.

        Returns
        -------
        Any
            Orchestra response listing every connection row for the owner
            (ids, app slugs, status, labels). Multiple accounts for one app
            appear as separate rows — use ``connection_id`` when executing
            under ``explicit`` usage mode. Prefer
            ``search_integrations`` / ``search_tools`` when you want
            per-app ``connections`` arrays already grouped.

        Notes
        -----
        Do not ask the user for raw credentials from inside the actor loop.
        If a needed app is not connected, tell the user to connect it in
        Console and continue with any other available data.
        """

        return integration_ops.list_connections(
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

    async def get_app_usage_mode(
        self,
        canonical_app_slug: str,
        *,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """Return the account selection mode for an app (``primary``/``explicit``/``pool``)."""

        return integration_ops.get_app_preference(
            canonical_app_slug,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

    async def set_app_usage_mode(
        self,
        canonical_app_slug: str,
        *,
        usage_mode: str,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """Set account selection mode for an app.

        ``primary`` uses the latest live connection, ``explicit`` requires
        ``connection_id`` on execute, and ``pool`` round-robins across live
        accounts (useful for sharing API rate limits).
        """

        return integration_ops.update_app_preference(
            canonical_app_slug,
            usage_mode=usage_mode,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

    async def search_tools(
        self,
        query: Optional[str] = None,
        *,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
        include_unconnected: bool = False,
        limit: int = 20,
    ) -> Any:
        """Search concrete provider-backed tools with activation metadata.

        Use this as a narrow integration-specific helper when you already know
        the task involves external SaaS apps. For normal actor discovery, prefer
        FunctionManager search so integration tools appear alongside normal
        functions and system primitives.

        Parameters
        ----------
        query : str, optional
            Natural-language query such as ``"recent HubSpot leads"`` or
            ``"Slack send message"``. If omitted, returns the default tool
            page from Orchestra.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.
        include_unconnected : bool, default False
            Keep unavailable tools out of normal actor discovery. If no tools
            are returned for a requested app, tell the user to connect that app
            in Console and retry once the tools are available.
        limit : int, default 20
            Maximum number of tool results to return.

        Returns
        -------
        Any
            Search results from Orchestra. Each result should include
            ``tool_id``, ``canonical_name``, ``activation_state``,
            ``required_scopes``, ``granted_scopes`` when available,
            ``action_class``, ``confirmation_required``, and
            ``schema_available``.

        Notes
        -----
        ``connected_ready`` means the tool can be executed. Blocked states
        should only appear when explicitly requested with
        ``include_unconnected=True``; explain the required Console action
        instead of inventing credentials or bypasses.
        """

        effective_scope = self._effective_owner_scope(
            owner_scope=owner_scope,
            org_id=org_id,
            team_id=team_id,
            user_id=user_id,
            assistant_id=assistant_id,
        )
        rows = list_catalog_tools(limit=max(limit, 100))
        if query:
            needle = query.strip().lower()
            rows = [
                row
                for row in rows
                if needle in str(row.get("name") or "").lower()
                or needle in str(row.get("docstring") or "").lower()
                or needle in str(row.get("embedding_text") or "").lower()
            ]
        connections = integration_ops.list_connections(**effective_scope)
        if isinstance(connections, dict) and connections.get("error"):
            return connections
        connected_by_app: dict[str, list[dict[str, Any]]] = {}
        for connection in connections or []:
            if not isinstance(connection, dict):
                continue
            if connection.get("status") != "connected":
                continue
            slug = connection.get("canonical_app_slug")
            if not slug:
                continue
            connected_by_app.setdefault(_normalize_app_slug(slug), []).append(
                connection,
            )
        connected_apps = set(connected_by_app)
        results = []
        for row in rows:
            metadata = integration_metadata(row)
            app_slug = _normalize_app_slug(metadata.get("app_slug"))
            activation_state = (
                "connected_ready" if app_slug in connected_apps else "not_connected"
            )
            if not include_unconnected and activation_state != "connected_ready":
                continue
            app_connections = connected_by_app.get(app_slug, [])
            results.append(
                {
                    "tool_id": integration_tool_id(row),
                    "canonical_name": row.get("name"),
                    "display_name": metadata.get("tool_display_name")
                    or metadata.get("labels", {}).get("tool_display_name"),
                    "description": row.get("docstring"),
                    "activation_state": activation_state,
                    "required_scopes": metadata.get("required_scopes") or [],
                    "action_class": metadata.get("action_class"),
                    "confirmation_required": metadata.get("confirmation_required"),
                    "schema_available": metadata.get("schema_available", True),
                    "available_connections": [
                        {
                            "connection_id": connection.get("connection_id"),
                            "external_account_label": connection.get(
                                "external_account_label",
                            ),
                            "status": connection.get("status"),
                        }
                        for connection in app_connections
                    ],
                    "account_count": len(app_connections),
                },
            )
            if len(results) >= limit:
                break
        return results

    async def get_tool_schema(
        self,
        tool_id: str,
        *,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """Return schema, examples, and activation metadata for a provider tool.

        Inspect schemas before executing a provider tool when the required
        arguments are not obvious from FunctionManager metadata. This keeps the
        actor from guessing provider-specific argument names.

        Parameters
        ----------
        tool_id : str
            Stable Orchestra tool identifier returned by FunctionManager search
            or ``search_tools``.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.

        Returns
        -------
        Any
            Tool schema envelope including input schema, output schema,
            examples, required scopes, activation state, and connection status.
        """

        rows = list_catalog_tools(tool_id=tool_id, limit=1)
        if not rows:
            return {
                "status": "error",
                "error": {
                    "code": "integration_tool_not_found",
                    "message": f"No integration tool catalog row found for {tool_id}.",
                },
            }
        row = rows[0]
        metadata = integration_metadata(row)
        return {
            "status": "ok",
            "tool_id": tool_id,
            "canonical_name": row.get("name"),
            "input_schema": integration_input_schema(row),
            "output_schema": metadata.get("output_schema") or {},
            "examples": metadata.get("examples") or [],
            "required_scopes": metadata.get("required_scopes") or [],
            "action_class": metadata.get("action_class"),
            "confirmation_required": metadata.get("confirmation_required"),
            "schema_available": metadata.get("schema_available", True),
        }

    async def execute_tool(
        self,
        tool_id: str,
        arguments: Optional[dict[str, Any]] = None,
        *,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
        confirmation_token: Optional[str] = None,
        connection_id: Optional[str] = None,
        approval_audit_id: Optional[int] = None,
        _tool_descriptor: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute a provider tool through Orchestra policy and audit checks.

        Parameters
        ----------
        tool_id : str
            Stable Orchestra tool identifier returned by FunctionManager search
            or ``search_tools``.
        arguments : dict, optional
            Provider-tool arguments. Inspect ``get_tool_schema`` first when the
            argument names or required fields are unclear.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.
        confirmation_token : str, optional
            Confirmation token required by Orchestra for write, destructive,
            bulk export, or sensitive actions.
        connection_id : str, optional
            Stable Orchestra connection identifier for account-scoped execution.
        approval_audit_id : int, optional
            Audit identifier for an approved execution retry. Use this only when
            retrying the same tool with the same original arguments.

        Returns
        -------
        Any
            Structured execution envelope. Expected statuses include ``ok``,
            ``connect_required``, ``missing_scope``, ``expired``, ``error``,
            ``blocked_by_policy``, and ``confirmation_required``.

        Notes
        -----
        Treat non-``ok`` statuses as first-class outcomes. Surface the blocked
        state to the user and ask them to connect, reconnect, grant scope, or
        confirm in the approved UI flow.

        Transient provider failures (HTTP 429/5xx, GitHub GraphQL platform
        blips on reads) are retried inside Orchestra before this returns.
        Transient Orchestra connectivity failures are retried in Unify ops.
        Do **not** wrap this call in a custom retry loop — call once and
        handle the final envelope.
        """

        effective_scope = self._effective_owner_scope(
            owner_scope=owner_scope,
            org_id=org_id,
            team_id=team_id,
            user_id=user_id,
            assistant_id=assistant_id,
            connection_id=connection_id,
        )
        if approval_audit_id is not None:
            effective_scope["approval_audit_id"] = approval_audit_id

        return await integration_ops.async_run_tool(
            tool_id,
            arguments or {},
            confirmation_token=confirmation_token,
            **(_tool_descriptor or {}),
            **effective_scope,
        )

    async def review_tool_permissions(
        self,
        connection_id: str,
        *,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """Review tool permissions for one connected integration account.

        Use this when the user asks what an integration account is allowed to
        do, why a tool asks for confirmation, or which tools are blocked. The
        returned policy is scoped to ``connection_id``. If the same app has
        multiple connected accounts, review the account the user named or ask
        which account they mean before changing permissions.

        Parameters
        ----------
        connection_id : str
            Identifier of the connected integration account to review, e.g.
            ``"ic_work"``. Scopes the returned policy to that one account.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope, usually ``"assistant"``. Use ``"org"``, ``"team"``,
            or ``"user"`` when explicitly operating at that scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.

        Examples
        --------
        - ``await primitives.integrations.review_tool_permissions(connection_id="ic_work")``
        - "What Gmail tools are allowed for my Work Gmail account?"

        Returns
        -------
        dict
            Account-scoped policy with app/account labels and per-tool approval
            levels: ``auto``, ``specific_approval``, or ``forbidden``.
        """

        return integration_ops.get_tool_policy(
            connection_id,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

    async def update_tool_permissions(
        self,
        connection_id: str,
        *,
        tool_policies: Optional[dict[str, str]] = None,
        bulk_approval_level: Optional[str] = None,
        action_classes: Optional[list[str]] = None,
        reset_to_defaults: bool = False,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """Change durable tool permissions for one connected account.

        Use this only after the user clearly asks to change future behavior for
        a specific connected account. For one-off approval of a pending
        execution, use ``resolve_tool_execution`` instead.

        Permission levels:
        - ``auto`` means allow this tool for this account without asking first.
        - ``specific_approval`` means ask every time for this account.
        - ``forbidden`` means block this tool for this account.

        Parameters
        ----------
        connection_id : str
            Identifier of the connected integration account to change, e.g.
            ``"ic_work"``. All changes are scoped to this one account.
        tool_policies : dict[str, str], optional
            Per-tool approval levels keyed by fully-qualified tool id, e.g.
            ``{"composio:gmail:list_labels": "auto"}``. Values are ``auto``,
            ``specific_approval``, or ``forbidden``.
        bulk_approval_level : str, optional
            Approval level to apply in bulk (``auto``, ``specific_approval``,
            or ``forbidden``), typically combined with ``action_classes``.
        action_classes : list[str], optional
            Action classes the bulk change targets, e.g. ``["write"]`` or
            ``["read", "write"]``.
        reset_to_defaults : bool, default False
            When True, discard custom policy for the account and restore the
            backend defaults.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope, usually ``"assistant"``. Use ``"org"``, ``"team"``,
            or ``"user"`` when explicitly operating at that scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.

        Examples
        --------
        - Allow one tool for one account:
          ``await primitives.integrations.update_tool_permissions(connection_id="ic_work", tool_policies={"composio:gmail:list_labels": "auto"})``
        - Require confirmation for write tools:
          ``await primitives.integrations.update_tool_permissions(connection_id="ic_work", bulk_approval_level="specific_approval", action_classes=["write"])``
        - Reset a connected account to backend defaults:
          ``await primitives.integrations.update_tool_permissions(connection_id="ic_work", reset_to_defaults=True)``

        Returns
        -------
        dict
            Updated account-scoped policy from Orchestra.
        """

        return integration_ops.patch_tool_policy(
            connection_id,
            tool_policies=tool_policies,
            bulk_approval_level=bulk_approval_level,
            action_classes=action_classes,
            reset_to_defaults=reset_to_defaults,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

    async def resolve_tool_execution(
        self,
        audit_id: int,
        *,
        decision: str,
        scope: str = "once",
        persist_policy: bool = False,
        approval_level: str = "auto",
        actor_id: Optional[str] = None,
        reason: Optional[str] = None,
        expires_at: Optional[str] = None,
        owner_scope: Optional[str] = None,
        org_id: Optional[int] = None,
        team_id: Optional[int] = None,
        user_id: Optional[str] = None,
        assistant_id: Optional[int] = None,
    ) -> Any:
        """Approve or deny a pending integration tool execution.

        Use this when a provider-backed tool returned ``pending_approval`` and
        the user responds in natural language. ``scope="once"`` approves or
        denies only the pending audit. Set ``persist_policy=True`` only when
        the user explicitly asks to change future permissions for the connected
        account too.

        After approval, retry the original tool with the same original
        arguments and the returned ``confirmation_token`` or
        ``approval_audit_id``. Do not start a fresh unrelated tool call.

        Parameters
        ----------
        audit_id : int
            Identifier of the pending execution audit returned alongside the
            tool's ``pending_approval`` response.
        decision : str
            ``"approve"`` to allow the pending execution or ``"deny"`` to block
            it.
        scope : str, default "once"
            Breadth of the decision. ``"once"`` affects only this audit;
            ``"tool"`` applies to future calls of the same tool when combined
            with ``persist_policy``.
        persist_policy : bool, default False
            When True, also update the connected account's durable policy. Set
            only when the user explicitly asks to change future behavior.
        approval_level : str, default "auto"
            Durable level to persist when ``persist_policy`` is True (``auto``,
            ``specific_approval``, or ``forbidden``). For denials, ``auto`` is
            mapped to ``forbidden``.
        actor_id : str, optional
            Identifier of the principal recording the decision, for audit.
        reason : str, optional
            Human-readable justification, most useful when denying.
        expires_at : str, optional
            ISO-8601 timestamp after which a persisted approval lapses.
        owner_scope : str, optional
            Effective connection owner lane. Defaults to the current session's
            owner scope, usually ``"assistant"``. Use ``"org"``, ``"team"``,
            or ``"user"`` when explicitly operating at that scope.
        org_id : int, optional
            Organization identifier for org-scoped or inherited connections.
        team_id : int, optional
            Team identifier for team-scoped connections.
        user_id : str, optional
            User identifier for user-scoped connections.
        assistant_id : int, optional
            Assistant identifier for assistant-scoped connections.

        Examples
        --------
        - ``await primitives.integrations.resolve_tool_execution(audit_id=17, decision="approve")``
        - ``await primitives.integrations.resolve_tool_execution(audit_id=17, decision="deny", reason="wrong account")``
        - ``await primitives.integrations.resolve_tool_execution(audit_id=17, decision="approve", scope="tool", persist_policy=True, approval_level="auto")``

        Returns
        -------
        dict
            Approval or denial envelope with audit/tool/connection identity.
            Approval responses include a ``confirmation_token`` for retry.
        """

        resolved_scope = self._effective_owner_scope(
            owner_scope=owner_scope,
            org_id=org_id,
            team_id=team_id,
            user_id=user_id,
            assistant_id=assistant_id,
        )
        if decision == "approve":
            return integration_ops.approve_tool_execution(
                audit_id,
                scope=scope,
                persist_policy=persist_policy,
                approval_level=approval_level,
                actor_id=actor_id,
                expires_at=expires_at,
                **resolved_scope,
            )
        if decision == "deny":
            denial_level = "forbidden" if approval_level == "auto" else approval_level
            return integration_ops.deny_tool_execution(
                audit_id,
                scope=scope,
                persist_policy=persist_policy,
                approval_level=denial_level,
                actor_id=actor_id,
                reason=reason,
                **resolved_scope,
            )
        raise ValueError("decision must be 'approve' or 'deny'")

    async def manage_connection(
        self,
        connection_id: str,
        *,
        action: str = "test",
    ) -> Any:
        """Run a safe connection management action.

        Parameters
        ----------
        connection_id : str
            Connection identifier returned by ``list_connected`` or tool
            activation metadata.
        action : str, default "test"
            Management action. The actor-facing MVP supports ``"test"`` for
            health checks. Console owns connect, reconnect, disconnect, and
            permission review flows.

        Returns
        -------
        Any
            Health-check result for ``action="test"`` or a structured
            ``unsupported_action`` response for Console-owned actions.
        """

        if action != "test":
            return {
                "status": "unsupported_action",
                "message": "Use Console to connect, reconnect, or disconnect integrations.",
            }
        return integration_ops.test_connection(connection_id)

    async def resolve_tool_id(self, app_slug: str, tool_name: str) -> str:
        row = self._materialized_tool_row(app_slug, tool_name)
        tool_id = integration_tool_id(row)
        if isinstance(tool_id, str) and tool_id:
            return tool_id
        raise AttributeError(
            f"Unknown integration tool: primitives.integrations.{app_slug}.{tool_name}",
        )

    def _materialized_tool_row(self, app_slug: str, tool_name: str) -> dict[str, Any]:
        cached = self._tool_row_cache.get((app_slug, tool_name))
        if cached is not None:
            return cached
        preferred_backend = self._connected_backend_for_app(app_slug)
        name = f"primitives.integrations.{app_slug}.{tool_name}"

        def _matches_backend(row: dict[str, Any]) -> bool:
            if not preferred_backend:
                return True
            return integration_backend_id(row) == preferred_backend

        try:
            from unify.manager_registry import ManagerRegistry

            fm = ManagerRegistry.get_function_manager()
            resolver = getattr(fm, "_get_stored_primitive_data_by_name", None)
            if callable(resolver):
                row = resolver(name=name, provider_backed_only=True)
                if (
                    isinstance(row, dict)
                    and integration_tool_id(row)
                    and _matches_backend(row)
                ):
                    self._tool_row_cache[(app_slug, tool_name)] = row
                    return row
        except Exception:
            pass
        try:
            import unisdk

            active = unisdk.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            contexts = (
                [f"{root}/Functions/Primitives"] if root else ["Functions/Primitives"]
            )
            for context in contexts:
                rows = unisdk.get_logs(
                    context=context,
                    filter=(
                        f"name == {json.dumps(name)} "
                        'and metadata["source"] == "provider_backed"'
                    ),
                    limit=10,
                )
                for raw_row in rows or []:
                    row = dict(raw_row.entries)
                    if (
                        is_provider_backed_function(row)
                        and integration_tool_id(row)
                        and _matches_backend(row)
                    ):
                        self._tool_row_cache[(app_slug, tool_name)] = row
                        return row
        except Exception:
            pass
        raise AttributeError(
            f"Unknown integration tool: primitives.integrations.{app_slug}.{tool_name}",
        )

    def callable_for_tool(self, primitive_data: dict[str, Any]):
        tool_id = integration_tool_id(primitive_data)
        if not tool_id:
            return None
        connection_id = integration_connection_id(primitive_data)

        async def _call(**arguments: Any) -> Any:
            confirmation_token = arguments.pop("confirmation_token", None)
            approval_audit_id = arguments.pop("approval_audit_id", None)
            override_connection_id = (
                arguments.pop("connection_id", None) or connection_id
            )
            return await self.execute_tool(
                tool_id=tool_id,
                arguments=arguments,
                confirmation_token=confirmation_token,
                connection_id=override_connection_id,
                approval_audit_id=approval_audit_id,
                _tool_descriptor=_execution_descriptor_from_row(primitive_data),
            )

        _call.__name__ = primitive_data.get("primitive_method") or "integration_tool"
        _call.__doc__ = (
            primitive_data.get("docstring")
            or "Execute a provider-backed integration tool."
        )
        signature = _signature_for_input_schema(
            integration_input_schema(primitive_data),
        )
        if signature is not None:
            _call.__signature__ = signature
        return _call

    def callable_for_app_tool(self, app_slug: str, tool_name: str):
        return self.callable_for_tool(self._materialized_tool_row(app_slug, tool_name))

    def __getattr__(self, app_slug: str) -> _IntegrationAppNamespace:
        if app_slug.startswith("_"):
            raise AttributeError(app_slug)
        return _IntegrationAppNamespace(self, app_slug)
