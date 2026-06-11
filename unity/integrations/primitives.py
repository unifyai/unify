"""Actor-facing integration app discovery primitives."""

from __future__ import annotations

import json
from typing import Any, Optional

from unity.integrations import ops as integration_ops


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


def integration_owner_scope_from_session() -> dict[str, Any]:
    """Best-effort owner scope for Orchestra-backed provider integrations."""

    scope: dict[str, Any] = {"owner_scope": "assistant"}
    try:
        from unity.session_details import SESSION_DETAILS

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
    """

    # Concrete app/tool rows are materialized by FunctionManager sync. This
    # single helper is catalog/status discovery only, not execution discovery.
    _PRIMITIVE_METHODS: tuple[str, ...] = ("search_integrations",)

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
        query: str,
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
        It searches Orchestra's global app catalog for both ``Native`` Unity-
        deploy packages and ``Third-party`` provider apps, then enriches each
        result with Unity's local deployment/materialization state.

        Do not use this to discover executable functions or provider tools.
        Once a result says the app is ready/materialized, search FunctionManager
        for concrete native functions or materialized provider rows.

        Parameters
        ----------
        query : str
            Natural-language app search query, such as ``"Slack"`` or
            ``"CRM integrations"``.
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

        raw_results = integration_ops.search_apps(
            query,
            limit=limit,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )
        if isinstance(raw_results, dict) and raw_results.get("error"):
            return {
                "status": "error",
                "query": query,
                "error": raw_results.get("error"),
                "results": [],
            }
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
            materialized_rows = (
                self._materialized_function_rows_for_native_app(manifest_row)
                if source_type == "native"
                else (
                    self._materialized_tool_rows_for_app(app_slug)
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
                else app.get("connection_status")
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
                "connection_id": app.get("connection_id"),
                "external_account_label": app.get("external_account_label"),
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
                        "action_class": (
                            row.get("action_class")
                            or (row.get("integration_metadata") or {}).get(
                                "action_class",
                            )
                        ),
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
            import unify

            active = unify.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            context = (
                f"{root}/Integrations/Manifests" if root else "Integrations/Manifests"
            )
            rows = unify.get_logs(
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
            import unify

            active = unify.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            context = f"{root}/Functions/Primitives" if root else "Functions/Primitives"
            rows: list[dict[str, Any]] = []
            for name in function_names[:100]:
                matched = unify.get_logs(
                    context=context,
                    filter=f'name == "{name}"',
                    limit=1,
                )
                rows.extend(dict(row.entries) for row in matched)
            return rows
        except Exception:
            return []

    def _materialized_tool_rows_for_app(self, app_slug: str) -> list[dict[str, Any]]:
        normalized = _normalize_app_slug(app_slug)
        try:
            import unify

            active = unify.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            contexts = (
                [f"{root}/Functions/Primitives"] if root else ["Functions/Primitives"]
            )
            for context in contexts:
                rows = unify.get_logs(
                    context=context,
                    filter=(
                        f'app_slug == "{normalized}" and integration_source == "provider_backed"'
                    ),
                    limit=100,
                )
                if rows:
                    return [
                        dict(row.entries)
                        for row in rows
                        if dict(row.entries).get("integration_source")
                        == "provider_backed"
                    ]
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
            Orchestra response listing connection IDs, app slugs, status,
            granted scopes, health metadata, and account labels when available.

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

    async def search_tools(
        self,
        query: str,
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
        query : str
            Natural-language query such as ``"recent HubSpot leads"`` or
            ``"Slack send message"``.
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

        return integration_ops.search_tools(
            query,
            include_unconnected=include_unconnected,
            limit=limit,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

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

        return integration_ops.get_tool_schema(
            tool_id,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

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
        """

        return integration_ops.run_tool(
            tool_id,
            arguments or {},
            confirmation_token=confirmation_token,
            **self._effective_owner_scope(
                owner_scope=owner_scope,
                org_id=org_id,
                team_id=team_id,
                user_id=user_id,
                assistant_id=assistant_id,
            ),
        )

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
        tool_id = row.get("integration_tool_id")
        if isinstance(tool_id, str) and tool_id:
            return tool_id
        raise AttributeError(
            f"Unknown integration tool: primitives.integrations.{app_slug}.{tool_name}",
        )

    def _materialized_tool_row(self, app_slug: str, tool_name: str) -> dict[str, Any]:
        cached = self._tool_row_cache.get((app_slug, tool_name))
        if cached is not None:
            return cached
        name = f"primitives.integrations.{app_slug}.{tool_name}"
        try:
            import unify

            active = unify.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            contexts = (
                [f"{root}/Functions/Primitives"] if root else ["Functions/Primitives"]
            )
            for context in contexts:
                rows = unify.get_logs(
                    context=context,
                    filter=(
                        f'name == "{name}" and integration_source == "provider_backed"'
                    ),
                    limit=1,
                )
                if rows:
                    row = dict(rows[0].entries)
                    if row.get("integration_source") == "provider_backed" and row.get(
                        "integration_tool_id",
                    ):
                        self._tool_row_cache[(app_slug, tool_name)] = row
                        return row
        except Exception:
            pass
        raise AttributeError(
            f"Unknown integration tool: primitives.integrations.{app_slug}.{tool_name}",
        )

    def callable_for_tool(self, primitive_data: dict[str, Any]):
        tool_id = primitive_data.get("integration_tool_id") or primitive_data.get(
            "tool_id",
        )
        if not tool_id:
            return None

        async def _call(**arguments: Any) -> Any:
            return await self.execute_tool(tool_id=tool_id, arguments=arguments)

        _call.__name__ = primitive_data.get("primitive_method") or "integration_tool"
        _call.__doc__ = (
            primitive_data.get("docstring")
            or "Execute a provider-backed integration tool."
        )
        return _call

    def callable_for_app_tool(self, app_slug: str, tool_name: str):
        return self.callable_for_tool(self._materialized_tool_row(app_slug, tool_name))

    def __getattr__(self, app_slug: str) -> _IntegrationAppNamespace:
        if app_slug.startswith("_"):
            raise AttributeError(app_slug)
        return _IntegrationAppNamespace(self, app_slug)
