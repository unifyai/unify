"""Unity-owned sync state for provider-backed integration tools."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any, Literal

from unity.common.diagnostic_logging import (
    integration_sync_timing,
    log_staging_diagnostic,
    staging_diagnostics_enabled,
)
from unity.common.prompt_helpers import now as prompt_now
from unity.integrations import ops as integration_ops
from unity.integrations.primitives import integration_owner_scope_from_session

IntegrationSyncStatus = Literal["pending", "syncing", "ready", "failed"]
logger = logging.getLogger(__name__)


@dataclass
class IntegrationSyncState:
    app_slug: str
    status: IntegrationSyncStatus = "pending"
    app_display_name: str | None = None
    connection_id: str | None = None
    message: str = ""
    tool_count: int | None = None
    error: str | None = None
    updated_at: datetime = field(default_factory=lambda: prompt_now(as_string=False))

    @property
    def display_name(self) -> str:
        if self.app_display_name:
            return self.app_display_name
        return self.app_slug.replace("_", " ").title()

    def to_prompt_line(self) -> str:
        suffix = ""
        if self.status == "ready" and self.tool_count is not None:
            suffix = f" ({self.tool_count} active tools)"
        elif self.status == "failed" and self.error:
            suffix = f" ({self.error})"
        return f"- {self.display_name}: {self.status}{suffix}"


def normalize_app_slug(value: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


class IntegrationSyncCoordinator:
    """Tracks active provider app tool readiness without preloading inactive apps."""

    def __init__(
        self,
        *,
        owner_scope: dict[str, Any] | None = None,
    ) -> None:
        self.owner_scope = owner_scope
        self._states: dict[str, IntegrationSyncState] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    def _effective_owner_scope(self) -> dict[str, Any]:
        scope = dict(
            (
                self.owner_scope
                if self.owner_scope is not None
                else integration_owner_scope_from_session()
            ),
        )
        scope.setdefault("owner_scope", "assistant")
        return scope

    def snapshot(self) -> dict[str, IntegrationSyncState]:
        return dict(self._states)

    def prompt_summary(self) -> str:
        states = [
            state
            for state in self._states.values()
            if state.status in {"syncing", "failed"} or state.status == "ready"
        ]
        if not states:
            return ""
        lines = "\n".join(
            state.to_prompt_line()
            for state in sorted(states, key=lambda item: item.display_name)
        )
        return (
            "<integration_tool_sync>\n"
            "Provider-backed integration tools are active-app-only. Apps listed as "
            "`syncing` should be deferred until they become `ready`. Apps not listed "
            "do not have materialized FunctionManager rows yet; ask the user to "
            "connect the app in Console if FunctionManager search finds no matching tool.\n"
            f"{lines}\n"
            "</integration_tool_sync>"
        )

    def set_status(
        self,
        app_slug: str,
        status: IntegrationSyncStatus,
        *,
        app_display_name: str | None = None,
        connection_id: str | None = None,
        message: str = "",
        tool_count: int | None = None,
        error: str | None = None,
    ) -> IntegrationSyncState:
        normalized = normalize_app_slug(app_slug)
        existing = self._states.get(normalized)
        state = IntegrationSyncState(
            app_slug=normalized,
            status=status,
            app_display_name=app_display_name
            or (existing.app_display_name if existing else None),
            connection_id=connection_id
            or (existing.connection_id if existing else None),
            message=message,
            tool_count=tool_count,
            error=error,
        )
        self._states[normalized] = state
        return state

    def schedule_sync(
        self,
        app_slug: str,
        *,
        app_display_name: str | None = None,
        connection_id: str | None = None,
    ) -> asyncio.Task:
        normalized = normalize_app_slug(app_slug)
        existing_task = self._tasks.get(normalized)
        if existing_task and not existing_task.done():
            return existing_task
        self.set_status(
            normalized,
            "syncing",
            app_display_name=app_display_name,
            connection_id=connection_id,
            message=f"{app_display_name or normalized} tools are syncing.",
        )
        task = asyncio.create_task(self.sync_app(normalized))
        self._tasks[normalized] = task
        return task

    async def sync_app(self, app_slug: str) -> IntegrationSyncState:
        normalized = normalize_app_slug(app_slug)
        existing = self._states.get(normalized)
        self.set_status(
            normalized,
            "syncing",
            app_display_name=existing.app_display_name if existing else None,
            connection_id=existing.connection_id if existing else None,
        )
        with integration_sync_timing(
            logger,
            "coordinator.sync_app",
            f"app_slug={normalized}",
        ):
            try:
                from unity.function_manager.primitives.scope import PrimitiveScope
                from unity.manager_registry import ManagerRegistry

                result = await asyncio.to_thread(
                    lambda: ManagerRegistry.get_function_manager(
                        primitive_scope=PrimitiveScope.single("integrations"),
                    ).sync_provider_integration_tools(app_slug=normalized),
                )
            except Exception as exc:
                if staging_diagnostics_enabled():
                    logger.exception(
                        "Integration sync coordinator failed app_slug=%s",
                        normalized,
                    )
                return self.set_status(normalized, "failed", error=str(exc))

            changed_rows = 0
            unchanged_rows = 0
            removed_count = 0
            rows_deleted = 0
            raw_status = type(result).__name__
            raw_error: Any = None
            if isinstance(result, dict):
                raw_status = str(result.get("status") or "missing")
                raw_error = result.get("error")
                for item in result.get("apps") or []:
                    if isinstance(item, dict):
                        changed_rows += int(item.get("rows") or 0)
                for item in result.get("unchanged_apps") or []:
                    if isinstance(item, dict):
                        unchanged_rows += int(item.get("rows") or 0)
                removed_count = len(result.get("removed_apps") or [])
                rows_deleted = int(result.get("rows_deleted") or 0)
                for item in result.get("apps") or []:
                    if isinstance(item, dict):
                        rows_deleted += int(item.get("rows_deleted") or 0)
            log_staging_diagnostic(
                logger,
                (
                    "Integration sync coordinator result app_slug=%s raw_status=%s "
                    "changed_rows=%d unchanged_rows=%d removed_apps=%d "
                    "rows_deleted=%d error=%s"
                ),
                normalized,
                raw_status,
                changed_rows,
                unchanged_rows,
                removed_count,
                rows_deleted,
                raw_error,
            )

            if isinstance(result, dict) and result.get("status") == "error":
                error_payload = result.get("error")
                error = (
                    error_payload.get("message")
                    if isinstance(error_payload, dict)
                    else str(error_payload or "Integration tool sync failed")
                )
                state = self.set_status(normalized, "failed", error=str(error))
                log_staging_diagnostic(
                    logger,
                    "Integration sync coordinator mapped app_slug=%s status=%s error=%s",
                    normalized,
                    state.status,
                    state.error,
                )
                return state

            tool_count = 0
            if isinstance(result, dict):
                changed = result.get("apps") or []
                unchanged = result.get("unchanged_apps") or []
                for item in [*changed, *unchanged]:
                    if isinstance(item, dict):
                        tool_count += int(item.get("rows") or 0)
            state = self.set_status(normalized, "ready", tool_count=tool_count)
            log_staging_diagnostic(
                logger,
                "Integration sync coordinator mapped app_slug=%s status=%s tool_count=%d",
                normalized,
                state.status,
                tool_count,
            )
            return state

    async def schedule_connected_apps(self) -> list[IntegrationSyncState]:
        connections = await asyncio.to_thread(
            integration_ops.list_connections,
            **self._effective_owner_scope(),
        )
        if isinstance(connections, dict) and connections.get("error"):
            return [
                self.set_status(
                    "provider_integrations",
                    "failed",
                    error=connections["error"].get("message")
                    or "Unable to list connected integrations",
                ),
            ]
        states: list[IntegrationSyncState] = []
        for connection in connections or []:
            if connection.get("status") != "connected":
                continue
            app_slug = connection.get("canonical_app_slug")
            if not app_slug:
                continue
            self.schedule_sync(
                app_slug,
                connection_id=connection.get("connection_id"),
            )
            states.append(self._states[normalize_app_slug(app_slug)])
        return states
