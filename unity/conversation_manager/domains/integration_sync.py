"""Conversation-manager handling for provider integration tool sync state."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from unity.common.prompt_helpers import now as prompt_now
from unity.conversation_manager.events import (
    IntegrationToolsSyncCompleted,
    IntegrationToolsSyncFailed,
    IntegrationToolsSyncRequested,
)
from unity.integrations.sync_state import normalize_app_slug

if TYPE_CHECKING:
    from unity.conversation_manager.conversation_manager import ConversationManager

MATERIALIZE_OPERATION = "materialize"
CLEANUP_OPERATION = "cleanup"


def _payload_value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    for extra_key in ("extra_event_fields", "extraEventFields"):
        extra = payload.get(extra_key)
        if not isinstance(extra, dict):
            continue
        for key in keys:
            value = extra.get(key)
            if value not in (None, ""):
                return value
    return None


def _payload_details(payload: dict[str, Any]) -> dict[str, Any]:
    details = payload.get("details")
    if isinstance(details, dict):
        return details
    for extra_key in ("extra_event_fields", "extraEventFields"):
        extra = payload.get(extra_key)
        if isinstance(extra, dict):
            details = extra.get("details")
            if isinstance(details, dict):
                return details
    return {}


def _app_event_fields(payload: dict[str, Any]) -> tuple[str, str | None, str | None]:
    details = _payload_details(payload)
    app_slug = (
        _payload_value(payload, "app_slug", "appSlug")
        or _payload_value(payload, "canonical_app_slug", "canonicalAppSlug")
        or details.get("app_slug")
        or details.get("appSlug")
        or details.get("canonical_app_slug")
        or details.get("canonicalAppSlug")
        or ""
    )
    app_display_name = (
        _payload_value(payload, "app_display_name", "appDisplayName")
        or details.get("app_display_name")
        or details.get("appDisplayName")
        or details.get("display_name")
        or details.get("displayName")
    )
    connection_id = (
        _payload_value(payload, "connection_id", "connectionId")
        or details.get("connection_id")
        or details.get("connectionId")
    )
    return (
        str(app_slug),
        str(app_display_name) if app_display_name else None,
        str(connection_id) if connection_id else None,
    )


def _sync_operation(payload: dict[str, Any]) -> str:
    raw = _payload_value(payload, "operation")
    normalized = str(raw or "").strip().lower()
    if normalized == CLEANUP_OPERATION:
        return CLEANUP_OPERATION
    return MATERIALIZE_OPERATION


def _tool_count(payload: dict[str, Any]) -> int | None:
    value = _payload_value(payload, "tool_count")
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _integration_tools_sync_requested_from_payload(
    payload: dict[str, Any],
    *,
    message: str = "",
) -> IntegrationToolsSyncRequested | None:
    if not isinstance(payload, dict):
        return None
    app_slug, app_display_name, connection_id = _app_event_fields(payload)
    if not app_slug:
        return None
    return IntegrationToolsSyncRequested(
        app_slug=app_slug,
        app_display_name=app_display_name,
        connection_id=connection_id,
        operation=_sync_operation(payload),
        message=message or str(payload.get("message") or ""),
    )


def _integration_tools_sync_completed_from_payload(
    payload: dict[str, Any],
    *,
    message: str = "",
) -> IntegrationToolsSyncCompleted | None:
    if not isinstance(payload, dict):
        return None
    app_slug, app_display_name, connection_id = _app_event_fields(payload)
    if not app_slug:
        return None
    return IntegrationToolsSyncCompleted(
        app_slug=app_slug,
        app_display_name=app_display_name,
        connection_id=connection_id,
        tool_count=_tool_count(payload),
        operation=_sync_operation(payload),
        message=message or str(payload.get("message") or ""),
    )


def _integration_tools_sync_failed_from_payload(
    payload: dict[str, Any],
    *,
    message: str = "",
) -> IntegrationToolsSyncFailed | None:
    if not isinstance(payload, dict):
        return None
    app_slug, app_display_name, connection_id = _app_event_fields(payload)
    if not app_slug:
        return None
    error = (
        _payload_value(payload, "error") or _payload_details(payload).get("error") or ""
    )
    return IntegrationToolsSyncFailed(
        app_slug=app_slug,
        app_display_name=app_display_name,
        connection_id=connection_id,
        error=str(error),
        operation=_sync_operation(payload),
        message=message or str(payload.get("message") or ""),
    )


async def _publish_sync_result(
    cm: "ConversationManager",
    task: asyncio.Task,
    app_slug: str,
) -> None:
    try:
        state = await task
    except Exception as exc:
        event = IntegrationToolsSyncFailed(app_slug=app_slug, error=str(exc))
    else:
        if state.status == "ready":
            event = IntegrationToolsSyncCompleted(
                app_slug=state.app_slug,
                app_display_name=state.app_display_name,
                connection_id=state.connection_id,
                tool_count=state.tool_count,
                operation=state.operation,
            )
        elif state.status == "removed":
            event = IntegrationToolsSyncCompleted(
                app_slug=state.app_slug,
                app_display_name=state.app_display_name,
                connection_id=state.connection_id,
                tool_count=0,
                operation=state.operation,
                message="Integration tools removed.",
            )
        else:
            event = IntegrationToolsSyncFailed(
                app_slug=state.app_slug,
                app_display_name=state.app_display_name,
                connection_id=state.connection_id,
                error=state.error or "Integration tool sync failed",
                operation=state.operation,
            )
    await cm.event_broker.publish(event.topic, event.to_json())


def _track_sync_task(
    cm: "ConversationManager",
    task: asyncio.Task,
    app_slug: str,
) -> None:
    asyncio.create_task(_publish_sync_result(cm, task, app_slug))


def _schedule_startup_integration_sync(cm: "ConversationManager") -> None:
    async def _run() -> None:
        coordinator = cm.integration_sync_coordinator
        states = await coordinator.schedule_connected_apps()
        for state in states:
            task = coordinator._tasks.get(state.app_slug)
            if task is not None:
                _track_sync_task(cm, task, state.app_slug)

    asyncio.create_task(_run())


async def _handle_integration_tools_sync_requested(
    event: IntegrationToolsSyncRequested,
    cm: "ConversationManager",
) -> bool:
    coordinator = cm.integration_sync_coordinator
    task = coordinator.schedule_sync(
        event.app_slug,
        app_display_name=event.app_display_name,
        connection_id=event.connection_id,
        operation=event.operation,
    )
    _track_sync_task(cm, task, normalize_app_slug(event.app_slug))
    state = coordinator.snapshot()[normalize_app_slug(event.app_slug)]
    cm.notifications_bar.push_notif(
        "Integrations",
        (
            f"{state.display_name} tools are being removed."
            if event.operation == CLEANUP_OPERATION
            else f"{state.display_name} tools are syncing and will be available shortly."
        ),
        event.timestamp,
    )
    return True


async def _handle_integration_tools_sync_completed(
    event: IntegrationToolsSyncCompleted,
    cm: "ConversationManager",
) -> bool:
    app_slug = normalize_app_slug(event.app_slug)
    status = "removed" if event.operation == CLEANUP_OPERATION else "ready"
    state = cm.integration_sync_coordinator.set_status(
        app_slug,
        status,
        app_display_name=event.app_display_name,
        connection_id=event.connection_id,
        tool_count=event.tool_count,
        operation=event.operation,
    )
    cm.notifications_bar.push_notif(
        "Integrations",
        (
            f"{state.display_name} tools were removed."
            if state.status == "removed"
            else f"{state.display_name} tools are ready."
        ),
        event.timestamp,
    )
    return True


async def _handle_integration_tools_sync_failed(
    event: IntegrationToolsSyncFailed,
    cm: "ConversationManager",
) -> bool:
    state = cm.integration_sync_coordinator.set_status(
        event.app_slug,
        "failed",
        app_display_name=event.app_display_name,
        connection_id=event.connection_id,
        error=event.error,
        operation=event.operation,
    )
    cm.notifications_bar.push_notif(
        "Integrations",
        f"{state.display_name} tools failed to sync. {event.error}".strip(),
        prompt_now(as_string=False),
    )
    return True
