"""Approval envelope helpers for provider-backed integration executions."""

from __future__ import annotations

from typing import Any

from unity.integrations.function_metadata import integration_tool_id


def _extract_provider_audit_id(envelope: dict[str, Any]) -> Any:
    for key in ("audit_id", "execution_audit_id", "tool_execution_id"):
        value = envelope.get(key)
        if value is not None:
            return value
    error = envelope.get("error")
    if isinstance(error, dict):
        for key in ("audit_id", "execution_audit_id", "tool_execution_id"):
            value = error.get(key)
            if value is not None:
                return value
    return None


def build_pending_approval_payload(
    *,
    function_name: str,
    function_data: dict[str, Any],
    call_kwargs: dict[str, Any],
    provider_envelope: dict[str, Any],
) -> dict[str, Any]:
    confirmation = provider_envelope.get("confirmation")
    if not isinstance(confirmation, dict):
        confirmation = {}
    audit_id = confirmation.get("audit_id") or _extract_provider_audit_id(
        provider_envelope,
    )
    tool_id = (
        confirmation.get("tool_id")
        or integration_tool_id(function_data)
        or provider_envelope.get("tool_id")
    )
    connection_id = confirmation.get("connection_id") or provider_envelope.get(
        "connection_id",
    )
    behavior_hints = confirmation.get("behavior_hints") or []
    if not isinstance(behavior_hints, list):
        behavior_hints = [str(behavior_hints)]
    arguments_summary = confirmation.get("arguments_summary") or call_kwargs

    return {
        "type": "integration_tool_pending_approval",
        "status": "pending_approval",
        "message": "Integration tool execution is waiting for user approval.",
        "approval": {
            "audit_id": audit_id,
            "connection_id": connection_id,
            "tool_id": tool_id,
            "function_name": function_name,
            "app_slug": confirmation.get("app_slug"),
            "app_display_name": confirmation.get("app_display_name"),
            "account_label": confirmation.get("account_label"),
            "tool_display_name": confirmation.get("tool_display_name"),
            "action_class": confirmation.get("action_class"),
            "behavior_hints": behavior_hints,
            "arguments_summary": arguments_summary,
            "approval_level": confirmation.get("approval_level"),
            "approval_options": confirmation.get("approval_options") or [],
            "confirmation_token": confirmation.get("confirmation_token"),
            "expires_at": confirmation.get("expires_at"),
        },
        "resume": {
            "tool_id": tool_id,
            "connection_id": connection_id,
            "audit_id": audit_id,
            "arguments": call_kwargs,
            "confirmation_token": confirmation.get("confirmation_token"),
            "approval_audit_id": audit_id,
            "confirmation_token_argument": "confirmation_token",
            "approval_audit_id_argument": "approval_audit_id",
        },
        "provider_response": provider_envelope,
    }
