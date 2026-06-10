"""Unity-side integration operations backed by Unify SDK helpers.

This module is intentionally not a raw HTTP client and it intentionally does not
wrap the helpers in a stateful client object. Unity calls these small functions,
the functions call Unify, and Unify owns the Orchestra route mapping, base URL
normalization, auth headers, and payload shape. The only shared behavior here is
scope cleanup plus consistent "Unify is unavailable/request failed" envelopes.
"""

from __future__ import annotations

import os
from typing import Any, Optional

import unify as unify_integrations


def _call_unify(
    helper_name: str,
    *args: Any,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    resolved_api_key = api_key or os.getenv("UNIFY_KEY")
    if not resolved_api_key:
        return {
            "status": "unavailable",
            "error": {
                "code": "unify_unavailable",
                "message": "UNIFY_KEY is required for provider integrations.",
            },
        }
    resolved_base_url = (base_url or os.getenv("ORCHESTRA_URL") or "").strip() or None
    try:
        helper = getattr(unify_integrations, helper_name)
        return helper(
            *args,
            api_key=resolved_api_key,
            base_url=resolved_base_url,
            **kwargs,
        )
    except Exception as exc:
        return {
            "status": "error",
            "error": {
                "code": "unify_integration_request_failed",
                "message": str(exc),
                "helper": helper_name,
            },
        }


def list_connections(
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **scope: Any,
) -> Any:
    return _call_unify(
        "list_integration_connections",
        api_key=api_key,
        base_url=base_url,
        **_clean_scope(scope),
    )


def search_apps(
    query: str,
    *,
    limit: int = 10,
    offset: int = 0,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **scope: Any,
) -> Any:
    return _call_unify(
        "search_integration_apps",
        query,
        limit=limit,
        offset=offset,
        api_key=api_key,
        base_url=base_url,
        **_clean_scope(scope),
    )


def get_tools(
    *,
    limit: int = 100,
    offset: int = 0,
    canonical_app_slug: Optional[str] = None,
    activation_state: Optional[str] = None,
    include_unconnected: bool = False,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **scope: Any,
) -> Any:
    return _call_unify(
        "get_integration_tools",
        limit=limit,
        offset=offset,
        canonical_app_slug=canonical_app_slug,
        activation_state=activation_state,
        include_unconnected=include_unconnected,
        api_key=api_key,
        base_url=base_url,
        **_clean_scope(scope),
    )


def search_tools(
    query: str,
    *,
    limit: int = 20,
    offset: int = 0,
    include_unconnected: bool = False,
    canonical_app_slug: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **scope: Any,
) -> Any:
    return _call_unify(
        "search_integration_tools",
        query,
        limit=limit,
        offset=offset,
        include_unconnected=include_unconnected,
        canonical_app_slug=canonical_app_slug,
        api_key=api_key,
        base_url=base_url,
        **_clean_scope(scope),
    )


def get_tool_schema(
    tool_id: str,
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **scope: Any,
) -> Any:
    return _call_unify(
        "get_integration_tool_schema",
        tool_id,
        api_key=api_key,
        base_url=base_url,
        **_clean_scope(scope),
    )


def run_tool(
    tool_id: str,
    arguments: dict[str, Any],
    *,
    confirmation_token: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    **scope: Any,
) -> Any:
    return _call_unify(
        "run_integration_tool",
        tool_id,
        arguments,
        confirmation_token=confirmation_token,
        api_key=api_key,
        base_url=base_url,
        **_clean_scope(scope),
    )


def test_connection(
    connection_id: str,
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Any:
    return _call_unify(
        "test_integration_connection",
        connection_id,
        api_key=api_key,
        base_url=base_url,
    )


def _clean_scope(scope: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in scope.items() if value is not None}
