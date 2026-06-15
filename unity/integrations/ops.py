"""Unity-side integration operations backed by Unify SDK helpers.

This module is intentionally not a raw HTTP client and it intentionally does not
wrap the helpers in a stateful client object. Unity calls these small functions,
the functions call Unify, and Unify owns the Orchestra route mapping, base URL
normalization, auth headers, and payload shape. Auth follows the same contract as
logging APIs: session bootstrap exports ``UNIFY_KEY``/``ORCHESTRA_URL`` and the
Unify SDK resolves credentials only when it builds the bearer header.

The only shared behavior here is scope cleanup plus consistent request-failed
envelopes for non-auth errors.
"""

from __future__ import annotations

from typing import Any, Optional

import unify


def _request_failed(helper_name: str, exc: Exception) -> dict[str, Any]:
    return {
        "status": "error",
        "error": {
            "code": "unify_integration_request_failed",
            "message": str(exc),
            "helper": helper_name,
        },
    }


def list_connections(**scope: Any) -> Any:
    try:
        return unify.list_integration_connections(**_clean_scope(scope))
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("list_integration_connections", exc)


def search_apps(
    query: Optional[str] = None,
    *,
    limit: int = 10,
    offset: int = 0,
    **scope: Any,
) -> Any:
    try:
        return unify.search_integration_apps(
            query,
            limit=limit,
            offset=offset,
            **_clean_scope(scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("search_integration_apps", exc)


def get_tools(
    *,
    limit: int = 100,
    offset: int = 0,
    canonical_app_slug: Optional[str] = None,
    activation_state: Optional[str] = None,
    include_unconnected: bool = False,
    include_schema: bool = False,
    **scope: Any,
) -> Any:
    try:
        return unify.get_integration_tools(
            limit=limit,
            offset=offset,
            canonical_app_slug=canonical_app_slug,
            activation_state=activation_state,
            include_unconnected=include_unconnected,
            include_schema=include_schema,
            **_clean_scope(scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("get_integration_tools", exc)


def search_tools(
    query: Optional[str] = None,
    *,
    limit: int = 20,
    offset: int = 0,
    include_unconnected: bool = False,
    canonical_app_slug: Optional[str] = None,
    include_schema: bool = False,
    **scope: Any,
) -> Any:
    try:
        return unify.search_integration_tools(
            query,
            limit=limit,
            offset=offset,
            include_unconnected=include_unconnected,
            canonical_app_slug=canonical_app_slug,
            include_schema=include_schema,
            **_clean_scope(scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("search_integration_tools", exc)


def get_tool_schema(
    tool_id: str,
    **scope: Any,
) -> Any:
    try:
        return unify.get_integration_tool_schema(tool_id, **_clean_scope(scope))
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("get_integration_tool_schema", exc)


def run_tool(
    tool_id: str,
    arguments: dict[str, Any],
    *,
    confirmation_token: Optional[str] = None,
    approval_audit_id: Optional[int] = None,
    **scope: Any,
) -> Any:
    try:
        return unify.run_integration_tool(
            tool_id,
            arguments,
            confirmation_token=confirmation_token,
            approval_audit_id=approval_audit_id,
            **_clean_scope(scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("run_integration_tool", exc)


def get_tool_policy(connection_id: str, **scope: Any) -> Any:
    try:
        return unify.get_integration_tool_policy(
            connection_id,
            **_clean_scope(scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("get_integration_tool_policy", exc)


def patch_tool_policy(
    connection_id: str,
    *,
    tool_policies: Optional[dict[str, str]] = None,
    bulk_approval_level: Optional[str] = None,
    action_classes: Optional[list[str]] = None,
    reset_to_defaults: bool = False,
    **scope: Any,
) -> Any:
    try:
        return unify.patch_integration_tool_policy(
            connection_id,
            tool_policies=tool_policies,
            bulk_approval_level=bulk_approval_level,
            action_classes=action_classes,
            reset_to_defaults=reset_to_defaults,
            **_clean_scope(scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("patch_integration_tool_policy", exc)


def approve_tool_execution(
    audit_id: int,
    *,
    scope: str = "once",
    persist_policy: bool = False,
    approval_level: str = "auto",
    actor_id: Optional[str] = None,
    expires_at: Optional[str] = None,
    **owner_scope: Any,
) -> Any:
    try:
        return unify.approve_integration_tool_execution(
            audit_id,
            scope=scope,
            persist_policy=persist_policy,
            approval_level=approval_level,
            actor_id=actor_id,
            expires_at=expires_at,
            **_clean_scope(owner_scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("approve_integration_tool_execution", exc)


def deny_tool_execution(
    audit_id: int,
    *,
    scope: str = "once",
    persist_policy: bool = False,
    approval_level: str = "forbidden",
    actor_id: Optional[str] = None,
    reason: Optional[str] = None,
    **owner_scope: Any,
) -> Any:
    try:
        return unify.deny_integration_tool_execution(
            audit_id,
            scope=scope,
            persist_policy=persist_policy,
            approval_level=approval_level,
            actor_id=actor_id,
            reason=reason,
            **_clean_scope(owner_scope),
        )
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("deny_integration_tool_execution", exc)


def test_connection(connection_id: str) -> Any:
    try:
        return unify.test_integration_connection(connection_id)
    except KeyError:
        raise
    except Exception as exc:
        return _request_failed("test_integration_connection", exc)


def _clean_scope(scope: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in scope.items() if value is not None}
