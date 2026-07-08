from __future__ import annotations

from unify.integrations.provider_resolution import (
    logical_app_key,
    resolve_public_catalog_apps,
    resolve_public_catalog_tools,
    tool_dedup_key,
)


def _tool_row(
    *,
    backend_id: str,
    app_slug: str,
    tool_name: str,
) -> dict:
    return {
        "name": f"primitives.integrations.{app_slug}.{tool_name}",
        "metadata": {
            "integration": {
                "backend_id": backend_id,
                "app_slug": app_slug,
                "provider_tool_id": tool_name,
            },
        },
    }


def test_resolve_public_catalog_tools_prefers_composio() -> None:
    tools = [
        _tool_row(backend_id="pipedream", app_slug="slack", tool_name="send_message"),
        _tool_row(backend_id="composio", app_slug="slack", tool_name="send_message"),
    ]
    resolved = resolve_public_catalog_tools(tools)
    assert len(resolved) == 1
    assert resolved[0]["metadata"]["integration"]["backend_id"] == "composio"


def test_tool_dedup_key_normalizes_microsoft_outlook_alias() -> None:
    app_key, tool_name = tool_dedup_key(
        canonical_app_slug="microsoft_outlook",
        display_name="Microsoft Outlook",
        tool_name="Send Email",
    )
    assert app_key == logical_app_key(
        canonical_app_slug="microsoft_outlook",
        display_name="Microsoft Outlook",
    )
    assert tool_name == "send_email"


def test_resolve_public_catalog_apps_prefers_composio() -> None:
    apps = [
        {
            "backend_id": "pipedream",
            "canonical_app_slug": "github",
            "display_name": "GitHub",
        },
        {
            "backend_id": "composio",
            "canonical_app_slug": "github",
            "display_name": "GitHub",
        },
    ]
    resolved = resolve_public_catalog_apps(apps)
    assert len(resolved) == 1
    assert resolved[0]["backend_id"] == "composio"
