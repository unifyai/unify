"""Microsoft Graph SDK helpers for gateway channels.

Promoted from ``communication.helpers`` for the channel migrations
that touch Microsoft 365 mailboxes (outlook) and Teams (chats,
channels, meetings). Functionally equivalent to the originals; only
the credential resolution path changes (env reads -> CredentialStore).

Two distinct authentication modes are supported, matching the
production setup:

* **Tenant-level admin (app-only)** -- ``get_admin_graph_client``
  uses ``MS365_ADMIN_TENANT_ID`` / ``MS365_ADMIN_CLIENT_ID`` /
  ``MS365_ADMIN_CLIENT_SECRET`` via the Azure SDK's
  ``ClientSecretCredential``. Used for provisioning / teardown
  operations and for mailbox operations on accounts that don't have
  per-user OAuth tokens.

* **Per-user BYOD (delegated)** --
  ``graph_client_from_assistant`` wraps the assistant's stored
  ``MICROSOFT_ACCESS_TOKEN`` secret in a ``TokenCredentialFromSecret``
  shim and uses it directly. Falls back to admin credentials when no
  per-user token is available.

``get_graph_client`` is the convenience wrapper that performs the
Orchestra lookup + dispatch.
"""

from __future__ import annotations

import logging
from typing import Any

from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import ClientSecretCredential
from fastapi import HTTPException
from msgraph import GraphServiceClient

from droid.gateway.common.orchestra import lookup_assistant, lookup_assistant_by_id
from droid.gateway.credentials import CredentialStore, EnvCredentialStore

logger = logging.getLogger("droid.gateway.common.graph")

GRAPH_SCOPES: list[str] = ["https://graph.microsoft.com/.default"]


class TokenCredentialFromSecret(TokenCredential):
    """Wraps a stored access token for use with the Microsoft Graph SDK.

    The token's actual expiry is irrelevant -- the hosted code path
    runs a scheduled job that refreshes per-user tokens before
    they expire, so by the time a Graph call uses the stored token
    it is always within its validity window.
    """

    def __init__(self, access_token: str) -> None:
        self._token = access_token

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        # Token freshness is the scheduled refresher's responsibility;
        # we just hand the stored value through with a far-future expiry
        # so the SDK doesn't try to refresh it itself.
        return AccessToken(self._token, expires_on=9999999999)


def get_admin_graph_client(
    credentials: CredentialStore | None = None,
) -> GraphServiceClient:
    """Build a Graph client using tenant-level client credentials.

    Used for provisioning operations (delete MS365 users, manage
    Teams subscriptions on app-only accounts) and for mailbox
    operations on accounts that don't have per-user OAuth tokens.

    Raises ``HTTPException(500)`` with a clear message naming all
    three required credentials when any is missing.
    """
    credentials = credentials or EnvCredentialStore()
    tenant_id = credentials.get_optional("MS365_ADMIN_TENANT_ID", "")
    client_id = credentials.get_optional("MS365_ADMIN_CLIENT_ID", "")
    client_secret = credentials.get_optional("MS365_ADMIN_CLIENT_SECRET", "")
    if not all([tenant_id, client_id, client_secret]):
        raise HTTPException(
            status_code=500,
            detail=(
                "MS365 admin credentials not configured "
                "(MS365_ADMIN_TENANT_ID, MS365_ADMIN_CLIENT_ID, "
                "MS365_ADMIN_CLIENT_SECRET)"
            ),
        )
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    return GraphServiceClient(credentials=credential, scopes=GRAPH_SCOPES)


def graph_client_from_assistant(
    assistant: dict,
    user_email: str,
    credentials: CredentialStore | None = None,
) -> GraphServiceClient:
    """Build a Graph client from an already-fetched assistant record.

    Uses per-user OAuth token when available, otherwise falls back to
    tenant-level admin credentials. Centralises the "BYOD takes
    precedence over admin" rule that all Outlook / Teams flows rely
    on.
    """
    credentials = credentials or EnvCredentialStore()
    access_token = assistant.get("secrets", {}).get("MICROSOFT_ACCESS_TOKEN")
    if access_token:
        return GraphServiceClient(
            credentials=TokenCredentialFromSecret(access_token),
            scopes=GRAPH_SCOPES,
        )
    logger.info(
        "no per-user OAuth token for %s; using admin credentials",
        user_email,
    )
    return get_admin_graph_client(credentials)


async def get_graph_client(
    user_email: str | None = None,
    credentials: CredentialStore | None = None,
    *,
    assistant_id: str | int | None = None,
) -> GraphServiceClient:
    """Look up the assistant and build a Graph client.

    Resolves the assistant by ``assistant_id`` when provided (the workspace-file
    picker's identity, which works for Coordinators too), otherwise by
    ``user_email``. Falls back to tenant-level admin credentials when the
    Orchestra lookup fails (e.g. during initial provisioning before the
    AssistantContact row exists, or transient Orchestra unavailability for
    read-only operations that admin credentials can still satisfy).
    """
    credentials = credentials or EnvCredentialStore()
    identity = f"id={assistant_id}" if assistant_id else user_email
    try:
        if assistant_id:
            assistant = await lookup_assistant_by_id(assistant_id, credentials)
        else:
            assistant = await lookup_assistant(user_email, credentials)
    except Exception:
        logger.warning(
            "failed to look up assistant for %s; falling back to admin Graph client",
            identity,
        )
        return get_admin_graph_client(credentials)
    return graph_client_from_assistant(assistant, identity or "", credentials)


def _get_user_node(graph: Any, sender: str, assistant: dict) -> Any:
    """Return the Graph request builder targeting the correct mailbox.

    Per-user OAuth tokens are scoped to a single mailbox, so addressing
    must use ``/me``. App-only admin credentials aren't scoped to a
    mailbox, so addressing must use ``/users/{email}``. The same
    dispatch logic applies to every Outlook / Teams call site that
    targets a specific user's mailbox.
    """
    has_user_token = bool(assistant.get("secrets", {}).get("MICROSOFT_ACCESS_TOKEN"))
    if has_user_token:
        return graph.me
    return graph.users.by_user_id(sender)


__all__ = [
    "GRAPH_SCOPES",
    "TokenCredentialFromSecret",
    "_get_user_node",
    "get_admin_graph_client",
    "get_graph_client",
    "graph_client_from_assistant",
    "lookup_assistant",
]
