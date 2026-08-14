"""Runtime helpers for refresh-token backed OAuth credentials.

SecretManager owns storage and synchronization: it mirrors allowlisted assistant
secrets from Orchestra into the local ``Secrets`` context, ``.env``, and
``os.environ``.  This module owns the runtime interpretation of those mirrored
values: provider aliases, access-token/expiry secret names, freshness checks,
and the sandbox helper exposed to actor-written Python.

The split is deliberate and security-relevant. ``get_provider_access_token(...)``
returns the REAL bearer token and is TRUSTED-RUNTIME ONLY (the localhost
provider proxy and first-party managers). The sandbox-facing
``get_oauth_access_token(...)`` never returns a real token: it returns a local
capability handle (the proxy nonce) to use against the localhost proxy base
URLs. Raw provider tokens are kept out of ``os.environ``/``.env`` and the
``Secrets`` context so that neither subprocess nor in-process actor code can read
them and bypass the file-access allowlist; connected-provider REST is reached
only through the proxy, which injects the real token and enforces the allowlist.
"""

import inspect
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class OAuthProviderMetadata:
    """Runtime metadata for a refresh-token backed OAuth provider."""

    canonical_name: str
    aliases: tuple[str, ...]
    access_token_secret: str
    refresh_token_secret: str | None = None
    expiry_secret: str | None = None
    granted_scopes_secret: str | None = None
    docs_label: str = ""

    @property
    def secret_names(self) -> frozenset[str]:
        return frozenset(
            name
            for name in (
                self.access_token_secret,
                self.refresh_token_secret,
                self.expiry_secret,
                self.granted_scopes_secret,
            )
            if name
        )


_OAUTH_PROVIDER_METADATA: dict[str, OAuthProviderMetadata] = {
    "google": OAuthProviderMetadata(
        canonical_name="google",
        aliases=("google", "gmail", "google_workspace", "drive"),
        access_token_secret="GOOGLE_ACCESS_TOKEN",
        refresh_token_secret="GOOGLE_REFRESH_TOKEN",
        expiry_secret="GOOGLE_TOKEN_EXPIRES_AT",
        granted_scopes_secret="GOOGLE_GRANTED_SCOPES",
        docs_label="Google APIs",
    ),
    "microsoft": OAuthProviderMetadata(
        canonical_name="microsoft",
        aliases=("microsoft", "msft", "ms365", "microsoft_365", "graph"),
        access_token_secret="MICROSOFT_ACCESS_TOKEN",
        refresh_token_secret="MICROSOFT_REFRESH_TOKEN",
        expiry_secret="MICROSOFT_TOKEN_EXPIRES_AT",
        granted_scopes_secret="MICROSOFT_GRANTED_SCOPES",
        docs_label="Microsoft Graph",
    ),
}
_OAUTH_PROVIDER_ALIASES: dict[str, str] = {
    alias.strip().lower().replace("-", "_"): metadata.canonical_name
    for metadata in _OAUTH_PROVIDER_METADATA.values()
    for alias in metadata.aliases
}


def _resolve_oauth_provider(provider: str) -> OAuthProviderMetadata:
    if not isinstance(provider, str) or not provider.strip():
        supported = ", ".join(sorted(_OAUTH_PROVIDER_METADATA))
        raise ValueError(
            "A refresh-token OAuth provider name is required. "
            f"Supported providers: {supported}",
        )
    normalized = provider.strip().lower().replace("-", "_")
    canonical = _OAUTH_PROVIDER_ALIASES.get(normalized, normalized)
    metadata = _OAUTH_PROVIDER_METADATA.get(canonical)
    if metadata is None:
        supported = ", ".join(sorted(_OAUTH_PROVIDER_METADATA))
        raise ValueError(
            f"Unknown refresh-token OAuth provider {provider!r}. "
            f"Supported providers: {supported}",
        )
    return metadata


def refresh_token_oauth_secret_names() -> frozenset[str]:
    names: set[str] = set()
    for metadata in _OAUTH_PROVIDER_METADATA.values():
        names.update(metadata.secret_names)
    return frozenset(names)


def refresh_token_oauth_token_names() -> frozenset[str]:
    """Return only the raw access/refresh token secret names (sensitive subset).

    These must never be mirrored to the Secrets context, ``.env`` or
    ``os.environ``; they are held in SecretManager's in-memory OAuth store.
    """
    names: set[str] = set()
    for metadata in _OAUTH_PROVIDER_METADATA.values():
        names.add(metadata.access_token_secret)
        if metadata.refresh_token_secret:
            names.add(metadata.refresh_token_secret)
    return frozenset(names)


def _get_secret_manager():
    from unify.manager_registry import ManagerRegistry

    return ManagerRegistry.get_secret_manager()


def _get_secret_value(secret_manager, name: str) -> str | None:
    getter = getattr(secret_manager, "_get_secret_value", None)
    if callable(getter):
        value = getter(name)
        if isinstance(value, str) and value:
            return value
    value = os.environ.get(name)
    return value if value else None


def _parse_expiry(value: str) -> datetime:
    if value.isdigit():
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _token_expires_within(
    secret_manager,
    metadata: OAuthProviderMetadata,
    min_ttl_seconds: int,
) -> bool:
    if metadata.expiry_secret is None:
        return False
    expiry_value = _get_secret_value(secret_manager, metadata.expiry_secret)
    if not expiry_value:
        return True
    try:
        expiry = _parse_expiry(expiry_value)
    except ValueError:
        return True
    remaining = (expiry - datetime.now(timezone.utc)).total_seconds()
    return remaining <= min_ttl_seconds


def _read_access_token(secret_manager: Any, name: str) -> str | None:
    """Read a real access token, preferring the in-memory OAuth store.

    Raw provider tokens are deliberately kept out of the ``Secrets`` context,
    ``.env`` and ``os.environ`` so sandboxed actor code cannot read them. They
    live only in SecretManager's in-memory OAuth store; fall back to the legacy
    secret lookup for environments/tests that still populate it.
    """
    getter = getattr(secret_manager, "get_oauth_token", None)
    if callable(getter):
        value = getter(name)
        if isinstance(value, str) and value:
            return value
    return _get_secret_value(secret_manager, name)


def get_provider_access_token(provider: str, *, min_ttl_seconds: int = 300) -> str:
    """Return a current REAL OAuth access token for a refresh-token provider.

    TRUSTED-RUNTIME ONLY. This returns the actual bearer token and must never be
    exposed to the ``execute_code`` sandbox. It is used by the localhost
    provider proxy and by first-party managers (e.g. workspace email) that run
    in the trusted parent process. Sandboxed code uses
    :func:`get_oauth_access_token`, which returns a local capability handle.

    If the current token is missing or expires within ``min_ttl_seconds``, an
    assistant-secret sync from Orchestra is forced before returning a token.
    """
    metadata = _resolve_oauth_provider(provider)
    secret_manager = _get_secret_manager()
    token = _read_access_token(secret_manager, metadata.access_token_secret)
    needs_force_sync = token is None or _token_expires_within(
        secret_manager,
        metadata,
        min_ttl_seconds,
    )
    secret_manager.sync_assistant_secrets_if_stale(
        ttl_seconds=60.0,
        force=needs_force_sync,
        reason=f"oauth_access_token:{metadata.canonical_name}",
    )
    token = _read_access_token(secret_manager, metadata.access_token_secret)
    if not token:
        raise ValueError(
            f"No access token is available for refresh-token OAuth provider "
            f"{metadata.canonical_name!r}.",
        )
    if _token_expires_within(secret_manager, metadata, min_ttl_seconds):
        raise ValueError(
            f"The access token for refresh-token OAuth provider "
            f"{metadata.canonical_name!r} is expired or near expiry after sync.",
        )
    return token


def get_provider_access_token_optimistic(provider: str) -> str | None:
    """Return the current access token WITHOUT a pre-emptive expiry gate.

    TRUSTED-RUNTIME ONLY. Unlike :func:`get_provider_access_token`, this does not
    refuse a token whose stored ``*_TOKEN_EXPIRES_AT`` looks stale/missing: it
    trusts the provider to reject a genuinely-expired token (the proxy then
    forces a refresh and retries once). This avoids blocking valid tokens on
    stale expiry metadata. Performs a debounced sync (forced only when no token
    is cached). Returns None if no token is available.
    """
    metadata = _resolve_oauth_provider(provider)
    secret_manager = _get_secret_manager()
    token = _read_access_token(secret_manager, metadata.access_token_secret)
    secret_manager.sync_assistant_secrets_if_stale(
        ttl_seconds=60.0,
        force=token is None,
        reason=f"oauth_optimistic:{metadata.canonical_name}",
    )
    return _read_access_token(secret_manager, metadata.access_token_secret)


def refresh_provider_access_token(provider: str) -> str | None:
    """Force a secret sync from Orchestra and return the freshest token, or None.

    TRUSTED-RUNTIME ONLY. Used by the proxy after the provider rejects a token
    with 401. This pulls whatever the platform refresh job has persisted to
    Orchestra; it does not itself call the provider's token endpoint, so it only
    recovers when Orchestra already holds a newer token (e.g. the 30-minute
    refresh cron has run). If it still returns a stale token, the proxy surfaces
    a clean "reconnect account" 401.
    """
    metadata = _resolve_oauth_provider(provider)
    secret_manager = _get_secret_manager()
    secret_manager.sync_assistant_secrets_if_stale(
        ttl_seconds=0.0,
        force=True,
        reason=f"oauth_refresh_on_401:{metadata.canonical_name}",
    )
    return _read_access_token(secret_manager, metadata.access_token_secret)


def get_oauth_access_token(provider: str, *, min_ttl_seconds: int = 300) -> str:
    """
    Authorize provider REST calls from ``execute_code`` via the local proxy.

    This does NOT return a raw provider access token. It returns a local
    capability handle (the workspace proxy nonce) to place in the
    ``Authorization: Bearer ...`` header. You must ALSO point your base URL at
    the local proxy so the request is authorized and policy-enforced:

    - Microsoft Graph: base URL ``os.environ["MICROSOFT_GRAPH_BASE"]`` (drop-in
      for ``https://graph.microsoft.com/v1.0``).
    - Google APIs: ``os.environ["GOOGLE_DRIVE_BASE"]`` (drop-in for
      ``https://www.googleapis.com/drive/v3``) or ``GOOGLE_API_BASE`` for other
      Google services.

    The proxy swaps this handle for the real upstream token and enforces the
    per-assistant file-access allowlist. Calling the provider hosts directly
    (``graph.microsoft.com`` / ``www.googleapis.com``) with this handle will
    fail: the sandbox holds no real token by design.

    The proxy gives you the FULL provider REST API (list, search, read,
    rename, move, upload, delete, ``$batch``, ...) but enforces the
    file-access allowlist: files and folders the user has not permitted are
    masked — absent from listings/search and not-found on direct access, and
    writes into a non-permitted location are rejected. Treat masked items as
    nonexistent. Provider SDKs work too — point the client's base/endpoint at
    the proxy (e.g. msgraph's ``request_adapter.base_url``, googleapiclient's
    ``client_options.api_endpoint``).

    Parameters
    ----------
    provider:
        Provider name or alias. Built-in aliases include ``"microsoft"``,
        ``"graph"``, ``"google"``, ``"gmail"``, and ``"drive"``.
    min_ttl_seconds:
        Accepted for signature compatibility; token freshness is handled by the
        proxy on each upstream call.

    Examples
    --------
    Multiple providers can be used in one sandbox; request each explicitly::

        microsoft_token = get_oauth_access_token("microsoft")
        google_token = get_oauth_access_token("google")

    A raw HTTP call through the proxy::

        import os, httpx
        token = get_oauth_access_token("microsoft")
        base = os.environ["MICROSOFT_GRAPH_BASE"]  # ~ https://graph.microsoft.com/v1.0
        resp = httpx.get(
            f"{base}/me/drive/root/children",
            headers={"Authorization": f"Bearer {token}"},
        )

    Scope checks before calling
    ---------------------------
    When the provider has a granted-scopes secret (``GOOGLE_GRANTED_SCOPES``
    / ``MICROSOFT_GRANTED_SCOPES``, space-separated raw OAuth scope strings,
    not feature names), check the scope the specific API call requires
    (from the provider's official docs/SDK) against it before calling:

    - Google scopes are full URLs, e.g.
      ``https://www.googleapis.com/auth/gmail.send``.
    - Microsoft docs list short names (``Sites.Read.All``); the secret stores
      them prefixed — search for
      ``https://graph.microsoft.com/Sites.Read.All``. Only ``offline_access``
      is stored bare.
    - Secret missing entirely → proceed normally (expected for
      admin-consented Microsoft enterprise tenants and self-managed tokens).
      Scope present → proceed. Scope absent → do not attempt the call; tell
      the user to reconnect the service from the Console Integrations tab
      with the missing access.

    Reuse in stored functions
    -------------------------
    Reusable OAuth integrations should call
    ``get_oauth_access_token(provider)`` at runtime, each run; never store or
    capture a concrete handle/token value inside a function implementation.

    Anti-patterns
    -------------
    - Do not call ``graph.microsoft.com`` / ``www.googleapis.com`` directly; use
      the proxy base URLs above.
    - Do not print, log, return, or store this handle.
    """
    _resolve_oauth_provider(provider)
    from unify.provider_proxy.proxy import ensure_proxy_running

    return ensure_proxy_running().nonce


def has_workspace_oauth_connection() -> bool:
    """Cheapest presence check: is any workspace OAuth provider connected?

    Used to gate the prompt's ``OAuth Access Token Helper`` section on
    assistant config. Reads SecretManager's in-memory OAuth store (populated
    by the forced sync at SecretManager construction) with an
    ``os.environ`` fallback for legacy/test environments — it never forces
    a network sync, so it is safe at prompt-build time. Best-effort: any
    failure reports no connection rather than raising.
    """
    try:
        secret_manager = _get_secret_manager()
    except Exception:
        secret_manager = None
    for metadata in _OAUTH_PROVIDER_METADATA.values():
        for name in (metadata.access_token_secret, metadata.refresh_token_secret):
            if not name:
                continue
            try:
                if secret_manager is not None and _read_access_token(
                    secret_manager,
                    name,
                ):
                    return True
            except Exception:
                pass
            if os.environ.get(name):
                return True
    return False


def get_refresh_token_oauth_env_overlay() -> dict[str, str]:
    """Return the proxy base URLs + nonce to overlay into subprocess sandboxes.

    Venv and persistent shell sessions may outlive the parent process's last
    environment update, so each execution overlays the current workspace proxy
    endpoints. The sandbox is never given raw OAuth tokens: connected-provider
    REST (files and non-file) is reached through the localhost proxy, which
    injects the real token and enforces the file-access allowlist.
    """
    from unify.provider_proxy.proxy import ensure_proxy_running

    return dict(ensure_proxy_running().sandbox_env())


def connected_oauth_providers() -> list[str]:
    """Canonical names of providers with a live workspace connection.

    Presence of the provider's refresh (or access) token in the runtime
    secret store is the connection: it is what the proxy will exchange for
    real access. No network call is made, so this is safe to evaluate at
    prompt-build time.

    The lookup must go through :func:`_read_access_token`, which consults the
    in-memory OAuth store first. Raw tokens are deliberately withheld from the
    ``Secrets`` context, ``.env`` and ``os.environ``, so a plain secret lookup
    sees a correctly-stored token as absent and reports every provider
    disconnected — inverting the check into one that passes only when the
    sandbox-isolation invariant has been broken.
    """
    secret_manager = _get_secret_manager()
    connected: list[str] = []
    for name, metadata in sorted(_OAUTH_PROVIDER_METADATA.items()):
        token_names = [metadata.refresh_token_secret, metadata.access_token_secret]
        if any(
            _read_access_token(secret_manager, token_name)
            for token_name in token_names
            if token_name
        ):
            connected.append(name)
    return connected


def get_oauth_prompt_context() -> str:
    """Return the actor-facing OAuth helper stub.

    Deliberately a contract stub: the full procedure (proxy base URLs,
    provider aliases, scope checks, examples, anti-patterns) lives in
    ``get_oauth_access_token.__doc__`` and renders in-sandbox via
    ``help(get_oauth_access_token)`` — do not duplicate it here.
    """
    signature = (
        f"def {get_oauth_access_token.__name__}"
        f"{inspect.signature(get_oauth_access_token)}"
    )
    connected = connected_oauth_providers()
    if connected:
        status = (
            "Currently connected workspace providers: "
            + ", ".join(f"`{name}`" for name in connected)
            + ". These work right now — do not ask the user to connect them "
            "again."
        )
    else:
        status = (
            "No workspace provider is currently connected. "
            "`get_oauth_access_token(...)` will fail until the user connects "
            "a Google or Microsoft account from the Workspace dialog in "
            "Console; say so instead of attempting workarounds."
        )
    return (
        "### OAuth Access Token Helper: `get_oauth_access_token(...)`\n\n"
        "`get_oauth_access_token(provider)` is a sandbox global (not a "
        "JSON tool) for connected-account provider REST:\n\n"
        f"{status}\n\n"
        f"```python\n{signature}\n```\n\n"
        "It returns a short-lived **local capability handle** for the "
        "workspace proxy — never a raw provider token. Do not print, log, "
        "return, or store this handle. Send it as the Bearer token "
        "against the proxy base URLs in `os.environ` "
        "(`MICROSOFT_GRAPH_BASE`, `GOOGLE_DRIVE_BASE`, `GOOGLE_API_BASE`), "
        "never the real provider hosts, which hold no valid token by "
        "design. The proxy exposes the full provider REST surface but "
        "masks files outside the user's allowlist — treat masked items "
        "as nonexistent. Full procedure, provider aliases, scope checks, "
        "and examples: `help(get_oauth_access_token)`."
    )
