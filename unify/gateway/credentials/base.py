"""``CredentialStore`` protocol for gateway transport credentials."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class CredentialNotFoundError(KeyError):
    """Raised when a requested credential name is not defined in this backend."""


@runtime_checkable
class CredentialStore(Protocol):
    """Pluggable credential store for gateway transports.

    The contract is intentionally tiny: get a credential, set a
    credential, list visible credential names. Backends are expected
    to fail loud on unknown credentials so misconfigurations surface
    during startup rather than as runtime ``None``-defaults silently
    sending unauthenticated requests.

    This is **not** the assistant's secret manager
    (``unify.secret_manager``). This holds operator infrastructure
    credentials for the gateway processes (e.g.
    ``TWILIO_ACCOUNT_SID``); the assistant never sees them.

    Backends:

    * ``EnvCredentialStore`` -- ships in Phase A; reads from process
      environment variables. The default for self-hosted Unity.
    * ``GcpCredentialStore`` -- planned; wraps
      ``google.cloud.secret_manager`` for the hosted code path.
    """

    def get(self, name: str) -> str:
        """Return the credential value bound to ``name``.

        Raises ``CredentialNotFoundError`` if the credential is not
        defined in this backend.
        """

    def get_optional(self, name: str, default: str = "") -> str:
        """Return the credential value bound to ``name`` or ``default``.

        Provided as a convenience for call sites that need the
        "missing-credential-is-not-fatal" semantics without writing
        their own try/except wrappers.
        """

    def set(self, name: str, value: str) -> None:
        """Bind ``value`` to ``name`` in this backend.

        Implementations may treat this as a no-op for read-only
        backends; backends that *do* implement it must persist the
        new value so subsequent ``get`` calls return it.
        """

    def list_names(self) -> list[str]:
        """Return the sorted list of credential names visible to this backend.

        Backends that cannot enumerate may return an empty list;
        callers must not treat the empty list as authoritative.
        """


__all__ = ["CredentialNotFoundError", "CredentialStore"]
