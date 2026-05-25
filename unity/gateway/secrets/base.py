"""SecretManager protocol for gateway transport credentials."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class SecretNotFoundError(KeyError):
    """Raised when a requested secret name is not defined in this backend."""


@runtime_checkable
class SecretManager(Protocol):
    """Pluggable credential store for gateway transports.

    The contract is intentionally tiny: get a secret, set a secret, list
    visible secret names. Backends are expected to fail loud on unknown
    secrets so misconfigurations surface during startup rather than as
    runtime ``None``-defaults silently sending unauthenticated requests.

    Backends:

    * ``EnvSecretManager`` -- ships in Phase A; reads from process
      environment variables. The default for self-hosted Unity.
    * ``GcpSecretManager`` -- planned; wraps
      ``google.cloud.secret_manager`` for the hosted code path.
    """

    def get(self, name: str) -> str:
        """Return the secret value bound to ``name``.

        Raises ``SecretNotFoundError`` if the secret is not defined in
        this backend.
        """

    def get_optional(self, name: str, default: str = "") -> str:
        """Return the secret value bound to ``name`` or ``default``.

        Provided as a convenience for call sites that need the
        "missing-secret-is-not-fatal" semantics without writing their own
        try/except wrappers.
        """

    def set(self, name: str, value: str) -> None:
        """Bind ``value`` to ``name`` in this backend.

        Implementations may treat this as a no-op for read-only backends;
        backends that *do* implement it must persist the new value so
        subsequent ``get`` calls return it.
        """

    def list_names(self) -> list[str]:
        """Return the sorted list of secret names visible to this backend.

        Backends that cannot enumerate may return an empty list; callers
        must not treat the empty list as authoritative.
        """


__all__ = ["SecretManager", "SecretNotFoundError"]
