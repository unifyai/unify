"""Environment-variable-backed ``SecretManager`` for self-hosted Unity.

Secrets are looked up case-sensitively in ``os.environ``. By default the
backend exposes every variable; callers can restrict it to a prefix
(e.g. ``UNITY_``) to keep ``list_names`` from leaking unrelated process
state.
"""

from __future__ import annotations

import os

from unity.gateway.secrets.base import SecretManager, SecretNotFoundError


class EnvSecretManager(SecretManager):
    """Read secrets from process environment variables."""

    def __init__(self, *, prefix: str = "") -> None:
        self._prefix = prefix

    @property
    def prefix(self) -> str:
        return self._prefix

    def _check_visible(self, name: str) -> None:
        if self._prefix and not name.startswith(self._prefix):
            raise SecretNotFoundError(
                f"secret {name!r} is outside the configured prefix {self._prefix!r}",
            )

    def get(self, name: str) -> str:
        self._check_visible(name)
        value = os.environ.get(name)
        if value is None:
            raise SecretNotFoundError(name)
        return value

    def get_optional(self, name: str, default: str = "") -> str:
        try:
            return self.get(name)
        except SecretNotFoundError:
            return default

    def set(self, name: str, value: str) -> None:
        self._check_visible(name)
        os.environ[name] = value

    def list_names(self) -> list[str]:
        if not self._prefix:
            return sorted(os.environ.keys())
        return sorted(k for k in os.environ if k.startswith(self._prefix))


__all__ = ["EnvSecretManager"]
