"""Runtime activation backends for gateway adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import httpx


@dataclass(frozen=True)
class RuntimeActivation:
    """Result returned by a runtime activation backend."""

    activated: bool
    detail: str = ""


@runtime_checkable
class RuntimeActivator(Protocol):
    """Ensure the assistant runtime is ready to receive gateway envelopes."""

    async def activate(
        self,
        assistant_id: str,
        *,
        reason: str,
        medium: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeActivation:
        """Activate ``assistant_id`` for a specific delivery reason."""


class LocalRuntimeActivator:
    """Runtime activator for already-running local assistants."""

    async def activate(
        self,
        assistant_id: str,
        *,
        reason: str,
        medium: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeActivation:
        del assistant_id, reason, medium, metadata
        return RuntimeActivation(
            activated=False,
            detail="local-runtime-already-running",
        )


class HostedRuntimeActivator:
    """Runtime activator that delegates startup to a hosted infra endpoint."""

    def __init__(
        self,
        *,
        base_url: str,
        admin_key: str,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._admin_key = admin_key
        self._timeout = timeout

    async def activate(
        self,
        assistant_id: str,
        *,
        reason: str,
        medium: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeActivation:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._base_url}/infra/assistant/{assistant_id}/activate",
                headers={"Authorization": f"Bearer {self._admin_key}"},
                json={
                    "reason": reason,
                    "medium": medium,
                    "metadata": metadata or {},
                },
            )
        response.raise_for_status()
        return RuntimeActivation(activated=True, detail=response.text)


__all__ = [
    "HostedRuntimeActivator",
    "LocalRuntimeActivator",
    "RuntimeActivation",
    "RuntimeActivator",
]
