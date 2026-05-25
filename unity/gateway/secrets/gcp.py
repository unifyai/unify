"""Google Cloud Secret Manager implementation of ``SecretManager``.

Stub. The hosted code path's Secret Manager usage currently lives inline
in the private ``communication`` repository; this module is the seam
where it will move when the Phase B channel migration brings those call
sites into ``unity.gateway``. See ``unity/gateway/PHASES.md``.
"""

from __future__ import annotations

from unity.gateway.secrets.base import SecretManager


class GcpSecretManager(SecretManager):
    """Google Cloud Secret Manager backend (not yet implemented)."""

    def __init__(self, project_id: str) -> None:
        self._project_id = project_id

    @property
    def project_id(self) -> str:
        return self._project_id

    def get(self, name: str) -> str:
        raise NotImplementedError(
            "GcpSecretManager is a Phase B deliverable. See unity/gateway/PHASES.md.",
        )

    def get_optional(self, name: str, default: str = "") -> str:
        raise NotImplementedError(
            "GcpSecretManager is a Phase B deliverable. See unity/gateway/PHASES.md.",
        )

    def set(self, name: str, value: str) -> None:
        raise NotImplementedError(
            "GcpSecretManager is a Phase B deliverable. See unity/gateway/PHASES.md.",
        )

    def list_names(self) -> list[str]:
        raise NotImplementedError(
            "GcpSecretManager is a Phase B deliverable. See unity/gateway/PHASES.md.",
        )


__all__ = ["GcpSecretManager"]
