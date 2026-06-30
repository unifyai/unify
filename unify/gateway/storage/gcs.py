"""Google Cloud Storage implementation of the gateway ``Storage`` protocol.

Stub. The hosted code path's GCS usage currently lives inline in the
private ``communication`` repository; this module is the seam where it
will move when the Phase B channel migration brings those call sites into
``unify.gateway``. See ``unify/gateway/PHASES.md``.
"""

from __future__ import annotations

from unify.gateway.storage.base import Storage, StorageObject


class GcsStorage(Storage):
    """Google Cloud Storage backend (not yet implemented)."""

    def __init__(self, bucket: str) -> None:
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        return self._bucket

    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> StorageObject:
        raise NotImplementedError(
            "GcsStorage is a Phase B deliverable. See unify/gateway/PHASES.md.",
        )

    async def read_bytes(self, key: str) -> bytes:
        raise NotImplementedError(
            "GcsStorage is a Phase B deliverable. See unify/gateway/PHASES.md.",
        )

    async def exists(self, key: str) -> bool:
        raise NotImplementedError(
            "GcsStorage is a Phase B deliverable. See unify/gateway/PHASES.md.",
        )

    async def list_keys(self, prefix: str = "") -> list[str]:
        raise NotImplementedError(
            "GcsStorage is a Phase B deliverable. See unify/gateway/PHASES.md.",
        )

    async def signed_url(
        self,
        key: str,
        *,
        expires_seconds: int = 900,
    ) -> str:
        raise NotImplementedError(
            "GcsStorage is a Phase B deliverable. See unify/gateway/PHASES.md.",
        )


__all__ = ["GcsStorage"]
