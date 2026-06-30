"""Storage protocol for gateway attachments and artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


class StorageError(Exception):
    """Raised by a storage backend when an operation cannot complete."""


@dataclass(frozen=True)
class StorageObject:
    """Lightweight descriptor returned by ``Storage`` reads and listings."""

    key: str
    size_bytes: int
    content_type: str = "application/octet-stream"


@runtime_checkable
class Storage(Protocol):
    """Pluggable blob store for gateway attachments and artifacts.

    The contract is intentionally minimal: write, read, exists, list, and
    a signed-URL accessor for callers that want to hand a download link
    to a third party (Twilio, a browser, etc.) without proxying bytes.

    Backends:

    * ``LocalDiskStorage`` -- ships in Phase A; stores objects on the
      local filesystem under a base directory.
    * ``GcsStorage`` -- planned; wraps ``google.cloud.storage`` for the
      hosted code path. Lands when the first call site needs it during
      the Phase B channel migration.
    """

    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> StorageObject:
        """Persist ``data`` under ``key``, overwriting any existing value."""

    async def read_bytes(self, key: str) -> bytes:
        """Return the raw bytes stored under ``key``.

        Raises ``StorageError`` if the object does not exist or the
        backend cannot serve the read.
        """

    async def exists(self, key: str) -> bool:
        """Return whether an object is stored under ``key``."""

    async def list_keys(self, prefix: str = "") -> list[str]:
        """List object keys matching ``prefix`` (empty -> all keys)."""

    async def signed_url(
        self,
        key: str,
        *,
        expires_seconds: int = 900,
    ) -> str:
        """Return a time-limited URL granting read access to ``key``.

        Self-hosted backends without a public URL surface may return a
        local-only URL (e.g. ``file://...`` or ``http://localhost/...``);
        cloud backends return a signed object URL.
        """


__all__ = ["Storage", "StorageError", "StorageObject"]
