"""Local-disk implementation of the gateway ``Storage`` protocol.

Used by the single-process self-hosted Unity. Backs gateway attachments
to a workspace-relative directory (``UNITY_GATEWAY_STORAGE_DIR``, default
``./.unity-gateway-storage``).

When a ``public_base_url`` is configured, ``signed_url`` returns
``{public_base_url}/{key}`` — the HTTP endpoint where another service
(Orchestra's local-object route, in the self-host compose stack) serves
the same directory. Without one, ``signed_url`` falls back to a
``file://`` URI usable only inside the gateway process.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from unify.gateway.storage.base import Storage, StorageError, StorageObject


def _default_base_dir() -> Path:
    raw = os.environ.get("UNITY_GATEWAY_STORAGE_DIR", "").strip()
    return Path(raw) if raw else Path.cwd() / ".unity-gateway-storage"


class LocalDiskStorage(Storage):
    """Filesystem-backed implementation of ``Storage``.

    Object keys are mapped to relative paths under ``base_dir``. Forward
    slashes in keys create subdirectories; ``..`` segments and absolute
    paths are rejected to prevent escapes out of ``base_dir``.
    """

    def __init__(
        self,
        base_dir: Path | str | None = None,
        public_base_url: str | None = None,
    ) -> None:
        self._base_dir = Path(base_dir) if base_dir is not None else _default_base_dir()
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._public_base_url = (public_base_url or "").rstrip("/") or None

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def _resolve(self, key: str) -> Path:
        if not key:
            raise StorageError("storage key must be non-empty")
        if key.startswith("/") or ".." in key.split("/"):
            raise StorageError(f"invalid storage key: {key!r}")
        target = (self._base_dir / key).resolve()
        try:
            target.relative_to(self._base_dir.resolve())
        except ValueError as exc:
            raise StorageError(f"storage key escapes base dir: {key!r}") from exc
        return target

    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> StorageObject:
        path = self._resolve(key)

        def _do_write() -> int:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            return len(data)

        size = await asyncio.to_thread(_do_write)
        return StorageObject(key=key, size_bytes=size, content_type=content_type)

    async def read_bytes(self, key: str) -> bytes:
        path = self._resolve(key)
        if not path.exists():
            raise StorageError(f"object not found: {key!r}")
        return await asyncio.to_thread(path.read_bytes)

    async def exists(self, key: str) -> bool:
        path = self._resolve(key)
        return await asyncio.to_thread(path.is_file)

    async def list_keys(self, prefix: str = "") -> list[str]:
        base = self._base_dir

        def _do_list() -> list[str]:
            if not base.exists():
                return []
            keys = [
                str(p.relative_to(base)).replace(os.sep, "/")
                for p in base.rglob("*")
                if p.is_file()
            ]
            if prefix:
                keys = [k for k in keys if k.startswith(prefix)]
            return sorted(keys)

        return await asyncio.to_thread(_do_list)

    async def signed_url(
        self,
        key: str,
        *,
        expires_seconds: int = 900,
    ) -> str:
        path = self._resolve(key)
        if not path.exists():
            raise StorageError(f"object not found: {key!r}")
        if self._public_base_url:
            return f"{self._public_base_url}/{key}"
        return path.resolve().as_uri()


__all__ = ["LocalDiskStorage"]
