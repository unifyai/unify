"""Behavioural tests for ``LocalDiskStorage``."""

from __future__ import annotations

from pathlib import Path

import pytest

from unity.gateway.storage import LocalDiskStorage, Storage, StorageError


def test_local_disk_storage_satisfies_storage_protocol(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    assert isinstance(storage, Storage)


@pytest.mark.asyncio
async def test_write_then_read_returns_same_bytes(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    obj = await storage.write_bytes("greeting.txt", b"hello world")
    assert obj.size_bytes == 11
    assert obj.key == "greeting.txt"
    assert await storage.exists("greeting.txt")
    assert await storage.read_bytes("greeting.txt") == b"hello world"


@pytest.mark.asyncio
async def test_write_creates_subdirectories(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    await storage.write_bytes("attachments/2026/05/photo.jpg", b"\xff\xd8")
    assert (tmp_path / "attachments" / "2026" / "05" / "photo.jpg").exists()


@pytest.mark.asyncio
async def test_read_missing_object_raises_storage_error(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    with pytest.raises(StorageError):
        await storage.read_bytes("nonexistent.txt")


@pytest.mark.asyncio
async def test_list_keys_returns_sorted_relative_keys(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    await storage.write_bytes("b.txt", b"b")
    await storage.write_bytes("a.txt", b"a")
    await storage.write_bytes("nested/c.txt", b"c")
    assert await storage.list_keys() == ["a.txt", "b.txt", "nested/c.txt"]


@pytest.mark.asyncio
async def test_list_keys_filters_by_prefix(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    await storage.write_bytes("inbox/1.txt", b"1")
    await storage.write_bytes("inbox/2.txt", b"2")
    await storage.write_bytes("outbox/1.txt", b"3")
    assert await storage.list_keys(prefix="inbox/") == ["inbox/1.txt", "inbox/2.txt"]


def test_rejects_absolute_keys(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    with pytest.raises(StorageError):
        storage._resolve("/etc/passwd")


def test_rejects_parent_traversal_keys(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    with pytest.raises(StorageError):
        storage._resolve("../../etc/passwd")


def test_rejects_empty_key(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    with pytest.raises(StorageError):
        storage._resolve("")


@pytest.mark.asyncio
async def test_signed_url_returns_file_uri_for_existing_object(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    await storage.write_bytes("present.txt", b"x")
    url = await storage.signed_url("present.txt")
    assert url.startswith("file://")


@pytest.mark.asyncio
async def test_signed_url_returns_public_base_url_when_configured(
    tmp_path: Path,
) -> None:
    storage = LocalDiskStorage(
        base_dir=tmp_path,
        public_base_url="http://orchestra:8000/v0/storage/local/",
    )
    await storage.write_bytes("attachments/123/abc_hello.txt", b"x")
    url = await storage.signed_url("attachments/123/abc_hello.txt")
    assert url == (
        "http://orchestra:8000/v0/storage/local/attachments/123/abc_hello.txt"
    )


@pytest.mark.asyncio
async def test_signed_url_raises_for_missing_object(tmp_path: Path) -> None:
    storage = LocalDiskStorage(base_dir=tmp_path)
    with pytest.raises(StorageError):
        await storage.signed_url("missing.txt")


def test_default_base_dir_is_workspace_relative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("UNITY_GATEWAY_STORAGE_DIR", raising=False)
    storage = LocalDiskStorage()
    assert storage.base_dir == tmp_path / ".unity-gateway-storage"
    assert storage.base_dir.exists()


def test_default_base_dir_honors_environment_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "custom-storage"
    monkeypatch.setenv("UNITY_GATEWAY_STORAGE_DIR", str(target))
    storage = LocalDiskStorage()
    assert storage.base_dir == target
    assert target.exists()
