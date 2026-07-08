"""Tests for custom blacklist collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.blacklist_manager.blacklist_manager import BlackListManager
from unify.blacklist_manager.custom_blacklist import (
    BLACKLIST_JSONL_FILENAME,
    blacklist_entry_key,
    collect_blacklist_from_directories,
    collect_custom_blacklist,
    compute_custom_blacklist_hash,
)
from unify.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

_EXAMPLE_BLACKLIST_LINES = [
    {
        "key": "email|spam@x.com",
        "medium": "email",
        "contact_detail": "spam@x.com",
        "reason": "Known spam sender",
    },
    {
        "key": "phone_call|+15551234567",
        "medium": "phone_call",
        "contact_detail": "+15551234567",
        "reason": "Blocked caller",
        "destination": "team:42",
    },
    {
        "key": "email|draft@x.com",
        "medium": "email",
        "contact_detail": "draft@x.com",
        "reason": "Not synced",
        "auto_sync": False,
    },
]


@pytest.fixture
def custom_blacklist_dir(tmp_path: Path) -> Path:
    blacklist_dir = tmp_path / "blacklist"
    blacklist_dir.mkdir()
    lines = "\n".join(json.dumps(row) for row in _EXAMPLE_BLACKLIST_LINES)
    (blacklist_dir / BLACKLIST_JSONL_FILENAME).write_text(lines + "\n")
    return blacklist_dir


@pytest.fixture
def blacklist_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(BlackListManager, "BlackList")
        ContextRegistry.forget(BlackListManager, "BlackList/Meta")
        bm = BlackListManager()
        managers.append(bm)
        return bm

    yield _create

    for bm in managers:
        try:
            bm.clear()
        except Exception:
            pass


def test_blacklist_entry_key():
    assert blacklist_entry_key(medium="email", contact_detail="spam@x.com") == (
        "email|spam@x.com"
    )


def test_collect_custom_blacklist_finds_entries(custom_blacklist_dir):
    blacklist = collect_custom_blacklist(path=custom_blacklist_dir)
    assert "email|spam@x.com" in blacklist
    assert "phone_call|+15551234567" in blacklist


def test_collect_custom_blacklist_excludes_auto_sync_false(custom_blacklist_dir):
    blacklist = collect_custom_blacklist(path=custom_blacklist_dir)
    assert "email|draft@x.com" not in blacklist


def test_collect_custom_blacklist_has_required_fields(custom_blacklist_dir):
    entry = collect_custom_blacklist(path=custom_blacklist_dir)["email|spam@x.com"]
    assert entry["custom_key"] == "email|spam@x.com"
    assert entry["medium"] == "email"
    assert entry["contact_detail"] == "spam@x.com"
    assert len(entry["custom_hash"]) == 16


def test_compute_custom_blacklist_hash_is_deterministic(custom_blacklist_dir):
    blacklist = collect_custom_blacklist(path=custom_blacklist_dir)
    assert compute_custom_blacklist_hash(
        source_blacklist=blacklist,
    ) == compute_custom_blacklist_hash(source_blacklist=blacklist)


def test_collect_blacklist_from_directories_later_dir_overrides(tmp_path):
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / BLACKLIST_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "email|shared@x.com",
                "medium": "email",
                "contact_detail": "shared@x.com",
                "reason": "A",
            },
        )
        + "\n",
    )

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / BLACKLIST_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "key": "email|shared@x.com",
                "medium": "email",
                "contact_detail": "shared@x.com",
                "reason": "B",
            },
        )
        + "\n",
    )

    merged = collect_blacklist_from_directories([dir_a, dir_b])
    assert merged["email|shared@x.com"]["reason"] == "B"


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_blacklist_inserts_new_entries(
    blacklist_manager_factory,
    custom_blacklist_dir,
):
    bm = blacklist_manager_factory()
    source = collect_custom_blacklist(path=custom_blacklist_dir)
    assert bm.sync_custom_blacklist(source_blacklist=source) is True

    result = bm.filter_blacklist(limit=100)
    reasons = {entry.reason for entry in result["entries"]}
    assert "Known spam sender" in reasons
    assert "Blocked caller" in reasons


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_blacklist_is_idempotent(
    blacklist_manager_factory,
    custom_blacklist_dir,
):
    bm = blacklist_manager_factory()
    source = collect_custom_blacklist(path=custom_blacklist_dir)

    assert bm.sync_custom_blacklist(source_blacklist=source) is True
    bm._custom_blacklist_synced = False
    assert bm.sync_custom_blacklist(source_blacklist=source) is False


@_handle_project
@pytest.mark.asyncio
async def test_user_blacklist_without_custom_hash_is_preserved(
    blacklist_manager_factory,
    custom_blacklist_dir,
):
    bm = blacklist_manager_factory()
    bm.create_blacklist_entry(
        medium="email",
        contact_detail="user@x.com",
        reason="User block",
    )

    source = collect_custom_blacklist(path=custom_blacklist_dir)
    bm.sync_custom_blacklist(source_blacklist=source)

    result = bm.filter_blacklist(limit=100)
    details = {entry.contact_detail for entry in result["entries"]}
    assert "user@x.com" in details
    assert "spam@x.com" in details
