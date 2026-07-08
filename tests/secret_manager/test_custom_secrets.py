"""Tests for custom secret collection and synchronization."""

import json

import pytest
from pathlib import Path

from unify.secret_manager.secret_manager import SecretManager
from unify.secret_manager.custom_secrets import (
    SECRETS_JSONL_FILENAME,
    collect_custom_secrets,
    collect_secrets_from_directories,
    collect_secrets_from_secret_models,
    compute_custom_secrets_hash,
    secret_entry_key,
)
from unify.secret_manager.types import Secret
from unify.common.context_registry import ContextRegistry
from tests.helpers import _handle_project

_EXAMPLE_SECRET_LINES = [
    {
        "key": "API_KEY",
        "name": "API_KEY",
        "value": "deploy-value",
        "description": "Deploy-time key",
    },
    {
        "key": "TEAM_KEY",
        "name": "TEAM_KEY",
        "value": "team-value",
        "description": "Team credential",
        "destination": "team:42",
    },
    {
        "key": "EMPTY_KEY",
        "name": "EMPTY_KEY",
        "value": "",
        "description": "Should be skipped",
    },
    {
        "key": "DRAFT_KEY",
        "name": "DRAFT_KEY",
        "value": "draft",
        "description": "Not synced",
        "auto_sync": False,
    },
]


@pytest.fixture
def custom_secrets_dir(tmp_path: Path) -> Path:
    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir()
    lines = "\n".join(json.dumps(row) for row in _EXAMPLE_SECRET_LINES)
    (secrets_dir / SECRETS_JSONL_FILENAME).write_text(lines + "\n")
    return secrets_dir


@pytest.fixture
def secret_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(SecretManager, "Secrets")
        ContextRegistry.forget(SecretManager, "Secrets/Meta")
        sm = SecretManager()
        managers.append(sm)
        return sm

    yield _create

    for sm in managers:
        try:
            sm.clear()
        except Exception:
            pass


def test_secret_entry_key():
    assert secret_entry_key(name="API_KEY") == "API_KEY"


def test_collect_custom_secrets_finds_entries(custom_secrets_dir):
    secrets = collect_custom_secrets(path=custom_secrets_dir)
    assert "API_KEY" in secrets
    assert "TEAM_KEY" in secrets


def test_collect_custom_secrets_excludes_empty_and_auto_sync_false(custom_secrets_dir):
    secrets = collect_custom_secrets(path=custom_secrets_dir)
    assert "EMPTY_KEY" not in secrets
    assert "DRAFT_KEY" not in secrets


def test_collect_custom_secrets_has_required_fields(custom_secrets_dir):
    entry = collect_custom_secrets(path=custom_secrets_dir)["API_KEY"]
    assert entry["custom_key"] == "API_KEY"
    assert entry["value"] == "deploy-value"
    assert len(entry["custom_hash"]) == 16


def test_collect_secrets_from_secret_models_filters_empty_values():
    secrets = collect_secrets_from_secret_models(
        [
            Secret(name="FILE_KEY", value="file-value", description="from file"),
            Secret(name="ALLOWLIST_ONLY", value="", description="manifest"),
        ],
    )
    assert "FILE_KEY" in secrets
    assert "ALLOWLIST_ONLY" not in secrets


def test_compute_custom_secrets_hash_is_deterministic(custom_secrets_dir):
    secrets = collect_custom_secrets(path=custom_secrets_dir)
    assert compute_custom_secrets_hash(
        source_secrets=secrets,
    ) == compute_custom_secrets_hash(source_secrets=secrets)


def test_collect_secrets_from_directories_later_dir_overrides(tmp_path):
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    (dir_a / SECRETS_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "name": "SHARED_KEY",
                "value": "a",
                "description": "A",
            },
        )
        + "\n",
    )

    dir_b = tmp_path / "b"
    dir_b.mkdir()
    (dir_b / SECRETS_JSONL_FILENAME).write_text(
        json.dumps(
            {
                "name": "SHARED_KEY",
                "value": "b",
                "description": "B",
            },
        )
        + "\n",
    )

    merged = collect_secrets_from_directories([dir_a, dir_b])
    assert merged["SHARED_KEY"]["value"] == "b"


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_secrets_inserts_new_entries(
    secret_manager_factory,
    custom_secrets_dir,
):
    sm = secret_manager_factory()
    source = collect_custom_secrets(path=custom_secrets_dir)
    assert sm.sync_custom_secrets(source_secrets=source) is True

    names = set(sm._list_secret_keys())
    assert "API_KEY" in names
    assert "TEAM_KEY" in names


@_handle_project
@pytest.mark.asyncio
async def test_sync_custom_secrets_is_idempotent(
    secret_manager_factory,
    custom_secrets_dir,
):
    sm = secret_manager_factory()
    source = collect_custom_secrets(path=custom_secrets_dir)

    assert sm.sync_custom_secrets(source_secrets=source) is True
    sm._custom_secrets_synced = False
    assert sm.sync_custom_secrets(source_secrets=source) is False


@_handle_project
@pytest.mark.asyncio
async def test_user_secret_without_custom_hash_is_preserved(
    secret_manager_factory,
    custom_secrets_dir,
):
    sm = secret_manager_factory()
    sm._create_secret(
        name="USER_KEY",
        value="user-value",
        description="User credential",
    )

    source = collect_custom_secrets(path=custom_secrets_dir)
    sm.sync_custom_secrets(source_secrets=source)

    names = set(sm._list_secret_keys())
    assert "USER_KEY" in names
    assert "API_KEY" in names
