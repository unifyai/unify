"""Tests for custom integration registry collection and synchronization."""

import json

import pytest

from unify.integration_registry.custom_integration_registry import (
    collect_integration_registry_from_rows,
    compute_custom_integration_registry_hash,
    integration_registry_entry_key,
    registry_rows_from_source,
)
from unify.integration_registry.sync import IntegrationRegistrySync
from tests.helpers import _handle_project


def _sample_row(**overrides: object) -> dict[str, object]:
    row = {
        "slug": "github",
        "label": "GitHub",
        "category": "devtools",
        "version": "1.0.0",
        "tier": "native",
        "quality": "production",
        "required_secrets_json": json.dumps(["GITHUB_TOKEN"]),
        "optional_secrets_json": json.dumps([]),
        "capability_ids_json": json.dumps(["repos"]),
        "function_names_json": json.dumps(["list_repos"]),
        "guidance_titles_json": json.dumps(["GitHub Guide"]),
        "tags_json": json.dumps(["git"]),
        "homepage": "https://github.com",
        "description": "GitHub integration",
    }
    row.update(overrides)
    return row


def test_integration_registry_entry_key():
    assert integration_registry_entry_key(slug="github") == "github"


def test_collect_integration_registry_from_rows():
    registry = collect_integration_registry_from_rows([_sample_row()])
    assert "github" in registry
    assert registry["github"]["custom_key"] == "github"
    assert registry["github"]["custom_hash"]


def test_collect_integration_registry_skips_missing_slug():
    registry = collect_integration_registry_from_rows([{"label": "No Slug"}])
    assert registry == {}


def test_compute_custom_integration_registry_hash_empty():
    assert compute_custom_integration_registry_hash(source_registry={}) == ""


def test_registry_rows_from_source_strips_custom_metadata():
    source = collect_integration_registry_from_rows([_sample_row()])
    rows = registry_rows_from_source(source)
    assert rows[0]["slug"] == "github"
    assert "custom_hash" not in rows[0]


@pytest.mark.asyncio
async def test_sync_custom_integration_registry_inserts_rows():
    _handle_project("IntegrationRegistryCustomSync")
    syncer = IntegrationRegistrySync()
    source = collect_integration_registry_from_rows([_sample_row()])
    assert syncer.sync_custom(source_registry=source) is True

    import unisdk

    active = unisdk.get_active_context()["read"]
    ctx = f"{active}/Integrations/Manifests"
    logs = unisdk.get_logs(context=ctx, filter='slug == "github"', limit=1)
    assert logs
    assert logs[0].entries.get("custom_hash")


@pytest.mark.asyncio
async def test_sync_custom_integration_registry_is_idempotent():
    _handle_project("IntegrationRegistryCustomSync")
    syncer = IntegrationRegistrySync()
    source = collect_integration_registry_from_rows([_sample_row()])
    assert syncer.sync_custom(source_registry=source) is True
    syncer._synced = False
    assert syncer.sync_custom(source_registry=source) is False
