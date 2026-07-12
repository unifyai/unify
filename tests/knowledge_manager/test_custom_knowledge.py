"""Tests for custom knowledge collection and synchronization."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.knowledge_manager.custom_knowledge import (
    KNOWLEDGE_JSONL_FILENAME,
    collect_custom_knowledge,
    collect_knowledge_from_directories,
    compute_custom_knowledge_hash,
    knowledge_titles_from_source,
)
from unify.knowledge_manager.knowledge_manager import KnowledgeManager


def _write_knowledge_jsonl(root: Path, lines: list[dict]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / KNOWLEDGE_JSONL_FILENAME
    path.write_text("\n".join(json.dumps(row) for row in lines) + "\n")
    return path


@pytest.fixture
def custom_knowledge_dir(tmp_path: Path) -> Path:
    root = tmp_path / "knowledge"
    _write_knowledge_jsonl(
        root,
        [
            {
                "key": "warranty-tesla",
                "title": "Tesla battery warranty",
                "content": "Eight years or 100k miles.",
                "kind": "fact",
                "topics": ["warranty", "tesla"],
            },
            {
                "key": "team-ops-sla",
                "title": "Team SLA",
                "content": "P0 within 1 hour.",
                "kind": "policy",
                "destination": "team:42",
            },
            {
                "key": "skipped",
                "title": "Skipped",
                "content": "Should not sync.",
                "auto_sync": False,
            },
        ],
    )
    return root


@pytest.fixture
def knowledge_manager_factory():
    managers = []

    def _create():
        ContextRegistry.forget(KnowledgeManager, "Knowledge")
        ContextRegistry.forget(KnowledgeManager, "Knowledge/Meta")
        km = KnowledgeManager()
        managers.append(km)
        return km

    yield _create

    for km in managers:
        try:
            km.clear()
        except Exception:
            pass


def test_collect_custom_knowledge_finds_claims(custom_knowledge_dir):
    claims = collect_custom_knowledge(path=custom_knowledge_dir)
    assert "warranty-tesla" in claims
    assert "team-ops-sla" in claims
    assert "skipped" not in claims


def test_collect_custom_knowledge_claim_fields(custom_knowledge_dir):
    claims = collect_custom_knowledge(path=custom_knowledge_dir)
    row = claims["warranty-tesla"]
    assert row["title"] == "Tesla battery warranty"
    assert row["custom_key"] == "warranty-tesla"
    assert row["custom_hash"]
    assert row["kind"] == "fact"
    assert row["topics"] == ["warranty", "tesla"]
    assert row["destination"] == "personal"


def test_collect_knowledge_from_directories_later_dir_wins(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write_knowledge_jsonl(
        first,
        [
            {
                "key": "office-hours",
                "title": "Office hours",
                "content": "9-5 PT",
            },
        ],
    )
    _write_knowledge_jsonl(
        second,
        [
            {
                "key": "office-hours",
                "title": "Office hours",
                "content": "10-6 PT",
            },
        ],
    )
    claims = collect_knowledge_from_directories([first, second])
    assert claims["office-hours"]["content"] == "10-6 PT"


def test_compute_custom_knowledge_hash_empty():
    assert compute_custom_knowledge_hash(source_claims={}) == ""


def test_knowledge_titles_from_source(custom_knowledge_dir):
    claims = collect_custom_knowledge(path=custom_knowledge_dir)
    titles = knowledge_titles_from_source(claims)
    assert "Tesla battery warranty" in titles
    assert "Team SLA" in titles


@_handle_project
def test_sync_custom_knowledge_inserts_claims(
    knowledge_manager_factory,
    custom_knowledge_dir,
):
    km = knowledge_manager_factory()
    # Only sync personal claims in this isolated project (no team:42 root).
    source = {
        k: v
        for k, v in collect_custom_knowledge(path=custom_knowledge_dir).items()
        if (v.get("destination") or "personal") == "personal"
    }
    assert km.sync_custom(source_claims=source) is True
    rows = km.filter(filter="custom_hash != None")
    assert len(rows) == 1
    assert rows[0].title == "Tesla battery warranty"
    assert rows[0].custom_key == "warranty-tesla"


@_handle_project
def test_sync_custom_knowledge_is_idempotent(
    knowledge_manager_factory,
    custom_knowledge_dir,
):
    km = knowledge_manager_factory()
    source = {
        k: v
        for k, v in collect_custom_knowledge(path=custom_knowledge_dir).items()
        if (v.get("destination") or "personal") == "personal"
    }
    assert km.sync_custom(source_claims=source) is True
    km._custom_knowledge_synced = False
    assert km.sync_custom(source_claims=source) is False


@_handle_project
def test_user_knowledge_without_custom_hash_is_preserved(
    knowledge_manager_factory,
    custom_knowledge_dir,
):
    km = knowledge_manager_factory()
    km.add_knowledge(
        title="User-authored note",
        content="Keep this entry.",
    )

    source = {
        k: v
        for k, v in collect_custom_knowledge(path=custom_knowledge_dir).items()
        if (v.get("destination") or "personal") == "personal"
    }
    km.sync_custom(source_claims=source)

    titles = {row.title for row in km.filter()}
    assert "User-authored note" in titles
    assert "Tesla battery warranty" in titles
