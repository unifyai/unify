"""Unit tests for coverage-aware embedding ensure/backfill."""

from __future__ import annotations

from typing import Any

import pytest

from unify.common import embed_utils


@pytest.fixture
def embed_mocks(monkeypatch: pytest.MonkeyPatch):
    """Stub unisdk field/log APIs used by ensure_vector_column."""
    state: dict[str, Any] = {
        "fields": {},
        "missing_ids": [],
        "derived_calls": [],
    }

    def get_fields(*, context: str, project: str | None = None):
        return dict(state["fields"])

    def get_logs(
        *,
        context: str,
        filter: str | None = None,
        return_ids_only: bool = False,
        limit: int = 1000,
        offset: int = 0,
        project: str | None = None,
        **kwargs,
    ):
        assert return_ids_only
        ids = list(state["missing_ids"])
        return ids[offset : offset + limit]

    def ensure_derived_column(
        context: str,
        key: str,
        equation: str,
        *,
        derived: bool | None = None,
        from_ids: list[int] | None = None,
        project: str | None = None,
        referenced_logs_context: str | None = None,
    ):
        state["derived_calls"].append(
            {
                "context": context,
                "key": key,
                "equation": equation,
                "from_ids": list(from_ids) if from_ids is not None else None,
            },
        )
        state["fields"][key] = {"type": "vector"}

    monkeypatch.setattr(embed_utils.unisdk, "get_fields", get_fields)
    monkeypatch.setattr(embed_utils.unisdk, "get_logs", get_logs)
    monkeypatch.setattr(embed_utils, "ensure_derived_column", ensure_derived_column)
    return state


def test_ensure_vector_column_creates_when_field_missing(embed_mocks):
    state = embed_mocks
    did_work = embed_utils.ensure_vector_column(
        context="Ctx/Transcripts",
        embed_column="_content_emb",
        source_column="content",
    )
    assert did_work is True
    assert len(state["derived_calls"]) == 1
    assert state["derived_calls"][0]["key"] == "_content_emb"
    assert state["derived_calls"][0]["from_ids"] is None


def test_ensure_vector_column_noops_when_fully_covered(embed_mocks):
    state = embed_mocks
    state["fields"]["_content_emb"] = {"type": "vector"}
    state["missing_ids"] = []

    did_work = embed_utils.ensure_vector_column(
        context="Ctx/Transcripts",
        embed_column="_content_emb",
        source_column="content",
    )
    assert did_work is False
    assert state["derived_calls"] == []


def test_ensure_vector_column_backfills_orphaned_rows(embed_mocks):
    state = embed_mocks
    state["fields"]["_content_emb"] = {"type": "vector"}
    state["missing_ids"] = [10, 20, 30]

    did_work = embed_utils.ensure_vector_column(
        context="Ctx/Transcripts",
        embed_column="_content_emb",
        source_column="content",
    )
    assert did_work is True
    assert len(state["derived_calls"]) == 1
    call = state["derived_calls"][0]
    assert call["key"] == "_content_emb"
    assert call["from_ids"] == [10, 20, 30]
    assert "embed({lg:content}" in call["equation"]


def test_ensure_vector_column_targeted_from_ids_skips_coverage_scan(embed_mocks):
    state = embed_mocks
    state["fields"]["_content_emb"] = {"type": "vector"}
    state["missing_ids"] = [99]  # would be used only if coverage scan ran

    did_work = embed_utils.ensure_vector_column(
        context="Ctx/Transcripts",
        embed_column="_content_emb",
        source_column="content",
        from_ids=[1, 2],
    )
    assert did_work is True
    assert len(state["derived_calls"]) == 1
    assert state["derived_calls"][0]["from_ids"] == [1, 2]
