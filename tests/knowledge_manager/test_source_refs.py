"""SourceRef persistence, discrimination, and stale-source reconciliation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tests.helpers import _handle_project
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.types.knowledge import KnowledgeStatus
from unify.knowledge_manager.types.source_ref import (
    ContactSourceRef,
    DataSourceRef,
    SourceKind,
    coerce_source_refs,
)


def test_source_refs_persist_on_add_and_update(simulated_km):
    refs = [
        {"kind": "user_statement", "note": "said in chat"},
        {"kind": "web", "url": "https://example.com/warranty"},
    ]
    kid = simulated_km.add_knowledge(
        title="Warranty",
        content="Eight years.",
        source_refs=refs,
    )["details"]["knowledge_id"]

    claim = simulated_km.get_knowledge(knowledge_id=kid)
    assert len(claim.source_refs) == 2
    assert claim.source_refs[0].kind == SourceKind.user_statement
    assert claim.source_refs[0].note == "said in chat"
    assert claim.source_refs[1].kind == SourceKind.web
    assert claim.source_refs[1].url == "https://example.com/warranty"

    simulated_km.update_knowledge(
        knowledge_id=kid,
        source_refs=[{"kind": "manual", "note": "ops confirmed"}],
    )
    updated = simulated_km.get_knowledge(knowledge_id=kid)
    assert len(updated.source_refs) == 1
    assert updated.source_refs[0].kind == SourceKind.manual


def test_contact_and_data_refs_are_discriminated():
    refs = coerce_source_refs(
        [
            {"kind": "contact", "contact_id": 17, "note": "account owner"},
            {"kind": "data", "context": "Teams/4/Orders"},
        ],
    )

    assert isinstance(refs[0], ContactSourceRef)
    assert refs[0].contact_id == 17
    assert isinstance(refs[1], DataSourceRef)
    assert refs[1].context == "Teams/4/Orders"


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "contact", "context": "Teams/4/Contacts"},
        {"kind": "data", "contact_id": 17},
    ],
)
def test_identity_refs_reject_fields_for_the_wrong_kind(payload):
    with pytest.raises(ValidationError):
        coerce_source_refs([payload])


def test_reconcile_sources_marks_partial_stale_without_changing_status(simulated_km):
    parent_id = simulated_km.add_knowledge(
        title="Parent",
        content="Root fact.",
    )[
        "details"
    ]["knowledge_id"]
    child_id = simulated_km.add_knowledge(
        title="Derived",
        content="Depends on parent.",
        source_refs=[
            {
                "kind": "derived_from_knowledge",
                "knowledge_id": parent_id,
            },
            {
                "kind": "derived_from_knowledge",
                "knowledge_id": 999_001,
            },
        ],
    )["details"]["knowledge_id"]

    out = simulated_km.reconcile_sources(knowledge_ids=[child_id])
    assert out["details"] == {
        "checked": 1,
        "stale_knowledge_ids": [child_id],
        "stale_count": 1,
    }
    claim = simulated_km.get_knowledge(knowledge_id=child_id)
    assert claim.status == KnowledgeStatus.active
    assert [(reason.dep_kind, reason.id) for reason in claim.stale_reasons] == [
        ("knowledge", 999_001),
    ]
    assert [item.knowledge_id for item in simulated_km.filter()] == [
        parent_id,
        child_id,
    ]


def test_reconcile_sources_keeps_active_when_all_identity_sources_are_missing(
    simulated_km,
):
    child_id = simulated_km.add_knowledge(
        title="Derived",
        content="Depends on missing claims.",
        source_refs=[
            {
                "kind": "derived_from_knowledge",
                "knowledge_id": 999_001,
            },
            {
                "kind": "derived_from_knowledge",
                "knowledge_id": 999_002,
            },
        ],
    )["details"]["knowledge_id"]

    out = simulated_km.reconcile_sources(knowledge_ids=[child_id])
    assert out["details"]["stale_knowledge_ids"] == [child_id]
    claim = simulated_km.get_knowledge(knowledge_id=child_id)
    assert claim.status == KnowledgeStatus.active
    assert {reason.id for reason in claim.stale_reasons} == {999_001, 999_002}
    assert simulated_km.filter() == [claim]


def test_reconcile_sources_keeps_valid_derived_claims(simulated_km):
    parent_id = simulated_km.add_knowledge(title="Parent", content="Root fact.")[
        "details"
    ]["knowledge_id"]
    child_id = simulated_km.add_knowledge(
        title="Derived",
        content="Depends on parent.",
        source_refs=[
            {"kind": "derived_from_knowledge", "knowledge_id": parent_id},
        ],
    )["details"]["knowledge_id"]

    out = simulated_km.reconcile_sources(knowledge_ids=[child_id])
    assert out["details"] == {
        "checked": 1,
        "stale_knowledge_ids": [],
        "stale_count": 0,
    }
    assert simulated_km.get_knowledge(knowledge_id=child_id).stale_reasons == []


@_handle_project
def test_real_source_refs_persist_on_add():
    km = KnowledgeManager()
    kid = int(
        km.add_knowledge(
            title="Warranty",
            content="Eight years.",
            source_refs=[
                {"kind": "user_statement", "note": "said in chat"},
                {"kind": "web", "url": "https://example.com/warranty"},
            ],
        )["details"]["knowledge_id"],
    )
    claim = km.get_knowledge(knowledge_id=kid)
    assert len(claim.source_refs) == 2
    assert claim.source_refs[0].kind == SourceKind.user_statement
    assert claim.source_refs[1].kind == SourceKind.web
    assert claim.source_refs[1].url == "https://example.com/warranty"


@_handle_project
def test_reconcile_sources_marks_missing_file_ref_stale(monkeypatch):
    km = KnowledgeManager()
    monkeypatch.setattr(km, "_file_id_exists", lambda _file_id: False)
    kid = int(
        km.add_knowledge(
            title="From file",
            content="Extracted from a missing file.",
            source_refs=[{"kind": "file", "file_id": 999_001}],
        )["details"]["knowledge_id"],
    )

    out = km.reconcile_sources(knowledge_ids=[kid])
    assert out["details"] == {
        "checked": 1,
        "stale_knowledge_ids": [kid],
        "stale_count": 1,
    }
    claim = km.get_knowledge(knowledge_id=kid)
    assert claim.status == KnowledgeStatus.active
    assert [(reason.dep_kind, reason.id) for reason in claim.stale_reasons] == [
        ("file", 999_001),
    ]
    assert [row.knowledge_id for row in km.filter(filter=f"knowledge_id == {kid}")] == [
        kid,
    ]


@_handle_project
def test_reconcile_sources_keeps_valid_file_ref(monkeypatch):
    km = KnowledgeManager()
    monkeypatch.setattr(km, "_file_id_exists", lambda _file_id: True)
    kid = int(
        km.add_knowledge(
            title="From file",
            content="Extracted from an existing file.",
            source_refs=[{"kind": "file", "file_id": 42}],
        )["details"]["knowledge_id"],
    )

    out = km.reconcile_sources(knowledge_ids=[kid])
    assert out["details"] == {
        "checked": 1,
        "stale_knowledge_ids": [],
        "stale_count": 0,
    }
    assert km.get_knowledge(knowledge_id=kid).status == KnowledgeStatus.active
    assert km.get_knowledge(knowledge_id=kid).stale_reasons == []
