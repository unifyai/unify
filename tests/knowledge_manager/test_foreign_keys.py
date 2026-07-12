"""Identity-bearing source references and structured link-debt coverage."""

from __future__ import annotations

from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.types.knowledge import Knowledge, KnowledgeStatus


def _manager_with_source_existence(
    monkeypatch,
    *,
    files: set[int] = frozenset(),
    contacts: set[int] = frozenset(),
    contexts: set[str] = frozenset(),
    knowledge: set[int] = frozenset(),
) -> KnowledgeManager:
    km = object.__new__(KnowledgeManager)
    monkeypatch.setattr(km, "_file_id_exists", lambda value: value in files)
    monkeypatch.setattr(km, "_contact_id_exists", lambda value: value in contacts)
    monkeypatch.setattr(km, "_data_context_exists", lambda value: value in contexts)
    monkeypatch.setattr(km, "_knowledge_id_exists", lambda value: value in knowledge)
    return km


def test_missing_source_reasons_cover_file_contact_data_and_knowledge(monkeypatch):
    km = _manager_with_source_existence(monkeypatch)
    claim = Knowledge(
        knowledge_id=8,
        title="Composite claim",
        content="Distilled from several identity-bearing sources.",
        source_refs=[
            {"kind": "file", "file_id": 11, "filepath": "reports/q2.pdf"},
            {"kind": "contact", "contact_id": 12},
            {"kind": "data", "context": "Teams/4/Orders"},
            {"kind": "derived_from_knowledge", "knowledge_id": 13},
        ],
    )

    reasons = km._missing_source_reasons(claim)

    assert [(reason.dep_kind, reason.id) for reason in reasons] == [
        ("file", 11),
        ("contact", 12),
        ("data", None),
        ("knowledge", 13),
    ]
    assert reasons[0].path == "reports/q2.pdf"
    assert reasons[2].context == "Teams/4/Orders"
    assert claim.status == KnowledgeStatus.active


def test_missing_source_reasons_only_include_unresolved_identities(monkeypatch):
    km = _manager_with_source_existence(
        monkeypatch,
        files={11},
        contacts={12},
        contexts={"Teams/4/Orders"},
        knowledge={13},
    )
    claim = Knowledge(
        knowledge_id=8,
        title="Composite claim",
        content="All identity-bearing sources still resolve.",
        source_refs=[
            {"kind": "file", "file_id": 11},
            {"kind": "contact", "contact_id": 12},
            {"kind": "data", "context": "Teams/4/Orders"},
            {"kind": "derived_from_knowledge", "knowledge_id": 13},
            {"kind": "web", "url": "https://example.com"},
        ],
    )

    assert km._missing_source_reasons(claim) == []


def test_mark_knowledge_stale_for_deleted_sources_appends_debt(monkeypatch):
    from unify.common.stale_reason import StaleReason
    from unify.knowledge_manager import knowledge_manager as km_mod

    class _Log:
        def __init__(self, log_id: int, entries: dict):
            self.id = log_id
            self.entries = entries

    claim_entries = {
        "knowledge_id": 8,
        "title": "From file",
        "content": "Claim text",
        "kind": "fact",
        "topics": [],
        "source_refs": [{"kind": "file", "file_id": 11, "filepath": "a.pdf"}],
        "stale_reasons": [],
        "status": "active",
    }
    updates: list[dict] = []

    monkeypatch.setattr(
        km_mod.ContextRegistry,
        "read_roots",
        staticmethod(lambda *_a, **_k: ["Assistants/1"]),
    )
    monkeypatch.setattr(km_mod, "list_private_fields", lambda *_a, **_k: [])
    monkeypatch.setattr(
        km_mod.unisdk,
        "get_logs",
        lambda **kwargs: [_Log(1, claim_entries)],
    )

    def _update_logs(**kwargs):
        updates.append(kwargs)

    monkeypatch.setattr(km_mod.unisdk, "update_logs", _update_logs)

    km_mod.mark_knowledge_stale_for_deleted_sources(
        reasons=[
            StaleReason(
                dep_kind="file",
                id=11,
                path="a.pdf",
                message="missing file_id=11",
            ),
        ],
    )

    assert len(updates) == 1
    reasons = updates[0]["entries"]["stale_reasons"]
    assert reasons[0]["dep_kind"] == "file"
    assert reasons[0]["id"] == 11


def test_file_path_without_file_id_is_not_an_identity_fk(monkeypatch):
    km = _manager_with_source_existence(monkeypatch)
    claim = Knowledge(
        knowledge_id=8,
        title="Workspace note",
        content="The source is identified by path only.",
        source_refs=[{"kind": "file", "filepath": "notes/decision.md"}],
    )

    assert km._missing_source_reasons(claim) == []
