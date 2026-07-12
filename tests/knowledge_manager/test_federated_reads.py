"""Shape tests for KnowledgeManager federated reads across destination roots."""

from __future__ import annotations

from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.types.knowledge import Knowledge


def _manager_stub() -> KnowledgeManager:
    km = object.__new__(KnowledgeManager)
    km._filter_scope = None
    km._exclude_ids = None
    km._BUILTIN_FIELDS = tuple(Knowledge.model_fields.keys())
    km._read_knowledge_contexts = lambda: [
        "tests/x/y/Knowledge",
        "Teams/42/Knowledge",
    ]
    return km


def test_search_fans_out_over_read_roots(monkeypatch):
    km = _manager_stub()
    km._filter_scope = "title != 'hidden'"
    captured: dict = {}

    def fake_ranked_search(contexts, references, *, limit, **kwargs):
        captured["contexts"] = contexts
        captured["references"] = references
        captured["limit"] = limit
        return []

    monkeypatch.setattr(
        "unify.knowledge_manager.knowledge_manager.federated_ranked_search",
        fake_ranked_search,
    )

    km.search(references={"content": "warranty"}, k=4)

    assert captured["references"] == {"content": "warranty"}
    assert captured["limit"] == 4
    contexts = captured["contexts"]
    assert [(spec.context, spec.source) for spec in contexts] == [
        ("tests/x/y/Knowledge", "tests/x/y/Knowledge"),
        ("Teams/42/Knowledge", "Teams/42/Knowledge"),
    ]
    assert all(
        spec.row_filter is not None and "title != 'hidden'" in spec.row_filter
        for spec in contexts
    )
    assert all(
        spec.row_filter is not None and "status == 'active'" in spec.row_filter
        for spec in contexts
    )
    assert all("knowledge_id" in spec.allowed_fields for spec in contexts)


def test_filter_fans_out_over_read_roots(monkeypatch):
    km = _manager_stub()
    captured: dict = {}

    def fake_filter(contexts, *, filter, offset, limit, **kwargs):
        captured["contexts"] = contexts
        captured["filter"] = filter
        return []

    monkeypatch.setattr(
        "unify.knowledge_manager.knowledge_manager.federated_filter",
        fake_filter,
    )

    km.filter(filter="kind == 'policy'", limit=10)

    assert "kind == 'policy'" in captured["filter"]
    assert "status == 'active'" in captured["filter"]
    assert [(spec.context, spec.source) for spec in captured["contexts"]] == [
        ("tests/x/y/Knowledge", "tests/x/y/Knowledge"),
        ("Teams/42/Knowledge", "Teams/42/Knowledge"),
    ]


def test_filter_normalizes_legacy_null_is_builtin(monkeypatch):
    km = _manager_stub()

    def fake_filter(*_args, **_kwargs):
        return [
            {
                "knowledge_id": 1,
                "title": "Legacy row",
                "content": "Created before is_builtin existed.",
                "kind": "fact",
                "topics": [],
                "source_refs": [],
                "status": "active",
                "supersedes_ids": [],
                "stale_reasons": [],
                "is_builtin": None,
            },
        ]

    monkeypatch.setattr(
        "unify.knowledge_manager.knowledge_manager.federated_filter",
        fake_filter,
    )

    rows = km.filter(limit=10)
    assert rows[0].is_builtin is False


def test_num_items_fans_out_and_applies_exclusion(monkeypatch):
    km = _manager_stub()
    km._exclude_ids = frozenset({7})
    captured: dict = {}

    def fake_count(contexts, *, key, filter):
        captured["contexts"] = contexts
        captured["key"] = key
        captured["filter"] = filter
        return 3

    monkeypatch.setattr(
        "unify.knowledge_manager.knowledge_manager.federated_count",
        fake_count,
    )

    assert km._num_items() == 3
    assert captured["key"] == "knowledge_id"
    assert "knowledge_id != 7" in captured["filter"]
    assert [(spec.context, spec.source) for spec in captured["contexts"]] == [
        ("tests/x/y/Knowledge", "tests/x/y/Knowledge"),
        ("Teams/42/Knowledge", "Teams/42/Knowledge"),
    ]


def test_get_knowledge_disables_default_active_filter(monkeypatch):
    km = _manager_stub()
    captured: dict = {}

    def fake_filter(contexts, *, filter, limit, **kwargs):
        captured["filter"] = filter
        return [
            {
                "knowledge_id": 9,
                "title": "Withdrawn",
                "content": "No longer true.",
                "kind": "fact",
                "topics": [],
                "source_refs": [],
                "status": "invalidated",
                "supersedes_ids": [],
                "stale_reasons": [],
                "is_builtin": False,
            },
        ]

    monkeypatch.setattr(
        "unify.knowledge_manager.knowledge_manager.federated_filter",
        fake_filter,
    )

    claim = km.get_knowledge(knowledge_id=9)
    assert claim.status.value == "invalidated"
    assert "knowledge_id == 9" in captured["filter"]
    assert "status == 'active'" not in captured["filter"]
