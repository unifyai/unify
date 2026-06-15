"""Shape tests for GuidanceManager federated reads.

Verify that search / filter / _num_items fan out over the per-assistant
Guidance contexts plus the global builtins catalogue (public-read project),
with scoping and field projections applied per source.
"""

from __future__ import annotations

from unity.guidance_manager.guidance_manager import GuidanceManager
from unity.guidance_manager.types.guidance import Guidance


def _manager_stub() -> GuidanceManager:
    gm = object.__new__(GuidanceManager)
    gm._filter_scope = None
    gm._exclude_ids = None
    gm._BUILTIN_FIELDS = tuple(Guidance.model_fields.keys())
    gm._read_guidance_contexts = lambda: ["tests/x/y/Guidance"]
    return gm


def test_search_includes_builtins_catalog_spec(monkeypatch):
    gm = _manager_stub()
    gm._filter_scope = "title != 'hidden'"
    captured = {}

    def fake_ranked_search(contexts, references, *, limit, **kwargs):
        captured["contexts"] = contexts
        captured["references"] = references
        captured["limit"] = limit
        return []

    monkeypatch.setattr(
        "unity.guidance_manager.guidance_manager.federated_ranked_search",
        fake_ranked_search,
    )

    gm.search(references={"content": "how to deploy"}, k=4)

    assert captured["references"] == {"content": "how to deploy"}
    assert captured["limit"] == 4
    contexts = captured["contexts"]
    assert [(spec.context, spec.project, spec.source) for spec in contexts] == [
        ("tests/x/y/Guidance", None, "tests/x/y/Guidance"),
        ("Guidance", "Builtins", "builtins"),
    ]
    assert all(spec.row_filter == "title != 'hidden'" for spec in contexts)
    assert all("is_builtin" in spec.allowed_fields for spec in contexts)


def test_filter_includes_builtins_catalog_spec(monkeypatch):
    gm = _manager_stub()
    captured = {}

    def fake_filter(contexts, *, filter, offset, limit, **kwargs):
        captured["contexts"] = contexts
        captured["filter"] = filter
        return []

    monkeypatch.setattr(
        "unity.guidance_manager.guidance_manager.federated_filter",
        fake_filter,
    )

    gm.filter(filter="is_builtin == True", limit=10)

    assert captured["filter"] == "is_builtin == True"
    contexts = captured["contexts"]
    assert [(spec.context, spec.project) for spec in contexts] == [
        ("tests/x/y/Guidance", None),
        ("Guidance", "Builtins"),
    ]


def test_num_items_counts_builtins_catalog(monkeypatch):
    gm = _manager_stub()
    gm._exclude_ids = frozenset({7})
    captured = {}

    def fake_count(contexts, *, key, filter):
        captured["contexts"] = contexts
        captured["key"] = key
        captured["filter"] = filter
        return 3

    monkeypatch.setattr(
        "unity.guidance_manager.guidance_manager.federated_count",
        fake_count,
    )

    assert gm._num_items() == 3
    assert captured["key"] == "guidance_id"
    assert captured["filter"] == "guidance_id != 7"
    assert [(spec.context, spec.project) for spec in captured["contexts"]] == [
        ("tests/x/y/Guidance", None),
        ("Guidance", "Builtins"),
    ]
