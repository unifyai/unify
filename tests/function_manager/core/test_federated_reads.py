from __future__ import annotations

from types import SimpleNamespace

from unity.function_manager.function_manager import FunctionManager


def _manager_stub(*, include_primitives: bool = True) -> FunctionManager:
    fm = object.__new__(FunctionManager)
    fm._include_primitives = include_primitives
    fm._filter_scope = None
    fm._exclude_compositional_ids = None
    fm._exclude_primitive_ids = None
    fm._primitive_scope = object()
    fm._registry = SimpleNamespace(
        primitive_row_filter=lambda _scope: "primitive_class == 'Primitives'",
    )
    fm._read_compositional_contexts = lambda: ["Functions/Compositional"]
    fm._read_function_contexts = lambda _table: ["Functions/Primitives"]
    return fm


def test_filter_functions_uses_federated_filter_window_and_context_filters():
    fm = _manager_stub()
    fm._filter_scope = "language == 'python'"
    fm._exclude_primitive_ids = frozenset({99})
    calls = []
    sync_calls = []

    def get_logs(context, *, filter=None, offset=0, limit=None):
        calls.append((context, filter, offset, limit))
        rows = {
            "Functions/Compositional": [
                {"name": "comp-1", "implementation": "def comp_1(): pass"},
                {"name": "comp-2", "implementation": "def comp_2(): pass"},
            ],
            "Functions/Primitives": [
                {"name": "prim-1", "implementation": None},
                {"name": "prim-2", "implementation": None},
            ],
        }[context]
        return rows[:limit]

    fm._get_logs_with_retry = get_logs
    fm.sync_primitives = lambda: sync_calls.append("sync")

    rows = fm.filter_functions(
        filter="'tool' in docstring",
        offset=1,
        limit=2,
        include_implementations=False,
    )

    assert [row["name"] for row in rows] == ["comp-2", "prim-1"]
    assert all("implementation" not in row for row in rows)
    assert [row["_federated_source"] for row in rows] == [
        "compositional",
        "primitives",
    ]
    assert sync_calls == ["sync"]
    assert calls == [
        (
            "Functions/Compositional",
            "('tool' in docstring) and (language == 'python')",
            0,
            3,
        ),
        (
            "Functions/Primitives",
            "('tool' in docstring) and ((primitive_class == 'Primitives') and (function_id != 99))",
            0,
            3,
        ),
    ]


def test_filter_functions_skips_primitive_contexts_when_disabled():
    fm = _manager_stub(include_primitives=False)
    calls = []

    def get_logs(context, *, filter=None, offset=0, limit=None):
        calls.append(context)
        return [{"name": "comp-1"}]

    fm._get_logs_with_retry = get_logs

    rows = fm.filter_functions(limit=5)

    assert [row["name"] for row in rows] == ["comp-1"]
    assert calls == ["Functions/Compositional"]


def test_search_functions_uses_federated_ranked_search_contexts(monkeypatch):
    fm = _manager_stub()
    fm._filter_scope = "language == 'python'"
    fm._exclude_compositional_ids = frozenset({1})
    captured = {}
    sync_calls = []

    def fake_ranked_search(contexts, references, *, limit, **kwargs):
        captured["contexts"] = contexts
        captured["references"] = references
        captured["limit"] = limit
        captured["kwargs"] = kwargs
        return [
            {
                "name": "ranked",
                "implementation": "def ranked(): pass",
                "_federated_score": 0.1,
            },
        ]

    monkeypatch.setattr(
        "unity.function_manager.function_manager.federated_ranked_search",
        fake_ranked_search,
    )
    fm.sync_primitives = lambda: sync_calls.append("sync")

    rows = fm.search_functions(
        query="rank useful functions",
        n=7,
        include_implementations=False,
    )

    assert rows == [{"name": "ranked", "_federated_score": 0.1}]
    assert captured["references"] == {"embedding_text": "rank useful functions"}
    assert captured["limit"] == 7
    assert captured["kwargs"] == {"unique_id_field": "function_id", "backfill": True}
    assert sync_calls == ["sync"]

    contexts = captured["contexts"]
    assert [spec.context for spec in contexts] == [
        "Functions/Compositional",
        "Functions/Primitives",
    ]
    assert [spec.source for spec in contexts] == ["compositional", "primitives"]
    assert contexts[0].row_filter == "(language == 'python') and (function_id != 1)"
    assert contexts[1].row_filter == "primitive_class == 'Primitives'"
    assert "embedding_text" in contexts[0].allowed_fields
    assert contexts[0].allowed_fields == contexts[1].allowed_fields
