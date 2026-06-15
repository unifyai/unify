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
    fm._primitives_ctx = "Functions/Primitives"
    fm._registry = SimpleNamespace(
        primitive_row_filter=lambda _scope: "primitive_class == 'Primitives'",
    )
    fm._read_compositional_contexts = lambda: ["Functions/Compositional"]
    return fm


def test_filter_functions_uses_federated_filter_window_and_context_filters():
    fm = _manager_stub()
    fm._filter_scope = "language == 'python'"
    fm._exclude_primitive_ids = frozenset({99})
    calls = []

    def get_logs(context, *, filter=None, offset=0, limit=None, project=None):
        calls.append((context, project, filter, offset, limit))
        rows = {
            ("Functions/Compositional", None): [
                {"name": "comp-1", "implementation": "def comp_1(): pass"},
                {"name": "comp-2", "implementation": "def comp_2(): pass"},
            ],
            ("Functions/Primitives", "Builtins"): [
                {"name": "prim-1", "implementation": None},
                {"name": "prim-2", "implementation": None},
            ],
            ("Functions/Primitives", None): [
                {"name": "provider-1", "implementation": None},
            ],
        }[(context, project)]
        return rows[:limit]

    fm._get_logs_with_retry = get_logs

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

    scoped_primitive_filter = (
        "(primitive_class == 'Primitives') and (function_id != 99)"
    )
    assert calls == [
        (
            "Functions/Compositional",
            None,
            "('tool' in docstring) and (language == 'python')",
            0,
            3,
        ),
        (
            "Functions/Primitives",
            "Builtins",
            f"('tool' in docstring) and ({scoped_primitive_filter})",
            0,
            3,
        ),
        (
            "Functions/Primitives",
            None,
            "('tool' in docstring) and "
            f'(({scoped_primitive_filter}) and integration_source == "provider_backed")',
            0,
            3,
        ),
    ]


def test_filter_functions_skips_primitive_contexts_when_disabled():
    fm = _manager_stub(include_primitives=False)
    calls = []

    def get_logs(context, *, filter=None, offset=0, limit=None, project=None):
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

    rows = fm.search_functions(
        query="rank useful functions",
        n=7,
        include_implementations=False,
    )

    assert rows == [{"name": "ranked", "_federated_score": 0.1}]
    assert captured["references"] == {"embedding_text": "rank useful functions"}
    assert captured["limit"] == 7
    assert captured["kwargs"] == {"unique_id_field": "function_id", "backfill": True}

    contexts = captured["contexts"]
    assert [(spec.context, spec.project) for spec in contexts] == [
        ("Functions/Compositional", None),
        ("Functions/Primitives", "Builtins"),
        ("Functions/Primitives", None),
    ]
    assert [spec.source for spec in contexts] == [
        "compositional",
        "primitives",
        "primitives",
    ]
    assert contexts[0].row_filter == "(language == 'python') and (function_id != 1)"
    assert contexts[1].row_filter == "primitive_class == 'Primitives'"
    assert contexts[2].row_filter == (
        "(primitive_class == 'Primitives') "
        'and integration_source == "provider_backed"'
    )
    assert "embedding_text" in contexts[0].allowed_fields
    assert contexts[0].allowed_fields == contexts[1].allowed_fields
