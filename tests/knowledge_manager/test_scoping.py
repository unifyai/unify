"""filter_scope / exclude_ids / _num_items for KnowledgeManager (mirrors Guidance)."""

from __future__ import annotations

from tests.helpers import _handle_project
from unify.knowledge_manager.knowledge_manager import KnowledgeManager


def _seed(km: KnowledgeManager) -> dict[str, int]:
    ids: dict[str, int] = {}
    for title, content in [
        ("Alpha", "Claim about alpha policy."),
        ("Beta", "Claim about beta policy."),
        ("Gamma", "Claim about gamma policy."),
    ]:
        out = km.add_knowledge(title=title, content=content, kind="policy")
        ids[title] = int(out["details"]["knowledge_id"])
    return ids


@_handle_project
def test_filter_scope_restricts_filter():
    km = KnowledgeManager()
    ids = _seed(km)

    km.filter_scope = f"knowledge_id == {ids['Beta']}"
    rows = km.filter()
    assert len(rows) == 1
    assert rows[0].knowledge_id == ids["Beta"]

    km.filter_scope = None


@_handle_project
def test_filter_scope_composes_with_caller_filter():
    km = KnowledgeManager()
    ids = _seed(km)

    km.filter_scope = f"knowledge_id == {ids['Alpha']} or knowledge_id == {ids['Beta']}"

    rows = km.filter(filter="title == 'Alpha'")
    assert len(rows) == 1
    assert rows[0].title == "Alpha"

    rows = km.filter(filter="title == 'Gamma'")
    assert len(rows) == 0

    km.filter_scope = None


@_handle_project
def test_exclude_ids_restricts_filter():
    km = KnowledgeManager()
    ids = _seed(km)

    km.exclude_ids = frozenset({ids["Alpha"]})

    returned_ids = {r.knowledge_id for r in km.filter()}
    assert ids["Alpha"] not in returned_ids
    assert ids["Beta"] in returned_ids
    assert ids["Gamma"] in returned_ids

    km.exclude_ids = None


@_handle_project
def test_exclude_ids_multiple():
    km = KnowledgeManager()
    ids = _seed(km)

    km.exclude_ids = frozenset({ids["Alpha"], ids["Gamma"]})
    rows = km.filter()
    assert len(rows) == 1
    assert rows[0].knowledge_id == ids["Beta"]

    km.exclude_ids = None


@_handle_project
def test_scope_and_exclusion_combined():
    km = KnowledgeManager()
    ids = _seed(km)

    km.filter_scope = f"knowledge_id == {ids['Alpha']} or knowledge_id == {ids['Beta']}"
    km.exclude_ids = frozenset({ids["Alpha"]})

    rows = km.filter()
    assert len(rows) == 1
    assert rows[0].knowledge_id == ids["Beta"]

    km.filter_scope = None
    km.exclude_ids = None


@_handle_project
def test_search_respects_filter_scope():
    km = KnowledgeManager()
    ids = _seed(km)

    km.filter_scope = f"knowledge_id == {ids['Alpha']}"
    results = km.search(references={"content": "policy"}, k=10)
    assert {r.knowledge_id for r in results} == {ids["Alpha"]}

    km.filter_scope = None


@_handle_project
def test_search_respects_exclude_ids():
    km = KnowledgeManager()
    ids = _seed(km)

    km.exclude_ids = frozenset({ids["Beta"]})
    results = km.search(references={"content": "policy"}, k=30)
    returned_ids = {r.knowledge_id for r in results}
    assert ids["Beta"] not in returned_ids
    assert ids["Alpha"] in returned_ids
    assert ids["Gamma"] in returned_ids

    km.exclude_ids = None


@_handle_project
def test_num_items_respects_filter_scope():
    km = KnowledgeManager()
    assert km._num_items() == 0
    ids = _seed(km)
    assert km._num_items() == 3

    km.filter_scope = f"knowledge_id == {ids['Alpha']}"
    assert km._num_items() == 1

    km.filter_scope = None


@_handle_project
def test_num_items_respects_exclude_ids():
    km = KnowledgeManager()
    ids = _seed(km)

    km.exclude_ids = frozenset({ids["Alpha"], ids["Gamma"]})
    assert km._num_items() == 1

    km.exclude_ids = None


@_handle_project
def test_clearing_scope_restores_full_view():
    km = KnowledgeManager()
    ids = _seed(km)

    km.filter_scope = f"knowledge_id == {ids['Gamma']}"
    assert len(km.filter()) == 1

    km.filter_scope = None
    assert len(km.filter()) == 3

    km.exclude_ids = frozenset({ids["Alpha"], ids["Beta"]})
    assert len(km.filter()) == 1

    km.exclude_ids = None
    assert len(km.filter()) == 3


@_handle_project
def test_limit_with_scope():
    km = KnowledgeManager()
    ids = _seed(km)

    km.filter_scope = f"knowledge_id == {ids['Alpha']} or knowledge_id == {ids['Beta']}"

    rows = km.filter(limit=1)
    assert len(rows) == 1
    assert rows[0].knowledge_id in {ids["Alpha"], ids["Beta"]}

    rows = km.filter(limit=10)
    assert len(rows) == 2

    km.filter_scope = None
