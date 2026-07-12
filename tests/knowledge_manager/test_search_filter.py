"""search / filter defaults and content preview vs get_knowledge."""

from __future__ import annotations

from tests.helpers import _handle_project
from unify.knowledge_manager.knowledge_manager import (
    KNOWLEDGE_PREVIEW_CHARS,
    KnowledgeManager,
)
from unify.knowledge_manager.types.knowledge import Knowledge, KnowledgeStatus


def test_simulated_filter_defaults_to_active(simulated_km):
    a = simulated_km.add_knowledge(title="Active", content="still true")["details"][
        "knowledge_id"
    ]
    b = simulated_km.add_knowledge(title="Gone", content="withdrawn")["details"][
        "knowledge_id"
    ]
    simulated_km.invalidate_knowledge(knowledge_id=b)

    active = simulated_km.filter()
    assert {c.knowledge_id for c in active} == {a}
    assert all(c.status == KnowledgeStatus.active for c in active)

    invalidated = simulated_km.filter(filter="status == 'invalidated'")
    assert {c.knowledge_id for c in invalidated} == {b}


def test_simulated_filter_by_kind_and_topics(simulated_km):
    simulated_km.add_knowledge(
        title="Policy A",
        content="Always escalate P0s.",
        kind="policy",
        topics=["ops", "escalation"],
    )
    simulated_km.add_knowledge(
        title="Fact B",
        content="HQ is in Austin.",
        kind="fact",
        topics=["hq"],
    )

    policies = simulated_km.filter(filter="kind == 'policy'")
    assert len(policies) == 1
    assert policies[0].title == "Policy A"

    tagged = simulated_km.filter(filter="'escalation' in topics")
    assert len(tagged) == 1
    assert tagged[0].title == "Policy A"


def test_simulated_search_returns_active_only(simulated_km):
    kid = simulated_km.add_knowledge(
        title="Onboarding",
        content="New hires start with a laptop kit.",
    )["details"]["knowledge_id"]
    simulated_km.invalidate_knowledge(knowledge_id=kid)
    simulated_km.add_knowledge(title="Still active", content="Keep this.")

    hits = simulated_km.search(references={"content": "laptop"}, k=10)
    assert all(c.status == KnowledgeStatus.active for c in hits)
    assert {c.title for c in hits} == {"Still active"}


def test_content_preview_truncation_helper():
    # Far enough past the threshold that the truncation notice cannot make the
    # preview longer than the original body.
    long_body = "x" * (KNOWLEDGE_PREVIEW_CHARS + 500)
    row = Knowledge(knowledge_id=7, title="Long", content=long_body)
    previewed = KnowledgeManager._with_content_preview(row)
    assert previewed.content.startswith(long_body[:KNOWLEDGE_PREVIEW_CHARS])
    assert len(previewed.content) < len(long_body)
    assert "content preview truncated" in previewed.content
    assert "get_knowledge(knowledge_id=7)" in previewed.content

    short = Knowledge(knowledge_id=8, title="Short", content="brief")
    assert KnowledgeManager._with_content_preview(short).content == "brief"


@_handle_project
def test_real_filter_preview_vs_get_knowledge():
    km = KnowledgeManager()
    long_body = ("Warranty clause. " * 200).strip()
    assert len(long_body) > KNOWLEDGE_PREVIEW_CHARS

    kid = km.add_knowledge(
        title="Long warranty",
        content=long_body,
        kind="policy",
    )[
        "details"
    ]["knowledge_id"]

    filtered = km.filter(filter=f"knowledge_id == {kid}")
    assert len(filtered) == 1
    assert "content preview truncated" in filtered[0].content
    assert len(filtered[0].content) < len(long_body)

    full = km.get_knowledge(knowledge_id=kid)
    assert full.content == long_body
    assert "content preview truncated" not in full.content


@_handle_project
def test_real_search_returns_seeded_claim():
    km = KnowledgeManager()
    kid = int(
        km.add_knowledge(
            title="Battery warranty terms",
            content="The battery warranty covers eight years or 100k miles.",
            kind="fact",
            topics=["warranty"],
        )["details"]["knowledge_id"],
    )
    km.add_knowledge(
        title="Office snacks",
        content="The kitchen stocks coffee and tea.",
        kind="fact",
    )

    hits = km.search(references={"content": "battery warranty miles"}, k=10)
    assert hits
    assert all(c.status == KnowledgeStatus.active for c in hits)
    assert any(c.knowledge_id == kid for c in hits)
