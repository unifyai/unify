"""Shared fixtures for simulated KnowledgeManager Actor routing tests."""

from __future__ import annotations

import pytest

from unify.knowledge_manager.simulated import SimulatedKnowledgeManager
from unify.manager_registry import ManagerRegistry


@pytest.fixture
def seeded_knowledge_manager() -> SimulatedKnowledgeManager:
    """In-memory KM preloaded with claims the routing prompts refer to."""
    ManagerRegistry.clear()
    km = SimulatedKnowledgeManager(description="seeded knowledge for actor routing")
    km.add_knowledge(
        title="Employee onboarding policy",
        content=(
            "New hires complete paperwork on day one, receive a laptop kit, "
            "and finish security training in week one."
        ),
        kind="policy",
        topics=["onboarding", "hr"],
    )
    km.add_knowledge(
        title="Office hours",
        content="Office hours are 9am–5pm Pacific on weekdays.",
        kind="fact",
        topics=["ops"],
    )
    return km
