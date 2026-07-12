"""Minimal fixtures for KnowledgeManager typed-claim tests."""

from __future__ import annotations

import pytest

from unify.knowledge_manager.simulated import SimulatedKnowledgeManager
from unify.manager_registry import ManagerRegistry


@pytest.fixture
def simulated_km() -> SimulatedKnowledgeManager:
    """Fresh in-memory KnowledgeManager for symbolic / infrastructure tests."""
    ManagerRegistry.clear()
    return SimulatedKnowledgeManager(description="test knowledge ledger")
