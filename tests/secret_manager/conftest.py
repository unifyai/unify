from __future__ import annotations

import uuid

import pytest
import unify

from unity.common.context_registry import ContextRegistry
from unity.session_details import SESSION_DETAILS


@pytest.fixture(scope="function")
def secret_manager_context(request):
    """Provide an isolated Unify context for each secret-manager test."""
    ctx = f"tests/secret_manager/{request.node.name}"
    ContextRegistry.clear()
    SESSION_DETAILS.reset()
    # Create a fresh, test-specific context and make it active
    try:
        unify.set_context(ctx, relative=False)
    except Exception:
        pass
    yield ctx
    unify.delete_context(ctx)
    unify.unset_context()
    ContextRegistry.clear()
    SESSION_DETAILS.reset()


@pytest.fixture(scope="function")
def secret_manager_spaces():
    """Attach the active assistant to two isolated shared teams."""
    base_team_id = 10_000_000 + uuid.uuid4().int % 1_000_000_000
    team_ids = [base_team_id, base_team_id + 1]
    SESSION_DETAILS.team_ids = list(team_ids)
    SESSION_DETAILS.team_summaries = [
        {
            "team_id": team_ids[0],
            "name": "Patch Team",
            "description": "Shared workspace for production operations and team integrations.",
        },
        {
            "team_id": team_ids[1],
            "name": "Family Operations",
            "description": "Private family workspace for home and calendar coordination.",
        },
    ]
    yield team_ids
    for team_id in team_ids:
        try:
            unify.delete_context(f"Teams/{team_id}/Secrets")
        except Exception:
            pass
    SESSION_DETAILS.team_ids = []
    SESSION_DETAILS.team_summaries = []
    ContextRegistry.forget_departed_team_roots([])
