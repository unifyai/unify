from __future__ import annotations

import pytest
from unify import db
from unify.common.context_registry import ContextRegistry
from unify.session_details import SESSION_DETAILS


@pytest.fixture(scope="function")
def secret_manager_context(request):
    """Provide an isolated Unify context for each secret-manager test."""
    ctx = f"tests/secret_manager/{request.node.name}"
    ContextRegistry.clear()
    SESSION_DETAILS.reset()
    # Create a fresh, test-specific context and make it active
    try:
        db.set_context(ctx, relative=False)
    except Exception:
        pass
    yield ctx
    db.delete_context(ctx)
    db.unset_context()
    ContextRegistry.clear()
    SESSION_DETAILS.reset()
