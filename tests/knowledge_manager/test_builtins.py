"""Builtin / read-only claim protection for KnowledgeManager."""

from __future__ import annotations

import pytest

from tests.helpers import _handle_project
from unify.common.log_utils import log as unity_log
from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from unify.knowledge_manager.types.knowledge import Knowledge


def test_knowledge_legacy_null_is_builtin_normalizes_to_false():
    claim = Knowledge(
        knowledge_id=1,
        title="Legacy",
        content="Pre-builtins row.",
        is_builtin=None,
    )
    assert claim.is_builtin is False


@_handle_project
def test_update_delete_invalidate_supersede_builtin_refused():
    km = KnowledgeManager()
    # Insert a builtin row directly so we exercise _raise_if_builtin without
    # a platform builtins catalogue (KM has no Builtins project today).
    log = unity_log(
        context=km._ctx,
        title="Platform SLA",
        content="Built-in platform claim.",
        kind="policy",
        topics=[],
        source_refs=[],
        status="active",
        supersedes_ids=[],
        stale_reasons=[],
        is_builtin=True,
        new=True,
        mutable=True,
        stamp_authoring=True,
    )
    kid = int(log.entries["knowledge_id"])

    with pytest.raises(ValueError, match="built-in platform knowledge"):
        km.update_knowledge(knowledge_id=kid, content="mutated")

    with pytest.raises(ValueError, match="built-in platform knowledge"):
        km.delete_knowledge(knowledge_id=kid)

    with pytest.raises(ValueError, match="built-in platform knowledge"):
        km.invalidate_knowledge(knowledge_id=kid)

    with pytest.raises(ValueError, match="built-in platform knowledge"):
        km.supersede_knowledge(
            old_knowledge_id=kid,
            title="Replacement",
            content="Tenant replacement.",
        )

    # Reads still work for builtins.
    claim = km.get_knowledge(knowledge_id=kid)
    assert claim.is_builtin is True
    assert claim.title == "Platform SLA"
