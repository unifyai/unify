"""Guidance writes must never yield rows without a ``guidance_id``.

The Guidance context is provisioned with ``guidance_id`` auto-counting, but a
context that was first created bare (no unique keys, no auto-counting) keeps
that shape, and every row it then accepts has no identity — unreachable by
``get_guidance``/``update_guidance``/``delete_guidance`` and rendered as the
``-1`` sentinel by reads. These tests pin the two guards: provisioning
refuses a bare context outright, and ``add_guidance`` fails loudly (removing
the orphan row) if a created row ever comes back without its id.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from unify import db
from tests.helpers import _handle_project
from unify.common.context_store import ContextIdentityError
from unify.common.log_utils import MissingRowIdentityError
from unify.guidance_manager import guidance_manager as gm_module
from unify.guidance_manager.guidance_manager import GuidanceManager


@_handle_project
def test_provisioning_refuses_bare_guidance_context():
    base = db.get_active_context()["write"]

    # A context created ahead of provisioning, without identity, stays bare.
    db.create_context(f"{base}/Guidance")
    db.log(
        context=f"{base}/Guidance",
        new=True,
        title="orphan",
        content="written before provisioning",
    )

    with pytest.raises(ContextIdentityError):
        GuidanceManager()


@_handle_project
def test_add_guidance_without_assigned_id_fails_loud(monkeypatch):
    gm = GuidanceManager()

    fake_log = SimpleNamespace(
        id=987654,
        entries={"title": "T", "content": "C"},
    )
    deleted: list[dict] = []
    monkeypatch.setattr(gm_module, "write_log", lambda **kwargs: fake_log)
    monkeypatch.setattr(
        db,
        "delete_logs",
        lambda **kwargs: deleted.append(kwargs),
    )

    with pytest.raises(MissingRowIdentityError):
        gm.add_guidance(title="T", content="C")

    assert deleted and deleted[0]["logs"] == 987654
