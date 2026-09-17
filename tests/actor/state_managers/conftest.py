from __future__ import annotations

import os

import pytest

from unify.manager_registry import ManagerRegistry


def _in_tree(request: pytest.FixtureRequest, segment: str) -> bool:
    try:
        p = str(getattr(request.node, "fspath", "") or "")
    except Exception:
        p = ""
    return segment in p


def _apply_simulated_impl_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the simulated implementation of every state manager the Actor primitives reach."""
    impl = "simulated"

    # Env vars (documented contract): each manager settings uses prefix UNIFY_<X>_.
    monkeypatch.setenv("UNIFY_CONTACT_IMPL", impl)
    monkeypatch.setenv("UNIFY_TRANSCRIPT_IMPL", impl)
    monkeypatch.setenv("UNIFY_KNOWLEDGE_IMPL", impl)
    monkeypatch.setenv("UNIFY_GUIDANCE_IMPL", impl)
    monkeypatch.setenv("UNIFY_SECRET_IMPL", impl)
    monkeypatch.setenv("UNIFY_FILE_IMPL", impl)
    monkeypatch.setenv("UNIFY_DATA_IMPL", impl)

    # Optional managers are disabled by default; enable them for simulated manager tests.
    # This keeps routing tests meaningful (the Actor can actually call these tools).
    monkeypatch.setenv("UNIFY_FILE_ENABLED", "true")
    monkeypatch.setenv("UNIFY_GUIDANCE_ENABLED", "true")
    monkeypatch.setenv("UNIFY_KNOWLEDGE_ENABLED", "true")

    # Also update the already-instantiated SETTINGS singleton so ManagerRegistry's
    # settings accessors (lambda: SETTINGS.<x>) see the new IMPL values.
    from unify.settings import SETTINGS

    monkeypatch.setattr(SETTINGS.contact, "IMPL", impl, raising=False)
    monkeypatch.setattr(SETTINGS.transcript, "IMPL", impl, raising=False)
    monkeypatch.setattr(SETTINGS.knowledge, "IMPL", impl, raising=False)
    monkeypatch.setattr(SETTINGS.guidance, "IMPL", impl, raising=False)
    monkeypatch.setattr(SETTINGS.secret, "IMPL", impl, raising=False)
    monkeypatch.setattr(SETTINGS.file, "IMPL", impl, raising=False)
    monkeypatch.setattr(SETTINGS.data, "IMPL", impl, raising=False)

    monkeypatch.setattr(SETTINGS.file, "ENABLED", True, raising=False)
    monkeypatch.setattr(SETTINGS.guidance, "ENABLED", True, raising=False)
    monkeypatch.setattr(SETTINGS.knowledge, "ENABLED", True, raising=False)

    # Ensure subsequent ManagerRegistry.get_* returns fresh instances per test.
    ManagerRegistry.clear()


@pytest.fixture(autouse=True)
def configure_simulated_managers(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Autouse fixture for tests under simulated/: force simulated managers."""
    if not _in_tree(request, os.path.join("state_managers", "simulated")):
        return
    _apply_simulated_impl_overrides(monkeypatch)
