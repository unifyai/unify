"""Steered-mode introspection smoke test.

The prompt no longer inlines primitive method docs — models are pointed at
``help()`` / ``inspect.signature`` / ``dir()`` inside ``execute_code``. In
steered execution the sandbox swaps ``primitives`` for a proxy stack
(``MemoisedDispatch`` → ``_MemoisedNamespace``, optionally
``ContextForwardingProxy`` → ``_ManagerProxy``), and those wrappers used to
swallow introspection: a bare-sandbox test would pass while steered mode
returned ``(*args, **kwargs)`` signatures and empty ``dir()`` listings.

So this test runs the introspection calls *through the steered path*: a
``PythonExecutionSession`` with an active ``SteeringSession`` and a parent
chat context bound, exactly as ``execute_code`` composes them.
"""

from __future__ import annotations

import textwrap

import pytest

from unify.manager_registry import ManagerRegistry


@pytest.fixture
def simulated_managers(monkeypatch: pytest.MonkeyPatch):
    """Switch state managers to simulated impls so no backend is touched."""
    from unify.settings import SETTINGS

    for name in (
        "CONTACT",
        "TRANSCRIPT",
        "KNOWLEDGE",
        "GUIDANCE",
        "SECRET",
        "WEB",
        "FILE",
        "DATA",
    ):
        monkeypatch.setenv(f"UNIFY_{name}_IMPL", "simulated")
        attr = name.lower()
        if hasattr(SETTINGS, attr):
            monkeypatch.setattr(
                getattr(SETTINGS, attr),
                "IMPL",
                "simulated",
                raising=False,
            )

    ManagerRegistry.clear()
    yield
    ManagerRegistry.clear()


def _steered_environments():
    """State-manager environment exposed under ``primitives``."""
    from unify.actor.environments.base import _CompositeEnvironment
    from unify.actor.environments.state_managers import StateManagerEnvironment
    from unify.function_manager.primitives import Primitives, PrimitiveScope

    scope = PrimitiveScope(
        scoped_managers=frozenset({"contacts", "data"}),
    )
    composite = _CompositeEnvironment(
        [StateManagerEnvironment(Primitives(primitive_scope=scope))],
    )
    return {"primitives": composite}


_INTROSPECTION_CODE = textwrap.dedent(
    """
    import inspect
    import pydoc

    info = {}
    info["help_contacts_ask"] = pydoc.render_doc(primitives.contacts.ask)
    info["sig_data_filter"] = str(inspect.signature(primitives.data.filter))
    info["dir_contacts"] = dir(primitives.contacts)
    info["dir_data"] = dir(primitives.data)
    info
    """,
).strip()


async def _run_steered(code: str) -> dict:
    """Execute *code* in a sandbox under an active steering session."""
    from unify.actor.execution.session import (
        _PARENT_CHAT_CONTEXT,
        PythonExecutionSession,
    )
    from unify.function_manager.steering import SteeringSession, use_session

    sandbox = PythonExecutionSession(environments=_steered_environments())
    pcc_token = _PARENT_CHAT_CONTEXT.set(
        [{"role": "user", "content": "steered introspection smoke test"}],
    )
    try:
        with use_session(SteeringSession()):
            return await sandbox.execute(code)
    finally:
        _PARENT_CHAT_CONTEXT.reset(pcc_token)
        await sandbox.close()


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_steered_sandbox_introspection_renders_real_docs(
    simulated_managers,
):
    """help/inspect.signature/dir resolve real metadata through the steered
    proxy stack (MemoisedDispatch → _MemoisedNamespace → _ManagerProxy)."""
    outcome = await _run_steered(_INTROSPECTION_CODE)
    assert outcome["error"] is None, outcome["error"]
    info = outcome["result"]
    assert isinstance(info, dict), f"unexpected sandbox result: {info!r}"

    # help(primitives.contacts.ask) shows the real contract docstring.
    help_text = info["help_contacts_ask"]
    assert "text" in help_text
    assert "existing contact" in help_text, help_text[:500]

    # inspect.signature(primitives.data.filter) — sync manager behind
    # _AsyncPrimitiveWrapper — shows the real parameters, not (*args, **kwargs).
    sig_filter = info["sig_data_filter"]
    assert "*args" not in sig_filter, sig_filter
    assert "context" in sig_filter and "filter" in sig_filter, sig_filter

    # dir(primitives.<manager>) lists the primitive method surface.
    assert "ask" in info["dir_contacts"], info["dir_contacts"]
    assert "update" in info["dir_contacts"], info["dir_contacts"]
    assert "filter" in info["dir_data"], info["dir_data"]
    assert "reduce" in info["dir_data"], info["dir_data"]
