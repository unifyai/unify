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

import inspect
import textwrap

import pytest

from unify.manager_registry import ManagerRegistry


@pytest.fixture
def simulated_managers(monkeypatch: pytest.MonkeyPatch):
    """Switch state managers to simulated impls so no backend is touched."""
    from unify.settings import SETTINGS

    for name in (
        "CONTACT",
        "TASK",
        "TRANSCRIPT",
        "KNOWLEDGE",
        "GUIDANCE",
        "SECRET",
        "WEB",
        "FILE",
        "DATA",
    ):
        monkeypatch.setenv(f"UNITY_{name}_IMPL", "simulated")
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
    """State-manager + computer environments merged under ``primitives``."""
    from unify.actor.environments.base import _CompositeEnvironment
    from unify.actor.environments.computer import ComputerEnvironment
    from unify.actor.environments.state_managers import StateManagerEnvironment
    from unify.function_manager.primitives import (
        ComputerPrimitives,
        Primitives,
        PrimitiveScope,
    )

    cp = ComputerPrimitives(computer_mode="mock")
    scope = PrimitiveScope(
        scoped_managers=frozenset({"contacts", "data", "tasks"}),
    )
    composite = _CompositeEnvironment(
        [
            StateManagerEnvironment(Primitives(primitive_scope=scope)),
            ComputerEnvironment(cp),
        ],
    )
    # The composite builds a fresh merged Primitives; pin the mock computer
    # instance on it so no real backend is constructed on attribute access.
    composite.get_instance()._managers["computer"] = cp
    return {"primitives": composite}


_INTROSPECTION_CODE = textwrap.dedent(
    """
    import inspect
    import pydoc

    info = {}
    info["help_contacts_ask"] = pydoc.render_doc(primitives.contacts.ask)
    info["sig_data_filter"] = str(inspect.signature(primitives.data.filter))
    info["sig_desktop_click"] = str(
        inspect.signature(primitives.computer.desktop.click)
    )
    info["dir_tasks"] = dir(primitives.tasks)
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

    # inspect.signature on a computer session method shows the backend
    # contract (``__signature__`` copied by _make_session_method).
    sig_click = info["sig_desktop_click"]
    assert "*args" not in sig_click, sig_click
    assert "x" in sig_click and "y" in sig_click, sig_click

    # dir(primitives.<manager>) lists the primitive method surface.
    assert "ask" in info["dir_tasks"], info["dir_tasks"]
    assert "update" in info["dir_tasks"], info["dir_tasks"]
    assert "filter" in info["dir_data"], info["dir_data"]
    assert "reduce" in info["dir_data"], info["dir_data"]


@pytest.mark.timeout(60)
def test_computer_session_methods_carry_signature_metadata():
    """_make_session_method copies __signature__/__wrapped__/__doc__ so
    introspection works on desktop and web session methods directly."""
    from unify.function_manager.computer_backends import ComputerBackend
    from unify.function_manager.primitives import ComputerPrimitives

    cp = ComputerPrimitives(computer_mode="mock")
    click = cp.desktop.click

    sig = inspect.signature(click)
    assert list(sig.parameters) == ["x", "y"], sig
    assert click.__wrapped__ is ComputerBackend.click
    assert click.__doc__ == ComputerBackend.click.__doc__
    assert click.__doc__, "backend click docstring must be non-empty"

    # get_screenshot goes through the dedicated screenshot branch.
    screenshot = cp.desktop.get_screenshot
    assert screenshot.__doc__
    assert "self" not in inspect.signature(screenshot).parameters
