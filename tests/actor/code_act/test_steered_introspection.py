"""Steered-mode introspection smoke test.

Models are pointed at ``help()`` / ``inspect.signature`` / ``dir()`` inside
``execute_code`` to read live primitive docs. In steered execution the
sandbox swaps ``primitives`` for a proxy stack (``MemoisedDispatch`` →
``_MemoisedNamespace``, optionally ``ContextForwardingProxy`` →
``_ManagerProxy``), and those wrappers can swallow introspection: a
bare-sandbox test would pass while steered mode returned
``(*args, **kwargs)`` signatures and empty ``dir()`` listings.

So this test runs the introspection calls *through the steered path*: a
``PythonExecutionSession`` with an active ``SteeringSession`` and a parent
chat context bound, exactly as ``execute_code`` composes them.
"""

from __future__ import annotations

import textwrap

import pytest


def _steered_environments():
    """The actor environment exposed under ``primitives``."""
    from unify.actor.environments.actor import ActorEnvironment
    from unify.actor.environments.base import _CompositeEnvironment

    composite = _CompositeEnvironment([ActorEnvironment()])
    return {"primitives": composite}


_INTROSPECTION_CODE = textwrap.dedent(
    """
    import inspect
    import pydoc

    info = {}
    info["help_actor_act"] = pydoc.render_doc(primitives.actor.act)
    info["sig_actor_act"] = str(inspect.signature(primitives.actor.act))
    info["dir_actor"] = dir(primitives.actor)
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
async def test_steered_sandbox_introspection_renders_real_docs():
    """help/inspect.signature/dir resolve real metadata through the steered
    proxy stack (MemoisedDispatch → _MemoisedNamespace → _ManagerProxy)."""
    outcome = await _run_steered(_INTROSPECTION_CODE)
    assert outcome["error"] is None, outcome["error"]
    info = outcome["result"]
    assert isinstance(info, dict), f"unexpected sandbox result: {info!r}"

    # help(primitives.actor.act) shows the real contract docstring.
    help_text = info["help_actor_act"]
    assert "request" in help_text
    assert "Spawn an actor to work on a focused sub-task" in help_text, help_text[:500]

    # inspect.signature(primitives.actor.act) shows the real parameters,
    # not (*args, **kwargs).
    sig = info["sig_actor_act"]
    assert "*args" not in sig, sig
    assert "request" in sig and "prompt_functions" in sig, sig

    # dir(primitives.actor) lists the primitive method surface.
    assert "act" in info["dir_actor"], info["dir_actor"]
