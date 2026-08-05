"""A nested manager's failed LLM call must not abort the enclosing plan.

``execute_code`` is the boundary where an incidental primitive call — a web
search the task never needed, say — meets the plan that called it. A provider
failure inside that primitive's own tool loop has to arrive back as an ordinary
tool error the actor can read and route around, leaving the sandbox and every
later block usable.

Both paths are covered because they differ: a bare ``execute_code`` call runs
uninstrumented, while a live ``act()`` binds a sandbox and steering channels,
which rewrites the block and dispatches primitives through the memoisation
layer.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from unify.actor.code_act_actor import CodeActActor
from unify.actor.execution.session import _CURRENT_SANDBOX, PythonExecutionSession

# Verbatim shape of the OpenRouter failure that ended two benchmark runs: a 200
# whose body was whitespace, which litellm reports as an unparseable response.
PROVIDER_ERROR = (
    "litellm.APIError: APIError: OpenrouterException - Unable to get json "
    "response - Expecting value: line 127 column 1 (char 693), Original "
    "Response: " + "\n" * 40
)

WEB_SEARCH_CODE = """
handle = await primitives.web.ask('what does the newest model cost per token?')
answer = await handle.result()
print('ANSWER:', answer)
"""


def _field(out: Any, name: str) -> Any:
    if isinstance(out, dict):
        return out.get(name)
    return getattr(out, name, None)


@pytest.fixture
def real_web_searcher(
    _force_simulated_web: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Undo the actor tree's simulated-web default.

    The simulated searcher answers from a single shot, so it has no tool loop
    to fail in — the property under test only exists on the real one.
    """
    from unify.manager_registry import ManagerRegistry
    from unify.settings import SETTINGS

    monkeypatch.setenv("UNITY_WEB_IMPL", "real")
    monkeypatch.setattr(SETTINGS.web, "IMPL", "real", raising=False)
    ManagerRegistry.clear()


@pytest.fixture
def failing_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail every tool-loop LLM turn the way the live provider failed."""
    from unify.common._async_tool import loop as _loop

    async def _unparseable_response(_client, preprocess_msgs, **gen_kwargs):
        raise Exception(PROVIDER_ERROR)

    monkeypatch.setattr(
        _loop,
        "generate_with_preprocess",
        _unparseable_response,
        raising=True,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(120)
@pytest.mark.parametrize("steered", [False, True], ids=["bare", "steered"])
async def test_web_ask_provider_failure_surfaces_as_tool_error(
    real_web_searcher: None,
    failing_llm: None,
    steered: bool,
) -> None:
    actor = CodeActActor()
    execute_code = actor.get_tools("act")["execute_code"]

    call_kwargs: dict[str, Any] = {}
    sandbox_token = None
    if steered:
        sandbox = PythonExecutionSession(
            computer_primitives=actor._computer_primitives,
            environments={env.namespace: env for env in actor.environments},
            venv_pool=actor._venv_pool,
            shell_pool=actor._shell_pool,
        )
        sandbox_token = _CURRENT_SANDBOX.set(sandbox)
        call_kwargs = {
            "_notification_up_q": asyncio.Queue(),
            "_interject_queue": asyncio.Queue(),
            "_clarification_up_q": asyncio.Queue(),
            "_clarification_down_q": asyncio.Queue(),
        }

    try:
        out = await execute_code(
            "Checking model pricing on the web before drafting.",
            WEB_SEARCH_CODE,
            language="python",
            state_mode="stateless",
            **call_kwargs,
        )

        # The failure is reported, not raised: the actor gets a block it can
        # read and decide about, rather than losing the whole trajectory.
        error = _field(out, "error")
        assert error is not None
        assert "LLM call failed" in error
        assert "Unable to get json response" in error

        # ...and the next block still runs, so the plan can continue without
        # whatever the search was going to contribute.
        recovery = await execute_code(
            "Continuing without the pricing lookup.",
            "print('PLAN_CONTINUES')",
            language="python",
            state_mode="stateless",
            **call_kwargs,
        )
        assert _field(recovery, "error") is None
    finally:
        if sandbox_token is not None:
            _CURRENT_SANDBOX.reset(sandbox_token)
        await actor.close()
