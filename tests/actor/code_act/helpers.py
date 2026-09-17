"""Shared helpers for CodeActActor tests.

Transcript readers pull tool-call names and ``execute_code`` snippets out of
a handle's history.  ``StaticActorRunner`` and ``patch_actor_act`` stand in
for ``primitives.actor`` so a test can exercise handle adoption, output
capture and context forwarding without spawning a real inner actor.
"""

from __future__ import annotations

import functools
import json
from typing import Any, Awaitable, Callable, Iterator

import pytest

from unify.actor.environments.actor import _ActorRunner
from unify.actor.simulated import _StaticAnswerHandle


def _iter_tool_calls(chat_history: list[dict[str, Any]]) -> Iterator[dict]:
    for msg in chat_history:
        tool_calls = msg.get("tool_calls") or []
        if isinstance(tool_calls, list):
            for tc in tool_calls:
                if isinstance(tc, dict):
                    yield tc


def _tool_call_name_and_args(tc: dict) -> tuple[Any, Any]:
    fn = tc.get("function")
    if isinstance(fn, dict):
        return fn.get("name"), fn.get("arguments")
    return tc.get("name"), tc.get("arguments")


def get_code_act_tool_calls(handle: Any) -> list[str]:
    """Tool-call names from a CodeActActor handle's chat history, in order."""
    names: list[str] = []
    for tc in _iter_tool_calls(list(handle.get_history() or [])):
        name, _args = _tool_call_name_and_args(tc)
        if isinstance(name, str):
            names.append(name)
    return names


def extract_code_act_execute_code_snippets(handle: Any) -> list[str]:
    """The ``code`` argument of every ``execute_code`` call in the handle's history."""
    snippets: list[str] = []
    for tc in _iter_tool_calls(list(handle.get_history() or [])):
        name, args = _tool_call_name_and_args(tc)
        if name != "execute_code":
            continue
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except json.JSONDecodeError:
                args = None
        if isinstance(args, dict):
            code = args.get("code")
            if isinstance(code, str) and code.strip():
                snippets.append(code)
    return snippets


class StaticActorRunner:
    """Stand-in for ``primitives.actor`` whose ``act`` completes immediately.

    Each call is recorded in ``act_calls`` and answered with a completed
    ``SteerableToolHandle`` carrying ``answer_for(request)``.  Install it on
    an ``ActorEnvironment`` with ``env.get_instance()._managers["actor"]``
    for tools invoked directly on the actor (outside ``act()``).
    """

    _PRIMITIVE_METHODS = ("act",)

    def __init__(
        self,
        answer_for: Callable[[str], str] = lambda request: f"done: {request}",
    ) -> None:
        self.act_calls: list[dict[str, Any]] = []
        self._answer_for = answer_for

    async def act(self, request: str, **kwargs: Any) -> _StaticAnswerHandle:
        """Record the request and return a completed handle."""
        self.act_calls.append({"request": request, **kwargs})
        return _StaticAnswerHandle(self._answer_for(request))


def patch_actor_act(
    monkeypatch: pytest.MonkeyPatch,
    impl: Callable[..., Awaitable[Any]],
) -> None:
    """Route every ``primitives.actor.act(...)`` call to *impl* for one test.

    ``CodeActActor.act()`` rebuilds its ``ActorEnvironment`` (and therefore
    its ``Primitives``) per call, so an instance-level stand-in does not reach
    code running inside ``act()``.  Patching the class method does, and
    ``functools.wraps`` keeps the docstring and signature the prompt and the
    primitives registry read.  *impl* receives ``(request, **kwargs)``.
    """

    @functools.wraps(_ActorRunner.act)
    async def _patched(self: _ActorRunner, request: str, **kwargs: Any) -> Any:
        return await impl(request, **kwargs)

    monkeypatch.setattr(_ActorRunner, "act", _patched)
