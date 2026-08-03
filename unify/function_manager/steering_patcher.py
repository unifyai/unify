"""Turning a correction into a code change, without leaving the execution engine.

When someone corrects work that is already running, something has to decide
what the correction means for the code. That decision is made here rather than
by the actor, for two reasons: the block is suspended while it happens, so the
round trip through an outer tool loop is latency the correction cannot afford;
and the decision is about *this source*, which the actor does not have in front
of it.

The output is deliberately narrow. A correction may rewrite one or more of the
functions the block defines and may declare which cached calls are now stale.
It may not do anything else — not call tools, not reach outside the block, not
touch anything the block did not define. A correction is an edit to work in
progress, not a new instruction.
"""

from __future__ import annotations

import ast
import json
import logging
from typing import Any, Dict, List, Optional, Sequence

from .steering import InterruptionRequest, Patch, SteeringSession

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are correcting a Python block that is running right now. It has been \
suspended mid-execution so your edit can be applied.

You are given the source, which of its functions are currently executing, and \
which calls have already completed. Decide what the correction means for the \
code that has not run yet.

Rules that matter:

- Work already listed as completed HAS ALREADY HAPPENED. Sends have been sent. \
You cannot undo it. Write the remaining code as someone would who knows those \
steps are done.
- Rewrite whole functions. Return the complete new definition, not a diff or a \
fragment. Only functions the block itself defines can be rewritten.
- Rewriting a function does NOT repeat its completed calls: identical calls \
replay from a record instead of executing again. So a rewritten loop that \
still covers earlier items is safe.
- If the correction means an earlier call should now happen *differently*, \
name it in `invalidate` so its record is discarded and it runs again. Use this \
sparingly and never for something irreversible that already happened.
- If the correction does not change any remaining work, return no patches.

Respond with JSON only:

{"reason": "<short restatement of the correction>",
 "patches": [{"function_name": "<name>",
              "source": "<complete def or async def>",
              "reason": "<what changed>",
              "invalidate": ["<tool path prefix>", ...]}]}
"""


def _defined_functions(source: str) -> List[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _build_user_prompt(
    *,
    source: str,
    interjections: Sequence[str],
    completed: Sequence[str],
    defined: Sequence[str],
) -> str:
    parts = [
        "The correction:",
        *(f"  {text}" for text in interjections),
        "",
        "The running block:",
        "```python",
        source,
        "```",
        "",
        f"Functions you may rewrite: {', '.join(defined) or '(none)'}",
    ]
    if completed:
        parts += [
            "",
            "Calls that have ALREADY COMPLETED (do not plan to repeat these):",
            *(f"  {call}" for call in completed),
        ]
    else:
        parts += ["", "No calls have completed yet."]
    return "\n".join(parts)


def _parse_decision(
    raw: str,
    *,
    defined: Sequence[str],
) -> Optional[InterruptionRequest]:
    """Read the model's JSON, discarding anything it was not allowed to say."""
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        text = text[len("json") :] if text.startswith("json") else text
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("steering: patch author returned non-JSON")
        return None

    patches: List[Patch] = []
    for entry in payload.get("patches") or []:
        name = entry.get("function_name")
        source = entry.get("source") or ""
        # A patch naming a function this block does not define cannot be
        # spliced, and letting it through would burn a retry to discover that.
        if name not in defined:
            logger.warning("steering: patch author named unknown function %s", name)
            continue
        patches.append(
            Patch(
                function_name=str(name),
                source=str(source),
                reason=str(entry.get("reason") or ""),
                invalidate=tuple(entry.get("invalidate") or ()),
            ),
        )

    if not patches:
        return None
    return InterruptionRequest(
        reason=str(payload.get("reason") or "steered"),
        patches=patches,
    )


class LLMPatchAuthor:
    """Decides what a correction means for a running block.

    Held by the execution engine and given the source of whatever it is
    currently running. Returning ``None`` means the correction changes nothing
    about the remaining work, which leaves the block to continue untouched
    rather than paying for a retry that would produce identical code.
    """

    def __init__(self, *, client_factory: Any) -> None:
        self._client_factory = client_factory

    async def __call__(
        self,
        *,
        interjections: Sequence[str],
        session: SteeringSession,
    ) -> Optional[InterruptionRequest]:
        source = session.source
        if not source:
            return None
        defined = _defined_functions(source)
        if not defined:
            # Nothing to rewrite. A block of bare statements can still be
            # stopped, but there is no unit for a patch to replace.
            return None

        prompt = _build_user_prompt(
            source=source,
            interjections=interjections,
            completed=session.cache.completed_calls(),
            defined=defined,
        )
        client = self._client_factory()
        raw = await client.generate(user_message=prompt, system_message=_SYSTEM)
        return _parse_decision(str(raw), defined=defined)


def build_patch_author(*, model: Optional[str] = None) -> LLMPatchAuthor:
    """A patch author backed by the project's usual LLM client."""

    def _factory() -> Any:
        from unify.common.llm_client import new_llm_client

        return new_llm_client(model, async_client=True, origin="steering_patch")

    return LLMPatchAuthor(client_factory=_factory)


def describe(request: InterruptionRequest) -> Dict[str, Any]:
    """A compact record of a correction, for progress reporting."""
    return {
        "reason": request.reason,
        "patches": [
            {
                "function": patch.function_name,
                "reason": patch.reason,
                "invalidated": list(patch.invalidate),
            }
            for patch in request.patches
        ],
    }
