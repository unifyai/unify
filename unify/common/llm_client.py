from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import unillm
from pydantic import BaseModel

from unify.logger import LOGGER
from unify.common.hierarchical_logger import ICONS
from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS

_THINKING_ICON = ICONS["llm_thinking"]


class PendingThinkingLog:
    """Manages the combined 'LLM thinking… → /path' log for LLM calls.

    Callers set a thinking suffix (the parenthesised metadata) before
    ``generate()``.  The pending callback emits the combined one-liner.
    If ``UNILLM_LOG_DIR`` is unset the callback never fires, so
    ``emit_fallback`` produces a plain thinking line instead.
    """

    def __init__(self, origin: str) -> None:
        self._origin = origin
        self._suffix: str = ""
        self._emitted: bool = False
        self.last_path: str | None = None

    def set_thinking_context(self, suffix: str) -> None:
        self._suffix = suffix
        self._emitted = False

    def on_pending_path(self, path: Path) -> None:
        self._emitted = True
        self.last_path = str(path)
        LOGGER.info(
            f"{_THINKING_ICON} [{self._origin}] LLM thinking…{self._suffix} → {path}",
        )

    def emit_fallback(self) -> None:
        if not self._emitted:
            self._emitted = True
            LOGGER.info(
                f"{_THINKING_ICON} [{self._origin}] LLM thinking…{self._suffix}",
            )


def resolve_default_model() -> tuple[str, str | None]:
    """Resolve the session's default LLM as (model, reasoning_effort).

    The per-assistant default (from Orchestra, via SESSION_DETAILS) takes
    priority over the deployment-wide UNIFY_MODEL. A returned effort of None
    means the assistant carries no effort override and per-call-site effort
    levels apply.
    """
    session_model = SESSION_DETAILS.assistant.default_model
    if session_model:
        return (
            session_model,
            SESSION_DETAILS.assistant.default_reasoning_effort or None,
        )
    return SETTINGS.UNIFY_MODEL, None


def new_llm_client(
    model: str | None = None,
    *,
    async_client: bool = True,
    stateful: bool = False,
    origin: str | None = None,
    **kwargs: Any,
) -> "unillm.AsyncUnify | unillm.Unify":
    """
    Create a configured Unify client.

    If model is not specified, uses the assistant's default model when one is
    set (which also pins its reasoning effort, overriding the call site), and
    otherwise UNIFY_MODEL from settings.
    Defaults to high reasoning_effort and priority service_tier where applicable.
    Callers that want a different setting (e.g. fast-path helpers at "low", or
    max-effort actor profiles) pass ``reasoning_effort`` explicitly.
    Caching is controlled by the UNILLM_CACHE env var (owned by unillm).
    Returns an AsyncUnify client by default, or a synchronous Unify client when
    async_client=False.
    """
    default_effort: str | None = None
    if model is None:
        model, default_effort = resolve_default_model()

    config = {
        "reasoning_effort": "high",
        "service_tier": "priority",
        "stateful": stateful,
        "origin": origin,
    }
    config.update(kwargs)
    if default_effort is not None:
        config["reasoning_effort"] = default_effort

    if async_client:
        client = unillm.AsyncUnify(model, **config)
    else:
        client = unillm.Unify(model, **config)

    if origin:
        pending_log = PendingThinkingLog(origin)
        client.set_on_log_file_pending(pending_log.on_pending_path)
        client._pending_thinking_log = pending_log

    return client


def new_vision_llm_client(
    *,
    async_client: bool = True,
    stateful: bool = False,
    origin: str | None = None,
    **kwargs: Any,
) -> "unillm.AsyncUnify | unillm.Unify":
    """Create an LLM client for single-shot image and vision analysis.

    Uses UNIFY_VISION_MODEL and UNIFY_VISION_REASONING_EFFORT from settings so
    image Q&A stays on a vision-capable endpoint even when UNIFY_MODEL is
    text-only (for example minimax-v3@minimax).
    """
    config = {
        "reasoning_effort": SETTINGS.UNIFY_VISION_REASONING_EFFORT,
        "service_tier": "priority",
        "stateful": stateful,
        "origin": origin,
    }
    config.update(kwargs)

    if async_client:
        client = unillm.AsyncUnify(SETTINGS.UNIFY_VISION_MODEL, **config)
    else:
        client = unillm.Unify(SETTINGS.UNIFY_VISION_MODEL, **config)

    if origin:
        pending_log = PendingThinkingLog(origin)
        client.set_on_log_file_pending(pending_log.on_pending_path)
        client._pending_thinking_log = pending_log

    return client


def _make_openai_strict_json_schema_compatible(node: Any) -> None:
    """Mutate a JSON schema in-place to satisfy OpenAI strict requirements.

    OpenAI's strict JSON-schema mode requires that:
    - For any schema object with explicit `properties`, the `required` array
      must exist and include *every* property key.
    - `additionalProperties` must be false (to forbid extra keys).

    Pydantic excludes fields with default values from `required` (because they're
    optional at validation time). This helper normalizes the schema to the strict
    subset that OpenAI enforces.
    """
    if isinstance(node, dict):
        props = node.get("properties")
        if node.get("type") == "object" and isinstance(props, dict):
            node["additionalProperties"] = False
            node["required"] = list(props.keys())
        for v in node.values():
            _make_openai_strict_json_schema_compatible(v)
        return

    if isinstance(node, list):
        for v in node:
            _make_openai_strict_json_schema_compatible(v)


def pydantic_to_json_schema_response_format(
    response_model: type[BaseModel],
    *,
    name: str | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """Build an OpenAI-style `response_format` dict from a Pydantic model.

    This returns the JSON-schema response format shape used by OpenAI:

        {"type": "json_schema", "json_schema": {"name": ..., "schema": ..., "strict": ...}}

    When `strict=True`, the schema is post-processed to satisfy OpenAI's strict
    constraints (see `_make_openai_strict_json_schema_compatible`).
    """
    schema = response_model.model_json_schema()
    if strict:
        schema = copy.deepcopy(schema)
        _make_openai_strict_json_schema_compatible(schema)
    else:
        # Keep the existing behaviour of forbidding unknown keys where possible.
        schema.setdefault("additionalProperties", False)
        for def_schema in schema.get("$defs", {}).values():
            if isinstance(def_schema, dict):
                def_schema.setdefault("additionalProperties", False)

    return {
        "type": "json_schema",
        "json_schema": {
            "name": name or response_model.__name__,
            "schema": schema,
            "strict": strict,
        },
    }
