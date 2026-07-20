"""Orchestra persistence filters for EventBus.

Unlike ``stream_filters`` (Pub/Sub / Live Actions only), these rules gate
which events are written to Unify ``Events/*`` contexts. In-memory deques,
subscriptions, and Pub/Sub streaming are unaffected.

In ``allowlist`` mode, ManagerMethod/ToolLoop events that carry task-run
lineage fields (``run_key``, or ``task_id`` + ``instance_id``) are persisted
in full so ``Tasks/Runs`` can join a dense execution tree via ``run_key``.
Non-task traffic stays on the tool allowlist.
"""

from __future__ import annotations

from typing import Any, Mapping

from .types.tool_loop import ToolLoopKind

_DEFAULT_ALLOWLIST_TOOLS: frozenset[str] = frozenset(
    {"execute_code", "execute_function"},
)
_TOOL_LOOP_MATCH_KINDS: frozenset[str] = frozenset(
    {
        ToolLoopKind.TOOL_CALL.value,
        ToolLoopKind.TOOL_RESULT.value,
    },
)
_TASK_RUN_DENSE_EVENT_TYPES: frozenset[str] = frozenset(
    {"ManagerMethod", "ToolLoop"},
)


def parse_persist_tools(raw: str | None) -> frozenset[str]:
    """Parse a comma-separated tool/method allowlist."""

    if not raw or not str(raw).strip():
        return _DEFAULT_ALLOWLIST_TOOLS
    names = {part.strip() for part in str(raw).split(",") if part.strip()}
    return frozenset(names) if names else _DEFAULT_ALLOWLIST_TOOLS


def payload_has_task_run_lineage(payload_dict: Mapping[str, Any]) -> bool:
    """Return True when a payload is attributed to a TaskScheduler run.

    Prefer payload fields stamped by ``enrich_payload_with_task_run_lineage``
    (applied in ``EventBus.publish`` before this gate) over reading ContextVars.
    """

    run_key = payload_dict.get("run_key")
    if isinstance(run_key, str) and run_key.strip():
        return True
    task_id = payload_dict.get("task_id")
    instance_id = payload_dict.get("instance_id")
    return task_id is not None and instance_id is not None


def _tool_names_from_tool_loop_message(message: Any) -> set[str]:
    """Extract tool names from a ToolLoop message dict."""

    if not isinstance(message, Mapping):
        return set()
    names: set[str] = set()
    tool_name = message.get("name")
    if isinstance(tool_name, str) and tool_name:
        names.add(tool_name)
    tool_calls = message.get("tool_calls") or []
    if isinstance(tool_calls, list):
        for call in tool_calls:
            if not isinstance(call, Mapping):
                continue
            function = call.get("function") or {}
            if not isinstance(function, Mapping):
                continue
            name = function.get("name")
            if isinstance(name, str) and name:
                names.add(name)
    return names


def should_persist_to_orchestra(
    event_type: str,
    payload_dict: Mapping[str, Any],
    *,
    mode: str | None = None,
    tools: frozenset[str] | None = None,
) -> bool:
    """Return whether this event should be buffered for Orchestra ``Events/*``.

    Parameters
    ----------
    event_type:
        ``Event.type`` string (e.g. ``ManagerMethod``, ``ToolLoop``).
    payload_dict:
        Serialized payload fields.
    mode:
        ``all`` or ``allowlist``. When omitted, reads
        ``SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_MODE``.
    tools:
        Allowlisted tool/method names. When omitted, parses
        ``SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_TOOLS``.

    In ``allowlist`` mode, any ``ManagerMethod`` / ``ToolLoop`` payload that
    already carries task-run lineage is persisted (dense tree under an
    ``ActiveTask``). Other event types and non-attributed ManagerMethod /
    ToolLoop rows still follow the tool allowlist.
    """

    if mode is None or tools is None:
        from unify.settings import SETTINGS

        if mode is None:
            mode = SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_MODE
        if tools is None:
            tools = parse_persist_tools(SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_TOOLS)

    normalized_mode = (mode or "all").strip().lower()
    if normalized_mode != "allowlist":
        return True

    if event_type in _TASK_RUN_DENSE_EVENT_TYPES and payload_has_task_run_lineage(
        payload_dict,
    ):
        return True

    allow = tools if tools is not None else _DEFAULT_ALLOWLIST_TOOLS

    if event_type == "ManagerMethod":
        method = payload_dict.get("method")
        return isinstance(method, str) and method in allow

    if event_type == "ToolLoop":
        kind = payload_dict.get("kind", "")
        if kind not in _TOOL_LOOP_MATCH_KINDS:
            return False
        names = _tool_names_from_tool_loop_message(payload_dict.get("message"))
        return bool(names & allow)

    return False
