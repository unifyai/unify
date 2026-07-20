"""EventBus.publish Orchestra allowlist vs Pub/Sub independence."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unify.events.event_bus import Event, EventBus
from unify.events.types.manager_method import ManagerMethodPayload
from unify.events.types.tool_loop import ToolLoopKind, ToolLoopPayload
from tests.helpers import _handle_project


@pytest.mark.asyncio
@_handle_project
async def test_allowlist_mode_only_buffers_matching_orchestra_writes(monkeypatch):
    """Allowlisted ManagerMethod/ToolLoop rows hit Orchestra; others stay in-memory only."""

    monkeypatch.setattr(
        "unify.events.event_bus.EventBus._publishing_enabled",
        True,
        raising=False,
    )
    monkeypatch.setattr(
        "unify.settings.SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_MODE",
        "allowlist",
        raising=False,
    )
    monkeypatch.setattr(
        "unify.settings.SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_TOOLS",
        "execute_code,execute_function",
        raising=False,
    )
    monkeypatch.setattr(
        "unify.events.event_bus.EventBus._pubsub_streaming_enabled",
        False,
        raising=False,
    )

    bus = EventBus()
    bus._pending_writes.clear()

    await bus.publish(
        Event(
            type="ManagerMethod",
            payload=ManagerMethodPayload(
                manager="CodeActActor",
                method="execute_code",
                phase="incoming",
            ),
        ),
    )
    await bus.publish(
        Event(
            type="ManagerMethod",
            payload=ManagerMethodPayload(
                manager="ContactManager",
                method="ask",
                phase="incoming",
            ),
        ),
    )
    await bus.publish(
        Event(
            type="ToolLoop",
            payload=ToolLoopPayload(
                kind=ToolLoopKind.TOOL_RESULT.value,
                message={"role": "tool", "name": "execute_function", "content": "ok"},
                method="CodeActActor.act",
            ),
        ),
    )
    await bus.publish(
        Event(
            type="ToolLoop",
            payload=ToolLoopPayload(
                kind=ToolLoopKind.THOUGHT.value,
                message={"role": "assistant", "content": "hmm"},
                method="CodeActActor.act",
            ),
        ),
    )

    # In-memory deques keep everything when publishing is enabled.
    assert len(bus._deques.get("ManagerMethod", [])) >= 2
    assert len(bus._deques.get("ToolLoop", [])) >= 2

    buffered_methods = [
        entries.get("method")
        for entries, _ctx in bus._pending_writes
        if entries.get("type") == "ManagerMethod"
    ]
    buffered_tool_names = [
        (entries.get("message") or {}).get("name")
        for entries, _ctx in bus._pending_writes
        if entries.get("type") == "ToolLoop"
    ]
    assert buffered_methods == ["execute_code"]
    assert buffered_tool_names == ["execute_function"]


@pytest.mark.asyncio
@_handle_project
async def test_allowlist_does_not_block_pubsub_for_non_matching(monkeypatch):
    """Pub/Sub still streams non-allowlisted ManagerMethod when streaming is on."""

    monkeypatch.setattr(
        "unify.events.event_bus.EventBus._publishing_enabled",
        True,
        raising=False,
    )
    monkeypatch.setattr(
        "unify.settings.SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_MODE",
        "allowlist",
        raising=False,
    )
    monkeypatch.setattr(
        "unify.settings.SETTINGS.EVENTBUS_ORCHESTRA_PERSIST_TOOLS",
        "execute_code,execute_function",
        raising=False,
    )
    monkeypatch.setattr(
        "unify.events.event_bus.EventBus._pubsub_streaming_enabled",
        True,
        raising=False,
    )

    bus = EventBus()
    bus._pending_writes.clear()
    streamed: list[str] = []

    def _capture(event, base_entries, payload_dict):
        streamed.append(payload_dict.get("method", ""))

    bus._stream_action_to_pubsub = MagicMock(side_effect=_capture)  # type: ignore[method-assign]

    await bus.publish(
        Event(
            type="ManagerMethod",
            payload=ManagerMethodPayload(
                manager="ContactManager",
                method="ask",
                phase="incoming",
            ),
        ),
    )

    assert bus._pending_writes == []
    assert streamed == ["ask"]
    bus._stream_action_to_pubsub.assert_called_once()
