"""Tests for the per-type Event storage schema.

Events are stored only in their type-specific context (`Events/ToolLoop`,
`Events/ManagerMethod`, ...), with payload fields spread into top-level columns
for queryability. The base `Events` context is retained purely as the naming
parent (and for `_callbacks`); it no longer receives event rows.
"""

import pytest
import datetime as dt

import unisdk

from unify.common.log_utils import payload_from_log_entries
from unify.events.event_bus import EventBus, Event
from unify.events.types.manager_method import ManagerMethodPayload
from unify.events.types.tool_loop import ToolLoopPayload, ToolLoopKind
from tests.helpers import _handle_project


@pytest.mark.asyncio
@_handle_project
async def test_base_events_context_receives_no_event_rows():
    """Publishing writes only to the per-type context, not the base `Events`."""
    bus = EventBus()

    payload = ManagerMethodPayload(
        manager="TestManager",
        method="test_method",
        phase="incoming",
        question="test question",
    )
    event = Event(
        type="ManagerMethod",
        timestamp=dt.datetime.now(dt.UTC),
        payload=payload,
    )
    await bus.publish(event)
    bus.join_published()

    # The base/global context must not receive the event row anymore.
    global_logs = unisdk.get_logs(
        project=unisdk.active_project(),
        context=bus._global_ctx,
        filter=f"event_id == '{event.event_id}'",
    )
    assert global_logs == []

    # The per-type context holds exactly one spread row for the event.
    specific_logs = unisdk.get_logs(
        project=unisdk.active_project(),
        context=bus._specific_ctxs["ManagerMethod"],
        filter=f"event_id == '{event.event_id}'",
    )
    assert len(specific_logs) == 1


@pytest.mark.asyncio
@_handle_project
async def test_specific_context_spreads_payload():
    """Type-specific context should spread payload fields for queryability."""
    bus = EventBus()

    # Publish a ManagerMethod event
    payload = ManagerMethodPayload(
        manager="QueryManager",
        method="ask",
        phase="incoming",
        question="what is the answer?",
    )
    event = Event(
        type="ManagerMethod",
        timestamp=dt.datetime.now(dt.UTC),
        payload=payload,
    )
    await bus.publish(event)
    bus.join_published()

    # Query the type-specific context directly
    specific_ctx = bus._specific_ctxs["ManagerMethod"]
    logs = unisdk.get_logs(
        project=unisdk.active_project(),
        context=specific_ctx,
        filter=f"event_id == '{event.event_id}'",
    )

    assert len(logs) == 1
    log_entry = logs[0].entries

    # Should have spread payload fields (for queryability)
    assert log_entry["manager"] == "QueryManager"
    assert log_entry["method"] == "ask"
    assert log_entry["phase"] == "incoming"

    # Should NOT have a payload_json blob column
    assert "payload_json" not in log_entry


@pytest.mark.asyncio
@_handle_project
async def test_toolloop_complex_message_no_type_conflict():
    """ToolLoop events with complex LLM messages should not cause type conflicts.

    The 'message' field is typed as Dict[str, Any] to handle varied LLM response
    shapes including None values, nested arrays, etc.
    """
    bus = EventBus()

    # Simple message
    simple_payload = ToolLoopPayload(
        kind=ToolLoopKind.REQUEST,
        message={"role": "user", "content": "hello"},
        method="test_method",
        hierarchy=["root"],
        hierarchy_label="Test",
    )
    event1 = Event(
        type="ToolLoop",
        timestamp=dt.datetime.now(dt.UTC),
        payload=simple_payload,
    )
    await bus.publish(event1)
    bus.join_published()

    # Complex message with None and nested structures
    complex_payload = ToolLoopPayload(
        kind=ToolLoopKind.TOOL_CALL,
        message={
            "content": None,
            "role": "assistant",
            "tool_calls": [
                {
                    "function": {"arguments": '{"k": 1}', "name": "search"},
                    "id": "call_123",
                    "type": "function",
                },
            ],
            "function_call": None,
        },
        method="test_method",
        hierarchy=["root"],
        hierarchy_label="Test",
    )
    event2 = Event(
        type="ToolLoop",
        timestamp=dt.datetime.now(dt.UTC),
        payload=complex_payload,
    )
    await bus.publish(event2)
    bus.join_published()  # Should not raise type mismatch error

    # Verify both events are retrievable
    results = await bus.search(
        filter=f"event_id == '{event1.event_id}' or event_id == '{event2.event_id}'",
        limit=10,
    )
    assert len(results) == 2


@pytest.mark.asyncio
@_handle_project
async def test_rehydration_from_spread_fields():
    """search() should rehydrate the payload from the per-type spread columns."""
    bus = EventBus()

    # Publish a ToolLoop event with a complex message
    original_payload = ToolLoopPayload(
        kind=ToolLoopKind.RESPONSE,
        message={"role": "assistant", "content": "test content", "extra": [1, 2, 3]},
        method="test_rehydration",
        hierarchy=["level1", "level2"],
        hierarchy_label="Test > Nested",
        origin="test_origin",
    )
    event = Event(
        type="ToolLoop",
        timestamp=dt.datetime.now(dt.UTC),
        payload=original_payload,
    )
    await bus.publish(event)
    bus.join_published()

    # Search for the event (reads the per-type context spread schema)
    results = await bus.search(
        filter=f"event_id == '{event.event_id}'",
        limit=1,
    )

    assert len(results) == 1
    retrieved = results[0]

    # Payload is always a dict after validation
    assert retrieved.payload["method"] == "test_rehydration"
    assert retrieved.payload["hierarchy"] == ["level1", "level2"]

    assert retrieved.event_id == event.event_id


def test_payload_from_log_entries_strips_metadata_and_private_fields():
    """payload_from_log_entries recovers the original payload from a spread row."""
    row = {
        "row_id": 7,
        "event_id": "abc",
        "calling_id": "",
        "event_timestamp": "2026-01-01T00:00:00+00:00",
        "payload_cls": "unify.events.types.tool_loop.ToolLoopPayload",
        "type": "ToolLoop",
        "_user": "user-1",
        "_user_id": "user-1",
        "_assistant": "42",
        "_assistant_id": 42,
        "_org": None,
        "_org_id": None,
        "method": "act",
        "hierarchy": ["root"],
        "kind": "thought",
    }
    assert payload_from_log_entries(row) == {
        "method": "act",
        "hierarchy": ["root"],
        "kind": "thought",
    }
