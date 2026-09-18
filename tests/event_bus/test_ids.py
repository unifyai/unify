# tests/event_bus/test_ids.py
#
# Verifies that every Event published through EventBus
# (a) always carries a non-empty, unique `event_id` and
# (b) preserves a user-supplied `calling_id`.

from __future__ import annotations

import uuid

import pytest

from tests.helpers import _handle_project
from unify.events.event_bus import EventBus, Event
from unify.events.types.manager_method import ManagerMethodPayload


@pytest.mark.asyncio
@_handle_project
async def test_populated_and_unique() -> None:
    bus = EventBus()

    for txt in ("one", "two"):
        payload = ManagerMethodPayload(
            manager="TestManager",
            method="test",
            question=txt,
        )
        await bus.publish(Event(type="ManagerMethod", payload=payload))

    latest = bus.search(filter="type == 'ManagerMethod'", limit=2)
    assert len(latest) == 2

    e1, e2 = latest
    for evt in (e1, e2):
        assert evt.event_id, "event_id left blank"
        uuid.UUID(evt.event_id)
    assert e1.event_id != e2.event_id, "event_id should be unique per message"


@pytest.mark.asyncio
@_handle_project
async def test_calling_id_preserved() -> None:
    bus = EventBus()
    call_id = str(uuid.uuid4())

    for phase in ("incoming", "outgoing"):
        await bus.publish(
            Event(
                type="ManagerMethod",
                calling_id=call_id,
                payload=ManagerMethodPayload(manager="M", method="m", phase=phase),
            ),
        )

    latest = bus.search(filter=f"calling_id == '{call_id}'")
    assert [e.payload["phase"] for e in latest] == ["outgoing", "incoming"]
