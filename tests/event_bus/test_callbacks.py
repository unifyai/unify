# Verifies EventBus.register_callback:
#
#   • fires every N matching events
#   • honours a payload-level filter
#   • runs plain functions in the executor and coroutines as tasks
#   • stops delivering once unregistered

from __future__ import annotations

import pytest

from tests.helpers import _handle_project
from unify.events.event_bus import EventBus, Event
from unify.events.types.comms import CommsPayload
from unify.events.types.manager_method import ManagerMethodPayload


def _mk_msg(sender: int, receiver: int, seq: int) -> Event:
    return Event(
        type="ManagerMethod",
        payload=ManagerMethodPayload(
            manager="Test",
            method="filter_test",
            sender_id=sender,
            receiver_ids=[receiver],
            seq=seq,
        ),
    )


FILTER = "evt.payload.get('sender_id') == 1 and evt.payload.get('receiver_ids') == [2]"


@pytest.mark.asyncio
@_handle_project
async def test_fires_every_n_events() -> None:
    bus = EventBus()
    triggered: list[int] = []

    async def cb(events):  # noqa: ANN001
        triggered.append(events[0].payload["seq"])

    bus.register_callback(event_type="Comms", callback=cb, every_n=3)

    for seq in range(7):
        await bus.publish(Event(type="Comms", payload=CommsPayload(seq=seq)))
    await bus.ajoin_callbacks()

    assert triggered == [2, 5]


@pytest.mark.asyncio
@_handle_project
async def test_every_event_by_default() -> None:
    bus = EventBus()
    triggered: list[int] = []

    async def cb(events):  # noqa: ANN001
        triggered.append(events[0].payload["seq"])

    bus.register_callback(event_type="Comms", callback=cb)

    for seq in range(3):
        await bus.publish(Event(type="Comms", payload=CommsPayload(seq=seq)))
    await bus.ajoin_callbacks()

    assert triggered == [0, 1, 2]


@pytest.mark.asyncio
@_handle_project
async def test_only_matching_type() -> None:
    bus = EventBus()
    triggered: list[str] = []

    async def cb(events):  # noqa: ANN001
        triggered.append(events[0].type)

    bus.register_callback(event_type="Comms", callback=cb)

    await bus.publish(_mk_msg(1, 2, 0))
    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=0)))
    await bus.ajoin_callbacks()

    assert triggered == ["Comms"]


@pytest.mark.asyncio
@_handle_project
async def test_filter_counts_only_matching_events() -> None:
    bus = EventBus()
    trig: list[int] = []

    async def cb(evts):  # noqa: ANN001
        trig.append(evts[0].payload["seq"])

    bus.register_callback(
        event_type="ManagerMethod",
        callback=cb,
        every_n=2,
        filter=FILTER,
    )

    await bus.publish(_mk_msg(1, 2, 0))  # match   (1/2)
    await bus.publish(_mk_msg(1, 2, 1))  # match   → trigger
    await bus.publish(_mk_msg(9, 9, 2))  # non-match
    await bus.publish(_mk_msg(1, 2, 3))  # match   (1/2 of next batch)
    await bus.ajoin_callbacks()

    assert trig == [1]


@pytest.mark.asyncio
@_handle_project
async def test_sync_callback_runs_in_executor() -> None:
    bus = EventBus()
    seen: list[int] = []

    def cb(events):  # noqa: ANN001
        seen.append(events[0].payload["seq"])

    bus.register_callback(event_type="Comms", callback=cb)

    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=7)))
    await bus.ajoin_callbacks()

    assert seen == [7]


@pytest.mark.asyncio
@_handle_project
async def test_unregister_stops_delivery() -> None:
    bus = EventBus()
    seen: list[int] = []

    async def cb(events):  # noqa: ANN001
        seen.append(events[0].payload["seq"])

    sub_id = bus.register_callback(event_type="Comms", callback=cb)
    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=0)))
    await bus.ajoin_callbacks()

    bus.unregister_callback(sub_id)
    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=1)))
    await bus.ajoin_callbacks()

    assert seen == [0]


def test_register_unknown_type_rejected() -> None:
    bus = EventBus()
    with pytest.raises(ValueError, match="Unknown event type"):
        bus.register_callback(event_type="Nope", callback=lambda _: None)
