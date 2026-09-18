import pytest
import asyncio
import datetime as dt

from unify.events.event_bus import EventBus, Event, RING_SIZE
from unify.events.types.comms import CommsPayload
from tests.helpers import _handle_project


@pytest.mark.asyncio
@_handle_project
async def test_basic_publish():
    """A published event is visible to search."""
    bus = EventBus()
    event = Event(type="Comms", payload=CommsPayload(content="hello"))

    await bus.publish(event)

    assert bus.search() == [event]


@pytest.mark.asyncio
@_handle_project
async def test_concurrent_integrity():
    """A burst of concurrent publishes all land, each exactly once."""
    bus = EventBus()
    base_ts = dt.datetime.now(dt.UTC)
    n_events = 100
    events = [
        Event(
            type="Comms",
            timestamp=base_ts + dt.timedelta(microseconds=i),
            payload=CommsPayload(seq=i),
        )
        for i in range(n_events)
    ]

    await asyncio.gather(*(bus.publish(evt) for evt in events))
    bus.join_published()

    latest = bus.search(filter="type == 'Comms'", limit=n_events * 2)
    assert sorted(e.event_id for e in latest) == sorted(e.event_id for e in events)


@pytest.mark.asyncio
@_handle_project
async def test_ring_keeps_only_the_most_recent_events():
    """The ring is bounded: the oldest events fall off once it is full."""
    bus = EventBus()
    overflow = 5

    for seq in range(RING_SIZE + overflow):
        await bus.publish(Event(type="Comms", payload=CommsPayload(seq=seq)))

    kept = bus.search(limit=RING_SIZE * 2)
    assert len(kept) == RING_SIZE
    assert kept[0].payload["seq"] == RING_SIZE + overflow - 1
    assert kept[-1].payload["seq"] == overflow


def test_unknown_event_type_rejected():
    with pytest.raises(ValueError, match="Unknown event type"):
        Event(type="Nope", payload={})


def test_payload_validated_and_stored_as_dict():
    """A model payload is dumped to a dict; a dict payload is validated first."""
    from unify.events.types.manager_method import ManagerMethodPayload

    from_model = Event(
        type="ManagerMethod",
        payload=ManagerMethodPayload(manager="M", method="m"),
    )
    assert from_model.payload["manager"] == "M"
    assert from_model.payload_cls.endswith("ManagerMethodPayload")

    from_dict = Event(type="ManagerMethod", payload={"manager": "M", "method": "m"})
    assert from_dict.payload == from_model.payload

    with pytest.raises(ValueError):
        Event(type="ManagerMethod", payload={"method": "missing manager"})
