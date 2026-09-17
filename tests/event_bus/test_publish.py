import pytest
import asyncio
import datetime as dt
from collections import deque

from unify.events.event_bus import EventBus, Event
from unify.events.types.comms import CommsPayload
from tests.helpers import _handle_project


@pytest.mark.asyncio
@_handle_project
async def test_basic_publish():
    """Publishing a valid event should complete without exceptions
    and the event should be stored in the in-memory deque.
    """
    bus = EventBus()  # use defaults (50-event windows)

    # a minimal Comms payload: every field is optional
    payload = CommsPayload()

    event = Event(
        type="Comms",
        timestamp=dt.datetime.now(dt.UTC).isoformat(),
        payload=payload,
    )

    # This should run cleanly …
    await bus.publish(event)

    # … and the event should now be in the per-type deque
    assert event in bus._deques["Comms"]


@pytest.mark.asyncio
@_handle_project
async def test_concurrent_integrity():
    """
    Do a burst of concurrent publishes across two event types; all should succeed
    and be visible afterwards, demonstrating that the internal asyncio.Lock
    protects the critical section.
    """
    window = 200
    bus = EventBus()
    bus.set_default_window(200)

    # Clear any pre-existing state for determinism
    for typ in ("Comms",):
        bus._deques.setdefault(typ, deque(maxlen=window)).clear()

    base_ts = dt.datetime.now(dt.UTC)
    n_events = 100
    events: list[Event] = []
    publish_tasks = []
    etype, payload_cls = "Comms", CommsPayload

    for i in range(n_events):
        evt = Event(
            type=etype,
            timestamp=base_ts
            + dt.timedelta(microseconds=i),  # unique, strictly increasing
            payload=payload_cls(),
        )
        events.append(evt)
        publish_tasks.append(asyncio.create_task(bus.publish(evt)))

    # Run all publishes concurrently; will raise if any individual publish fails
    await asyncio.gather(*publish_tasks)

    # Join published
    bus.join_published()

    # Fetch back everything; limit well above what we sent
    latest = await bus.search(limit=window, grouped_by_type=True)
    latest = latest["Comms"]

    # Keep only the events we just published (ignore any older prefilled logs)
    our_ts = {e.timestamp for e in events}
    latest_ours = [e for e in latest if e.timestamp in our_ts]

    # Every event we published must be present
    assert len(latest_ours) == n_events
