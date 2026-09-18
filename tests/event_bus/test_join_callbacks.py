import asyncio

import pytest

from tests.helpers import _handle_project
from unify.events.event_bus import EventBus, Event
from unify.events.types.comms import CommsPayload
from unify.events.types.manager_method import ManagerMethodPayload


@pytest.mark.asyncio
@_handle_project
async def test_waits_for_pending() -> None:
    """join_callbacks must block until callbacks already running are done."""

    bus = EventBus()
    done_evt = asyncio.Event()

    async def cb(_):  # noqa: ANN001
        await asyncio.sleep(0.05)
        done_evt.set()

    bus.register_callback(event_type="Comms", callback=cb)
    await bus.publish(Event(type="Comms", payload=CommsPayload()))

    # join_callbacks blocks its thread, so run it off the loop.
    join_task = asyncio.create_task(asyncio.to_thread(bus.join_callbacks))

    await asyncio.sleep(0.01)
    assert not done_evt.is_set(), "Callback already finished unexpectedly fast"
    assert not join_task.done(), "join_callbacks returned before callback finished"

    await join_task
    assert done_evt.is_set(), "Callback did not finish before join_callbacks returned"


@pytest.mark.asyncio
@_handle_project
async def test_ignores_future() -> None:
    """join_callbacks must only wait for callbacks that were *already* pending at invocation."""

    bus = EventBus()
    done_first = asyncio.Event()
    done_second = asyncio.Event()

    async def cb(evts):  # noqa: ANN001
        seq = evts[0].payload.get("seq")
        if seq == 1:
            await asyncio.sleep(0.05)  # short – should be awaited by join
            done_first.set()
        else:
            await asyncio.sleep(0.2)  # long – should *not* block join
            done_second.set()

    bus.register_callback(event_type="Comms", callback=cb)

    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=1)))

    join_task = asyncio.create_task(asyncio.to_thread(bus.join_callbacks))
    # Give join_callbacks a chance to capture current tasks
    await asyncio.sleep(0.01)

    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=2)))

    await join_task

    assert (
        done_first.is_set()
    ), "First callback did not finish before join_callbacks returned"
    assert (
        not done_second.is_set()
    ), "join_callbacks incorrectly waited for a callback started after its invocation"

    await done_second.wait()


@pytest.mark.asyncio
@_handle_project
async def test_waits_for_cascade() -> None:
    """join_callbacks() must wait for callbacks spawned *within* other
    callbacks (same root-sequence) but still ignore unrelated fresh activity."""
    bus = EventBus()

    done_low = asyncio.Event()  # first-level callback completion
    done_high = asyncio.Event()  # second-level callback completion

    async def high_cb(_):  # noqa: ANN001
        await asyncio.sleep(0.05)
        done_high.set()

    bus.register_callback(event_type="ManagerMethod", callback=high_cb)

    async def low_cb(_):  # noqa: ANN001
        await bus.publish(
            Event(
                type="ManagerMethod",
                payload=ManagerMethodPayload(manager="Test", method="cascade"),
            ),
        )
        await asyncio.sleep(0.01)
        done_low.set()

    bus.register_callback(event_type="Comms", callback=low_cb)

    await bus.publish(Event(type="Comms", payload=CommsPayload()))

    join_task = asyncio.create_task(asyncio.to_thread(bus.join_callbacks))

    await asyncio.sleep(0.02)
    assert not join_task.done(), "join_callbacks returned before cascade finished"

    await join_task

    assert done_low.is_set(), "First-level callback not finished"
    assert done_high.is_set(), "Second-level (descendant) callback not finished"


@pytest.mark.asyncio
@_handle_project
async def test_ajoin_without_cascade_waits_once() -> None:
    """cascade=False awaits only the callbacks pending at the call, not their descendants."""
    bus = EventBus()
    done_high = asyncio.Event()

    async def high_cb(_):  # noqa: ANN001
        await asyncio.sleep(0.1)
        done_high.set()

    bus.register_callback(event_type="ManagerMethod", callback=high_cb)

    async def low_cb(_):  # noqa: ANN001
        await bus.publish(
            Event(
                type="ManagerMethod",
                payload=ManagerMethodPayload(manager="Test", method="cascade"),
            ),
        )

    bus.register_callback(event_type="Comms", callback=low_cb)

    await bus.publish(Event(type="Comms", payload=CommsPayload()))
    await bus.ajoin_callbacks(cascade=False)

    assert not done_high.is_set()
    await bus.ajoin_callbacks()
    assert done_high.is_set()


@pytest.mark.asyncio
@_handle_project
async def test_callbacks_parked_on_a_dead_loop_are_dropped() -> None:
    """A successor loop must not wait on callbacks a dead loop can never run."""
    bus = EventBus()
    dead = asyncio.new_event_loop()
    parked = dead.create_future()
    dead.close()
    setattr(parked, "_eb_seq", 1)
    setattr(parked, "_eb_root_seq", 1)
    bus._callback_seq = 1
    bus._callback_futures.add(parked)

    await asyncio.wait_for(bus.ajoin_callbacks(), timeout=1.0)

    assert parked not in bus._callback_futures
