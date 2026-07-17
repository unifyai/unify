"""Unit tests for the multi-assistant speaking-floor protocol.

Two (or more) ``MeetFloor`` instances are wired together with an in-memory
data-channel bus and driven through the claim/hold/release lifecycle:

- a lone assistant (peer probe false) speaks immediately, no claim window
- with peers, the floor serializes speakers: the second claimant waits for
  the holder's release
- simultaneous claims resolve deterministically (lowest assistant id wins)
- a crashed holder's TTL expires so the floor cannot deadlock
- acquire fails open after its timeout instead of muting the assistant

No LiveKit or LLM involvement — the bus is a plain fan-out of dicts.
"""

from __future__ import annotations

import asyncio

import pytest

from unify.conversation_manager.medium_scripts.meet_floor import MeetFloor


class Bus:
    """In-memory stand-in for the LiveKit data channel (topic fan-out)."""

    def __init__(self) -> None:
        self.floors: list[MeetFloor] = []
        self.dropped_kinds: set[str] = set()

    def register(self, floor: MeetFloor) -> None:
        self.floors.append(floor)

    def publisher(self, sender: MeetFloor):
        async def _publish(payload: dict) -> None:
            if payload.get("kind") in self.dropped_kinds:
                return
            for floor in self.floors:
                if floor is not sender:
                    floor.handle_message(dict(payload))

        return _publish


def make_pair(
    bus: Bus,
    ids: tuple[str, str] = ("1", "2"),
) -> tuple[MeetFloor, MeetFloor]:
    floors = []
    for local_id in ids:
        floor = MeetFloor(local_id=local_id, publish=None, peer_probe=lambda: True)  # type: ignore[arg-type]
        floor._publish = bus.publisher(floor)
        bus.register(floor)
        floors.append(floor)
    return floors[0], floors[1]


@pytest.mark.asyncio
async def test_lone_assistant_speaks_immediately():
    bus = Bus()
    floor = MeetFloor(local_id="1", publish=None, peer_probe=lambda: False)  # type: ignore[arg-type]
    floor._publish = bus.publisher(floor)
    bus.register(floor)

    start = asyncio.get_event_loop().time()
    assert await floor.acquire() is True
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed < 0.05, "solo path must skip the claim window"
    assert floor.holding
    await floor.release()
    assert not floor.holding


@pytest.mark.asyncio
async def test_second_claimant_waits_for_release():
    bus = Bus()
    first, second = make_pair(bus)

    assert await first.acquire() is True
    assert first.holding

    acquired_order: list[str] = []

    async def second_speaks() -> None:
        await second.acquire()
        acquired_order.append("second")

    task = asyncio.create_task(second_speaks())
    await asyncio.sleep(0.1)
    assert not task.done(), "second must wait while first holds the floor"
    acquired_order.append("first-released")
    await first.release()
    await asyncio.wait_for(task, timeout=2.0)
    assert acquired_order == ["first-released", "second"]
    assert second.holding
    await second.release()


@pytest.mark.asyncio
async def test_simultaneous_claims_lowest_id_wins():
    bus = Bus()
    low, high = make_pair(bus, ids=("3", "11"))

    results: dict[str, bool] = {}

    async def claim(name: str, floor: MeetFloor) -> None:
        results[name] = await floor.acquire(timeout=5.0)

    low_task = asyncio.create_task(claim("low", low))
    high_task = asyncio.create_task(claim("high", high))
    await asyncio.sleep(0.5)

    # The low id wins the overlapping claim window; the high id is still
    # parked behind the hold.
    assert low_task.done() and results["low"] is True
    assert low.holding
    assert not high_task.done()
    assert not high.holding

    await low.release()
    await asyncio.wait_for(high_task, timeout=2.0)
    assert high.holding
    await high.release()


@pytest.mark.asyncio
async def test_ttl_expiry_frees_a_crashed_holder():
    bus = Bus()
    first, second = make_pair(bus)

    assert await first.acquire(ttl=0.3) is True
    # Simulate a crash: stop the heartbeat without releasing.
    first._stop_heartbeat()
    first._holding = False

    assert await second.acquire(timeout=3.0) is True
    assert second.holding
    await second.release()


@pytest.mark.asyncio
async def test_acquire_fails_open_on_timeout():
    bus = Bus()
    first, second = make_pair(bus)
    # All coordination frames are lost: the second assistant never observes a
    # release, so its acquire must eventually fail open rather than deadlock.
    bus.dropped_kinds = {"release"}

    assert await first.acquire() is True
    result = await second.acquire(timeout=0.8, ttl=60.0)
    assert result is False, "fail-open: speak anyway after the timeout"

    await first.release()


@pytest.mark.asyncio
async def test_release_is_idempotent_and_reacquirable():
    bus = Bus()
    first, second = make_pair(bus)

    assert await first.acquire() is True
    await first.release()
    await first.release()

    assert await second.acquire() is True
    await second.release()
    assert await first.acquire() is True
    await first.release()
