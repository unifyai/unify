import pytest
import datetime as dt
from tests.helpers import _handle_project
from unify.events.event_bus import EventBus, Event
from unify.events.types.comms import CommsPayload
from unify.events.types.manager_method import ManagerMethodPayload


def mk_evt(**kw):
    return Event(type="Comms", payload=CommsPayload(**kw))


def ts(i: int) -> str:
    """Deterministic, strictly-monotonic timestamps."""
    return (
        dt.datetime(2025, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=i)
    ).isoformat()


@pytest.mark.asyncio
@_handle_project
async def test_filter_on_payload():
    bus = EventBus()
    await bus.publish(mk_evt(level="INFO", msg="one"))
    await bus.publish(mk_evt(level="WARN", msg="two"))
    res = bus.search(
        filter='type == "Comms" and payload["level"] == "WARN"',
        limit=5,
    )
    assert len(res) == 1
    assert res[0].payload.get("msg") == "two"


@pytest.mark.asyncio
@_handle_project
async def test_newest_first():
    bus = EventBus()

    for seq in range(4):
        await bus.publish(Event(type="Comms", payload=CommsPayload(seq=seq)))

    out = bus.search(limit=3, filter='type == "Comms"')
    assert [e.payload.get("seq") for e in out] == [3, 2, 1]


@pytest.mark.asyncio
@_handle_project
async def test_offset_skips_newest():
    bus = EventBus()

    for seq in range(5):
        await bus.publish(
            Event(type="Comms", timestamp=ts(seq), payload=CommsPayload(seq=seq)),
        )

    out = bus.search(limit=2, offset=2, filter='type == "Comms"')
    assert [e.payload.get("seq") for e in out] == [2, 1]


@pytest.mark.asyncio
@_handle_project
async def test_flat_ordering_across_types():
    """With no filter the list interleaves every type, newest first."""
    bus = EventBus()

    await bus.publish(
        Event(
            type="ManagerMethod",
            timestamp=ts(0),
            payload=ManagerMethodPayload(manager="Test", method="heartbeat"),
        ),
    )
    await bus.publish(
        Event(type="Comms", timestamp=ts(1), payload=CommsPayload(seq=0)),
    )

    out = bus.search(limit=2)
    assert [(e.type, e.payload.get("seq")) for e in out] == [
        ("Comms", 0),
        ("ManagerMethod", None),
    ]


@pytest.mark.asyncio
@_handle_project
async def test_type_alias_in_filter():
    """The filter namespace offers `event_type` as an alias for `type`."""
    bus = EventBus()
    await bus.publish(
        Event(
            type="ManagerMethod",
            payload=ManagerMethodPayload(manager="Test", method="heartbeat"),
        ),
    )

    res = bus.search(filter='event_type == "ManagerMethod"', limit=10)
    assert len(res) == 1 and res[0].type == "ManagerMethod"


@pytest.mark.asyncio
@_handle_project
async def test_filter_with_no_matches():
    bus = EventBus()
    await bus.publish(Event(type="Comms", payload=CommsPayload(seq=0)))

    assert bus.search(filter='type == "ToolLoop"') == []
