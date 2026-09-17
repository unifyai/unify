import datetime as dt
import pytest
from unify import db
from collections import deque
from unittest.mock import patch, MagicMock

from unify.events.event_bus import EventBus, Event
from unify.events.types.comms import CommsPayload
from tests.helpers import _handle_project


@pytest.mark.asyncio
@_handle_project
async def test_cache_only_skips_backend():
    """When cache has enough events for a type, backend should not be called for that type."""
    window = 5
    bus = EventBus()
    bus.set_window("Comms", window)

    # Start from a known clean state for this type
    bus._deques.setdefault(
        "Comms",
        bus._deques.get("Comms", deque(maxlen=window)),
    ).clear()

    # Publish events to fill the cache
    base_ts = dt.datetime.now(dt.UTC)
    for i in range(window):
        evt = Event(
            type="Comms",
            timestamp=base_ts + dt.timedelta(seconds=i),
            payload=CommsPayload(
                timestamp=dt.datetime.now(dt.UTC).isoformat(),
                content=f"{i}",
            ),
        )
        await bus.publish(evt, blocking=True)

    # Use a spy to track get_logs calls
    original_get_logs = db.get_logs
    spy = MagicMock(side_effect=original_get_logs)

    with patch("unify.db.get_logs", spy):
        # Search for fewer events than cache holds
        results = await bus.search(filter="type == 'Comms'", limit=3)

    # Verify we got the expected results from cache
    assert len(results) == 3
    contents = [r.payload.get("content") for r in results]
    assert contents == ["4", "3", "2"]

    # Verify no backend call was made against the Comms per-type context
    # (the deque/cache should have satisfied the search). Each type is read from
    # its own ``Events/{Type}`` context, so a Comms fetch targets that context.
    message_fetch_calls = [
        call
        for call in spy.call_args_list
        if str(call.kwargs.get("context", "")).endswith("Events/Comms")
    ]
    assert (
        len(message_fetch_calls) == 0
    ), f"Backend was called for Comms type when cache should have been sufficient: {message_fetch_calls}"
