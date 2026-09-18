"""Wire unillm LLM events to the EventBus.

Every completed LLM call becomes one ``LLM`` event carrying the full request
and response. The listener is registered once, during ``unify.init()``, and
stays active for the lifetime of the process.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import unillm

if TYPE_CHECKING:
    from unillm import LLMEvent

_HOOK_INSTALLED = False


def _llm_event_to_eventbus(event: "LLMEvent") -> None:
    """Publish a unillm ``LLMEvent`` as an ``LLM`` event.

    unillm calls this synchronously after each LLM call, so the publish is
    scheduled as a task rather than awaited. With no event loop running in
    the calling thread there is nothing to schedule onto and the event is
    dropped.
    """
    from .event_bus import EVENT_BUS, Event
    from .types.llm import LLMPayload

    payload = LLMPayload(
        request=event.request,
        response=event.response,
        provider_cost=event.provider_cost,
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(EVENT_BUS.publish(Event(type="LLM", payload=payload)))


def install_llm_event_hook() -> None:
    """Register the listener with unillm, once per process.

    unillm listeners are process-wide, so the registration works whichever
    thread performs it: ``unify.init()`` may run in a worker thread while LLM
    calls happen on the main async context. Registration is additive, so
    other consumers (metering, benchmark harnesses) register alongside this
    one in any order without displacing it.
    """
    global _HOOK_INSTALLED

    if _HOOK_INSTALLED:
        return
    unillm.add_llm_event_listener(_llm_event_to_eventbus)
    _HOOK_INSTALLED = True
