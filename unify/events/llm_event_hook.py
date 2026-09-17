"""Wire unillm LLM events to the Unity EventBus.

This module provides the hook function that converts unillm's LLMEvent
dataclass into Unity EventBus events, and the setup function to install
the hook during Unity initialization.

The hook is installed once during unify.init() and remains active for the
lifetime of the process.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unillm import LLMEvent

logger = logging.getLogger(__name__)

# Module-level flag to prevent double-registration
_HOOK_INSTALLED = False

# The registered unillm listener, kept so its delivery health is inspectable.
_LISTENER = None


def _llm_event_to_eventbus(event: "LLMEvent") -> None:
    """Convert a unillm LLMEvent to an EventBus event and publish it.

    This hook is called synchronously by unillm after each LLM call completes.
    We convert the event to our LLMPayload format and publish it asynchronously
    to avoid blocking the LLM call.

    The hook is designed to be resilient - any errors are silently ignored
    to ensure LLM calls are never disrupted by logging failures.
    """
    try:
        from datetime import datetime, timezone

        from ..session_details import SESSION_DETAILS
        from .cost_attribution import COST_ATTRIBUTION
        from .event_bus import EVENT_BUS, Event
        from .types.llm import LLMPayload

        # Generate timestamp once for consistency between Event and derived columns
        ts = datetime.now(timezone.utc)

        # Resolve attributed user: the user who triggered this LLM call.
        # COST_ATTRIBUTION is set by ConversationManager / act() when a
        # platform user (not the supervisor) triggers the interaction.
        attributed_ids = COST_ATTRIBUTION.get()
        attributed_user_id = (
            attributed_ids[0] if attributed_ids else SESSION_DETAILS.user.id
        )

        # Pass through the simplified event data with derived time columns
        # for aggregation/grouping in usage analytics
        payload = LLMPayload(
            request=event.request,
            response=event.response,
            provider_cost=event.provider_cost,
            # Attributed user for per-user usage filtering.
            # _inject_private_fields sets _user_id to the supervisor; this
            # field records who actually triggered the call.
            _attributed_user_id=attributed_user_id,
            # Derived time columns for time-based aggregation
            # All columns use formats the store infers as date/datetime types
            time_minute=ts.replace(second=0, microsecond=0).isoformat(),
            time_hour=ts.replace(minute=0, second=0, microsecond=0).isoformat(),
            time_day=ts.strftime("%Y-%m-%d"),
            time_month=ts.replace(day=1).strftime("%Y-%m-%d"),  # First day of month
            time_year=ts.replace(month=1, day=1).strftime(
                "%Y-%m-%d",
            ),  # First day of year
        )
        llm_event = Event(type="LLM", payload=payload, timestamp=ts)

        # Publish asynchronously to avoid blocking the LLM call
        try:
            loop = asyncio.get_running_loop()
            # Fire-and-forget: schedule the publish but don't wait for it
            loop.create_task(EVENT_BUS.publish(llm_event))

        except RuntimeError:
            # No event loop running - skip publishing
            # This can happen during synchronous test teardown
            pass

    except Exception:
        # Never let hook failures break LLM calls
        pass


def install_llm_event_hook() -> None:
    """Install the LLM event hook to wire unillm events to EventBus.

    This function is idempotent - calling it multiple times has no effect
    after the first successful installation.

    Should be called during unify.init() after the EventBus is initialized.

    Registers a unillm listener, which is process-wide and works across all
    threads. This is critical because unify.init() may be called from a worker
    thread (via asyncio.to_thread in managers_utils.py) while LLM calls happen
    from the main async context. Registration is additive, so other consumers
    (metering, benchmark harnesses) can register alongside this one in any
    order without either displacing the other.
    """
    global _HOOK_INSTALLED, _LISTENER

    if _HOOK_INSTALLED:
        return

    try:
        import unillm

        # Use a listener to ensure it works across all threads/contexts.
        # This is essential because unify.init() may run in a thread pool
        # worker while LLM calls happen from the main async context.
        _LISTENER = unillm.add_llm_event_listener(_llm_event_to_eventbus)
        _HOOK_INSTALLED = True
    except ImportError:
        # unillm not available - skip hook installation
        pass
    except Exception:
        # Any other error - skip silently to not break initialization
        pass
